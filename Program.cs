using ETL.HubspotService.Domain.Interfaces;
using ETL.HubspotService.Infrastructure.Data;
using ETL.HubspotService.Infrastructure.Services;
using ETL.HubspotService.Jobs;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Serilog;
using System.IO;

var builder = Host.CreateApplicationBuilder(args);

// --------------------- SERILOG CONFIGURATION ---------------------
Log.Logger = new LoggerConfiguration()
    .WriteTo.Console()
    .WriteTo.File("Logs/etl-hubspot-.txt", rollingInterval: RollingInterval.Day)
    .CreateLogger();

builder.Services.AddSerilog();

// --------------------- DEBUG: ENVIRONMENT + CONFIG ---------------------
Console.WriteLine("=== DEBUG INFO ===");
Console.WriteLine($"Environment: {builder.Environment.EnvironmentName}");

var originalConnectionString = builder.Configuration.GetConnectionString("DefaultConnection") ?? "(null)";
Console.WriteLine("DefaultConnection from configuration:");
Console.WriteLine(originalConnectionString);
Console.WriteLine("==================");

// --------------------- PARSE CONNECTION STRING ---------------------
string serverValue = "";
string database = "";
string userId = "";
string password = "";
bool trustServerCertificate = false;

if (!string.IsNullOrEmpty(originalConnectionString))
{
    var parts = originalConnectionString.Split(';', StringSplitOptions.RemoveEmptyEntries);
    foreach (var part in parts)
    {
        var keyValue = part.Split('=', 2);
        if (keyValue.Length == 2)
        {
            var key = keyValue[0].Trim().ToLowerInvariant();
            var value = keyValue[1].Trim();

            switch (key)
            {
                case "server":
                    serverValue = value;
                    break;
                case "database":
                    database = value;
                    break;
                case "user id":
                case "uid":
                    userId = value;
                    break;
                case "password":
                case "pwd":
                    password = value;
                    break;
                case "trustservercertificate":
                    if (bool.TryParse(value, out var trustCert))
                        trustServerCertificate = trustCert;
                    break;
            }
        }
    }
}

// Add protocol prefix if missing
var serverWithProtocol = serverValue.StartsWith("tcp:", StringComparison.OrdinalIgnoreCase) ||
                         serverValue.StartsWith("np:", StringComparison.OrdinalIgnoreCase)
    ? serverValue
    : $"tcp:{serverValue}";

var finalConnectionString =
    $"Server={serverWithProtocol};Database={database};User Id={userId};Password={password};TrustServerCertificate={trustServerCertificate};Connect Timeout=120;Command Timeout=600;";

// Log connection target
Log.Information("Connecting to SQL Server: {Server}", serverWithProtocol);

// --------------------- EF CORE / DEPENDENCY INJECTION ---------------------
builder.Services.AddDbContext<ETLHubspotDbContext>(options =>
{
    options.UseSqlServer(finalConnectionString, sqlOptions =>
    {
        sqlOptions.CommandTimeout(600); // 10 minutes for long-running queries
        sqlOptions.EnableRetryOnFailure(
            maxRetryCount: 5,
            maxRetryDelay: TimeSpan.FromSeconds(30),
            errorNumbersToAdd: null);
    });
});

builder.Services.AddScoped<IUnitOfWork, UnitOfWork>();
builder.Services.AddHttpClient<IHubSpotApiService, HubSpotApiService>();
builder.Services.AddScoped<IETLService, ETLService>();
builder.Services.AddQuartzJobs(builder.Configuration);
builder.Services.AddMemoryCache();

var app = builder.Build();

// --------------------- DATABASE INITIALIZATION ---------------------
using (var scope = app.Services.CreateScope())
{
    var context = scope.ServiceProvider.GetRequiredService<ETLHubspotDbContext>();

    Log.Information("Creating database and tables on remote SQL Server...");
    try
    {
        // Check if database exists first
        var canConnect = await context.Database.CanConnectAsync();
        Log.Information("Database connection check: {CanConnect}", canConnect);
        
        // Rebuild database from scratch - drop and recreate everything
        var rebuildDatabase = builder.Configuration.GetValue<bool>("ETL:RebuildDatabase", false);
        if (rebuildDatabase)
        {
            Log.Warning("⚠️  ETL:RebuildDatabase is enabled. This will DROP and RECREATE the entire database!");
            Log.Warning("⚠️  All data will be lost!");
            
            if (canConnect)
            {
                Log.Information("Dropping existing database...");
                await context.Database.EnsureDeletedAsync();
                Log.Information("Database dropped successfully.");
            }
        }
        
        if (canConnect && !rebuildDatabase)
        {
            Log.Information("Database exists. Checking if tables need to be created...");
        }
        
        var created = await context.Database.EnsureCreatedAsync();

        if (created)
            Log.Information("✅ Database and tables created successfully!");
        else
        {
            Log.Information("Database and tables already exist.");
            // Verify that all expected tables exist
            var tables = await context.Database.SqlQueryRaw<string>(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'Hubspot' ORDER BY TABLE_NAME"
            ).ToListAsync();
            Log.Information("Existing tables in Hubspot schema: {Tables}", string.Join(", ", tables));
            
            // Check if ContactActivityTimelines table exists, create if not (for existing databases)
            var contactTimelineTableExists = tables.Any(t => t.Equals("ContactActivityTimelines", StringComparison.OrdinalIgnoreCase));
            if (!contactTimelineTableExists)
            {
                Log.Information("ContactActivityTimelines table does not exist. Creating it...");
                try
                {
                    var createTableSql = @"
                        IF NOT EXISTS (
                            SELECT * FROM INFORMATION_SCHEMA.TABLES 
                            WHERE TABLE_SCHEMA = 'Hubspot' AND TABLE_NAME = 'ContactActivityTimelines'
                        )
                        BEGIN
                            CREATE TABLE [Hubspot].[ContactActivityTimelines] (
                                [Id] BIGINT IDENTITY(1,1) NOT NULL,
                                [ContactHubSpotId] NVARCHAR(50) NOT NULL,
                                [EventType] NVARCHAR(100) NOT NULL,
                                [EventDate] DATETIME2 NOT NULL,
                                [Description] NVARCHAR(2000) NOT NULL,
                                [RelatedObjectType] NVARCHAR(50) NULL,
                                [RelatedObjectId] NVARCHAR(50) NULL,
                                [RelatedObjectName] NVARCHAR(500) NULL,
                                [ActorId] NVARCHAR(255) NULL,
                                [ActorName] NVARCHAR(255) NULL,
                                [Metadata] NVARCHAR(2000) NULL,
                                [ETLDate] DATETIME2 NOT NULL,
                                CONSTRAINT [PK_ContactActivityTimelines] PRIMARY KEY CLUSTERED ([Id] ASC)
                            );
                            
                            CREATE NONCLUSTERED INDEX [IX_ContactActivityTimelines_ContactHubSpotId] 
                            ON [Hubspot].[ContactActivityTimelines]([ContactHubSpotId] ASC);
                            
                            CREATE NONCLUSTERED INDEX [IX_ContactActivityTimelines_EventType] 
                            ON [Hubspot].[ContactActivityTimelines]([EventType] ASC);
                            
                            CREATE NONCLUSTERED INDEX [IX_ContactActivityTimelines_EventDate] 
                            ON [Hubspot].[ContactActivityTimelines]([EventDate] ASC);
                            
                            CREATE NONCLUSTERED INDEX [IX_ContactActivityTimelines_ContactHubSpotId_EventDate] 
                            ON [Hubspot].[ContactActivityTimelines]([ContactHubSpotId] ASC, [EventDate] ASC);
                            
                            CREATE NONCLUSTERED INDEX [IX_ContactActivityTimelines_ETLDate] 
                            ON [Hubspot].[ContactActivityTimelines]([ETLDate] ASC);
                        END";
                    
                    await context.Database.ExecuteSqlRawAsync(createTableSql);
                    Log.Information("✅ ContactActivityTimelines table created successfully!");
                }
                catch (Exception ex)
                {
                    Log.Warning(ex, "Could not create ContactActivityTimelines table automatically. You may need to run the SQL script manually: {Message}", ex.Message);
                }
            }
            else
            {
                Log.Information("ContactActivityTimelines table already exists.");
            }
        }


        // --------------------- CREATE DATABASE VIEWS ---------------------
        try
        {
            Log.Information("Creating database views...");
            var viewsScriptPath = Path.Combine(AppContext.BaseDirectory, "Scripts", "CreateViews.sql");
            
            if (File.Exists(viewsScriptPath))
            {
                var viewsScript = await File.ReadAllTextAsync(viewsScriptPath);
                
                // Remove GO statements and split by semicolons, then filter out empty statements
                var cleanedScript = viewsScript
                    .Replace("GO", ";", StringComparison.OrdinalIgnoreCase)
                    .Replace("go", ";", StringComparison.OrdinalIgnoreCase)
                    .Replace("Go", ";", StringComparison.OrdinalIgnoreCase);
                
                // Split by semicolons and execute each CREATE VIEW statement separately
                var statements = cleanedScript
                    .Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries)
                    .Select(s => s.Trim())
                    .Where(s => !string.IsNullOrWhiteSpace(s) && 
                                (s.StartsWith("CREATE", StringComparison.OrdinalIgnoreCase) ||
                                 s.StartsWith("ALTER", StringComparison.OrdinalIgnoreCase)))
                    .ToList();

                foreach (var statement in statements)
                {
                    if (!string.IsNullOrWhiteSpace(statement))
                    {
                        try
                        {
                            // Add semicolon back if not present
                            var sqlStatement = statement.EndsWith(';') ? statement : statement + ";";
                            await context.Database.ExecuteSqlRawAsync(sqlStatement);
                            Log.Information("Executed view creation statement successfully.");
                        }
                        catch (Exception ex)
                        {
                            // Log but don't fail - views might already exist
                            Log.Warning(ex, "Could not execute view statement (may already exist): {Message}", ex.Message);
                        }
                    }
                }
                
                Log.Information("✅ Database views creation completed!");
            }
            else
            {
                Log.Warning("Views script not found at: {Path}", viewsScriptPath);
            }
        }
        catch (Exception ex)
        {
            Log.Warning(ex, "Error creating database views (non-critical): {Message}", ex.Message);
            // Don't throw - views creation is not critical for ETL to run
        }
    }
    catch (Exception ex)
    {
        Log.Error(ex, "❌ Error creating database or tables: {Message}", ex.Message);
        Log.Error("Exception type: {Type}", ex.GetType().Name);
        if (ex.InnerException != null)
        {
            Log.Error("Inner exception: {InnerMessage}", ex.InnerException.Message);
        }
        Log.Error("Stack trace: {StackTrace}", ex.StackTrace);
        throw; // Re-throw to stop the application
    }

    // --------------------- RUN ETL ON STARTUP ---------------------
    var configuration = scope.ServiceProvider.GetRequiredService<IConfiguration>();
    var runOnStartup = configuration.GetValue<bool>("ETL:RunOnStartup", false);


    if (runOnStartup)
    {
        Log.Information("ETL:RunOnStartup is enabled. Running ETL process immediately...");
        var etlService = scope.ServiceProvider.GetRequiredService<IETLService>();
        var result = await etlService.RunFullETLAsync();

        if (result.IsSuccess)
            Log.Information("ETL process completed successfully!");
        else
            Log.Error("ETL process failed: {Error}", result.Error);

        Log.Information("ETL process finished. The service will continue running for scheduled jobs.");
    }
}

Log.Information("ETL HubSpot Service started successfully.");
await app.RunAsync();

// =============================================
// Helper method to recreate PropertyHistories table
// =============================================
static async Task RecreatePropertyHistoriesTable(ETLHubspotDbContext context)
{
    try
    {
        Log.Information("Recreating PropertyHistories table with corrected schema...");
        
        // Drop the existing table
        var dropTableSql = @"
            IF OBJECT_ID('[Hubspot].[PropertyHistories]', 'U') IS NOT NULL
            BEGIN
                DROP TABLE [Hubspot].[PropertyHistories]
            END";
        
        await context.Database.ExecuteSqlRawAsync(dropTableSql);
        Log.Information("Dropped existing PropertyHistories table.");
        
        // Recreate the table with correct schema (swapped OldValue/NewValue)
        var createTableSql = @"
            CREATE TABLE [Hubspot].[PropertyHistories] (
                [Id] BIGINT IDENTITY(1,1) NOT NULL,
                [ObjectType] NVARCHAR(50) NOT NULL,
                [ObjectId] NVARCHAR(50) NOT NULL,
                [PropertyName] NVARCHAR(100) NOT NULL,
                [NewValue] NVARCHAR(500) NULL,
                [OldValue] NVARCHAR(500) NULL,
                [ChangeDate] DATETIME2 NOT NULL,
                [Source] NVARCHAR(200) NULL,
                [SourceId] NVARCHAR(255) NULL,
                [ETLDate] DATETIME2 NOT NULL,
                CONSTRAINT [PK_PropertyHistories] PRIMARY KEY CLUSTERED ([Id] ASC)
            )";
        
        await context.Database.ExecuteSqlRawAsync(createTableSql);
        Log.Information("Created PropertyHistories table with corrected schema.");
        
        // Create indexes
        var indexes = new[]
        {
            "CREATE NONCLUSTERED INDEX [IX_PropertyHistories_ObjectType_ObjectId_PropertyName_ChangeDate] ON [Hubspot].[PropertyHistories]([ObjectType] ASC, [ObjectId] ASC, [PropertyName] ASC, [ChangeDate] ASC)",
            "CREATE NONCLUSTERED INDEX [IX_PropertyHistories_ObjectType] ON [Hubspot].[PropertyHistories]([ObjectType] ASC)",
            "CREATE NONCLUSTERED INDEX [IX_PropertyHistories_ObjectId] ON [Hubspot].[PropertyHistories]([ObjectId] ASC)",
            "CREATE NONCLUSTERED INDEX [IX_PropertyHistories_PropertyName] ON [Hubspot].[PropertyHistories]([PropertyName] ASC)",
            "CREATE NONCLUSTERED INDEX [IX_PropertyHistories_ChangeDate] ON [Hubspot].[PropertyHistories]([ChangeDate] ASC)",
            "CREATE NONCLUSTERED INDEX [IX_PropertyHistories_ETLDate] ON [Hubspot].[PropertyHistories]([ETLDate] ASC)"
        };
        
        foreach (var indexSql in indexes)
        {
            try
            {
                await context.Database.ExecuteSqlRawAsync(indexSql);
            }
            catch (Exception ex)
            {
                Log.Warning(ex, "Could not create index (may already exist): {Message}", ex.Message);
            }
        }
        
        Log.Information("✅ PropertyHistories table recreated successfully! The table is now empty and ready to be populated by the ETL process.");
    }
    catch (Exception ex)
    {
        Log.Error(ex, "❌ Error recreating PropertyHistories table: {Message}", ex.Message);
        throw;
    }
}
