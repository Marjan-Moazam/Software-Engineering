-- Add ContactActivityTimelines table to existing database
-- This script can be run on an existing database without affecting other tables
-- Note: Remove the USE statement - it will use the current database context

-- Check if schema exists, create if not
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Hubspot')
BEGIN
    EXEC('CREATE SCHEMA [Hubspot]');
    PRINT 'Schema Hubspot created.';
END
ELSE
BEGIN
    PRINT 'Schema Hubspot already exists.';
END
GO

-- Check if table exists, create if not
IF NOT EXISTS (
    SELECT * 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = 'Hubspot' 
    AND TABLE_NAME = 'ContactActivityTimelines'
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
    
    PRINT 'Table ContactActivityTimelines created successfully.';
END
ELSE
BEGIN
    PRINT 'Table ContactActivityTimelines already exists.';
END
GO

-- Create indexes for efficient querying
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_ContactActivityTimelines_ContactHubSpotId' AND object_id = OBJECT_ID('Hubspot.ContactActivityTimelines'))
BEGIN
    CREATE NONCLUSTERED INDEX [IX_ContactActivityTimelines_ContactHubSpotId] 
    ON [Hubspot].[ContactActivityTimelines]([ContactHubSpotId] ASC);
    PRINT 'Index IX_ContactActivityTimelines_ContactHubSpotId created.';
END
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_ContactActivityTimelines_EventType' AND object_id = OBJECT_ID('Hubspot.ContactActivityTimelines'))
BEGIN
    CREATE NONCLUSTERED INDEX [IX_ContactActivityTimelines_EventType] 
    ON [Hubspot].[ContactActivityTimelines]([EventType] ASC);
    PRINT 'Index IX_ContactActivityTimelines_EventType created.';
END
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_ContactActivityTimelines_EventDate' AND object_id = OBJECT_ID('Hubspot.ContactActivityTimelines'))
BEGIN
    CREATE NONCLUSTERED INDEX [IX_ContactActivityTimelines_EventDate] 
    ON [Hubspot].[ContactActivityTimelines]([EventDate] ASC);
    PRINT 'Index IX_ContactActivityTimelines_EventDate created.';
END
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_ContactActivityTimelines_ContactHubSpotId_EventDate' AND object_id = OBJECT_ID('Hubspot.ContactActivityTimelines'))
BEGIN
    CREATE NONCLUSTERED INDEX [IX_ContactActivityTimelines_ContactHubSpotId_EventDate] 
    ON [Hubspot].[ContactActivityTimelines]([ContactHubSpotId] ASC, [EventDate] ASC);
    PRINT 'Index IX_ContactActivityTimelines_ContactHubSpotId_EventDate created.';
END
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_ContactActivityTimelines_ETLDate' AND object_id = OBJECT_ID('Hubspot.ContactActivityTimelines'))
BEGIN
    CREATE NONCLUSTERED INDEX [IX_ContactActivityTimelines_ETLDate] 
    ON [Hubspot].[ContactActivityTimelines]([ETLDate] ASC);
    PRINT 'Index IX_ContactActivityTimelines_ETLDate created.';
END
GO

PRINT 'ContactActivityTimelines table setup completed successfully!';
GO

