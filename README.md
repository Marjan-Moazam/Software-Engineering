# ETL HubSpot Service

A robust service for extracting data from HubSpot CRM and loading it into a SQL Server database.

## Overview

This service extracts the following data from HubSpot:
- **Contacts** - Customer contact information (Name, Email, Phone, Owner, Company, Activity Dates, Status)
- **Companies** - Company/organization data (Name, Owner, Phone, Location, Activity Dates)
- **Deals** - Sales opportunities (Deal Name, Stage, Close Date, Owner, Amount)
- **Tickets** - Support ticket information (Ticket Name, Status, Priority, Owner, Source)
- **Communications** - Call activities (Channel Type, Body, Assigned To, Date)
- **Emails** - Email activities (Subject, Body, Status, Assigned To, Date)
- **Notes** - Note activities (Body Preview, Assigned To, Date)

## Features

- ✅ **Robust Upsert Logic** - Creates new records or updates existing ones based on HubSpot ID
- ✅ **Full Pagination** - Handles datasets with >1000 records automatically
- ✅ **Immediate Load** - Runs ETL on startup when configured
- ✅ **Scheduled Jobs** - Daily incremental updates at 09:00 CET via Quartz.NET
- ✅ **Schema Resilient** - Preserves `CreatedAt` timestamps and handles null values safely
- ✅ **HubSpot ID Tracking** - Uses string-based HubSpot IDs for reliable upserts

## Architecture

The service follows a clean architecture pattern:

### Domain Layer
- **Entities**: Contact, Company, Deal, Ticket, Communication, Email, Note
- **Interfaces**: Repository and Unit of Work patterns
- **Helpers**: HubSpotEntityHelper for safe JSON parsing

### Infrastructure Layer
- **Data Access**: Entity Framework Core with SQL Server
- **External APIs**: HubSpot CRM v3 API integration
- **Services**: ETL orchestration with upsert logic

### Application Layer
- **Background Jobs**: Quartz.NET for scheduled ETL execution
- **Configuration**: Environment-specific settings

## Prerequisites

- .NET 9.0 SDK
- SQL Server (local or remote)
- HubSpot Private App Access Token

## Configuration

### 1. Database Connection

Update `appsettings.Development.json`:

```json
{
  "ConnectionStrings": {
    "DefaultConnection": "Server=YOUR_SERVER;Database=ETLHubspotDb_Dev;User Id=YOUR_USER;Password=YOUR_PASSWORD;TrustServerCertificate=True;"
  }
}
```

### 2. HubSpot API Token

```json
{
  "HubSpot": {
    "BaseUrl": "https://api.hubapi.com",
    "AccessToken": "pat-eu1-YOUR-TOKEN"
  }
}
```

### 3. ETL Settings

```json
{
  "ETL": {
    "RunOnStartup": true,
    "BatchSize": 100
  }
}
```

## Running the Service

```bash
dotnet run --project ETL.HubspotService --environment Development
```

The service will:
1. Create database and tables if they don't exist
2. Run full ETL immediately if `RunOnStartup` is enabled
3. Schedule daily ETL jobs at 09:00 CET

## Database Schema

All tables are created in the `Hubspot` schema with:
- Auto-increment `Id` (internal primary key)
- Unique `HubSpotId` (string) for upsert lookups
- `ETLDate` timestamp for tracking when data was last synced

## Project Structure

```
ETL.HubspotService/
├── Domain/
│   ├── Entities/          # Domain entities (Contact, Company, Deal, etc.)
│   └── Interfaces/       # Repository and Unit of Work interfaces
├── Infrastructure/
│   ├── Data/             # EF Core DbContext and repositories
│   └── Services/         # HubSpot API service and ETL service
├── Jobs/                 # Quartz.NET job definitions
└── Program.cs            # Application entry point
```

## License

MIT
