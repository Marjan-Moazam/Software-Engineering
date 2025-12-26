-- =============================================
-- Script to Drop and Recreate PropertyHistories Table
-- This script removes the old PropertyHistories table and recreates it
-- with the correct schema (swapped OldValue/NewValue columns)
-- 
-- IMPORTANT: Make sure you're connected to the correct database before running this script!
-- =============================================

-- Drop the existing table and all its indexes/constraints
IF OBJECT_ID('[Hubspot].[PropertyHistories]', 'U') IS NOT NULL
BEGIN
    PRINT 'Dropping existing PropertyHistories table...'
    DROP TABLE [Hubspot].[PropertyHistories]
    PRINT 'PropertyHistories table dropped successfully.'
END
ELSE
BEGIN
    PRINT 'PropertyHistories table does not exist. Skipping drop.'
END
GO

-- Recreate the table with correct schema
-- Note: OldValue and NewValue are swapped compared to the original
-- NewValue now stores the previous value (what was OldValue)
-- OldValue now stores the new value (what was NewValue)
PRINT 'Creating PropertyHistories table with corrected schema...'
GO

CREATE TABLE [Hubspot].[PropertyHistories] (
    [Id] BIGINT IDENTITY(1,1) NOT NULL,
    [ObjectType] NVARCHAR(50) NOT NULL,
    [ObjectId] NVARCHAR(50) NOT NULL,
    [PropertyName] NVARCHAR(100) NOT NULL,
    [NewValue] NVARCHAR(500) NULL,  -- Previous value (null for first value) - SWAPPED: was OldValue
    [OldValue] NVARCHAR(500) NULL,   -- New value - SWAPPED: was NewValue
    [ChangeDate] DATETIME2 NOT NULL,
    [Source] NVARCHAR(200) NULL,
    [SourceId] NVARCHAR(255) NULL,
    [ETLDate] DATETIME2 NOT NULL,
    CONSTRAINT [PK_PropertyHistories] PRIMARY KEY CLUSTERED ([Id] ASC)
)
GO

-- Create indexes for efficient querying
PRINT 'Creating indexes on PropertyHistories table...'
GO

CREATE NONCLUSTERED INDEX [IX_PropertyHistories_ObjectType_ObjectId_PropertyName_ChangeDate]
    ON [Hubspot].[PropertyHistories]([ObjectType] ASC, [ObjectId] ASC, [PropertyName] ASC, [ChangeDate] ASC)
GO

CREATE NONCLUSTERED INDEX [IX_PropertyHistories_ObjectType]
    ON [Hubspot].[PropertyHistories]([ObjectType] ASC)
GO

CREATE NONCLUSTERED INDEX [IX_PropertyHistories_ObjectId]
    ON [Hubspot].[PropertyHistories]([ObjectId] ASC)
GO

CREATE NONCLUSTERED INDEX [IX_PropertyHistories_PropertyName]
    ON [Hubspot].[PropertyHistories]([PropertyName] ASC)
GO

CREATE NONCLUSTERED INDEX [IX_PropertyHistories_ChangeDate]
    ON [Hubspot].[PropertyHistories]([ChangeDate] ASC)
GO

CREATE NONCLUSTERED INDEX [IX_PropertyHistories_ETLDate]
    ON [Hubspot].[PropertyHistories]([ETLDate] ASC)
GO

PRINT 'PropertyHistories table recreated successfully with corrected schema!'
PRINT 'Next: Run your ETL application to populate the table with fresh data.'
GO

