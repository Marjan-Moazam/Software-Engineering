-- =============================================
-- Script to Swap OldValue and NewValue Columns in PropertyHistories Table
-- This script swaps the column names and data to match the corrected C# code
-- IMPORTANT: Make sure you're connected to the correct database before running this script!
-- =============================================

USE [YourDatabaseName]  -- Replace with your actual database name, or remove this line if already connected
GO

PRINT 'Starting column swap for PropertyHistories table...'
GO

-- Step 1: Add temporary columns
PRINT 'Step 1: Adding temporary columns...'
GO

ALTER TABLE [Hubspot].[PropertyHistories]
ADD [NewValue_Temp] NVARCHAR(500) NULL,
    [OldValue_Temp] NVARCHAR(500) NULL
GO

-- Step 2: Copy data with swapped values
-- OldValue data goes to NewValue_Temp (because OldValue parameter now goes to NewValue property)
-- NewValue data goes to OldValue_Temp (because NewValue parameter now goes to OldValue property)
PRINT 'Step 2: Copying data with swapped values...'
GO

UPDATE [Hubspot].[PropertyHistories]
SET [NewValue_Temp] = [OldValue],
    [OldValue_Temp] = [NewValue]
GO

-- Step 3: Drop old columns
PRINT 'Step 3: Dropping old columns...'
GO

ALTER TABLE [Hubspot].[PropertyHistories]
DROP COLUMN [OldValue], [NewValue]
GO

-- Step 4: Rename temporary columns to final names
PRINT 'Step 4: Renaming temporary columns to final names...'
GO

EXEC sp_rename '[Hubspot].[PropertyHistories].[NewValue_Temp]', 'NewValue', 'COLUMN'
GO

EXEC sp_rename '[Hubspot].[PropertyHistories].[OldValue_Temp]', 'OldValue', 'COLUMN'
GO

PRINT '✅ PropertyHistories columns swapped successfully!'
PRINT 'NewValue now stores the previous value (what was OldValue)'
PRINT 'OldValue now stores the new value (what was NewValue)'
GO

