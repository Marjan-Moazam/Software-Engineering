-- Add CreatedAt column to Deals table if it doesn't exist
IF NOT EXISTS (
    SELECT 1 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'Hubspot' 
    AND TABLE_NAME = 'Deals' 
    AND COLUMN_NAME = 'CreatedAt'
)
BEGIN
    ALTER TABLE [Hubspot].[Deals]
    ADD [CreatedAt] DATETIME2 NULL;
    
    PRINT 'CreatedAt column added successfully to Deals table.';
END
ELSE
BEGIN
    PRINT 'CreatedAt column already exists in Deals table.';
END
GO

