-- =============================================
-- HubSpot ETL Database Views
-- Created for data warehouse/reporting purposes
-- =============================================

-- 2.1 FactDeals View
-- Note: Using ETLDate as CreatedDate since Deal entity doesn't have CreatedDate field
-- If CreatedDate exists in the database, replace ETLDate with CreatedDate
CREATE OR ALTER VIEW Hubspot.vw_FactDeals AS
SELECT 
    d.Id AS DealId,
    d.HubSpotId,
    d.DealName,
    d.DealStage,
    d.Pipeline,
    d.Amount,
    d.ETLDate AS CreatedDate,  -- Using ETLDate as CreatedDate
    d.CloseDate AS ClosedDate,  -- Note: Field is CloseDate in entity, not ClosedDate
    DATEDIFF(day, d.ETLDate, d.CloseDate) AS DaysToClose
FROM Hubspot.Deals d;

-- 2.2 FactActivities View
CREATE OR ALTER VIEW Hubspot.vw_FactActivities AS
SELECT 
    a.Id AS ActivityId,
    a.HubSpotId,
    a.ActivityType,
    a.Subject,
    a.ActivityDate,
    a.Status,
    a.SourceObjectType,
    a.SourceObjectId
FROM Hubspot.Activities a;

-- 2.3 Activity → Contact Bridge Table
CREATE OR ALTER VIEW Hubspot.vw_BridgeActivityContact AS
SELECT 
    aa.ActivityHubSpotId,
    aa.AssociatedObjectId AS ContactHubSpotId
FROM Hubspot.ActivityAssociations aa
WHERE aa.AssociatedObjectType = 'contact';

-- 2.4 Activity → Deal Bridge Table
CREATE OR ALTER VIEW Hubspot.vw_BridgeActivityDeal AS
SELECT 
    aa.ActivityHubSpotId,
    aa.AssociatedObjectId AS DealHubSpotId
FROM Hubspot.ActivityAssociations aa
WHERE aa.AssociatedObjectType = 'deal';

-- 2.5 Activity → Company Bridge Table
CREATE OR ALTER VIEW Hubspot.vw_BridgeActivityCompany AS
SELECT 
    aa.ActivityHubSpotId,
    aa.AssociatedObjectId AS CompanyHubSpotId
FROM Hubspot.ActivityAssociations aa
WHERE aa.AssociatedObjectType = 'company';

-- 2.6 Contact–Deal Associations
-- Note: This view joins ContactCompanyAssociations with ActivityAssociations
-- to find deals associated with contacts through companies
CREATE OR ALTER VIEW Hubspot.vw_BridgeContactDeal AS
SELECT DISTINCT
    ca.ContactHubSpotId,
    da.AssociatedObjectId AS DealHubSpotId
FROM Hubspot.ContactCompanyAssociations ca
INNER JOIN Hubspot.ActivityAssociations da
    ON da.AssociatedObjectId = ca.CompanyHubSpotId
    AND da.AssociatedObjectType = 'deal';

-- 2.7 Contact–Company Associations
CREATE OR ALTER VIEW Hubspot.vw_BridgeContactCompany AS
SELECT 
    ContactHubSpotId,
    CompanyHubSpotId,
    AssociationType
FROM Hubspot.ContactCompanyAssociations;


