using CSharpFunctionalExtensions;
using System.Text.Json;

namespace ETL.HubspotService.Domain.Entities
{
    /// <summary>
    /// Represents a generic many-to-many association between any two HubSpot objects.
    /// This can represent: Contact-Deal, Contact-Ticket, Company-Deal, Company-Ticket, Deal-Ticket,
    /// Ticket-Activities (calls, emails, notes, tasks, meetings), Email-Deals, etc.
    /// </summary>
    public class ObjectAssociation : Entity<long>
    {
        public string SourceObjectType { get; internal set; } = default!; // contact, company, deal, ticket, email, call, note, task, meeting
        public string SourceObjectId { get; internal set; } = default!; // HubSpot ID of source object
        public string TargetObjectType { get; internal set; } = default!; // deal, ticket, company, contact, call, email, note, task, meeting
        public string TargetObjectId { get; internal set; } = default!; // HubSpot ID of target object
        
        // Association metadata
        public string? AssociationLabel { get; internal set; } // Primary label: "Primary", "Decision Maker", "Billing Contact", etc.
        public string? AssociationLabelsJson { get; internal set; } // JSON array of all association labels: ["Decision Maker", "Primary Contact"]
        public int? AssociationTypeId { get; internal set; } // Numeric stable ID (e.g., 199) - required for syncing/updating associations
        public string? AssociationCategory { get; internal set; } // "HUBSPOT_DEFINED" or "USER_DEFINED"
        public DateTime? AssociationCreatedAt { get; internal set; } // When HubSpot created the association
        public DateTime? AssociationUpdatedAt { get; internal set; } // When the association was last modified
        public string? AssociationSource { get; internal set; } // Source: CRM_UI, WORKFLOWS, INTEGRATIONS, API, CONTACT_IMPORT, SALESFORCE_SYNC
        public string? AssociationSourceId { get; internal set; } // Source-specific ID (e.g., workflow ID, integration ID)
        
        // Derived/computed fields
        public bool? IsPrimary { get; internal set; } // Derived from label containing "Primary"
        
        public DateTime ETLDate { get; internal set; }

        private ObjectAssociation() { } // EF Core constructor

        public ObjectAssociation(
            string sourceObjectType,
            string sourceObjectId,
            string targetObjectType,
            string targetObjectId,
            string? associationLabel = null,
            string? associationLabelsJson = null,
            int? associationTypeId = null,
            string? associationCategory = null,
            DateTime? associationCreatedAt = null,
            DateTime? associationUpdatedAt = null,
            string? associationSource = null,
            string? associationSourceId = null)
        {
            SourceObjectType = sourceObjectType;
            SourceObjectId = sourceObjectId;
            TargetObjectType = targetObjectType;
            TargetObjectId = targetObjectId;
            AssociationLabel = associationLabel;
            AssociationLabelsJson = associationLabelsJson;
            AssociationTypeId = associationTypeId;
            AssociationCategory = associationCategory;
            AssociationCreatedAt = associationCreatedAt;
            AssociationUpdatedAt = associationUpdatedAt;
            AssociationSource = associationSource;
            AssociationSourceId = associationSourceId;
            
            // Derive IsPrimary from label
            IsPrimary = !string.IsNullOrWhiteSpace(associationLabel) && 
                       associationLabel.Contains("Primary", StringComparison.OrdinalIgnoreCase);
            
            ETLDate = DateTime.UtcNow;
        }

        public static Result<ObjectAssociation> CreateFromHubSpotData(
            string sourceObjectType,
            string sourceObjectId,
            string targetObjectType,
            string targetObjectId,
            string? associationLabel = null,
            string? associationLabelsJson = null,
            int? associationTypeId = null,
            string? associationCategory = null,
            DateTime? associationCreatedAt = null,
            DateTime? associationUpdatedAt = null,
            string? associationSource = null,
            string? associationSourceId = null)
        {
            if (string.IsNullOrWhiteSpace(sourceObjectType))
            {
                return Result.Failure<ObjectAssociation>("SourceObjectType is required");
            }

            if (string.IsNullOrWhiteSpace(sourceObjectId))
            {
                return Result.Failure<ObjectAssociation>("SourceObjectId is required");
            }

            if (string.IsNullOrWhiteSpace(targetObjectType))
            {
                return Result.Failure<ObjectAssociation>("TargetObjectType is required");
            }

            if (string.IsNullOrWhiteSpace(targetObjectId))
            {
                return Result.Failure<ObjectAssociation>("TargetObjectId is required");
            }

            return Result.Success(new ObjectAssociation(
                sourceObjectType,
                sourceObjectId,
                targetObjectType,
                targetObjectId,
                associationLabel,
                associationLabelsJson,
                associationTypeId,
                associationCategory,
                associationCreatedAt,
                associationUpdatedAt,
                associationSource,
                associationSourceId));
        }

        public void UpdateFrom(ObjectAssociation other)
        {
            // Always update ETLDate
            ETLDate = DateTime.UtcNow;
            
            // Update association metadata if provided
            AssociationLabel = other.AssociationLabel ?? AssociationLabel;
            AssociationLabelsJson = other.AssociationLabelsJson ?? AssociationLabelsJson;
            AssociationTypeId = other.AssociationTypeId ?? AssociationTypeId;
            AssociationCategory = other.AssociationCategory ?? AssociationCategory;
            AssociationCreatedAt = other.AssociationCreatedAt ?? AssociationCreatedAt;
            AssociationUpdatedAt = other.AssociationUpdatedAt ?? AssociationUpdatedAt;
            AssociationSource = other.AssociationSource ?? AssociationSource;
            AssociationSourceId = other.AssociationSourceId ?? AssociationSourceId;
            
            // Re-derive IsPrimary
            IsPrimary = !string.IsNullOrWhiteSpace(AssociationLabel) && 
                       AssociationLabel.Contains("Primary", StringComparison.OrdinalIgnoreCase);
        }
    }
}

