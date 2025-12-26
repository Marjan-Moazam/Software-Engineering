using CSharpFunctionalExtensions;
using System.Text.Json;

namespace ETL.HubspotService.Domain.Entities
{
    /// <summary>
    /// Represents an association between an Activity and another HubSpot object (Contact, Company, Deal, Ticket, etc.).
    /// This allows multiple associations per activity, matching HubSpot's many-to-many association model.
    /// </summary>
    public class ActivityAssociation : Entity<long>
    {
        public string ActivityHubSpotId { get; internal set; } = default!;
        public long? ActivityId { get; internal set; } // Foreign key to Activity.Id
        public string AssociatedObjectType { get; internal set; } = default!; // contact, company, deal, ticket, etc.
        public string AssociatedObjectId { get; internal set; } = default!; // HubSpot ID of the associated object
        public string? AssociationLabel { get; internal set; } // Human-readable label: "Primary", "Created by", "Activity assigned to", etc.
        public int? AssociationTypeId { get; internal set; } // Numeric stable ID (e.g., 17) - required for syncing/updating associations
        public string? AssociationCategory { get; internal set; } // "HUBSPOT_DEFINED" or "USER_DEFINED"
        public DateTime ETLDate { get; internal set; }

        // Navigation property
        public Activity? Activity { get; internal set; }

        private ActivityAssociation() { } // EF Core constructor

        public ActivityAssociation(
            string activityHubSpotId, 
            string associatedObjectType, 
            string associatedObjectId,
            string? associationLabel = null,
            int? associationTypeId = null,
            string? associationCategory = null)
        {
            ActivityHubSpotId = activityHubSpotId;
            AssociatedObjectType = associatedObjectType;
            AssociatedObjectId = associatedObjectId;
            AssociationLabel = associationLabel;
            AssociationTypeId = associationTypeId;
            AssociationCategory = associationCategory;
            ETLDate = DateTime.UtcNow;
        }

        public static Result<ActivityAssociation> CreateFromHubSpotData(
            string activityHubSpotId,
            string associatedObjectType,
            string associatedObjectId,
            string? associationLabel = null,
            int? associationTypeId = null,
            string? associationCategory = null)
        {
            if (string.IsNullOrWhiteSpace(activityHubSpotId))
            {
                return Result.Failure<ActivityAssociation>("ActivityHubSpotId is required");
            }

            if (string.IsNullOrWhiteSpace(associatedObjectType))
            {
                return Result.Failure<ActivityAssociation>("AssociatedObjectType is required");
            }

            if (string.IsNullOrWhiteSpace(associatedObjectId))
            {
                return Result.Failure<ActivityAssociation>("AssociatedObjectId is required");
            }

            return Result.Success(new ActivityAssociation(
                activityHubSpotId, 
                associatedObjectType, 
                associatedObjectId,
                associationLabel,
                associationTypeId,
                associationCategory));
        }

        public void UpdateFrom(ActivityAssociation other)
        {
            // Always update ETLDate
            ETLDate = DateTime.UtcNow;
            
            // Update association metadata if provided (these can change)
            AssociationLabel = other.AssociationLabel ?? AssociationLabel;
            AssociationTypeId = other.AssociationTypeId ?? AssociationTypeId;
            AssociationCategory = other.AssociationCategory ?? AssociationCategory;
        }
    }
}

