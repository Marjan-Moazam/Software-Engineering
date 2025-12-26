using CSharpFunctionalExtensions;
using System.Text.Json;

namespace ETL.HubspotService.Domain.Entities
{
    /// <summary>
    /// Represents a many-to-many association between Contacts and Companies.
    /// This matches HubSpot's Associations API model where a contact can be associated with multiple companies.
    /// </summary>
    public class ContactCompanyAssociation : Entity<long>
    {
        public string ContactHubSpotId { get; internal set; } = default!;
        public string CompanyHubSpotId { get; internal set; } = default!;
        public long? ContactId { get; internal set; } // Foreign key to Contact.Id
        public long? CompanyId { get; internal set; } // Foreign key to Company.Id
        public string? AssociationType { get; internal set; } // Primary association type/label (for backward compatibility)
        public string? AssociationLabelsJson { get; internal set; } // JSON array of all association labels: ["Employee", "Billing Contact", "Decision Maker"]
        public DateTime? AssociationCreatedAt { get; internal set; } // When HubSpot created the association
        public DateTime? AssociationUpdatedAt { get; internal set; } // When the association was last modified
        public string? AssociationSource { get; internal set; } // Source of association: CRM_UI, WORKFLOWS, INTEGRATIONS, API, CONTACT_IMPORT, SALESFORCE_SYNC
        public string? AssociationSourceId { get; internal set; } // Source-specific ID (e.g., workflow ID, integration ID)
        public DateTime ETLDate { get; internal set; }

        // Navigation properties
        public Contact? Contact { get; internal set; }
        public Company? Company { get; internal set; }

        private ContactCompanyAssociation() { } // EF Core constructor

        public ContactCompanyAssociation(
            string contactHubSpotId, 
            string companyHubSpotId, 
            string? associationType = null,
            string? associationLabelsJson = null,
            DateTime? associationCreatedAt = null,
            DateTime? associationUpdatedAt = null,
            string? associationSource = null,
            string? associationSourceId = null)
        {
            ContactHubSpotId = contactHubSpotId;
            CompanyHubSpotId = companyHubSpotId;
            AssociationType = associationType;
            AssociationLabelsJson = associationLabelsJson;
            AssociationCreatedAt = associationCreatedAt;
            AssociationUpdatedAt = associationUpdatedAt;
            AssociationSource = associationSource;
            AssociationSourceId = associationSourceId;
            ETLDate = DateTime.UtcNow;
        }

        public static Result<ContactCompanyAssociation> CreateFromHubSpotData(
            string contactHubSpotId, 
            string companyHubSpotId, 
            string? associationType = null,
            string? associationLabelsJson = null,
            DateTime? associationCreatedAt = null,
            DateTime? associationUpdatedAt = null,
            string? associationSource = null,
            string? associationSourceId = null)
        {
            if (string.IsNullOrWhiteSpace(contactHubSpotId))
            {
                return Result.Failure<ContactCompanyAssociation>("ContactHubSpotId is required");
            }

            if (string.IsNullOrWhiteSpace(companyHubSpotId))
            {
                return Result.Failure<ContactCompanyAssociation>("CompanyHubSpotId is required");
            }

            return Result.Success(new ContactCompanyAssociation(
                contactHubSpotId, 
                companyHubSpotId, 
                associationType,
                associationLabelsJson,
                associationCreatedAt,
                associationUpdatedAt,
                associationSource,
                associationSourceId));
        }

        public void UpdateFrom(ContactCompanyAssociation other)
        {
            // Always update ETLDate
            ETLDate = DateTime.UtcNow;

            // Update association type if provided
            AssociationType = other.AssociationType ?? AssociationType;
            
            // Update labels JSON if provided
            AssociationLabelsJson = other.AssociationLabelsJson ?? AssociationLabelsJson;
            
            // Update timestamps if provided
            AssociationCreatedAt = other.AssociationCreatedAt ?? AssociationCreatedAt;
            AssociationUpdatedAt = other.AssociationUpdatedAt ?? AssociationUpdatedAt;
            
            // Update source information if provided
            AssociationSource = other.AssociationSource ?? AssociationSource;
            AssociationSourceId = other.AssociationSourceId ?? AssociationSourceId;
        }
    }
}

