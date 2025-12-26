using CSharpFunctionalExtensions;

namespace ETL.HubspotService.Domain.Entities
{
    /// <summary>
    /// Tracks all activities and events related to a Contact, including:
    /// - Email tracking (sent, opened, clicked)
    /// - Ticket activities
    /// - Lifecycle stage changes
    /// - Other activities associated with the contact
    /// </summary>
    public class ContactActivityTimeline : Entity<long>
    {
        public string ContactHubSpotId { get; internal set; } = default!; // HubSpot ID of the contact
        public string EventType { get; internal set; } = default!; // email_sent, email_opened, email_clicked, lifecycle_changed, ticket_created, ticket_updated, activity_created, etc.
        public DateTime EventDate { get; internal set; } // When the event occurred
        public string Description { get; internal set; } = default!; // Full explanation of what happened
        public string? RelatedObjectType { get; internal set; } // email, ticket, deal, activity, etc.
        public string? RelatedObjectId { get; internal set; } // HubSpot ID of the related object
        public string? RelatedObjectName { get; internal set; } // Name/subject of the related object
        public string? ActorId { get; internal set; } // Who performed the action (owner ID, user ID)
        public string? ActorName { get; internal set; } // Name of the person who performed the action
        public string? Metadata { get; internal set; } // JSON or additional details (email open count, click count, etc.)
        public DateTime ETLDate { get; internal set; } // When this record was extracted

        private ContactActivityTimeline() { } // EF Core constructor

        public ContactActivityTimeline(
            string contactHubSpotId,
            string eventType,
            DateTime eventDate,
            string description,
            string? relatedObjectType = null,
            string? relatedObjectId = null,
            string? relatedObjectName = null,
            string? actorId = null,
            string? actorName = null,
            string? metadata = null)
        {
            ContactHubSpotId = contactHubSpotId;
            EventType = eventType;
            EventDate = eventDate;
            Description = description;
            RelatedObjectType = relatedObjectType;
            RelatedObjectId = relatedObjectId;
            RelatedObjectName = relatedObjectName;
            ActorId = actorId;
            ActorName = actorName;
            Metadata = metadata;
            ETLDate = DateTime.UtcNow;
        }

        public void UpdateFrom(ContactActivityTimeline other)
        {
            // Update fields if provided
            Description = other.Description ?? Description;
            RelatedObjectType = other.RelatedObjectType ?? RelatedObjectType;
            RelatedObjectId = other.RelatedObjectId ?? RelatedObjectId;
            RelatedObjectName = other.RelatedObjectName ?? RelatedObjectName;
            ActorId = other.ActorId ?? ActorId;
            ActorName = other.ActorName ?? ActorName;
            Metadata = other.Metadata ?? Metadata;
            ETLDate = DateTime.UtcNow;
        }
    }
}

