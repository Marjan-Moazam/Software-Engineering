using CSharpFunctionalExtensions;
using System.Text.Json;

namespace ETL.HubspotService.Domain.Entities
{
    public class Ticket : Entity<long>
    {
        public string HubSpotId { get; internal set; } = default!;
        public string? RecordId { get; internal set; }
        public string? TicketName { get; internal set; } // TICKET NAME
        public string? Pipeline { get; internal set; } // PIPELINE
        public string? TicketStatus { get; internal set; } // TICKET STATUS
        public DateTime? CreatedAt { get; internal set; } // CREATE DATE
        public string? Priority { get; internal set; } // PRIORITY
        public string? TicketOwner { get; internal set; } // TICKET OWNER
        public string? Source { get; internal set; } // SOURCE
        public DateTime? LastActivityDate { get; internal set; } // LAST ACTIVITY DATE
        public DateTime ETLDate { get; internal set; }

        private Ticket() { } // EF Core constructor

        public Ticket(string hubSpotId, string? recordId, string? ticketName, string? pipeline, string? ticketStatus,
            DateTime? createdAt, string? priority, string? ticketOwner, string? source,
            DateTime? lastActivityDate)
        {
            HubSpotId = hubSpotId;
            RecordId = recordId;
            TicketName = ticketName;
            Pipeline = pipeline;
            TicketStatus = ticketStatus;
            CreatedAt = createdAt;
            Priority = priority;
            TicketOwner = ticketOwner;
            Source = source;
            LastActivityDate = lastActivityDate;
            ETLDate = DateTime.UtcNow;
        }

        public static Result<Ticket> CreateFromHubSpotData(JsonElement hubspotData)
        {
            try
            {
                var properties = hubspotData.GetProperty("properties");
                
                var hubSpotId = HubSpotEntityHelper.GetHubSpotId(hubspotData);
                if (string.IsNullOrEmpty(hubSpotId))
                {
                    return Result.Failure<Ticket>("Missing or invalid HubSpotId");
                }
                
                var ticketName = HubSpotEntityHelper.GetStringProperty(properties, "subject") ??
                                 HubSpotEntityHelper.GetStringProperty(properties, "hs_ticket_subject");
                var pipeline = HubSpotEntityHelper.GetStringProperty(properties, "hs_pipeline");
                var ticketStatus = HubSpotEntityHelper.GetStringProperty(properties, "hs_pipeline_stage") ??
                                   HubSpotEntityHelper.GetStringProperty(properties, "hs_ticket_status");
                var createdAt = HubSpotEntityHelper.GetDateTimeProperty(properties, "createdate");
                var priority = HubSpotEntityHelper.GetStringProperty(properties, "hs_ticket_priority");
                var ticketOwnerId = HubSpotEntityHelper.GetStringProperty(properties, "hubspot_owner_id");
                var source = HubSpotEntityHelper.GetStringProperty(properties, "source_type") ??
                             HubSpotEntityHelper.GetStringProperty(properties, "created_by_source") ??
                             HubSpotEntityHelper.GetStringProperty(properties, "hs_ticket_source");
                var lastActivityDate = HubSpotEntityHelper.GetDateTimeProperty(properties, "hs_lastactivitydate");
                var recordId = HubSpotEntityHelper.GetStringProperty(properties, "hs_object_id") ?? hubSpotId;

                return Result.Success(new Ticket(hubSpotId, recordId, ticketName, pipeline, ticketStatus,
                    createdAt, priority, ticketOwnerId, source, lastActivityDate));
            }
            catch (Exception ex)
            {
                return Result.Failure<Ticket>($"Failed to create Ticket from HubSpot data: {ex.Message}");
            }
        }
        
        public void UpdateFrom(Ticket other)
        {
            // Never overwrite CreatedAt
            // Always update ETLDate
            ETLDate = DateTime.UtcNow;

            // Update other fields (use null-coalescing to preserve existing values if new value is null)
            RecordId = other.RecordId ?? RecordId;
            TicketName = other.TicketName ?? TicketName;
            Pipeline = other.Pipeline ?? Pipeline;
            TicketStatus = other.TicketStatus ?? TicketStatus;
            Priority = other.Priority ?? Priority;
            TicketOwner = other.TicketOwner ?? TicketOwner;
            Source = other.Source ?? Source;
            LastActivityDate = other.LastActivityDate ?? LastActivityDate;
        }
    }
}
