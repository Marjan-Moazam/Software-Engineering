using CSharpFunctionalExtensions;
using System.Text.Json;

namespace ETL.HubspotService.Domain.Entities
{
    public class Communication : Entity<long>
    {
        public string HubSpotId { get; internal set; } = default!;
        public string? RecordId { get; internal set; }
        public string? ChannelType { get; internal set; } // CHANNEL TYPE
        public string? CommunicationBody { get; internal set; } // COMMUNICATION BODY
        public string? AssociatedContactId { get; internal set; } // COMMUNICATION->CONTACTS
        public string? AssociatedContactName { get; internal set; }
        public string? AssociatedContactEmail { get; internal set; }
        public string? ActivityAssignedTo { get; internal set; } // ACTIVITY ASSIGNED TO
        public DateTime? ActivityDate { get; internal set; } // ACTIVITY DATE
        public DateTime ETLDate { get; internal set; }

        private Communication() { } // EF Core constructor

        public Communication(string hubSpotId, string? recordId, string? channelType, string? communicationBody,
            string? associatedContactId, string? activityAssignedTo, DateTime? activityDate)
        {
            HubSpotId = hubSpotId;
            RecordId = recordId;
            ChannelType = channelType;
            CommunicationBody = communicationBody;
            AssociatedContactId = associatedContactId;
            ActivityAssignedTo = activityAssignedTo;
            ActivityDate = activityDate;
            ETLDate = DateTime.UtcNow;
        }

        public static Result<Communication> CreateFromHubSpotData(JsonElement hubspotData)
        {
            try
            {
                // CRM v3 Communications API structure: { "id": "...", "properties": {...}, "associations": {...} }
                var hubSpotId = HubSpotEntityHelper.GetHubSpotId(hubspotData);
                if (string.IsNullOrEmpty(hubSpotId))
                {
                    return Result.Failure<Communication>("Missing or invalid HubSpotId");
                }
                
                var properties = hubspotData.GetProperty("properties");
                
                // CHANNEL TYPE
                var channelType = HubSpotEntityHelper.GetStringProperty(properties, "hs_communication_channel_type") ??
                                  HubSpotEntityHelper.GetStringProperty(properties, "hs_channel_type");
                
                // COMMUNICATION BODY
                var communicationBodyRaw = HubSpotEntityHelper.GetStringProperty(properties, "hs_communication_body") ??
                                           HubSpotEntityHelper.GetStringProperty(properties, "hs_body_preview");
                var communicationBody = HubSpotEntityHelper.StripHtml(communicationBodyRaw);
                
                // ACTIVITY ASSIGNED TO
                var activityAssignedToId = HubSpotEntityHelper.GetStringProperty(properties, "hubspot_owner_id");
                
                // ACTIVITY DATE
                var activityDate = HubSpotEntityHelper.GetDateTimeProperty(properties, "hs_timestamp") ??
                                   HubSpotEntityHelper.GetDateTimeProperty(properties, "hs_createdate");
                var recordId = HubSpotEntityHelper.GetStringProperty(properties, "hs_object_id") ?? hubSpotId;
                
                // COMMUNICATION->CONTACTS - from associations
                var associatedContactId = hubspotData.TryGetProperty("associations", out var assoc) &&
                    assoc.TryGetProperty("contacts", out var contacts) &&
                    contacts.TryGetProperty("results", out var contactResults) &&
                    contactResults.GetArrayLength() > 0 &&
                    contactResults[0].TryGetProperty("id", out var contactId)
                    ? HubSpotEntityHelper.GetHubSpotId(contactResults[0])
                    : null;

                return Result.Success(new Communication(hubSpotId, recordId, channelType, communicationBody,
                    associatedContactId, activityAssignedToId, activityDate));
            }
            catch (Exception ex)
            {
                return Result.Failure<Communication>($"Failed to create Communication from HubSpot data: {ex.Message}");
            }
        }
        
        public void UpdateFrom(Communication other)
        {
            // Always update ETLDate
            ETLDate = DateTime.UtcNow;

            // Update other fields (use null-coalescing to preserve existing values if new value is null)
            RecordId = other.RecordId ?? RecordId;
            ChannelType = other.ChannelType ?? ChannelType;
            CommunicationBody = other.CommunicationBody ?? CommunicationBody;
            AssociatedContactId = other.AssociatedContactId ?? AssociatedContactId;
            AssociatedContactName = other.AssociatedContactName ?? AssociatedContactName;
            AssociatedContactEmail = other.AssociatedContactEmail ?? AssociatedContactEmail;
            ActivityAssignedTo = other.ActivityAssignedTo ?? ActivityAssignedTo;
            ActivityDate = other.ActivityDate ?? ActivityDate;
        }
    }
}
