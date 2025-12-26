using CSharpFunctionalExtensions;
using System.Text.Json;

namespace ETL.HubspotService.Domain.Entities
{
    public class Email : Entity<long>
    {
        public string HubSpotId { get; internal set; } = default!;
        public string? RecordId { get; internal set; }
        public string? EmailSubject { get; internal set; } // EMAIL SUBJECT
        public DateTime? ActivityDate { get; internal set; } // ACTIVITY DATE
        public string? AssociatedContactId { get; internal set; } // EMAIL->CONTACTS
        public string? AssociatedContactName { get; internal set; }
        public string? AssociatedContactEmail { get; internal set; }
        public string? ActivityAssignedTo { get; internal set; } // ACTIVITY ASSIGNED TO
        public string? EmailBody { get; internal set; } // EMAIL BODY
        public string? EmailSendStatus { get; internal set; } // EMAIL SEND STATUS
        public DateTime ETLDate { get; internal set; }

        private Email() { } // EF Core constructor

        public Email(string hubSpotId, string? recordId, string? emailSubject, DateTime? activityDate,
            string? associatedContactId, string? activityAssignedTo, string? emailBody, string? emailSendStatus)
        {
            HubSpotId = hubSpotId;
            RecordId = recordId;
            EmailSubject = emailSubject;
            ActivityDate = activityDate;
            AssociatedContactId = associatedContactId;
            ActivityAssignedTo = activityAssignedTo;
            EmailBody = emailBody;
            EmailSendStatus = emailSendStatus;
            ETLDate = DateTime.UtcNow;
        }

        public static Result<Email> CreateFromHubSpotData(JsonElement hubspotData)
        {
            try
            {
                // CRM v3 Emails API structure: { "id": "...", "properties": {...}, "associations": {...} }
                var hubSpotId = HubSpotEntityHelper.GetHubSpotId(hubspotData);
                if (string.IsNullOrEmpty(hubSpotId))
                {
                    return Result.Failure<Email>("Missing or invalid HubSpotId");
                }
                
                var properties = hubspotData.GetProperty("properties");
                
                // EMAIL SUBJECT
                var emailSubject = HubSpotEntityHelper.GetStringProperty(properties, "hs_email_subject");
                
                // ACTIVITY DATE
                var activityDate = HubSpotEntityHelper.GetDateTimeProperty(properties, "hs_timestamp") ??
                                  HubSpotEntityHelper.GetDateTimeProperty(properties, "hs_createdate");
                
                // ACTIVITY ASSIGNED TO
                var activityAssignedToId = HubSpotEntityHelper.GetStringProperty(properties, "hubspot_owner_id");
                
                // EMAIL BODY - prefer HTML, fallback to text
                var emailBody = HubSpotEntityHelper.GetStringProperty(properties, "hs_email_html") ??
                               HubSpotEntityHelper.GetStringProperty(properties, "hs_email_text");
                
                // EMAIL SEND STATUS
                var emailSendStatus = HubSpotEntityHelper.GetStringProperty(properties, "hs_email_status");
                
                // EMAIL->CONTACTS - from associations
                var associatedContactId = hubspotData.TryGetProperty("associations", out var assoc) &&
                    assoc.TryGetProperty("contacts", out var contacts) &&
                    contacts.TryGetProperty("results", out var contactResults) &&
                    contactResults.GetArrayLength() > 0 &&
                    contactResults[0].TryGetProperty("id", out var contactId)
                    ? HubSpotEntityHelper.GetHubSpotId(contactResults[0])
                    : null;
                var recordId = HubSpotEntityHelper.GetStringProperty(properties, "hs_object_id") ?? hubSpotId;

                return Result.Success(new Email(hubSpotId, recordId, emailSubject, activityDate,
                    associatedContactId, activityAssignedToId, emailBody, emailSendStatus));
            }
            catch (Exception ex)
            {
                return Result.Failure<Email>($"Failed to create Email from HubSpot data: {ex.Message}");
            }
        }
        
        public void UpdateFrom(Email other)
        {
            // Always update ETLDate
            ETLDate = DateTime.UtcNow;

            // Update other fields (use null-coalescing to preserve existing values if new value is null)
            RecordId = other.RecordId ?? RecordId;
            EmailSubject = other.EmailSubject ?? EmailSubject;
            ActivityDate = other.ActivityDate ?? ActivityDate;
            AssociatedContactId = other.AssociatedContactId ?? AssociatedContactId;
            AssociatedContactName = other.AssociatedContactName ?? AssociatedContactName;
            AssociatedContactEmail = other.AssociatedContactEmail ?? AssociatedContactEmail;
            ActivityAssignedTo = other.ActivityAssignedTo ?? ActivityAssignedTo;
            EmailBody = other.EmailBody ?? EmailBody;
            EmailSendStatus = other.EmailSendStatus ?? EmailSendStatus;
        }
    }
}
