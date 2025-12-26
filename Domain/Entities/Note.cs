using CSharpFunctionalExtensions;
using System.Text.Json;

namespace ETL.HubspotService.Domain.Entities
{
    public class Note : Entity<long>
    {
        public string HubSpotId { get; internal set; } = default!;
        public string? RecordId { get; internal set; }
        public string? BodyPreview { get; internal set; } // BODY PREVIEW
        public string? ActivityAssignedTo { get; internal set; } // ACTIVITY ASSIGNED TO
        public DateTime? ActivityDate { get; internal set; } // ACTIVITY DATE
        public DateTime ETLDate { get; internal set; }

        private Note() { } // EF Core constructor

        public Note(string hubSpotId, string? recordId, string? bodyPreview, string? activityAssignedTo, DateTime? activityDate)
        {
            HubSpotId = hubSpotId;
            RecordId = recordId;
            BodyPreview = bodyPreview;
            ActivityAssignedTo = activityAssignedTo;
            ActivityDate = activityDate;
            ETLDate = DateTime.UtcNow;
        }

        public static Result<Note> CreateFromHubSpotData(JsonElement hubspotData)
        {
            try
            {
                // CRM v3 Notes API structure: { "id": "...", "properties": {...}, "associations": {...} }
                var hubSpotId = HubSpotEntityHelper.GetHubSpotId(hubspotData);
                if (string.IsNullOrEmpty(hubSpotId))
                {
                    return Result.Failure<Note>("Missing or invalid HubSpotId");
                }
                
                var properties = hubspotData.GetProperty("properties");
                
                // BODY PREVIEW
                var bodyPreviewRaw = HubSpotEntityHelper.GetStringProperty(properties, "hs_note_body");
                var bodyPreview = HubSpotEntityHelper.StripHtml(bodyPreviewRaw);
                
                // ACTIVITY ASSIGNED TO
                var activityAssignedToId = HubSpotEntityHelper.GetStringProperty(properties, "hubspot_owner_id");
                
                // ACTIVITY DATE
                var activityDate = HubSpotEntityHelper.GetDateTimeProperty(properties, "hs_timestamp") ??
                                  HubSpotEntityHelper.GetDateTimeProperty(properties, "hs_createdate");
                var recordId = HubSpotEntityHelper.GetStringProperty(properties, "hs_object_id") ?? hubSpotId;

                return Result.Success(new Note(hubSpotId, recordId, bodyPreview, activityAssignedToId, activityDate));
            }
            catch (Exception ex)
            {
                return Result.Failure<Note>($"Failed to create Note from HubSpot data: {ex.Message}");
            }
        }
        
        public void UpdateFrom(Note other)
        {
            // Always update ETLDate
            ETLDate = DateTime.UtcNow;

            // Update other fields (use null-coalescing to preserve existing values if new value is null)
            RecordId = other.RecordId ?? RecordId;
            BodyPreview = other.BodyPreview ?? BodyPreview;
            ActivityAssignedTo = other.ActivityAssignedTo ?? ActivityAssignedTo;
            ActivityDate = other.ActivityDate ?? ActivityDate;
        }
    }
}
