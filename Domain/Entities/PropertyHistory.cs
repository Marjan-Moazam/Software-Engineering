using CSharpFunctionalExtensions;
using System.Text.Json;

namespace ETL.HubspotService.Domain.Entities
{
    /// <summary>
    /// Tracks historical changes to specific properties in HubSpot objects (Contacts, Deals, Tickets, Companies).
    /// Used to track stage/status changes over time for Power BI visualization.
    /// </summary>
    public class PropertyHistory : Entity<long>
    {
        public string ObjectType { get; internal set; } = default!; // contact, deal, ticket, company
        public string ObjectId { get; internal set; } = default!; // HubSpot ID of the object
        public string PropertyName { get; internal set; } = default!; // hs_lead_status, dealstage, hs_pipeline_stage, meeting_invite
        public string? NewValue { get; internal set; } // Previous value (null for first value) - SWAPPED: was OldValue
        public string? OldValue { get; internal set; } // New value - SWAPPED: was NewValue
        public DateTime ChangeDate { get; internal set; } // When the change occurred
        public string? Source { get; internal set; } // Source of change: CRM_UI, API, Workflow, etc.
        public string? SourceId { get; internal set; } // Additional source identifier
        public DateTime ETLDate { get; internal set; } // When this record was extracted

        private PropertyHistory() { } // EF Core constructor

        public PropertyHistory(
            string objectType,
            string objectId,
            string propertyName,
            string? oldValue,
            string? newValue,
            DateTime changeDate,
            string? source = null,
            string? sourceId = null)
        {
            ObjectType = objectType;
            ObjectId = objectId;
            PropertyName = propertyName;
            NewValue = oldValue;  // SWAPPED: oldValue parameter goes to NewValue property
            OldValue = newValue;  // SWAPPED: newValue parameter goes to OldValue property
            ChangeDate = changeDate;
            Source = source;
            SourceId = sourceId;
            ETLDate = DateTime.UtcNow;
        }

        /// <summary>
        /// Creates PropertyHistory from HubSpot propertiesWithHistory JSON data
        /// </summary>
        public static List<PropertyHistory> CreateFromHubSpotHistory(
            string objectType,
            string objectId,
            string propertyName,
            JsonElement historyArray)
        {
            var histories = new List<PropertyHistory>();
            
            if (historyArray.ValueKind != JsonValueKind.Array)
            {
                return histories;
            }

            string? previousValue = null;
            
            foreach (var historyItem in historyArray.EnumerateArray())
            {
                if (historyItem.ValueKind != JsonValueKind.Object)
                    continue;

                var newValue = historyItem.TryGetProperty("value", out var valueProp) 
                    ? valueProp.GetString() 
                    : null;
                
                DateTime? timestamp = null;
                if (historyItem.TryGetProperty("timestamp", out var timestampProp))
                {
                    var timestampStr = timestampProp.GetString();
                    if (DateTime.TryParse(timestampStr, out var parsedDate))
                    {
                        timestamp = parsedDate;
                    }
                }
                
                var source = historyItem.TryGetProperty("source", out var sourceProp)
                    ? sourceProp.GetString()
                    : null;
                
                var sourceId = historyItem.TryGetProperty("sourceId", out var sourceIdProp)
                    ? sourceIdProp.GetString()
                    : null;

                if (timestamp.HasValue && !string.IsNullOrEmpty(newValue))
                {
                    var history = new PropertyHistory(
                        objectType,
                        objectId,
                        propertyName,
                        previousValue,
                        newValue,
                        timestamp.Value,
                        source,
                        sourceId);
                    
                    histories.Add(history);
                    previousValue = newValue;
                }
            }

            return histories;
        }
    }
}

