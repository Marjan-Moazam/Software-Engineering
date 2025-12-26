using CSharpFunctionalExtensions;
using System.Text.Json;

namespace ETL.HubspotService.Domain.Entities
{
    public class Deal : Entity<long>
    {
        public string HubSpotId { get; internal set; } = default!;
        public string? RecordId { get; internal set; }
        public string? DealName { get; internal set; } // DEAL NAME
        public string? DealStage { get; internal set; } // DEAL STAGE
        public string? Pipeline { get; internal set; } // PIPELINE
        public DateTime? CloseDate { get; internal set; } // CLOSE DATE
        public string? DealOwner { get; internal set; } // DEAL OWNER
        public decimal? Amount { get; internal set; } // AMOUNT
        public string? DealType { get; internal set; } // dealtype
        public DateTime? CreatedAt { get; internal set; } // CREATED DATE (createdate)
        public DateTime ETLDate { get; internal set; }

        private Deal() { } // EF Core constructor

        public Deal(string hubSpotId, string? recordId, string? dealName, string? dealStage, string? pipeline, DateTime? closeDate,
            string? dealOwner, decimal? amount, string? dealType, DateTime? createdAt = null)
        {
            HubSpotId = hubSpotId;
            RecordId = recordId;
            DealName = dealName;
            DealStage = dealStage;
            Pipeline = pipeline;
            CloseDate = closeDate;
            DealOwner = dealOwner;
            Amount = amount;
            DealType = dealType;
            CreatedAt = createdAt;
            ETLDate = DateTime.UtcNow;
        }

        public static Result<Deal> CreateFromHubSpotData(JsonElement hubspotData)
        {
            try
            {
                var properties = hubspotData.GetProperty("properties");
                
                var hubSpotId = HubSpotEntityHelper.GetHubSpotId(hubspotData);
                if (string.IsNullOrEmpty(hubSpotId))
                {
                    return Result.Failure<Deal>("Missing or invalid HubSpotId");
                }
                
                var dealName = HubSpotEntityHelper.GetStringProperty(properties, "dealname");
                var dealStage = HubSpotEntityHelper.GetStringProperty(properties, "dealstage");
                var pipeline = HubSpotEntityHelper.GetStringProperty(properties, "pipeline");
                var closeDate = HubSpotEntityHelper.GetDateTimeProperty(properties, "closedate");
                var dealOwnerId = HubSpotEntityHelper.GetStringProperty(properties, "hubspot_owner_id");
                var amount = HubSpotEntityHelper.GetDecimalProperty(properties, "amount");
                var dealType = HubSpotEntityHelper.GetStringProperty(properties, "dealtype");
                var createdAt = HubSpotEntityHelper.GetDateTimeProperty(properties, "createdate");
                var recordId = HubSpotEntityHelper.GetStringProperty(properties, "hs_object_id") ?? hubSpotId;

                return Result.Success(new Deal(hubSpotId, recordId, dealName, dealStage, pipeline, closeDate, dealOwnerId, amount, dealType, createdAt));
            }
            catch (Exception ex)
            {
                return Result.Failure<Deal>($"Failed to create Deal from HubSpot data: {ex.Message}");
            }
        }
        
        public void UpdateFrom(Deal other)
        {
            // Always update ETLDate
            ETLDate = DateTime.UtcNow;

            // Update other fields (use null-coalescing to preserve existing values if new value is null)
            RecordId = other.RecordId ?? RecordId;
            DealName = other.DealName ?? DealName;
            DealStage = other.DealStage ?? DealStage;
            Pipeline = other.Pipeline ?? Pipeline;
            CloseDate = other.CloseDate ?? CloseDate;
            DealOwner = other.DealOwner ?? DealOwner;
            Amount = other.Amount ?? Amount;
            DealType = other.DealType ?? DealType;
            CreatedAt = other.CreatedAt ?? CreatedAt;
        }
    }
}
