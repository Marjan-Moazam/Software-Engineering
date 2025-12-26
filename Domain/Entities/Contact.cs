using CSharpFunctionalExtensions;
using System.Text.Json;

namespace ETL.HubspotService.Domain.Entities
{
    public class Contact : Entity<long>
    {
        public string HubSpotId { get; internal set; } = default!;
        public string? RecordId { get; internal set; }
        public string? FullName { get; internal set; } // NAME (firstname + lastname)
        public string? Email { get; internal set; }
        public string? Phone { get; internal set; } // PHONE NUMBER
        public string? ContactOwner { get; internal set; } // CONTACT OWNER
        public string? Company { get; internal set; } // PRIMARY COMPANY NAME
        public DateTime? LastActivityDate { get; internal set; } // LAST ACTIVITY DATE
        public string? LeadStatus { get; internal set; } // LEAD STATUS
        public string? LifecycleStage { get; internal set; }
        public string? PostalCode { get; internal set; }
        public string? ContactType { get; internal set; }
        public string? InverterBrand { get; internal set; }
        public DateTime? LastContacted { get; internal set; }
        public DateTime? CreatedAt { get; internal set; } // CREATE DATE
        public string? AnalyticsSource { get; internal set; } // hs_analytics_source - High-level channel of origin
        public string? AnalyticsSourceData1 { get; internal set; } // hs_analytics_source_data_1 - First sub-source
        public string? AnalyticsSourceData2 { get; internal set; } // hs_analytics_source_data_2 - Second-level detail
        public DateTime ETLDate { get; internal set; }

        private Contact() { } // EF Core constructor

        public Contact(string hubSpotId, string? recordId, string? fullName, string? email, string? phone,
            string? contactOwner, string? company, DateTime? lastActivityDate,
            string? leadStatus, string? lifecycleStage,
            string? postalCode, string? contactType,
            string? inverterBrand, DateTime? lastContacted, DateTime? createdAt,
            string? analyticsSource, string? analyticsSourceData1, string? analyticsSourceData2)
        {
            HubSpotId = hubSpotId;
            RecordId = recordId;
            FullName = fullName;
            Email = email;
            Phone = phone;
            ContactOwner = contactOwner;
            Company = company;
            LastActivityDate = lastActivityDate;
            LeadStatus = leadStatus;
            LifecycleStage = lifecycleStage;
            PostalCode = postalCode;
            ContactType = contactType;
            InverterBrand = inverterBrand;
            LastContacted = lastContacted;
            CreatedAt = createdAt;
            AnalyticsSource = analyticsSource;
            AnalyticsSourceData1 = analyticsSourceData1;
            AnalyticsSourceData2 = analyticsSourceData2;
            ETLDate = DateTime.UtcNow;
        }

        public static Result<Contact> CreateFromHubSpotData(JsonElement hubspotData)
        {
            try
            {
                var properties = hubspotData.GetProperty("properties");
                
                var hubSpotId = HubSpotEntityHelper.GetHubSpotId(hubspotData);
                if (string.IsNullOrEmpty(hubSpotId))
                {
                    return Result.Failure<Contact>("Missing or invalid HubSpotId");
                }
                
                var firstName = HubSpotEntityHelper.GetStringProperty(properties, "firstname");
                var lastName = HubSpotEntityHelper.GetStringProperty(properties, "lastname");
                var fullName = string.IsNullOrEmpty(firstName) && string.IsNullOrEmpty(lastName)
                    ? null
                    : $"{firstName} {lastName}".Trim();
                
                var email = HubSpotEntityHelper.GetStringProperty(properties, "email");
                var phone = HubSpotEntityHelper.GetStringProperty(properties, "phone");
                var contactOwnerId = HubSpotEntityHelper.GetStringProperty(properties, "hubspot_owner_id");
                var companyName = HubSpotEntityHelper.GetStringProperty(properties, "company");
                var lastActivityDate = HubSpotEntityHelper.GetDateTimeProperty(properties, "notes_last_activity_date") ??
                                       HubSpotEntityHelper.GetDateTimeProperty(properties, "notes_last_updated");
                var leadStatus = HubSpotEntityHelper.GetStringProperty(properties, "hs_lead_status");
                var lifecycleStage = HubSpotEntityHelper.GetStringProperty(properties, "lifecyclestage");
                var postalCode = HubSpotEntityHelper.GetStringProperty(properties, "zip");
                var contactType = HubSpotEntityHelper.GetStringProperty(properties, "contact_type");
                var inverterBrand = HubSpotEntityHelper.GetStringProperty(properties, "inverter_brand");
                var lastContacted = HubSpotEntityHelper.GetDateTimeProperty(properties, "notes_last_contacted");
                var createdAt = HubSpotEntityHelper.GetDateTimeProperty(properties, "createdate");
                var recordId = HubSpotEntityHelper.GetStringProperty(properties, "hs_object_id") ?? hubSpotId;
                
                // Creation source properties
                var analyticsSource = HubSpotEntityHelper.GetStringProperty(properties, "hs_analytics_source");
                var analyticsSourceData1 = HubSpotEntityHelper.GetStringProperty(properties, "hs_analytics_source_data_1");
                var analyticsSourceData2 = HubSpotEntityHelper.GetStringProperty(properties, "hs_analytics_source_data_2");

                return Result.Success(new Contact(hubSpotId, recordId, fullName, email, phone, contactOwnerId,
                    companyName, lastActivityDate, leadStatus, lifecycleStage,
                    postalCode, contactType, inverterBrand, lastContacted, createdAt,
                    analyticsSource, analyticsSourceData1, analyticsSourceData2));
            }
            catch (Exception ex)
            {
                return Result.Failure<Contact>($"Failed to create Contact from HubSpot data: {ex.Message}");
            }
        }
        
        public void UpdateFrom(Contact other)
        {
            // Never overwrite CreatedAt
            // Always update ETLDate
            ETLDate = DateTime.UtcNow;

            // Update other fields (use null-coalescing to preserve existing values if new value is null)
            RecordId = other.RecordId ?? RecordId;
            FullName = other.FullName ?? FullName;
            Email = other.Email ?? Email;
            Phone = other.Phone ?? Phone;
            ContactOwner = other.ContactOwner ?? ContactOwner;
            Company = other.Company ?? Company;
            LastActivityDate = other.LastActivityDate ?? LastActivityDate;
            LeadStatus = other.LeadStatus ?? LeadStatus;
            LifecycleStage = other.LifecycleStage ?? LifecycleStage;
            PostalCode = other.PostalCode ?? PostalCode;
            ContactType = other.ContactType ?? ContactType;
            InverterBrand = other.InverterBrand ?? InverterBrand;
            LastContacted = other.LastContacted ?? LastContacted;
            AnalyticsSource = other.AnalyticsSource ?? AnalyticsSource;
            AnalyticsSourceData1 = other.AnalyticsSourceData1 ?? AnalyticsSourceData1;
            AnalyticsSourceData2 = other.AnalyticsSourceData2 ?? AnalyticsSourceData2;
        }
    }
}
