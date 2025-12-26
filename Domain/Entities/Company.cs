using CSharpFunctionalExtensions;
using System.Text.Json;

namespace ETL.HubspotService.Domain.Entities
{
    public class Company : Entity<long>
    {
        public string HubSpotId { get; internal set; } = default!;
        public string? RecordId { get; internal set; }
        public string? Name { get; internal set; } // COMPANY NAME
        public string? CompanyOwner { get; internal set; } // COMPANY OWNER
        public DateTime? CreatedAt { get; internal set; } // CREATE DATE
        public string? Phone { get; internal set; } // PHONE NUMBER
        public DateTime? LastActivityDate { get; internal set; } // LAST ACTIVITY DATE
        public string? City { get; internal set; } // CITY
        public string? Country { get; internal set; } // COUNTRY/REGION
        public string? Cvr { get; internal set; } // CVR (Company Registration Number)
        public string? PostalCode { get; internal set; } // POSTAL CODE
        public string? Type { get; internal set; } // COMPANY TYPE
        public DateTime ETLDate { get; internal set; }

        private Company() { } // EF Core constructor

        public Company(string hubSpotId, string? recordId, string? name, string? companyOwner, DateTime? createdAt,
            string? phone, DateTime? lastActivityDate, string? city, string? country, 
            string? cvr, string? postalCode, string? type)
        {
            HubSpotId = hubSpotId;
            RecordId = recordId;
            Name = name;
            CompanyOwner = companyOwner;
            CreatedAt = createdAt;
            Phone = phone;
            LastActivityDate = lastActivityDate;
            City = city;
            Country = country;
            Cvr = cvr;
            PostalCode = postalCode;
            Type = type;
            ETLDate = DateTime.UtcNow;
        }

        public static Result<Company> CreateFromHubSpotData(JsonElement hubspotData)
        {
            try
            {
                var properties = hubspotData.GetProperty("properties");
                
                var hubSpotId = HubSpotEntityHelper.GetHubSpotId(hubspotData);
                if (string.IsNullOrEmpty(hubSpotId))
                {
                    return Result.Failure<Company>("Missing or invalid HubSpotId");
                }
                
                var name = HubSpotEntityHelper.GetStringProperty(properties, "name");
                var companyOwnerId = HubSpotEntityHelper.GetStringProperty(properties, "hubspot_owner_id");
                var createdAt = HubSpotEntityHelper.GetDateTimeProperty(properties, "createdate");
                var phone = HubSpotEntityHelper.GetStringProperty(properties, "phone");
                var lastActivityDate = HubSpotEntityHelper.GetDateTimeProperty(properties, "hs_last_activity_date") ??
                                       HubSpotEntityHelper.GetDateTimeProperty(properties, "notes_last_updated");
                var city = HubSpotEntityHelper.GetStringProperty(properties, "city");
                var country = HubSpotEntityHelper.GetStringProperty(properties, "country");
                var cvr = HubSpotEntityHelper.GetStringProperty(properties, "cvr") ?? 
                         HubSpotEntityHelper.GetStringProperty(properties, "company_registration_number");
                var postalCode = HubSpotEntityHelper.GetStringProperty(properties, "zip") ?? 
                                HubSpotEntityHelper.GetStringProperty(properties, "postal_code");
                var type = HubSpotEntityHelper.GetStringProperty(properties, "type") ?? 
                          HubSpotEntityHelper.GetStringProperty(properties, "company_type") ??
                          HubSpotEntityHelper.GetStringProperty(properties, "hs_company_type");
                var recordId = HubSpotEntityHelper.GetStringProperty(properties, "hs_object_id") ?? hubSpotId;

                return Result.Success(new Company(hubSpotId, recordId, name, companyOwnerId, createdAt,
                    phone, lastActivityDate, city, country, cvr, postalCode, type));
            }
            catch (Exception ex)
            {
                return Result.Failure<Company>($"Failed to create Company from HubSpot data: {ex.Message}");
            }
        }
        
        public void UpdateFrom(Company other)
        {
            // Never overwrite CreatedAt
            // Always update ETLDate
            ETLDate = DateTime.UtcNow;

            // Update other fields (use null-coalescing to preserve existing values if new value is null)
            RecordId = other.RecordId ?? RecordId;
            Name = other.Name ?? Name;
            CompanyOwner = other.CompanyOwner ?? CompanyOwner;
            Phone = other.Phone ?? Phone;
            LastActivityDate = other.LastActivityDate ?? LastActivityDate;
            City = other.City ?? City;
            Country = other.Country ?? Country;
            Cvr = other.Cvr ?? Cvr;
            PostalCode = other.PostalCode ?? PostalCode;
            Type = other.Type ?? Type;
        }
    }
}
