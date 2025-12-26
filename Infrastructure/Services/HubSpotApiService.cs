using CSharpFunctionalExtensions;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace ETL.HubspotService.Infrastructure.Services
{
    /// <summary>
    /// Helper class to store association details from HubSpot v4 API
    /// </summary>
    public class AssociationDetails
    {
        public string TargetObjectId { get; set; } = default!; // Generic target object ID (company, deal, ticket, etc.)
        public string? PrimaryLabel { get; set; }
        public List<string> AllLabels { get; set; } = new();
        public int? AssociationTypeId { get; set; }
        public string? AssociationCategory { get; set; }
        public DateTime? CreatedAt { get; set; }
        public DateTime? UpdatedAt { get; set; }
        public string? Source { get; set; }
        public string? SourceId { get; set; }
        
        // Backward compatibility property
        [Obsolete("Use TargetObjectId instead")]
        public string CompanyId 
        { 
            get => TargetObjectId; 
            set => TargetObjectId = value; 
        }
    }
    public interface IHubSpotApiService
    {
        Dictionary<string, List<AssociationDetails>>? GetLastDetailedAssociations();
        Task<Result<HubSpotApiResponse<JsonElement>>> GetContactsAsync(int limit = 100, string? after = null);
        Task<Result<HubSpotApiResponse<JsonElement>>> GetCompaniesAsync(int limit = 100, string? after = null);
        Task<Result<HubSpotApiResponse<JsonElement>>> GetDealsAsync(int limit = 100, string? after = null);
        Task<Result<HubSpotApiResponse<JsonElement>>> GetTicketsAsync(int limit = 100, string? after = null);
        Task<Result<HubSpotApiResponse<JsonElement>>> GetCommunicationsAsync(int limit = 100, string? after = null);
        Task<Result<HubSpotApiResponse<JsonElement>>> GetEmailsAsync(int limit = 100, string? after = null);
        Task<Result<HubSpotApiResponse<JsonElement>>> GetNotesAsync(int limit = 100, string? after = null);
        Task<Result<HubSpotApiResponse<JsonElement>>> GetCallsAsync(int limit = 100, string? after = null);
        Task<Result<HubSpotApiResponse<JsonElement>>> GetMeetingsAsync(int limit = 100, string? after = null);
        Task<Result<HubSpotApiResponse<JsonElement>>> GetTasksAsync(int limit = 100, string? after = null);
        Task<Result<HubSpotApiResponse<JsonElement>>> GetSmsAsync(int limit = 100, string? after = null);
        Task<Result<JsonElement>> GetContactByIdAsync(string contactId);
        Task<Result<Dictionary<string, string>>> GetTicketPropertyOptionsAsync(string propertyName);
        Task<Result<Dictionary<string, string>>> GetOwnersAsync();
        Task<Result<List<string>>> GetContactCompanyAssociationsAsync(string contactId);
        Task<Result<List<string>>> GetCompanyContactAssociationsAsync(string companyId);
        Task<Result<Dictionary<string, List<string>>>> GetContactCompanyAssociationsBatchAsync(List<string> contactIds);
        Task<Result<Dictionary<string, List<string>>>> GetCompanyContactAssociationsBatchAsync(List<string> companyIds);
        
        /// <summary>
        /// Generic method to fetch associations between any two HubSpot object types using v4 batch/read API.
        /// Returns detailed association information including labels, type IDs, categories, etc.
        /// </summary>
        /// <param name="sourceObjectType">Source object type (singular): contact, company, deal, ticket, email, call, note, task, meeting</param>
        /// <param name="targetObjectType">Target object type (singular): deal, ticket, company, contact, call, email, note, task, meeting</param>
        /// <param name="sourceObjectIds">List of source object HubSpot IDs</param>
        /// <returns>Dictionary mapping source object ID to list of AssociationDetails for target objects</returns>
        Task<Result<Dictionary<string, List<AssociationDetails>>>> GetObjectAssociationsBatchAsync(
            string sourceObjectType, 
            string targetObjectType, 
            List<string> sourceObjectIds);
        
        /// <summary>
        /// Fetches property history for a specific object using HubSpot's propertiesWithHistory parameter.
        /// Returns the full history of property changes with timestamps.
        /// </summary>
        /// <param name="objectType">Object type: deals, tickets, companies</param>
        /// <param name="objectId">HubSpot ID of the object</param>
        /// <param name="propertyName">Property name to get history for (e.g., dealstage, hs_pipeline_stage, hs_lead_status)</param>
        Task<Result<JsonElement>> GetObjectPropertyHistoryAsync(string objectType, string objectId, string propertyName);
    }

    public class HubSpotApiResponse<T>
    {
        public List<T> Results { get; set; } = new();
        public PagingInfo? Paging { get; set; }
    }

    public class PagingInfo
    {
        [JsonPropertyName("next")]
        public PagingNext? Next { get; set; }
    }

    public class PagingNext
    {
        [JsonPropertyName("after")]
        public string? After { get; set; }
        [JsonPropertyName("link")]
        public string? Link { get; set; }
    }

    public class HubSpotApiService : IHubSpotApiService
    {
        private readonly HttpClient _httpClient;
        private readonly ILogger<HubSpotApiService> _logger;
        private readonly IConfiguration _configuration;
        private static readonly JsonSerializerOptions JsonOptions = new()
        {
            PropertyNameCaseInsensitive = true
        };

        // Property lists for each entity - only the fields we need
        private static readonly string[] ContactProps = new[]
        {
            "firstname", "lastname", "email", "phone", "hubspot_owner_id",
            "company", "notes_last_activity_date", "notes_last_updated", "notes_last_contacted", "hs_lead_status",
            "hs_marketing_contact_status", "lifecyclestage", "zip",
            "contact-type", "epicore_package", "inverter_brand",
            "createdate", "hs_object_id",
            "hs_analytics_source", "hs_analytics_source_data_1", "hs_analytics_source_data_2"
        };

        private static readonly string[] CompanyProps = new[]
        {
            "name", "hubspot_owner_id", "createdate", "phone",
            "hs_last_activity_date", "notes_last_updated", "city", "country", "hs_object_id",
            "cvr", "company_registration_number", "zip", "postal_code",
            "hs_record_source", "hs_created_by_source",
            "type", "company_type", "hs_company_type"
        };

        private static readonly string[] DealProps = new[]
        {
            "dealname", "dealstage", "pipeline", "closedate", "hubspot_owner_id", "amount", "hs_tag_ids", "dealtype", "description", "hs_object_id", "createdate"
        };

        private static readonly string[] TicketProps = new[]
        {
            "subject", "hs_pipeline", "hs_pipeline_stage", "createdate",
            "hs_ticket_priority", "hubspot_owner_id", "source_type",
            "created_by_source", "hs_ticket_source", "hs_lastactivitydate",
            "hs_object_id"
        };

        private static readonly string[] CommunicationProps = new[]
        {
            "hs_communication_body", "hs_communication_channel_type",
            "hubspot_owner_id", "hs_timestamp", "hs_createdate", "hs_object_id"
        };

        private static readonly string[] EmailProps = new[]
        {
            "hs_email_subject", "hs_email_text", "hs_email_html", "hs_email_status",
            "hs_timestamp", "hubspot_owner_id", "hs_object_id",
            "hs_activity_assigned_to", "hs_activity_created_by", "hs_activity_date",
            "hs_createdate", "hs_created_by_user_id", "hs_email_bcc_address",
            "hs_email_cc_address", "hs_email_click_rate", "hs_email_direction",
            "hs_email_from_address", "hs_email_open_rate", "hs_email_reply_rate",
            "hs_email_to_address", "hubspot_team_id", "hs_lastmodifieddate",
            "hs_num_email_clicks", "hs_num_email_opens", "hs_num_email_replies",
            "hs_num_emails_sent", "hs_record_source", "hs_record_source_detail_1",
            "hs_updated_by_user_id"
        };

        private static readonly string[] NoteProps = new[]
        {
            "hs_note_body", "hs_timestamp", "hs_createdate", "hubspot_owner_id", "hs_object_id",
            "hs_activity_assigned_to", "hs_activity_created_by", "hs_activity_date",
            "hs_created_by_user_id", "hubspot_team_id", "hs_lastmodifieddate",
            "hs_record_source", "hs_record_source_detail_1"
        };

        private static readonly string[] CallProps = new[]
        {
            "hs_call_title", "hs_call_body", "hs_call_direction",
            "hs_call_status", "hs_timestamp", "hubspot_owner_id", "hs_object_id",
            "hs_activity_assigned_to", "hs_activity_created_by", "hs_activity_date",
            "hs_call_source", "hs_call_title", "hs_call_meeting_type", "hs_call_direction",
            "hs_call_duration", "hs_call_outcome", "hs_call_summary", "hs_createdate",
            "hs_created_by_user_id", "hs_call_from_number_name", "hs_call_from_number",
            "hubspot_team_id", "hs_lastmodifieddate", "hs_call_to_number", "hs_call_to_number_name",
            "hs_record_source", "hs_record_source_detail_1"
        };

        private static readonly string[] MeetingProps = new[]
        {
            "hs_meeting_title", "hs_meeting_body", "hs_meeting_start_time", "hs_meeting_end_time",
            "hubspot_owner_id", "hs_timestamp", "hs_object_id",
            "hs_meeting_assigned_to", "hs_meeting_activity_created_by", "hs_activity_date", "hs_meeting_type",
            "hs_contact_first_outreach_date", "hs_createdate", "hs_created_by_user_id", "hubspot_team_id",
            "hs_attendee_owner_ids", "hs_internal_meeting_notes", "hs_lastmodifieddate", "hs_meeting_location_type",
            "hs_meeting_location", "hs_meeting_source", "hs_record_source",
            "hs_time_to_book_meeting_from_first_contact"
        };

        private static readonly string[] TaskProps = new[]
        {
            "hs_task_subject", "hs_task_body", "hs_task_priority",
            "hs_task_status", "hs_timestamp", "hubspot_owner_id", "hs_object_id",
            "hs_activity_assigned_to", "hs_activity_date", "hs_task_channel_type",
            "hs_conversation_first_message_timestamp", "hs_created_by_user_id",
            "hubspot_team_id", "hs_logged_from", "hs_object_create_date",
            "hs_object_last_modified_date", "hs_task_completion_timestamp",
            "hs_contact_timezone", "hs_createdate", "hs_created_by", "hs_task_due_date",
            "hs_task_is_overdue", "hs_last_engagement_date", "hs_lastmodifieddate",
            "hs_notes_preview", "hs_task_pipeline", "hs_task_pipeline_stage",
            "hs_task_queue", "hs_record_source", "hs_record_source_detail_1",
            "hs_task_reminders", "hs_task_start_date", "hs_task_notes",
            "hs_task_title", "hs_task_type", "hs_updated_by_user_id"
        };

        private static readonly string[] SmsProps = new[]
        {
            "hs_sms_title", "hs_sms_body", "hs_sms_text", "hs_sms_message_body",
            "hs_sms_direction", "hs_sms_message_direction", "hs_sms_status", "hs_sms_message_status",
            "hs_sms_channel_account_name", "hs_sms_channel_name",
            "hs_timestamp", "hubspot_owner_id", "hs_object_id",
            "hs_activity_assigned_to", "hs_activity_date", "hs_sms_channel_type",
            "hs_sms_message_body", "hs_sms_conversation_first_message_timestamp",
            "hs_created_by_user_id", "hubspot_team_id", "hs_logged_from",
            "hs_object_create_date", "hs_object_last_modified_date", "hs_owner_assigneddate",
            "hs_record_source", "hs_record_source_detail_1", "hs_updated_by_user_id"
        };

        private static string BuildProps(string[] props) => string.Join(",", props);

        // Store detailed associations for access by ETLService
        private Dictionary<string, List<AssociationDetails>>? _lastDetailedAssociations;

        public HubSpotApiService(HttpClient httpClient, ILogger<HubSpotApiService> logger, IConfiguration configuration)
        {
            _httpClient = httpClient;
            _logger = logger;
            _configuration = configuration;
        }
        
        /// <summary>
        /// Gets the detailed associations from the last batch read call
        /// </summary>
        public Dictionary<string, List<AssociationDetails>>? GetLastDetailedAssociations()
        {
            return _lastDetailedAssociations;
        }

        public async Task<Result<HubSpotApiResponse<JsonElement>>> GetContactsAsync(int limit = 100, string? after = null)
        {
            var props = Uri.EscapeDataString(BuildProps(ContactProps));
            // Include associations parameter to get company associations via v3 API as fallback
            return await GetHubSpotDataAsync($"crm/v3/objects/contacts?properties={props}&archived=false&associations=companies", limit, after);
        }

        public async Task<Result<HubSpotApiResponse<JsonElement>>> GetCompaniesAsync(int limit = 100, string? after = null)
        {
            var props = Uri.EscapeDataString(BuildProps(CompanyProps));
            return await GetHubSpotDataAsync($"crm/v3/objects/companies?properties={props}&archived=false", limit, after);
        }

        public async Task<Result<HubSpotApiResponse<JsonElement>>> GetDealsAsync(int limit = 100, string? after = null)
        {
            var props = Uri.EscapeDataString(BuildProps(DealProps));
            return await GetHubSpotDataAsync($"crm/v3/objects/deals?properties={props}&archived=false", limit, after);
        }

        public async Task<Result<HubSpotApiResponse<JsonElement>>> GetTicketsAsync(int limit = 100, string? after = null)
        {
            var props = Uri.EscapeDataString(BuildProps(TicketProps));
            return await GetHubSpotDataAsync($"crm/v3/objects/tickets?properties={props}&archived=false", limit, after);
        }

        public async Task<Result<HubSpotApiResponse<JsonElement>>> GetCommunicationsAsync(int limit = 100, string? after = null)
        {
            var props = Uri.EscapeDataString(BuildProps(CommunicationProps));
            return await GetHubSpotDataAsync($"crm/v3/objects/communications?properties={props}&associations=contacts,companies,deals&archived=false", limit, after);
        }

        public async Task<Result<HubSpotApiResponse<JsonElement>>> GetEmailsAsync(int limit = 100, string? after = null)
        {
            var props = Uri.EscapeDataString(BuildProps(EmailProps));
            return await GetHubSpotDataAsync($"crm/v3/objects/emails?properties={props}&associations=contacts,companies,deals,tickets&archived=false", limit, after);
        }

        public async Task<Result<HubSpotApiResponse<JsonElement>>> GetNotesAsync(int limit = 100, string? after = null)
        {
            var props = Uri.EscapeDataString(BuildProps(NoteProps));
            return await GetHubSpotDataAsync($"crm/v3/objects/notes?properties={props}&associations=contacts,companies,deals,tickets&archived=false", limit, after);
        }

        public async Task<Result<HubSpotApiResponse<JsonElement>>> GetCallsAsync(int limit = 100, string? after = null)
        {
            var props = Uri.EscapeDataString(BuildProps(CallProps));
            return await GetHubSpotDataAsync($"crm/v3/objects/calls?properties={props}&associations=contacts,companies,deals,tickets&archived=false", limit, after);
        }

        public async Task<Result<HubSpotApiResponse<JsonElement>>> GetMeetingsAsync(int limit = 100, string? after = null)
        {
            var props = Uri.EscapeDataString(BuildProps(MeetingProps));
            return await GetHubSpotDataAsync($"crm/v3/objects/meetings?properties={props}&associations=contacts,companies,deals,tickets&archived=false", limit, after);
        }

        public async Task<Result<HubSpotApiResponse<JsonElement>>> GetTasksAsync(int limit = 100, string? after = null)
        {
            var props = Uri.EscapeDataString(BuildProps(TaskProps));
            return await GetHubSpotDataAsync($"crm/v3/objects/tasks?properties={props}&associations=contacts,companies,deals,tickets&archived=false", limit, after);
        }

        public async Task<Result<HubSpotApiResponse<JsonElement>>> GetSmsAsync(int limit = 100, string? after = null)
        {
            var props = Uri.EscapeDataString(BuildProps(SmsProps));
            return await GetHubSpotDataAsync($"crm/v3/objects/sms?properties={props}&associations=contacts,companies,deals,tickets&archived=false", limit, after);
        }

        public async Task<Result<JsonElement>> GetContactByIdAsync(string contactId)
        {
            if (string.IsNullOrWhiteSpace(contactId))
            {
                return Result.Failure<JsonElement>("ContactId is required");
            }

            var props = Uri.EscapeDataString(BuildProps(ContactProps));
            return await GetHubSpotObjectAsync($"crm/v3/objects/contacts/{contactId}?properties={props}&archived=false");
        }

        public async Task<Result<Dictionary<string, string>>> GetTicketPropertyOptionsAsync(string propertyName)
        {
            if (string.IsNullOrWhiteSpace(propertyName))
            {
                return Result.Failure<Dictionary<string, string>>("Property name is required");
            }

            return await GetPropertyOptionsAsync("tickets", propertyName);
        }

        private async Task<Result<HubSpotApiResponse<JsonElement>>> GetHubSpotDataAsync(string endpoint, int limit, string? after)
        {
            try
            {
                var baseUrl = _configuration["HubSpot:BaseUrl"] ?? "https://api.hubapi.com";
                var accessToken = _configuration["HubSpot:AccessToken"];

                if (string.IsNullOrEmpty(accessToken))
                {
                    return Result.Failure<HubSpotApiResponse<JsonElement>>("HubSpot access token is not configured");
                }

                var url = $"{baseUrl}/{endpoint}&limit={limit}";
                if (!string.IsNullOrEmpty(after))
                {
                    url += $"&after={after}";
                }

                _httpClient.DefaultRequestHeaders.Clear();
                _httpClient.DefaultRequestHeaders.Add("Authorization", $"Bearer {accessToken}");

                _logger.LogInformation("Making request to HubSpot API: {Url}", url);

                var response = await _httpClient.GetAsync(url);

                if (!response.IsSuccessStatusCode)
                {
                    var errorContent = await response.Content.ReadAsStringAsync();
                    _logger.LogError("HubSpot API request failed with status {StatusCode}: {ErrorContent}", 
                        response.StatusCode, errorContent);
                    return Result.Failure<HubSpotApiResponse<JsonElement>>(
                        $"HubSpot API request failed: {response.StatusCode} - {errorContent}");
                }

                var jsonContent = await response.Content.ReadAsStringAsync();
                var jsonDocument = JsonDocument.Parse(jsonContent);

                var result = new HubSpotApiResponse<JsonElement>();

                if (jsonDocument.RootElement.TryGetProperty("results", out var results))
                {
                    result.Results = results.EnumerateArray().ToList();
                }

                if (jsonDocument.RootElement.TryGetProperty("paging", out var paging))
                {
                    result.Paging = JsonSerializer.Deserialize<PagingInfo>(paging.GetRawText(), JsonOptions);
                }

                _logger.LogInformation("Successfully retrieved {Count} items from HubSpot API", result.Results.Count);

                return Result.Success(result);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error occurred while calling HubSpot API endpoint: {Endpoint}", endpoint);
                return Result.Failure<HubSpotApiResponse<JsonElement>>($"Error calling HubSpot API: {ex.Message}");
            }
        }

        private async Task<Result<JsonElement>> GetHubSpotObjectAsync(string endpoint)
        {
            try
            {
                var baseUrl = _configuration["HubSpot:BaseUrl"] ?? "https://api.hubapi.com";
                var accessToken = _configuration["HubSpot:AccessToken"];

                if (string.IsNullOrEmpty(accessToken))
                {
                    return Result.Failure<JsonElement>("HubSpot access token is not configured");
                }

                var url = $"{baseUrl}/{endpoint}";

                _httpClient.DefaultRequestHeaders.Clear();
                _httpClient.DefaultRequestHeaders.Add("Authorization", $"Bearer {accessToken}");

                _logger.LogInformation("Making request to HubSpot API: {Url}", url);

                var response = await _httpClient.GetAsync(url);

                if (!response.IsSuccessStatusCode)
                {
                    var errorContent = await response.Content.ReadAsStringAsync();
                    _logger.LogError("HubSpot API request failed with status {StatusCode}: {ErrorContent}",
                        response.StatusCode, errorContent);
                    return Result.Failure<JsonElement>(
                        $"HubSpot API request failed: {response.StatusCode} - {errorContent}");
                }

                var jsonContent = await response.Content.ReadAsStringAsync();
                var jsonDocument = JsonDocument.Parse(jsonContent);

                return Result.Success(jsonDocument.RootElement.Clone());
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error occurred while calling HubSpot API endpoint: {Endpoint}", endpoint);
                return Result.Failure<JsonElement>($"Error calling HubSpot API: {ex.Message}");
            }
        }

        private async Task<Result<Dictionary<string, string>>> GetPropertyOptionsAsync(string objectType, string propertyName)
        {
            try
            {
                var baseUrl = _configuration["HubSpot:BaseUrl"] ?? "https://api.hubapi.com";
                var accessToken = _configuration["HubSpot:AccessToken"];

                if (string.IsNullOrEmpty(accessToken))
                {
                    return Result.Failure<Dictionary<string, string>>("HubSpot access token is not configured");
                }

                var url = $"{baseUrl}/crm/v3/properties/{objectType}/{propertyName}";

                _httpClient.DefaultRequestHeaders.Clear();
                _httpClient.DefaultRequestHeaders.Add("Authorization", $"Bearer {accessToken}");

                _logger.LogInformation("Fetching property metadata from HubSpot API: {Url}", url);

                var response = await _httpClient.GetAsync(url);

                if (!response.IsSuccessStatusCode)
                {
                    var errorContent = await response.Content.ReadAsStringAsync();
                    _logger.LogError("HubSpot property API request failed with status {StatusCode}: {ErrorContent}",
                        response.StatusCode, errorContent);
                    return Result.Failure<Dictionary<string, string>>(
                        $"HubSpot property API request failed: {response.StatusCode} - {errorContent}");
                }

                var jsonContent = await response.Content.ReadAsStringAsync();
                var doc = JsonDocument.Parse(jsonContent);

                var options = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                if (doc.RootElement.TryGetProperty("options", out var optionsArray))
                {
                    foreach (var option in optionsArray.EnumerateArray())
                    {
                        var value = option.TryGetProperty("value", out var valueEl) ? valueEl.GetString() : null;
                        var label = option.TryGetProperty("label", out var labelEl) ? labelEl.GetString() : null;
                        if (!string.IsNullOrWhiteSpace(value))
                        {
                            options[value] = label ?? value;
                        }
                    }
                }

                return Result.Success(options);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error occurred while fetching property metadata for {ObjectType}.{PropertyName}", objectType, propertyName);
                return Result.Failure<Dictionary<string, string>>($"Error fetching property metadata: {ex.Message}");
            }
        }

        public async Task<Result<Dictionary<string, string>>> GetOwnersAsync()
        {
            try
            {
                var baseUrl = _configuration["HubSpot:BaseUrl"] ?? "https://api.hubapi.com";
                var accessToken = _configuration["HubSpot:AccessToken"];

                if (string.IsNullOrEmpty(accessToken))
                {
                    return Result.Failure<Dictionary<string, string>>("HubSpot access token is not configured");
                }

                var owners = new Dictionary<string, string>();
                var after = string.Empty;

                do
                {
                    var url = $"{baseUrl}/crm/v3/owners/?archived=false&limit=100";
                    if (!string.IsNullOrEmpty(after))
                    {
                        url += $"&after={after}";
                    }

                    _httpClient.DefaultRequestHeaders.Clear();
                    _httpClient.DefaultRequestHeaders.Add("Authorization", $"Bearer {accessToken}");

                    var response = await _httpClient.GetAsync(url);
                    if (!response.IsSuccessStatusCode)
                    {
                        var errorContent = await response.Content.ReadAsStringAsync();
                        _logger.LogError("HubSpot Owners API request failed with status {StatusCode}: {ErrorContent}",
                            response.StatusCode, errorContent);
                        return Result.Failure<Dictionary<string, string>>(
                            $"HubSpot Owners API request failed: {response.StatusCode} - {errorContent}");
                    }

                    var json = await response.Content.ReadAsStringAsync();
                    var doc = JsonDocument.Parse(json);

                    if (doc.RootElement.TryGetProperty("results", out var results))
                    {
                        foreach (var owner in results.EnumerateArray())
                        {
                            var id = owner.TryGetProperty("id", out var idElement) ? idElement.GetString() : null;
                            var firstName = owner.TryGetProperty("firstName", out var firstNameEl) ? firstNameEl.GetString() : null;
                            var lastName = owner.TryGetProperty("lastName", out var lastNameEl) ? lastNameEl.GetString() : null;
                            var email = owner.TryGetProperty("email", out var emailEl) ? emailEl.GetString() : null;

                            if (string.IsNullOrEmpty(id))
                                continue;

                            var displayName = $"{firstName} {lastName}".Trim();
                            if (string.IsNullOrWhiteSpace(displayName))
                            {
                                displayName = email ?? id;
                            }

                            owners[id] = displayName;
                        }
                    }

                    after = doc.RootElement.TryGetProperty("paging", out var paging) &&
                            paging.TryGetProperty("next", out var next) &&
                            next.TryGetProperty("after", out var afterElement)
                        ? afterElement.GetString()
                        : null;
                } while (!string.IsNullOrEmpty(after));

                return Result.Success(owners);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error occurred while fetching HubSpot owners");
                return Result.Failure<Dictionary<string, string>>($"Error fetching owners: {ex.Message}");
            }
        }

        /// <summary>
        /// Fetches all company associations for a given contact using HubSpot's Associations API.
        /// Returns a list of company HubSpot IDs.
        /// </summary>
        public async Task<Result<List<string>>> GetContactCompanyAssociationsAsync(string contactId)
        {
            try
            {
                var baseUrl = _configuration["HubSpot:BaseUrl"] ?? "https://api.hubapi.com";
                var accessToken = _configuration["HubSpot:AccessToken"];

                if (string.IsNullOrEmpty(accessToken))
                {
                    return Result.Failure<List<string>>("HubSpot access token is not configured");
                }

                if (string.IsNullOrWhiteSpace(contactId))
                {
                    return Result.Failure<List<string>>("ContactId is required");
                }

                var companyIds = new List<string>();
                var after = string.Empty;

                do
                {
                    var url = $"{baseUrl}/crm/v4/objects/contacts/{contactId}/associations/company?limit=100";
                    if (!string.IsNullOrEmpty(after))
                    {
                        url += $"&after={after}";
                    }

                    _httpClient.DefaultRequestHeaders.Clear();
                    _httpClient.DefaultRequestHeaders.Add("Authorization", $"Bearer {accessToken}");

                    _logger.LogInformation("Fetching company associations for contact {ContactId} from HubSpot API", contactId);

                    var response = await _httpClient.GetAsync(url);

                    if (!response.IsSuccessStatusCode)
                    {
                        var errorContent = await response.Content.ReadAsStringAsync();
                        _logger.LogError("HubSpot Associations API request failed with status {StatusCode}: {ErrorContent}",
                            response.StatusCode, errorContent);
                        return Result.Failure<List<string>>(
                            $"HubSpot Associations API request failed: {response.StatusCode} - {errorContent}");
                    }

                    var jsonContent = await response.Content.ReadAsStringAsync();
                    var jsonDocument = JsonDocument.Parse(jsonContent);

                    if (jsonDocument.RootElement.TryGetProperty("results", out var results))
                    {
                        foreach (var result in results.EnumerateArray())
                        {
                            if (result.TryGetProperty("toObjectId", out var toObjectId))
                            {
                                var companyId = toObjectId.GetString();
                                if (!string.IsNullOrWhiteSpace(companyId))
                                {
                                    companyIds.Add(companyId);
                                }
                            }
                        }
                    }

                    after = jsonDocument.RootElement.TryGetProperty("paging", out var paging) &&
                            paging.TryGetProperty("next", out var next) &&
                            next.TryGetProperty("after", out var afterElement)
                        ? afterElement.GetString()
                        : null;
                } while (!string.IsNullOrEmpty(after));

                _logger.LogInformation("Found {Count} company associations for contact {ContactId}", companyIds.Count, contactId);
                return Result.Success(companyIds);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error occurred while fetching company associations for contact {ContactId}", contactId);
                return Result.Failure<List<string>>($"Error fetching company associations: {ex.Message}");
            }
        }

        /// <summary>
        /// Fetches all contact associations for a given company using HubSpot's Associations API.
        /// Returns a list of contact HubSpot IDs.
        /// </summary>
        public async Task<Result<List<string>>> GetCompanyContactAssociationsAsync(string companyId)
        {
            try
            {
                var baseUrl = _configuration["HubSpot:BaseUrl"] ?? "https://api.hubapi.com";
                var accessToken = _configuration["HubSpot:AccessToken"];

                if (string.IsNullOrEmpty(accessToken))
                {
                    return Result.Failure<List<string>>("HubSpot access token is not configured");
                }

                if (string.IsNullOrWhiteSpace(companyId))
                {
                    return Result.Failure<List<string>>("CompanyId is required");
                }

                var contactIds = new List<string>();
                var after = string.Empty;

                do
                {
                    var url = $"{baseUrl}/crm/v4/objects/companies/{companyId}/associations/contact?limit=100";
                    if (!string.IsNullOrEmpty(after))
                    {
                        url += $"&after={after}";
                    }

                    _httpClient.DefaultRequestHeaders.Clear();
                    _httpClient.DefaultRequestHeaders.Add("Authorization", $"Bearer {accessToken}");

                    _logger.LogInformation("Fetching contact associations for company {CompanyId} from HubSpot API", companyId);

                    var response = await _httpClient.GetAsync(url);

                    if (!response.IsSuccessStatusCode)
                    {
                        var errorContent = await response.Content.ReadAsStringAsync();
                        _logger.LogError("HubSpot Associations API request failed with status {StatusCode}: {ErrorContent}",
                            response.StatusCode, errorContent);
                        return Result.Failure<List<string>>(
                            $"HubSpot Associations API request failed: {response.StatusCode} - {errorContent}");
                    }

                    var jsonContent = await response.Content.ReadAsStringAsync();
                    var jsonDocument = JsonDocument.Parse(jsonContent);

                    if (jsonDocument.RootElement.TryGetProperty("results", out var results))
                    {
                        foreach (var result in results.EnumerateArray())
                        {
                            if (result.TryGetProperty("toObjectId", out var toObjectId))
                            {
                                var contactId = toObjectId.GetString();
                                if (!string.IsNullOrWhiteSpace(contactId))
                                {
                                    contactIds.Add(contactId);
                                }
                            }
                        }
                    }

                    after = jsonDocument.RootElement.TryGetProperty("paging", out var paging) &&
                            paging.TryGetProperty("next", out var next) &&
                            next.TryGetProperty("after", out var afterElement)
                        ? afterElement.GetString()
                        : null;
                } while (!string.IsNullOrEmpty(after));

                _logger.LogInformation("Found {Count} contact associations for company {CompanyId}", contactIds.Count, companyId);
                return Result.Success(contactIds);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error occurred while fetching contact associations for company {CompanyId}", companyId);
                return Result.Failure<List<string>>($"Error fetching contact associations: {ex.Message}");
            }
        }

        /// <summary>
        /// Fetches all company associations for multiple contacts using HubSpot's v4 Associations API batch/read endpoint.
        /// This is the recommended method as it returns ALL associations (not just primary) and supports batch processing.
        /// Returns a dictionary mapping contact ID to list of company IDs.
        /// </summary>
        public async Task<Result<Dictionary<string, List<string>>>> GetContactCompanyAssociationsBatchAsync(List<string> contactIds)
        {
            try
            {
                var baseUrl = _configuration["HubSpot:BaseUrl"] ?? "https://api.hubapi.com";
                var accessToken = _configuration["HubSpot:AccessToken"];

                if (string.IsNullOrEmpty(accessToken))
                {
                    return Result.Failure<Dictionary<string, List<string>>>("HubSpot access token is not configured");
                }

                if (contactIds == null || contactIds.Count == 0)
                {
                    return Result.Success(new Dictionary<string, List<string>>());
                }

                // Dictionary to store associations: contactId -> list of (companyId, associationType)
                var associations = new Dictionary<string, List<AssociationDetails>>(StringComparer.OrdinalIgnoreCase);

                // Process in batches of 1000 (HubSpot max batch limit per docs)
                const int batchSize = 1000;
                for (int i = 0; i < contactIds.Count; i += batchSize)
                {
                    var batch = contactIds.Skip(i).Take(batchSize).ToList();
                    
                    // Log sample IDs for debugging
                    if (i == 0 && batch.Count > 0)
                    {
                        _logger.LogInformation("Sample contact IDs being queried (first 5): {Ids}", 
                            string.Join(", ", batch.Take(5)));
                    }
                    
                    // Build request body for batch/read - use numeric IDs as HubSpot prefers
                    var inputs = new List<object>();
                    foreach (var id in batch)
                    {
                        if (long.TryParse(id, out var numericId))
                        {
                            inputs.Add(new { id = numericId });
                        }
                        else
                        {
                            inputs.Add(new { id = id });
                        }
                    }
                    
                    var requestBody = new
                    {
                        inputs = inputs.ToArray()
                    };

                    var jsonBody = JsonSerializer.Serialize(requestBody);
                    // CRITICAL FIX: Use singular object names (contact/company) not plural (contacts/companies)
                    var url = $"{baseUrl}/crm/v4/associations/contact/company/batch/read";

                    _httpClient.DefaultRequestHeaders.Clear();
                    _httpClient.DefaultRequestHeaders.Add("Authorization", $"Bearer {accessToken}");

                    _logger.LogInformation("Fetching company associations for {Count} contacts (batch {Batch}/{TotalBatches})", 
                        batch.Count, (i / batchSize) + 1, (int)Math.Ceiling((double)contactIds.Count / batchSize));

                    // Handle pagination - loop until all pages are retrieved
                    string? after = null;
                    int pageNumber = 1;
                    do
                    {
                        var requestUrl = url;
                        if (!string.IsNullOrEmpty(after))
                        {
                            requestUrl += $"?after={Uri.EscapeDataString(after)}";
                            _logger.LogInformation("Fetching page {Page} for batch {Batch} (after token: {After})", 
                                pageNumber, (i / batchSize) + 1, after.Substring(0, Math.Min(20, after.Length)));
                        }

                        var content = new StringContent(jsonBody, System.Text.Encoding.UTF8, "application/json");
                        var response = await _httpClient.PostAsync(requestUrl, content);

                        // HTTP 207 (Multi-Status) is valid for batch operations
                        var isSuccess = response.IsSuccessStatusCode || response.StatusCode == System.Net.HttpStatusCode.MultiStatus;
                        
                        if (!isSuccess)
                        {
                            var errorContent = await response.Content.ReadAsStringAsync();
                            _logger.LogError("HubSpot v4 Associations API batch request failed with status {StatusCode}: {ErrorContent}",
                                response.StatusCode, errorContent);
                            return Result.Failure<Dictionary<string, List<string>>>(
                                $"HubSpot v4 Associations API batch request failed: {response.StatusCode} - {errorContent}");
                        }

                        var jsonContent = await response.Content.ReadAsStringAsync();
                        
                        // Log full response for first batch, first page to debug
                        if (i == 0 && pageNumber == 1)
                        {
                            var preview = jsonContent.Length > 3000 ? jsonContent.Substring(0, 3000) + "..." : jsonContent;
                            _logger.LogInformation("API Response (first 3000 chars): {Preview}", preview);
                            
                            // VALIDATION: Check if response indicates no associations
                            if (jsonContent.Contains("\"results\":[]") || jsonContent.Contains("\"results\": []"))
                            {
                                _logger.LogWarning("VALIDATION: API returned empty results array. " +
                                    "This means HubSpot has NO contact-company associations for the queried contacts. " +
                                    "Please verify in HubSpot UI that associations exist.");
                            }
                            
                            // Check for errors in response
                            if (jsonContent.Contains("\"errors\""))
                            {
                                _logger.LogWarning("VALIDATION: API response contains errors. Check the full response above for details.");
                            }
                        }
                        
                        var jsonDocument = JsonDocument.Parse(jsonContent);

                        // Parse results array
                        if (jsonDocument.RootElement.TryGetProperty("results", out var resultsArray))
                        {
                            foreach (var result in resultsArray.EnumerateArray())
                            {
                                ProcessAssociationResultWithTypes(result, associations);
                            }
                        }
                        else
                        {
                            _logger.LogWarning("Unexpected response format. Root element type: {ValueKind}, keys: {Keys}", 
                                jsonDocument.RootElement.ValueKind,
                                string.Join(", ", jsonDocument.RootElement.EnumerateObject().Select(p => p.Name)));
                        }

                        // Check for pagination token
                        after = null;
                        if (jsonDocument.RootElement.TryGetProperty("paging", out var paging))
                        {
                            if (paging.TryGetProperty("next", out var next))
                            {
                                if (next.TryGetProperty("after", out var afterElement))
                                {
                                    after = afterElement.GetString();
                                }
                            }
                        }

                        pageNumber++;
                    } while (!string.IsNullOrEmpty(after));
                }

                // Store detailed associations for later use
                _lastDetailedAssociations = associations;
                
                // Convert to simple dictionary format for backward compatibility
                var simpleAssociations = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
                foreach (var kvp in associations)
                {
                    simpleAssociations[kvp.Key] = kvp.Value.Select(x => x.TargetObjectId).ToList();
                }

                _logger.LogInformation("Found company associations for {Count} contacts out of {Total}", 
                    simpleAssociations.Count, contactIds.Count);
                
                return Result.Success(simpleAssociations);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error occurred while fetching company associations batch for contacts");
                return Result.Failure<Dictionary<string, List<string>>>($"Error fetching company associations batch: {ex.Message}");
            }
        }

        /// <summary>
        /// Fetches all contact associations for multiple companies using HubSpot's v4 Associations API batch/read endpoint (reverse direction).
        /// Returns a dictionary mapping company ID to list of contact IDs.
        /// </summary>
        public async Task<Result<Dictionary<string, List<string>>>> GetCompanyContactAssociationsBatchAsync(List<string> companyIds)
        {
            try
            {
                var baseUrl = _configuration["HubSpot:BaseUrl"] ?? "https://api.hubapi.com";
                var accessToken = _configuration["HubSpot:AccessToken"];

                if (string.IsNullOrEmpty(accessToken))
                {
                    return Result.Failure<Dictionary<string, List<string>>>("HubSpot access token is not configured");
                }

                if (companyIds == null || companyIds.Count == 0)
                {
                    return Result.Success(new Dictionary<string, List<string>>());
                }

                var associations = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);

                // Process in batches of 1000 (HubSpot max batch limit per docs)
                const int batchSize = 1000;
                for (int i = 0; i < companyIds.Count; i += batchSize)
                {
                    var batch = companyIds.Skip(i).Take(batchSize).ToList();
                    
                    // Build request body for batch/read (reverse: companies->contacts)
                    var inputs = new List<object>();
                    foreach (var id in batch)
                    {
                        if (long.TryParse(id, out var numericId))
                        {
                            inputs.Add(new { id = numericId });
                        }
                        else
                        {
                            inputs.Add(new { id = id });
                        }
                    }
                    
                    var requestBody = new
                    {
                        inputs = inputs.ToArray()
                    };

                    var jsonBody = JsonSerializer.Serialize(requestBody);
                    // CRITICAL FIX: Use singular object names (company/contact) not plural (companies/contacts)
                    var url = $"{baseUrl}/crm/v4/associations/company/contact/batch/read";

                    _httpClient.DefaultRequestHeaders.Clear();
                    _httpClient.DefaultRequestHeaders.Add("Authorization", $"Bearer {accessToken}");

                    _logger.LogInformation("Fetching contact associations for {Count} companies (batch {Batch}/{TotalBatches})", 
                        batch.Count, (i / batchSize) + 1, (int)Math.Ceiling((double)companyIds.Count / batchSize));

                    // Handle pagination - loop until all pages are retrieved
                    string? after = null;
                    int pageNumber = 1;
                    do
                    {
                        var requestUrl = url;
                        if (!string.IsNullOrEmpty(after))
                        {
                            requestUrl += $"?after={Uri.EscapeDataString(after)}";
                        }

                        var content = new StringContent(jsonBody, System.Text.Encoding.UTF8, "application/json");
                        var response = await _httpClient.PostAsync(requestUrl, content);

                        var isSuccess = response.IsSuccessStatusCode || response.StatusCode == System.Net.HttpStatusCode.MultiStatus;
                        
                        if (!isSuccess)
                        {
                            var errorContent = await response.Content.ReadAsStringAsync();
                            _logger.LogWarning("HubSpot v4 Associations API batch request (reverse) failed with status {StatusCode}: {ErrorContent}",
                                response.StatusCode, errorContent);
                            break; // Break pagination loop, continue with next batch
                        }

                        var jsonContent = await response.Content.ReadAsStringAsync();
                        var jsonDocument = JsonDocument.Parse(jsonContent);

                        if (jsonDocument.RootElement.TryGetProperty("results", out var resultsArray))
                        {
                            foreach (var result in resultsArray.EnumerateArray())
                            {
                                ProcessAssociationResult(result, associations);
                            }
                        }

                        // Check for pagination token
                        after = null;
                        if (jsonDocument.RootElement.TryGetProperty("paging", out var paging))
                        {
                            if (paging.TryGetProperty("next", out var next))
                            {
                                if (next.TryGetProperty("after", out var afterElement))
                                {
                                    after = afterElement.GetString();
                                }
                            }
                        }

                        pageNumber++;
                    } while (!string.IsNullOrEmpty(after));
                }

                _logger.LogInformation("Found contact associations for {Count} companies out of {Total}", 
                    associations.Count, companyIds.Count);
                
                return Result.Success(associations);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error occurred while fetching contact associations batch for companies");
                return Result.Failure<Dictionary<string, List<string>>>($"Error fetching contact associations batch: {ex.Message}");
            }
        }

        private void ProcessAssociationResultWithTypes(JsonElement result, Dictionary<string, List<AssociationDetails>> associations)
        {
            // Extract contact ID from "from" object
            string? contactId = null;
            DateTime? createdAt = null;
            DateTime? updatedAt = null;
            
            if (result.TryGetProperty("from", out var fromObj))
            {
                if (fromObj.TryGetProperty("id", out var fromId))
                {
                    contactId = fromId.ValueKind == JsonValueKind.Number 
                        ? fromId.GetInt64().ToString() 
                        : fromId.GetString();
                }
            }

            // Extract timestamps from result if available
            if (result.TryGetProperty("createdAt", out var createdAtProp))
            {
                if (DateTime.TryParse(createdAtProp.GetString(), out var created))
                {
                    createdAt = created;
                }
            }
            if (result.TryGetProperty("updatedAt", out var updatedAtProp))
            {
                if (DateTime.TryParse(updatedAtProp.GetString(), out var updated))
                {
                    updatedAt = updated;
                }
            }

            if (string.IsNullOrWhiteSpace(contactId))
            {
                _logger.LogDebug("Skipping result - no contact ID found. Result keys: {Keys}", 
                    string.Join(", ", result.EnumerateObject().Select(p => p.Name)));
                return;
            }

            // Extract company IDs and association details from "to" array
            if (result.TryGetProperty("to", out var toArray) && toArray.ValueKind == JsonValueKind.Array)
            {
                foreach (var toItem in toArray.EnumerateArray())
                {
                    var details = new AssociationDetails();

                    // Extract target object ID - v4 API uses "toObjectId" (not "id")
                    if (toItem.TryGetProperty("toObjectId", out var toObjectId))
                    {
                        details.TargetObjectId = toObjectId.ValueKind == JsonValueKind.Number 
                            ? toObjectId.GetInt64().ToString() 
                            : toObjectId.GetString();
                    }
                    // Fallback to "id" if "toObjectId" doesn't exist
                    else if (toItem.TryGetProperty("id", out var toId))
                    {
                        details.TargetObjectId = toId.ValueKind == JsonValueKind.Number 
                            ? toId.GetInt64().ToString() 
                            : toId.GetString();
                    }

                    // Extract all association labels, type IDs, and categories from associationTypes array (v4 API format)
                    if (toItem.TryGetProperty("associationTypes", out var typesArray) && typesArray.ValueKind == JsonValueKind.Array)
                    {
                        foreach (var typeItem in typesArray.EnumerateArray())
                        {
                            // Extract label
                            if (typeItem.TryGetProperty("label", out var label) && label.ValueKind == JsonValueKind.String)
                            {
                                var labelValue = label.GetString();
                                if (!string.IsNullOrWhiteSpace(labelValue))
                                {
                                    details.AllLabels.Add(labelValue);
                                    // Set primary label (first non-null label)
                                    if (string.IsNullOrWhiteSpace(details.PrimaryLabel))
                                    {
                                        details.PrimaryLabel = labelValue;
                                    }
                                }
                            }
                            
                            // Extract associationTypeId
                            if (typeItem.TryGetProperty("associationTypeId", out var typeIdProp) && typeIdProp.ValueKind == JsonValueKind.Number)
                            {
                                details.AssociationTypeId = typeIdProp.GetInt32();
                            }
                            else if (typeItem.TryGetProperty("typeId", out var typeIdAlt) && typeIdAlt.ValueKind == JsonValueKind.Number)
                            {
                                details.AssociationTypeId = typeIdAlt.GetInt32();
                            }
                            
                            // Extract category
                            if (typeItem.TryGetProperty("category", out var categoryProp) && categoryProp.ValueKind == JsonValueKind.String)
                            {
                                details.AssociationCategory = categoryProp.GetString();
                            }
                        }
                    }
                    // Fallback: check for "types" array (alternative format)
                    else if (toItem.TryGetProperty("types", out var typesArrayAlt) && typesArrayAlt.ValueKind == JsonValueKind.Array)
                    {
                        foreach (var typeItem in typesArrayAlt.EnumerateArray())
                        {
                            if (typeItem.TryGetProperty("label", out var label))
                            {
                                var labelValue = label.GetString();
                                if (!string.IsNullOrWhiteSpace(labelValue))
                                {
                                    details.AllLabels.Add(labelValue);
                                    if (string.IsNullOrWhiteSpace(details.PrimaryLabel))
                                    {
                                        details.PrimaryLabel = labelValue;
                                    }
                                }
                            }
                        }
                    }

                    // Extract source information if available
                    if (toItem.TryGetProperty("source", out var sourceProp))
                    {
                        details.Source = sourceProp.GetString();
                    }
                    if (toItem.TryGetProperty("sourceId", out var sourceIdProp))
                    {
                        details.SourceId = sourceIdProp.GetString();
                    }

                    // Extract timestamps from toItem if available (association-specific timestamps)
                    if (toItem.TryGetProperty("createdAt", out var itemCreatedAt))
                    {
                        if (DateTime.TryParse(itemCreatedAt.GetString(), out var itemCreated))
                        {
                            details.CreatedAt = itemCreated;
                        }
                    }
                    if (toItem.TryGetProperty("updatedAt", out var itemUpdatedAt))
                    {
                        if (DateTime.TryParse(itemUpdatedAt.GetString(), out var itemUpdated))
                        {
                            details.UpdatedAt = itemUpdated;
                        }
                    }

                    // Use result-level timestamps if item-level are not available
                    if (details.CreatedAt == null)
                    {
                        details.CreatedAt = createdAt;
                    }
                    if (details.UpdatedAt == null)
                    {
                        details.UpdatedAt = updatedAt;
                    }
                    
                    if (!string.IsNullOrWhiteSpace(details.TargetObjectId))
                    {
                        if (!associations.ContainsKey(contactId))
                        {
                            associations[contactId] = new List<AssociationDetails>();
                        }
                        associations[contactId].Add(details);
                        _logger.LogDebug("Found target object {TargetId} for source {SourceId} (labels: {Labels})", 
                            details.TargetObjectId, contactId, string.Join(", ", details.AllLabels));
                    }
                }
            }
        }

        private void ProcessAssociationResult(JsonElement result, Dictionary<string, List<string>> associations)
        {
            // Extract contact ID from "from" object
            string? contactId = null;
            if (result.TryGetProperty("from", out var fromObj))
            {
                if (fromObj.TryGetProperty("id", out var fromId))
                {
                    contactId = fromId.ValueKind == JsonValueKind.Number 
                        ? fromId.GetInt64().ToString() 
                        : fromId.GetString();
                }
                else if (fromObj.ValueKind == JsonValueKind.String)
                {
                    // "from" might be a direct string ID
                    contactId = fromObj.GetString();
                }
            }
            else if (result.TryGetProperty("id", out var directId))
            {
                // Response might have contact ID directly
                contactId = directId.ValueKind == JsonValueKind.Number 
                    ? directId.GetInt64().ToString() 
                    : directId.GetString();
            }

            if (string.IsNullOrWhiteSpace(contactId))
            {
                _logger.LogDebug("Skipping result - no contact ID found. Result keys: {Keys}", 
                    string.Join(", ", result.EnumerateObject().Select(p => p.Name)));
                return;
            }

            // Extract company IDs from "to" array
            var companyIds = new List<string>();
            if (result.TryGetProperty("to", out var toArray) && toArray.ValueKind == JsonValueKind.Array)
            {
                foreach (var toItem in toArray.EnumerateArray())
                {
                    string? companyId = null;
                    if (toItem.TryGetProperty("id", out var toId))
                    {
                        companyId = toId.ValueKind == JsonValueKind.Number 
                            ? toId.GetInt64().ToString() 
                            : toId.GetString();
                    }
                    else if (toItem.ValueKind == JsonValueKind.String)
                    {
                        // "to" item might be a direct string ID
                        companyId = toItem.GetString();
                    }
                    else if (toItem.ValueKind == JsonValueKind.Number)
                    {
                        companyId = toItem.GetInt64().ToString();
                    }
                    
                    if (!string.IsNullOrWhiteSpace(companyId))
                    {
                        companyIds.Add(companyId);
                    }
                }
            }

            if (companyIds.Count > 0)
            {
                associations[contactId] = companyIds;
                _logger.LogDebug("Found {Count} companies for contact {ContactId}", companyIds.Count, contactId);
            }
        }

        /// <summary>
        /// Generic method to fetch associations between any two HubSpot object types using v4 batch/read API.
        /// This method handles all object-to-object associations: Contact-Deal, Contact-Ticket, Company-Deal, etc.
        /// </summary>
        public async Task<Result<Dictionary<string, List<AssociationDetails>>>> GetObjectAssociationsBatchAsync(
            string sourceObjectType, 
            string targetObjectType, 
            List<string> sourceObjectIds)
        {
            try
            {
                var baseUrl = _configuration["HubSpot:BaseUrl"] ?? "https://api.hubapi.com";
                var accessToken = _configuration["HubSpot:AccessToken"];

                if (string.IsNullOrEmpty(accessToken))
                {
                    return Result.Failure<Dictionary<string, List<AssociationDetails>>>("HubSpot access token is not configured");
                }

                if (sourceObjectIds == null || sourceObjectIds.Count == 0)
                {
                    return Result.Success(new Dictionary<string, List<AssociationDetails>>());
                }

                // Normalize object types to singular form (required by HubSpot v4 API)
                var normalizedSourceType = NormalizeObjectType(sourceObjectType);
                var normalizedTargetType = NormalizeObjectType(targetObjectType);

                var associations = new Dictionary<string, List<AssociationDetails>>(StringComparer.OrdinalIgnoreCase);

                // Process in batches of 1000
                const int batchSize = 1000;
                for (int i = 0; i < sourceObjectIds.Count; i += batchSize)
                {
                    var batch = sourceObjectIds.Skip(i).Take(batchSize).ToList();
                    
                    // Build request body
                    var inputs = new List<object>();
                    foreach (var id in batch)
                    {
                        if (long.TryParse(id, out var numericId))
                        {
                            inputs.Add(new { id = numericId });
                        }
                        else
                        {
                            inputs.Add(new { id = id });
                        }
                    }

                    var requestBody = new { inputs = inputs };
                    var jsonBody = JsonSerializer.Serialize(requestBody);

                    // Build URL: /crm/v4/associations/{sourceType}/{targetType}/batch/read
                    var url = $"{baseUrl}/crm/v4/associations/{normalizedSourceType}/{normalizedTargetType}/batch/read";

                    _httpClient.DefaultRequestHeaders.Clear();
                    _httpClient.DefaultRequestHeaders.Add("Authorization", $"Bearer {accessToken}");

                    _logger.LogInformation("Fetching {TargetType} associations for {Count} {SourceType} objects (batch {Batch}/{TotalBatches})", 
                        normalizedTargetType, batch.Count, normalizedSourceType, (i / batchSize) + 1, (int)Math.Ceiling((double)sourceObjectIds.Count / batchSize));

                    // Handle pagination
                    string? after = null;
                    int pageNumber = 1;
                    do
                    {
                        var requestUrl = url;
                        if (!string.IsNullOrEmpty(after))
                        {
                            requestUrl += $"?after={Uri.EscapeDataString(after)}";
                        }

                        var content = new StringContent(jsonBody, System.Text.Encoding.UTF8, "application/json");
                        var response = await _httpClient.PostAsync(requestUrl, content);

                        var isSuccess = response.IsSuccessStatusCode || response.StatusCode == System.Net.HttpStatusCode.MultiStatus;
                        
                        if (!isSuccess)
                        {
                            var errorContent = await response.Content.ReadAsStringAsync();
                            _logger.LogError("HubSpot v4 Associations API batch request failed for {SourceType}/{TargetType} with status {StatusCode}: {ErrorContent}",
                                normalizedSourceType, normalizedTargetType, response.StatusCode, errorContent);
                            return Result.Failure<Dictionary<string, List<AssociationDetails>>>(
                                $"HubSpot v4 Associations API batch request failed: {response.StatusCode} - {errorContent}");
                        }

                        var jsonContent = await response.Content.ReadAsStringAsync();
                        var jsonDocument = JsonDocument.Parse(jsonContent);

                        // Parse results array
                        if (jsonDocument.RootElement.TryGetProperty("results", out var resultsArray))
                        {
                            foreach (var result in resultsArray.EnumerateArray())
                            {
                                ProcessAssociationResultWithTypes(result, associations);
                            }
                        }

                        // Check for pagination token
                        after = null;
                        if (jsonDocument.RootElement.TryGetProperty("paging", out var paging))
                        {
                            if (paging.TryGetProperty("next", out var next))
                            {
                                if (next.TryGetProperty("after", out var afterElement))
                                {
                                    after = afterElement.GetString();
                                }
                            }
                        }

                        pageNumber++;
                    } while (!string.IsNullOrEmpty(after));
                }

                _logger.LogInformation("Found {TargetType} associations for {Count} {SourceType} objects out of {Total}", 
                    normalizedTargetType, associations.Count, normalizedSourceType, sourceObjectIds.Count);
                
                return Result.Success(associations);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error occurred while fetching {TargetType} associations batch for {SourceType} objects", 
                    targetObjectType, sourceObjectType);
                return Result.Failure<Dictionary<string, List<AssociationDetails>>>(
                    $"Error fetching associations batch: {ex.Message}");
            }
        }

        /// <summary>
        /// Normalizes HubSpot object type names to singular form as required by v4 API.
        /// Examples: "contacts" -> "contact", "companies" -> "company", "deals" -> "deal"
        /// </summary>
        private static string NormalizeObjectType(string objectType)
        {
            if (string.IsNullOrWhiteSpace(objectType))
            {
                return objectType;
            }

            // Remove trailing 's' if present (plural -> singular)
            if (objectType.EndsWith("s", StringComparison.OrdinalIgnoreCase) && objectType.Length > 1)
            {
                return objectType.Substring(0, objectType.Length - 1);
            }

            return objectType.ToLowerInvariant();
        }

        /// <summary>
        /// Fetches property history for a specific object using HubSpot's propertiesWithHistory parameter.
        /// This uses the CRM v3 API endpoint: /crm/v3/objects/{objectType}/{objectId}?propertiesWithHistory={propertyName}
        /// </summary>
        public async Task<Result<JsonElement>> GetObjectPropertyHistoryAsync(string objectType, string objectId, string propertyName)
        {
            try
            {
                var baseUrl = _configuration["HubSpot:BaseUrl"] ?? "https://api.hubapi.com";
                var accessToken = _configuration["HubSpot:AccessToken"];

                if (string.IsNullOrEmpty(accessToken))
                {
                    return Result.Failure<JsonElement>("HubSpot access token is not configured");
                }

                if (string.IsNullOrWhiteSpace(objectType) || string.IsNullOrWhiteSpace(objectId) || string.IsNullOrWhiteSpace(propertyName))
                {
                    return Result.Failure<JsonElement>("ObjectType, ObjectId, and PropertyName are required");
                }

                // Normalize object type to plural form for v3 API (deals, tickets, companies)
                var lowerObjectType = objectType.ToLowerInvariant();
                var normalizedObjectType = lowerObjectType switch
                {
                    "company" => "companies",
                    "companies" => "companies",
                    var t when t.EndsWith("s") => t,
                    _ => lowerObjectType + "s"
                };

                var url = $"{baseUrl}/crm/v3/objects/{normalizedObjectType}/{objectId}?propertiesWithHistory={Uri.EscapeDataString(propertyName)}";

                _httpClient.DefaultRequestHeaders.Clear();
                _httpClient.DefaultRequestHeaders.Add("Authorization", $"Bearer {accessToken}");

                _logger.LogDebug("Fetching property history for {ObjectType}/{ObjectId}, property: {PropertyName}", 
                    normalizedObjectType, objectId, propertyName);

                var response = await _httpClient.GetAsync(url);

                if (!response.IsSuccessStatusCode)
                {
                    var errorContent = await response.Content.ReadAsStringAsync();
                    _logger.LogWarning("HubSpot property history API request failed for {ObjectType}/{ObjectId}/{PropertyName} with status {StatusCode}: {ErrorContent}",
                        normalizedObjectType, objectId, propertyName, response.StatusCode, errorContent);
                    return Result.Failure<JsonElement>(
                        $"HubSpot property history API request failed: {response.StatusCode} - {errorContent}");
                }

                var jsonContent = await response.Content.ReadAsStringAsync();
                var jsonDocument = JsonDocument.Parse(jsonContent);

                return Result.Success(jsonDocument.RootElement.Clone());
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error occurred while fetching property history for {ObjectType}/{ObjectId}/{PropertyName}", 
                    objectType, objectId, propertyName);
                return Result.Failure<JsonElement>($"Error fetching property history: {ex.Message}");
            }
        }

    }
}


