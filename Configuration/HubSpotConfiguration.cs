namespace ETL.HubspotService.Configuration
{
    public class HubSpotConfiguration
    {
        public string BaseUrl { get; set; } = "https://api.hubapi.com";
        public string AccessToken { get; set; } = string.Empty;
        public int BatchSize { get; set; } = 100;
        public int MaxRetries { get; set; } = 3;
        public int RetryDelaySeconds { get; set; } = 30;
    }
}


