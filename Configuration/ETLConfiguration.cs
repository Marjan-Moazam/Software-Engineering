namespace ETL.HubspotService.Configuration
{
    public class ETLConfiguration
    {
        public int BatchSize { get; set; } = 100;
        public int MaxRetries { get; set; } = 3;
        public int RetryDelaySeconds { get; set; } = 30;
        public bool EnableParallelProcessing { get; set; } = true;
        public int MaxConcurrency { get; set; } = 5;
    }
}


