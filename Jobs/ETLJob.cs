using ETL.HubspotService.Infrastructure.Services;
using Quartz;

namespace ETL.HubspotService.Jobs
{
    [DisallowConcurrentExecution]
    public class ETLJob : IJob
    {
        private readonly IETLService _etlService;
        private readonly ILogger<ETLJob> _logger;

        public ETLJob(IETLService etlService, ILogger<ETLJob> logger)
        {
            _etlService = etlService;
            _logger = logger;
        }

        public async Task Execute(IJobExecutionContext context)
        {
            _logger.LogInformation("Starting daily ETL job at {Time}", DateTime.UtcNow);

            try
            {
                var result = await _etlService.RunFullETLAsync();

                if (result.IsSuccess)
                {
                    _logger.LogInformation("Daily ETL job completed successfully at {Time}", DateTime.UtcNow);
                }
                else
                {
                    _logger.LogError("Daily ETL job failed: {Error}", result.Error);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error occurred during daily ETL job execution");
            }
        }
    }
}


