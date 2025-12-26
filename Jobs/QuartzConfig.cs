using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Quartz;

namespace ETL.HubspotService.Jobs
{
    public static class QuartzConfig
    {
        public static void AddQuartzJobs(this IServiceCollection services, IConfiguration configuration)
        {
            services.AddQuartz(q =>
            {
                // Configure the ETL job to run daily at 09:00 CET
                q.AddJob<ETLJob>(opts => opts.WithIdentity("ETLJob"));
                q.AddTrigger(opts => opts
                    .ForJob("ETLJob")
                    .WithIdentity("ETLJob-trigger")
                    .WithCronSchedule("0 0 9 * * ?", x => 
                        x.InTimeZone(TimeZoneInfo.FindSystemTimeZoneById("Central European Standard Time")))
                    .WithDescription("Daily ETL job trigger at 09:00 CET"));
            });

            services.AddQuartzHostedService(q =>
            {
                q.WaitForJobsToComplete = true;
            });
        }
    }
}


