using ETL.HubspotService.Domain.Interfaces;
using ETL.HubspotService.Infrastructure.Data;
using ETL.HubspotService.Infrastructure.Services;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Http;

namespace ETL.HubspotService
{
    public static class ETLServiceExtensions
    {
        public static IServiceCollection AddETLServices(this IServiceCollection services, IConfiguration configuration)
        {
            // Database
            services.AddDbContext<ETLHubspotDbContext>(options =>
                options.UseSqlServer(configuration.GetConnectionString("DefaultConnection")));

            // Repositories
            services.AddScoped<IUnitOfWork, UnitOfWork>();

            // Services
            services.AddHttpClient<IHubSpotApiService, HubSpotApiService>();
            services.AddScoped<IETLService, ETLService>();

            // Caching
            services.AddMemoryCache();

            return services;
        }
    }
}


