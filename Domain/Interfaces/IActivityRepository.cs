using ETL.HubspotService.Domain.Entities;

namespace ETL.HubspotService.Domain.Interfaces
{
    public interface IActivityRepository : IRepository<Activity>
    {
        Task<Activity?> GetByHubSpotIdWithDetailsAsync(string hubSpotId);
    }
}



