using ETL.HubspotService.Domain.Entities;
using ETL.HubspotService.Domain.Interfaces;
using Microsoft.EntityFrameworkCore;
using System.Linq;
using System.Linq.Expressions;

namespace ETL.HubspotService.Infrastructure.Data
{
    public class ActivityRepository : Repository<Activity>, IActivityRepository
    {
        public ActivityRepository(ETLHubspotDbContext context) : base(context)
        {
        }

        public Task<Activity?> GetByHubSpotIdWithDetailsAsync(string hubSpotId)
        {
            return IncludeDetails()
                .FirstOrDefaultAsync(a => a.HubSpotId == hubSpotId);
        }

        public override Task<Activity?> FirstOrDefaultAsync(Expression<Func<Activity, bool>> predicate)
        {
            return IncludeDetails().FirstOrDefaultAsync(predicate);
        }

        private IQueryable<Activity> IncludeDetails()
        {
            return _context.Activities
                .Include(a => a.CallDetail)
                .Include(a => a.EmailDetail)
                .Include(a => a.MeetingDetail)
                .Include(a => a.NoteDetail)
                .Include(a => a.SmsDetail)
                .Include(a => a.TaskDetail);
        }
    }
}


