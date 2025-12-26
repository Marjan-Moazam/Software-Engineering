namespace ETL.HubspotService.Domain.Entities
{
    public abstract class ActivityDetail : Entity<long>
    {
        public long ActivityId { get; protected set; }
        public Activity Activity { get; protected set; } = default!;
        public string RawPropertiesJson { get; protected set; } = "{}";
        public DateTime ETLDate { get; protected set; }

        protected ActivityDetail()
        {
        }

        protected ActivityDetail(string? rawPropertiesJson)
        {
            RawPropertiesJson = string.IsNullOrWhiteSpace(rawPropertiesJson)
                ? "{}"
                : rawPropertiesJson;
            ETLDate = DateTime.UtcNow;
        }

        internal void AttachActivity(Activity activity)
        {
            Activity = activity;
            if (activity.Id != 0)
            {
                ActivityId = activity.Id;
            }
        }

        public virtual void UpdateFrom(ActivityDetail other)
        {
            RawPropertiesJson = other.RawPropertiesJson ?? RawPropertiesJson;
            ETLDate = DateTime.UtcNow;
        }
    }
}


