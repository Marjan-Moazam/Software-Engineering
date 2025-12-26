namespace ETL.HubspotService.Domain.Entities
{
    public class ActivityCallDetail : ActivityDetail
    {
        public string? Direction { get; private set; }
        public string? Status { get; private set; }
        public string? CallTitle { get; private set; }
        public string? CallDirection { get; private set; }
        public DateTime? CreatedDate { get; private set; }
        public string? CreatedByUserId { get; private set; }
        public DateTime? LastModifiedDate { get; private set; }

        private ActivityCallDetail()
        {
        }

        private ActivityCallDetail(
            string? direction,
            string? status,
            string? callTitle,
            string? callDirection,
            DateTime? createdDate,
            string? createdByUserId,
            DateTime? lastModifiedDate,
            string? rawPropertiesJson)
            : base(rawPropertiesJson)
        {
            Direction = direction;
            Status = status;
            CallTitle = callTitle;
            CallDirection = callDirection;
            CreatedDate = createdDate;
            CreatedByUserId = createdByUserId;
            LastModifiedDate = lastModifiedDate;
        }

        public static ActivityCallDetail Create(
            string? direction,
            string? status,
            string? callTitle,
            string? callDirection,
            DateTime? createdDate,
            string? createdByUserId,
            DateTime? lastModifiedDate,
            string? rawPropertiesJson)
        {
            return new ActivityCallDetail(
                direction,
                status,
                callTitle,
                callDirection,
                createdDate,
                createdByUserId,
                lastModifiedDate,
                rawPropertiesJson);
        }

        public void UpdateFrom(ActivityCallDetail other)
        {
            base.UpdateFrom(other);
            Direction = other.Direction ?? Direction;
            Status = other.Status ?? Status;
            CallTitle = other.CallTitle ?? CallTitle;
            CallDirection = other.CallDirection ?? CallDirection;
            CreatedDate = other.CreatedDate ?? CreatedDate;
            CreatedByUserId = other.CreatedByUserId ?? CreatedByUserId;
            LastModifiedDate = other.LastModifiedDate ?? LastModifiedDate;
        }
    }
}



