namespace ETL.HubspotService.Domain.Entities
{
    public class ActivityNoteDetail : ActivityDetail
    {
        public DateTime? CreatedDate { get; private set; }
        public string? CreatedByUserId { get; private set; }
        public DateTime? LastModifiedDate { get; private set; }

        private ActivityNoteDetail()
        {
        }

        private ActivityNoteDetail(
            DateTime? createdDate,
            string? createdByUserId,
            DateTime? lastModifiedDate,
            string? rawPropertiesJson)
            : base(rawPropertiesJson)
        {
            CreatedDate = createdDate;
            CreatedByUserId = createdByUserId;
            LastModifiedDate = lastModifiedDate;
        }

        public static ActivityNoteDetail Create(
            DateTime? createdDate,
            string? createdByUserId,
            DateTime? lastModifiedDate,
            string? rawPropertiesJson)
        {
            return new ActivityNoteDetail(
                createdDate,
                createdByUserId,
                lastModifiedDate,
                rawPropertiesJson);
        }

        public void UpdateFrom(ActivityNoteDetail other)
        {
            base.UpdateFrom(other);
            CreatedDate = other.CreatedDate ?? CreatedDate;
            CreatedByUserId = other.CreatedByUserId ?? CreatedByUserId;
            LastModifiedDate = other.LastModifiedDate ?? LastModifiedDate;
        }
    }
}



