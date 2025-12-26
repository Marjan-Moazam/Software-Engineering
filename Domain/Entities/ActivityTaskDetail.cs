namespace ETL.HubspotService.Domain.Entities
{
    public class ActivityTaskDetail : ActivityDetail
    {
        public string? Priority { get; private set; }
        public string? Status { get; private set; }
        public string? CommunicationBody { get; private set; }
        public DateTime? CreatedAt { get; private set; }
        public string? IsOverdue { get; private set; }
        public DateTime? LastModifiedAt { get; private set; }
        public string? TaskType { get; private set; }
        public string? UpdatedByUserId { get; private set; }

        private ActivityTaskDetail()
        {
        }

        private ActivityTaskDetail(
            string? priority,
            string? status,
            string? communicationBody,
            DateTime? createdAt,
            string? isOverdue,
            DateTime? lastModifiedAt,
            string? taskType,
            string? updatedByUserId,
            string? rawPropertiesJson) : base(rawPropertiesJson)
        {
            Priority = priority;
            Status = status;
            CommunicationBody = communicationBody;
            CreatedAt = createdAt;
            IsOverdue = isOverdue;
            LastModifiedAt = lastModifiedAt;
            TaskType = taskType;
            UpdatedByUserId = updatedByUserId;
        }

        public static ActivityTaskDetail Create(
            string? priority,
            string? status,
            string? communicationBody,
            DateTime? createdAt,
            string? isOverdue,
            DateTime? lastModifiedAt,
            string? taskType,
            string? updatedByUserId,
            string? rawPropertiesJson)
        {
            return new ActivityTaskDetail(
                priority,
                status,
                communicationBody,
                createdAt,
                isOverdue,
                lastModifiedAt,
                taskType,
                updatedByUserId,
                rawPropertiesJson);
        }

        public void UpdateFrom(ActivityTaskDetail other)
        {
            base.UpdateFrom(other);
            Priority = other.Priority ?? Priority;
            Status = other.Status ?? Status;
            CommunicationBody = other.CommunicationBody ?? CommunicationBody;
            CreatedAt = other.CreatedAt ?? CreatedAt;
            IsOverdue = other.IsOverdue ?? IsOverdue;
            LastModifiedAt = other.LastModifiedAt ?? LastModifiedAt;
            TaskType = other.TaskType ?? TaskType;
            UpdatedByUserId = other.UpdatedByUserId ?? UpdatedByUserId;
        }
    }
}



