namespace ETL.HubspotService.Domain.Entities
{
    public class ActivitySmsDetail : ActivityDetail
    {
        public string? Direction { get; private set; }
        public string? Status { get; private set; }
        public string? ChannelAccountName { get; private set; }
        public string? ChannelName { get; private set; }
        public string? MessageBody { get; private set; }
        public string? ActivityAssignedTo { get; private set; }
        public DateTime? ActivityDate { get; private set; }
        public string? ChannelType { get; private set; }
        public string? CommunicationBody { get; private set; }
        public DateTime? ConversationFirstMessageTimestamp { get; private set; }
        public string? CreatedByUserId { get; private set; }
        public string? HubSpotTeam { get; private set; }
        public string? LoggedFrom { get; private set; }
        public DateTime? ObjectCreatedDateTime { get; private set; }
        public DateTime? ObjectLastModifiedDateTime { get; private set; }
        public DateTime? OwnerAssignedDate { get; private set; }
        public string? RecordSource { get; private set; }
        public string? RecordSourceDetail1 { get; private set; }
        public string? UpdatedByUserId { get; private set; }

        private ActivitySmsDetail()
        {
        }

        private ActivitySmsDetail(
            string? direction,
            string? status,
            string? channelAccountName,
            string? channelName,
            string? messageBody,
            string? activityAssignedTo,
            DateTime? activityDate,
            string? channelType,
            string? communicationBody,
            DateTime? conversationFirstMessageTimestamp,
            string? createdByUserId,
            string? hubSpotTeam,
            string? loggedFrom,
            DateTime? objectCreatedDateTime,
            DateTime? objectLastModifiedDateTime,
            DateTime? ownerAssignedDate,
            string? recordSource,
            string? recordSourceDetail1,
            string? updatedByUserId,
            string? rawPropertiesJson) : base(rawPropertiesJson)
        {
            Direction = direction;
            Status = status;
            ChannelAccountName = channelAccountName;
            ChannelName = channelName;
            MessageBody = messageBody;
            ActivityAssignedTo = activityAssignedTo;
            ActivityDate = activityDate;
            ChannelType = channelType;
            CommunicationBody = communicationBody;
            ConversationFirstMessageTimestamp = conversationFirstMessageTimestamp;
            CreatedByUserId = createdByUserId;
            HubSpotTeam = hubSpotTeam;
            LoggedFrom = loggedFrom;
            ObjectCreatedDateTime = objectCreatedDateTime;
            ObjectLastModifiedDateTime = objectLastModifiedDateTime;
            OwnerAssignedDate = ownerAssignedDate;
            RecordSource = recordSource;
            RecordSourceDetail1 = recordSourceDetail1;
            UpdatedByUserId = updatedByUserId;
        }

        public static ActivitySmsDetail Create(
            string? direction,
            string? status,
            string? channelAccountName,
            string? channelName,
            string? messageBody,
            string? activityAssignedTo,
            DateTime? activityDate,
            string? channelType,
            string? communicationBody,
            DateTime? conversationFirstMessageTimestamp,
            string? createdByUserId,
            string? hubSpotTeam,
            string? loggedFrom,
            DateTime? objectCreatedDateTime,
            DateTime? objectLastModifiedDateTime,
            DateTime? ownerAssignedDate,
            string? recordSource,
            string? recordSourceDetail1,
            string? updatedByUserId,
            string? rawPropertiesJson)
        {
            return new ActivitySmsDetail(
                direction,
                status,
                channelAccountName,
                channelName,
                messageBody,
                activityAssignedTo,
                activityDate,
                channelType,
                communicationBody,
                conversationFirstMessageTimestamp,
                createdByUserId,
                hubSpotTeam,
                loggedFrom,
                objectCreatedDateTime,
                objectLastModifiedDateTime,
                ownerAssignedDate,
                recordSource,
                recordSourceDetail1,
                updatedByUserId,
                rawPropertiesJson);
        }

        public void UpdateFrom(ActivitySmsDetail other)
        {
            base.UpdateFrom(other);
            Direction = other.Direction ?? Direction;
            Status = other.Status ?? Status;
            ChannelAccountName = other.ChannelAccountName ?? ChannelAccountName;
            ChannelName = other.ChannelName ?? ChannelName;
            MessageBody = other.MessageBody ?? MessageBody;
            ActivityAssignedTo = other.ActivityAssignedTo ?? ActivityAssignedTo;
            ActivityDate = other.ActivityDate ?? ActivityDate;
            ChannelType = other.ChannelType ?? ChannelType;
            CommunicationBody = other.CommunicationBody ?? CommunicationBody;
            ConversationFirstMessageTimestamp = other.ConversationFirstMessageTimestamp ?? ConversationFirstMessageTimestamp;
            CreatedByUserId = other.CreatedByUserId ?? CreatedByUserId;
            HubSpotTeam = other.HubSpotTeam ?? HubSpotTeam;
            LoggedFrom = other.LoggedFrom ?? LoggedFrom;
            ObjectCreatedDateTime = other.ObjectCreatedDateTime ?? ObjectCreatedDateTime;
            ObjectLastModifiedDateTime = other.ObjectLastModifiedDateTime ?? ObjectLastModifiedDateTime;
            OwnerAssignedDate = other.OwnerAssignedDate ?? OwnerAssignedDate;
            RecordSource = other.RecordSource ?? RecordSource;
            RecordSourceDetail1 = other.RecordSourceDetail1 ?? RecordSourceDetail1;
            UpdatedByUserId = other.UpdatedByUserId ?? UpdatedByUserId;
        }
    }
}



