namespace ETL.HubspotService.Domain.Entities
{
    public class ActivityMeetingDetail : ActivityDetail
    {
        public DateTime? StartTime { get; private set; }
        public DateTime? EndTime { get; private set; }
        public DateTime? ContactFirstOutreachDate { get; private set; }
        public DateTime? CreatedDate { get; private set; }
        public string? CreatedByUserId { get; private set; }
        public string? HubSpotTeam { get; private set; }
        public string? HubSpotAttendeeOwnerIds { get; private set; }
        public DateTime? LastModifiedDate { get; private set; }
        public string? LocationType { get; private set; }
        public string? MeetingLocation { get; private set; }
        public string? MeetingName { get; private set; }
        public string? MeetingSource { get; private set; }
        public string? TimeToBookFromFirstContact { get; private set; }

        private ActivityMeetingDetail()
        {
        }

        private ActivityMeetingDetail(
            DateTime? startTime,
            DateTime? endTime,
            DateTime? contactFirstOutreachDate,
            DateTime? createdDate,
            string? createdByUserId,
            string? hubSpotTeam,
            string? hubSpotAttendeeOwnerIds,
            DateTime? lastModifiedDate,
            string? locationType,
            string? meetingLocation,
            string? meetingName,
            string? meetingSource,
            string? timeToBookFromFirstContact,
            string? rawPropertiesJson) : base(rawPropertiesJson)
        {
            StartTime = startTime;
            EndTime = endTime;
            ContactFirstOutreachDate = contactFirstOutreachDate;
            CreatedDate = createdDate;
            CreatedByUserId = createdByUserId;
            HubSpotTeam = hubSpotTeam;
            HubSpotAttendeeOwnerIds = hubSpotAttendeeOwnerIds;
            LastModifiedDate = lastModifiedDate;
            LocationType = locationType;
            MeetingLocation = meetingLocation;
            MeetingName = meetingName;
            MeetingSource = meetingSource;
            TimeToBookFromFirstContact = timeToBookFromFirstContact;
        }

        public static ActivityMeetingDetail Create(
            DateTime? startTime,
            DateTime? endTime,
            DateTime? contactFirstOutreachDate,
            DateTime? createdDate,
            string? createdByUserId,
            string? hubSpotTeam,
            string? hubSpotAttendeeOwnerIds,
            DateTime? lastModifiedDate,
            string? locationType,
            string? meetingLocation,
            string? meetingName,
            string? meetingSource,
            string? timeToBookFromFirstContact,
            string? rawPropertiesJson)
        {
            return new ActivityMeetingDetail(
                startTime,
                endTime,
                contactFirstOutreachDate,
                createdDate,
                createdByUserId,
                hubSpotTeam,
                hubSpotAttendeeOwnerIds,
                lastModifiedDate,
                locationType,
                meetingLocation,
                meetingName,
                meetingSource,
                timeToBookFromFirstContact,
                rawPropertiesJson);
        }

        public void UpdateFrom(ActivityMeetingDetail other)
        {
            base.UpdateFrom(other);
            StartTime = other.StartTime ?? StartTime;
            EndTime = other.EndTime ?? EndTime;
            ContactFirstOutreachDate = other.ContactFirstOutreachDate ?? ContactFirstOutreachDate;
            CreatedDate = other.CreatedDate ?? CreatedDate;
            CreatedByUserId = other.CreatedByUserId ?? CreatedByUserId;
            HubSpotTeam = other.HubSpotTeam ?? HubSpotTeam;
            HubSpotAttendeeOwnerIds = other.HubSpotAttendeeOwnerIds ?? HubSpotAttendeeOwnerIds;
            LastModifiedDate = other.LastModifiedDate ?? LastModifiedDate;
            LocationType = other.LocationType ?? LocationType;
            MeetingLocation = other.MeetingLocation ?? MeetingLocation;
            MeetingName = other.MeetingName ?? MeetingName;
            MeetingSource = other.MeetingSource ?? MeetingSource;
            TimeToBookFromFirstContact = other.TimeToBookFromFirstContact ?? TimeToBookFromFirstContact;
        }
    }
}



