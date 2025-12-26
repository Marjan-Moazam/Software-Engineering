namespace ETL.HubspotService.Domain.Entities
{
    public class ActivityEmailDetail : ActivityDetail
    {
        public string? Status { get; private set; }
        public string? TextBody { get; private set; }
        public string? HtmlBody { get; private set; }
        public DateTime? CreatedDate { get; private set; }
        public string? CreatedByUserId { get; private set; }
        public string? EmailClickRate { get; private set; }
        public string? EmailDirection { get; private set; }
        public string? EmailOpenRate { get; private set; }
        public string? EmailReplyRate { get; private set; }
        public DateTime? LastModifiedDate { get; private set; }
        public string? NumberOfEmailClicks { get; private set; }
        public string? NumberOfEmailOpens { get; private set; }
        public string? UpdatedByUserId { get; private set; }

        private ActivityEmailDetail()
        {
        }

        private ActivityEmailDetail(
            string? status,
            string? textBody,
            string? htmlBody,
            DateTime? createdDate,
            string? createdByUserId,
            string? emailClickRate,
            string? emailDirection,
            string? emailOpenRate,
            string? emailReplyRate,
            DateTime? lastModifiedDate,
            string? numberOfEmailClicks,
            string? numberOfEmailOpens,
            string? updatedByUserId,
            string? rawPropertiesJson) : base(rawPropertiesJson)
        {
            Status = status;
            TextBody = textBody;
            HtmlBody = htmlBody;
            CreatedDate = createdDate;
            CreatedByUserId = createdByUserId;
            EmailClickRate = emailClickRate;
            EmailDirection = emailDirection;
            EmailOpenRate = emailOpenRate;
            EmailReplyRate = emailReplyRate;
            LastModifiedDate = lastModifiedDate;
            NumberOfEmailClicks = numberOfEmailClicks;
            NumberOfEmailOpens = numberOfEmailOpens;
            UpdatedByUserId = updatedByUserId;
        }

        public static ActivityEmailDetail Create(
            string? status,
            string? textBody,
            string? htmlBody,
            DateTime? createdDate,
            string? createdByUserId,
            string? emailClickRate,
            string? emailDirection,
            string? emailOpenRate,
            string? emailReplyRate,
            DateTime? lastModifiedDate,
            string? numberOfEmailClicks,
            string? numberOfEmailOpens,
            string? updatedByUserId,
            string? rawPropertiesJson)
        {
            return new ActivityEmailDetail(
                status,
                textBody,
                htmlBody,
                createdDate,
                createdByUserId,
                emailClickRate,
                emailDirection,
                emailOpenRate,
                emailReplyRate,
                lastModifiedDate,
                numberOfEmailClicks,
                numberOfEmailOpens,
                updatedByUserId,
                rawPropertiesJson);
        }

        public void UpdateFrom(ActivityEmailDetail other)
        {
            base.UpdateFrom(other);
            Status = other.Status ?? Status;
            TextBody = other.TextBody ?? TextBody;
            HtmlBody = other.HtmlBody ?? HtmlBody;
            CreatedDate = other.CreatedDate ?? CreatedDate;
            CreatedByUserId = other.CreatedByUserId ?? CreatedByUserId;
            EmailClickRate = other.EmailClickRate ?? EmailClickRate;
            EmailDirection = other.EmailDirection ?? EmailDirection;
            EmailOpenRate = other.EmailOpenRate ?? EmailOpenRate;
            EmailReplyRate = other.EmailReplyRate ?? EmailReplyRate;
            LastModifiedDate = other.LastModifiedDate ?? LastModifiedDate;
            NumberOfEmailClicks = other.NumberOfEmailClicks ?? NumberOfEmailClicks;
            NumberOfEmailOpens = other.NumberOfEmailOpens ?? NumberOfEmailOpens;
            UpdatedByUserId = other.UpdatedByUserId ?? UpdatedByUserId;
        }
    }
}



