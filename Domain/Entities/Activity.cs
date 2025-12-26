using CSharpFunctionalExtensions;
using System.Text.Json;
using System.Collections.Generic;

namespace ETL.HubspotService.Domain.Entities
{
    public class Activity : Entity<long>
    {
        public string HubSpotId { get; internal set; } = default!;
        public string? RecordId { get; internal set; }
        public string? ActivityType { get; internal set; }
        public string? Subject { get; internal set; }
        public string? Body { get; internal set; }
        public string? ActivityOwner { get; internal set; }
        public string? SourceObjectType { get; internal set; }
        public string? SourceObjectId { get; internal set; }
        public string? SourceObjectName { get; internal set; }
        public string? SourceObjectEmail { get; internal set; }
        public DateTime? ActivityDate { get; internal set; }
        public string? Status { get; internal set; } // "upcoming", "due", "overdue", or "completed"
        public bool IsUpcoming => Status == "upcoming";
        public bool IsOverdue => Status == "overdue";
        public bool IsCompleted => Status == "completed";
        public bool IsDue => Status == "due";
        public DateTime ETLDate { get; internal set; }
        public ActivityCallDetail? CallDetail { get; private set; }
        public ActivityEmailDetail? EmailDetail { get; private set; }
        public ActivityMeetingDetail? MeetingDetail { get; private set; }
        public ActivityNoteDetail? NoteDetail { get; private set; }
        public ActivitySmsDetail? SmsDetail { get; private set; }
        public ActivityTaskDetail? TaskDetail { get; private set; }

        private Activity() { }

        public Activity(string hubSpotId, string? recordId, string? activityType, string? subject, string? body,
            string? activityOwner, string? sourceObjectType, string? sourceObjectId, DateTime? activityDate, string? status = null)
        {
            HubSpotId = hubSpotId;
            RecordId = recordId;
            ActivityType = activityType;
            Subject = subject;
            Body = body;
            ActivityOwner = activityOwner;
            SourceObjectType = sourceObjectType;
            SourceObjectId = sourceObjectId;
            ActivityDate = activityDate;
            Status = status;
            ETLDate = DateTime.UtcNow;
        }

        public static Result<Activity> CreateFromHubSpotData(JsonElement hubspotData, string activityType)
        {
            try
            {
                var hubSpotId = HubSpotEntityHelper.GetHubSpotId(hubspotData);
                if (string.IsNullOrEmpty(hubSpotId))
                {
                    return Result.Failure<Activity>("Missing or invalid HubSpotId");
                }

                var properties = hubspotData.GetProperty("properties");

                string? subject = null;
                string? body = null;
                DateTime? activityDate = null;
                string? ownerId = null;
                var rawProperties = properties.GetRawText();

                switch (activityType)
                {
                    case "CALL":
                        subject = HubSpotEntityHelper.GetStringProperty(properties, "hs_call_title");
                        body = HubSpotEntityHelper.StripHtml(
                            HubSpotEntityHelper.GetStringProperty(properties, "hs_call_body"));
                        activityDate = HubSpotEntityHelper.GetDateTimeProperty(properties, "hs_timestamp");
                        ownerId = HubSpotEntityHelper.GetStringProperty(properties, "hubspot_owner_id");
                        break;
                    case "EMAIL":
                        subject = HubSpotEntityHelper.GetStringProperty(properties, "hs_email_subject");
                        body = HubSpotEntityHelper.StripHtml(
                            HubSpotEntityHelper.GetStringProperty(properties, "hs_email_text") ??
                            HubSpotEntityHelper.GetStringProperty(properties, "hs_email_html"));
                        activityDate = HubSpotEntityHelper.GetDateTimeProperty(properties, "hs_timestamp");
                        ownerId = HubSpotEntityHelper.GetStringProperty(properties, "hubspot_owner_id");
                        break;
                    case "MEETING":
                        subject = HubSpotEntityHelper.GetStringProperty(properties, "hs_meeting_title");
                        body = HubSpotEntityHelper.StripHtml(
                            HubSpotEntityHelper.GetStringProperty(properties, "hs_meeting_body"));
                        activityDate = HubSpotEntityHelper.GetDateTimeProperty(properties, "hs_meeting_start_time") ??
                                       HubSpotEntityHelper.GetDateTimeProperty(properties, "hs_timestamp");
                        ownerId = HubSpotEntityHelper.GetStringProperty(properties, "hubspot_owner_id");
                        break;
                    case "TASK":
                        subject = HubSpotEntityHelper.GetStringProperty(properties, "hs_task_subject");
                        body = HubSpotEntityHelper.StripHtml(
                            HubSpotEntityHelper.GetStringProperty(properties, "hs_task_body"));
                        activityDate = HubSpotEntityHelper.GetDateTimeProperty(properties, "hs_timestamp");
                        ownerId = HubSpotEntityHelper.GetStringProperty(properties, "hubspot_owner_id");
                        break;
                    case "NOTE":
                        body = HubSpotEntityHelper.StripHtml(
                            HubSpotEntityHelper.GetStringProperty(properties, "hs_note_body"));
                        activityDate = HubSpotEntityHelper.GetDateTimeProperty(properties, "hs_timestamp");
                        ownerId = HubSpotEntityHelper.GetStringProperty(properties, "hubspot_owner_id");
                        break;
                    case "SMS":
                        subject = HubSpotEntityHelper.GetStringProperty(properties, "hs_sms_title");
                        body = HubSpotEntityHelper.StripHtml(
                            HubSpotEntityHelper.GetStringProperty(properties, "hs_sms_body") ??
                            HubSpotEntityHelper.GetStringProperty(properties, "hs_sms_text"));
                        activityDate = HubSpotEntityHelper.GetDateTimeProperty(properties, "hs_timestamp");
                        ownerId = HubSpotEntityHelper.GetStringProperty(properties, "hubspot_owner_id");
                        break;
                    default:
                        return Result.Failure<Activity>($"Unsupported activity type: {activityType}");
                }
                var sourceAssociation = DetermineAssociation(hubspotData);

                var recordId = HubSpotEntityHelper.GetStringProperty(properties, "hs_object_id") ?? hubSpotId;

                // Determine activity status
                var status = DetermineActivityStatus(activityType, properties, activityDate);

                var activity = new Activity(hubSpotId, recordId, activityType, subject, body,
                    ownerId, sourceAssociation.Type, sourceAssociation.Id, activityDate, status);

                switch (activityType)
                {
                    case "CALL":
                        var callDirection = HubSpotEntityHelper.GetStringProperty(properties, "hs_call_direction");
                        var callStatus = HubSpotEntityHelper.GetStringProperty(properties, "hs_call_status");
                        var callTitle = HubSpotEntityHelper.GetStringProperty(properties, "hs_call_title");
                        var callCreatedDate = HubSpotEntityHelper.GetDateTimeProperty(properties, "hs_createdate");
                        var callCreatedByUserId = HubSpotEntityHelper.GetStringProperty(properties, "hs_created_by_user_id");
                        var callLastModified = HubSpotEntityHelper.GetDateTimeProperty(properties, "hs_lastmodifieddate");
                        activity.SetCallDetail(ActivityCallDetail.Create(
                            callDirection,
                            callStatus,
                            callTitle,
                            callDirection,
                            callCreatedDate,
                            callCreatedByUserId,
                            callLastModified,
                            rawProperties));
                        break;
                    case "EMAIL":
                        var emailStatus = HubSpotEntityHelper.GetStringProperty(properties, "hs_email_status");
                        var emailText = HubSpotEntityHelper.GetStringProperty(properties, "hs_email_text");
                        var emailHtml = HubSpotEntityHelper.GetStringProperty(properties, "hs_email_html");
                        var emailCreatedDate = HubSpotEntityHelper.GetDateTimeProperty(properties, "hs_createdate");
                        var emailCreatedByUserId = HubSpotEntityHelper.GetStringProperty(properties, "hs_created_by_user_id");
                        var emailClickRate = HubSpotEntityHelper.GetStringProperty(properties, "hs_email_click_rate");
                        var emailDirection = HubSpotEntityHelper.GetStringProperty(properties, "hs_email_direction");
                        var emailOpenRate = HubSpotEntityHelper.GetStringProperty(properties, "hs_email_open_rate");
                        var emailReplyRate = HubSpotEntityHelper.GetStringProperty(properties, "hs_email_reply_rate");
                        var emailLastModified = HubSpotEntityHelper.GetDateTimeProperty(properties, "hs_lastmodifieddate");
                        var emailClicks = HubSpotEntityHelper.GetStringProperty(properties, "hs_num_email_clicks");
                        var emailOpens = HubSpotEntityHelper.GetStringProperty(properties, "hs_num_email_opens");
                        var emailUpdatedByUserId = HubSpotEntityHelper.GetStringProperty(properties, "hs_updated_by_user_id");
                        activity.SetEmailDetail(ActivityEmailDetail.Create(
                            emailStatus,
                            emailText,
                            emailHtml,
                            emailCreatedDate,
                            emailCreatedByUserId,
                            emailClickRate,
                            emailDirection,
                            emailOpenRate,
                            emailReplyRate,
                            emailLastModified,
                            emailClicks,
                            emailOpens,
                            emailUpdatedByUserId,
                            rawProperties));
                        break;
                    case "MEETING":
                        var meetingStart = HubSpotEntityHelper.GetDateTimeProperty(properties, "hs_meeting_start_time");
                        var meetingEnd = HubSpotEntityHelper.GetDateTimeProperty(properties, "hs_meeting_end_time");
                        var contactFirstOutreachDate = HubSpotEntityHelper.GetDateTimeProperty(properties, "hs_contact_first_outreach_date");
                        var meetingCreatedDate = HubSpotEntityHelper.GetDateTimeProperty(properties, "hs_createdate");
                        var createdByUserId = HubSpotEntityHelper.GetStringProperty(properties, "hs_created_by_user_id");
                        var hubSpotTeam = HubSpotEntityHelper.GetStringProperty(properties, "hubspot_team_id");
                        var attendeeOwnerIds = HubSpotEntityHelper.GetStringProperty(properties, "hs_attendee_owner_ids");
                        var lastModifiedDate = HubSpotEntityHelper.GetDateTimeProperty(properties, "hs_lastmodifieddate");
                        var locationType = HubSpotEntityHelper.GetStringProperty(properties, "hs_meeting_location_type");
                        var meetingLocation = HubSpotEntityHelper.GetStringProperty(properties, "hs_meeting_location");
                        var meetingSource = HubSpotEntityHelper.GetStringProperty(properties, "hs_meeting_source");
                        var timeToBook = HubSpotEntityHelper.GetStringProperty(properties, "hs_time_to_book_meeting_from_first_contact");
                        activity.SetMeetingDetail(ActivityMeetingDetail.Create(
                            meetingStart,
                            meetingEnd,
                            contactFirstOutreachDate,
                            meetingCreatedDate,
                            createdByUserId,
                            hubSpotTeam,
                            attendeeOwnerIds,
                            lastModifiedDate,
                            locationType,
                            meetingLocation,
                            subject,
                            meetingSource,
                            timeToBook,
                            rawProperties));
                        break;
                    case "TASK":
                        var taskPriority = HubSpotEntityHelper.GetStringProperty(properties, "hs_task_priority");
                        var taskStatus = HubSpotEntityHelper.GetStringProperty(properties, "hs_task_status");
                        var taskCommunicationBody = HubSpotEntityHelper.GetStringProperty(properties, "hs_task_body");
                        var taskCreatedAt = HubSpotEntityHelper.GetDateTimeProperty(properties, "hs_createdate");
                        var taskIsOverdue = HubSpotEntityHelper.GetStringProperty(properties, "hs_task_is_overdue");
                        var taskLastModified = HubSpotEntityHelper.GetDateTimeProperty(properties, "hs_lastmodifieddate");
                        var taskType = HubSpotEntityHelper.GetStringProperty(properties, "hs_task_type");
                        var taskUpdatedByUserId = HubSpotEntityHelper.GetStringProperty(properties, "hs_updated_by_user_id");
                        activity.SetTaskDetail(ActivityTaskDetail.Create(
                            taskPriority,
                            taskStatus,
                            taskCommunicationBody,
                            taskCreatedAt,
                            taskIsOverdue,
                            taskLastModified,
                            taskType,
                            taskUpdatedByUserId,
                            rawProperties));
                        break;
                    case "NOTE":
                        var noteCreated = HubSpotEntityHelper.GetDateTimeProperty(properties, "hs_createdate");
                        var noteCreatedByUserId = HubSpotEntityHelper.GetStringProperty(properties, "hs_created_by_user_id");
                        var noteLastModified = HubSpotEntityHelper.GetDateTimeProperty(properties, "hs_lastmodifieddate");
                        activity.SetNoteDetail(ActivityNoteDetail.Create(
                            noteCreated,
                            noteCreatedByUserId,
                            noteLastModified,
                            rawProperties));
                        break;
                    case "SMS":
                        var smsDirection = GetSmsProperty(properties, "hs_sms_direction", "hs_sms_message_direction");
                        var smsStatus = GetSmsProperty(properties, "hs_sms_status", "hs_sms_message_status");
                        var smsChannelAccount = GetSmsProperty(properties, "hs_sms_channel_account_name");
                        var smsChannelName = GetSmsProperty(properties, "hs_sms_channel_name");
                        var smsBody = GetSmsProperty(properties, "hs_sms_message_body", "hs_sms_body", "hs_sms_text");
                        var smsAssignedTo = ManualOwnerResolver.Resolve(HubSpotEntityHelper.GetStringProperty(properties, "hs_activity_assigned_to"));
                        var smsActivityDate = HubSpotEntityHelper.GetDateTimeProperty(properties, "hs_activity_date");
                        var smsChannelType = HubSpotEntityHelper.GetStringProperty(properties, "hs_sms_channel_type");
                        var smsCommunicationBody = HubSpotEntityHelper.GetStringProperty(properties, "hs_sms_message_body");
                        var smsConversationFirstMessage = HubSpotEntityHelper.GetDateTimeProperty(properties, "hs_sms_conversation_first_message_timestamp");
                        var smsCreatedByUserId = HubSpotEntityHelper.GetStringProperty(properties, "hs_created_by_user_id");
                        var smsTeam = HubSpotEntityHelper.GetStringProperty(properties, "hubspot_team_id");
                        var smsLoggedFrom = HubSpotEntityHelper.GetStringProperty(properties, "hs_logged_from");
                        var smsObjectCreated = HubSpotEntityHelper.GetDateTimeProperty(properties, "hs_object_create_date");
                        var smsObjectLastModified = HubSpotEntityHelper.GetDateTimeProperty(properties, "hs_object_last_modified_date");
                        var smsOwnerAssignedDate = HubSpotEntityHelper.GetDateTimeProperty(properties, "hs_owner_assigneddate");
                        var smsRecordSource = HubSpotEntityHelper.GetStringProperty(properties, "hs_record_source");
                        var smsRecordSourceDetail = HubSpotEntityHelper.GetStringProperty(properties, "hs_record_source_detail_1");
                        var smsUpdatedByUserId = HubSpotEntityHelper.GetStringProperty(properties, "hs_updated_by_user_id");
                        activity.SetSmsDetail(ActivitySmsDetail.Create(
                            smsDirection,
                            smsStatus,
                            smsChannelAccount,
                            smsChannelName,
                            smsBody,
                            smsAssignedTo,
                            smsActivityDate,
                            smsChannelType,
                            smsCommunicationBody,
                            smsConversationFirstMessage,
                            smsCreatedByUserId,
                            smsTeam,
                            smsLoggedFrom,
                            smsObjectCreated,
                            smsObjectLastModified,
                            smsOwnerAssignedDate,
                            smsRecordSource,
                            smsRecordSourceDetail,
                            smsUpdatedByUserId,
                            rawProperties));
                            break;
                }

                return Result.Success(activity);
            }
            catch (Exception ex)
            {
                return Result.Failure<Activity>($"Failed to create Activity from HubSpot data: {ex.Message}");
            }
        }

        public void UpdateFrom(Activity other)
        {
            ETLDate = DateTime.UtcNow;

            RecordId = other.RecordId ?? RecordId;
            ActivityType = other.ActivityType ?? ActivityType;
            Subject = other.Subject ?? Subject;
            Body = other.Body ?? Body;
            ActivityOwner = other.ActivityOwner ?? ActivityOwner;
            SourceObjectType = other.SourceObjectType ?? SourceObjectType;
            SourceObjectId = other.SourceObjectId ?? SourceObjectId;
            SourceObjectName = other.SourceObjectName ?? SourceObjectName;
            SourceObjectEmail = other.SourceObjectEmail ?? SourceObjectEmail;
            ActivityDate = other.ActivityDate ?? ActivityDate;
            Status = other.Status ?? Status;
            MergeDetails(other);
        }

        private void MergeDetails(Activity other)
        {
            MergeDetail(
                other.CallDetail,
                () => CallDetail,
                detail => CallDetail = detail,
                (existing, incoming) => existing.UpdateFrom(incoming));

            MergeDetail(
                other.EmailDetail,
                () => EmailDetail,
                detail => EmailDetail = detail,
                (existing, incoming) => existing.UpdateFrom(incoming));

            MergeDetail(
                other.MeetingDetail,
                () => MeetingDetail,
                detail => MeetingDetail = detail,
                (existing, incoming) => existing.UpdateFrom(incoming));

            MergeDetail(
                other.NoteDetail,
                () => NoteDetail,
                detail => NoteDetail = detail,
                (existing, incoming) => existing.UpdateFrom(incoming));

            MergeDetail(
                other.SmsDetail,
                () => SmsDetail,
                detail => SmsDetail = detail,
                (existing, incoming) => existing.UpdateFrom(incoming));

            MergeDetail(
                other.TaskDetail,
                () => TaskDetail,
                detail => TaskDetail = detail,
                (existing, incoming) => existing.UpdateFrom(incoming));
        }

        private void MergeDetail<TDetail>(
            TDetail? incoming,
            Func<TDetail?> existingAccessor,
            Action<TDetail> assignAction,
            Action<TDetail, TDetail> updateAction)
            where TDetail : ActivityDetail
        {
            if (incoming == null)
            {
                return;
            }

            var existing = existingAccessor();
            if (existing == null)
            {
                incoming.AttachActivity(this);
                assignAction(incoming);
            }
            else
            {
                updateAction(existing, incoming);
            }
        }

        private void SetCallDetail(ActivityCallDetail? detail)
        {
            if (detail == null)
            {
                return;
            }

            detail.AttachActivity(this);
            CallDetail = detail;
        }

        private void SetEmailDetail(ActivityEmailDetail? detail)
        {
            if (detail == null)
            {
                return;
            }

            detail.AttachActivity(this);
            EmailDetail = detail;
        }

        private void SetMeetingDetail(ActivityMeetingDetail? detail)
        {
            if (detail == null)
            {
                return;
            }

            detail.AttachActivity(this);
            MeetingDetail = detail;
        }

        private void SetNoteDetail(ActivityNoteDetail? detail)
        {
            if (detail == null)
            {
                return;
            }

            detail.AttachActivity(this);
            NoteDetail = detail;
        }

        private void SetSmsDetail(ActivitySmsDetail? detail)
        {
            if (detail == null)
            {
                return;
            }

            detail.AttachActivity(this);
            SmsDetail = detail;
        }

        private void SetTaskDetail(ActivityTaskDetail? detail)
        {
            if (detail == null)
            {
                return;
            }

            detail.AttachActivity(this);
            TaskDetail = detail;
        }

        private static readonly Dictionary<string, string> SupportPipelineStageOverrides = new(StringComparer.OrdinalIgnoreCase)
        {
            ["1"] = "Ny ticket (Support Pipeline)",
            ["2"] = "Venter på kunde (Support Pipeline)",
            ["3"] = "Venter på os (Support Pipeline)",
            ["4"] = "Lukket (Support Pipeline)",
            ["861927400"] = "Venter på montage (Support Pipeline)"
        };

        private static string? NormalizeSupportPipeline(string? pipeline)
        {
            if (string.IsNullOrWhiteSpace(pipeline))
            {
                return pipeline;
            }

            return pipeline == "0" ? "Support Pipeline" : pipeline;
        }

        private static string? NormalizeSupportPipelineStage(string? stage)
        {
            if (string.IsNullOrWhiteSpace(stage))
            {
                return stage;
            }

            return SupportPipelineStageOverrides.TryGetValue(stage, out var label) ? label : stage;
        }

        /// <summary>
        /// Extracts all associations from HubSpot activity data.
        /// Returns a list of (ObjectType, ObjectId) tuples for all associated objects.
        /// </summary>
        /// <summary>
        /// Extracts all associations from HubSpot activity data.
        /// Returns a list of tuples containing: (ObjectType, ObjectId, Label, TypeId, Category)
        /// </summary>
        public static List<(string ObjectType, string ObjectId, string? Label, int? TypeId, string? Category)> ExtractAllAssociations(JsonElement hubspotData)
        {
            var associations = new List<(string, string, string?, int?, string?)>();

            if (hubspotData.TryGetProperty("associations", out var associationsElement))
            {
                foreach (var associationType in associationsElement.EnumerateObject())
                {
                    // Normalize object type name (remove plural 's' if present for consistency)
                    var objectType = associationType.Name; // e.g., "contacts", "companies", "deals", "tickets"
                    var normalizedType = objectType.EndsWith("s", StringComparison.OrdinalIgnoreCase) 
                        ? objectType.Substring(0, objectType.Length - 1) 
                        : objectType; // "contacts" -> "contact", "companies" -> "company"
                    
                    if (associationType.Value.TryGetProperty("results", out var results))
                    {
                        foreach (var result in results.EnumerateArray())
                        {
                            var objectId = HubSpotEntityHelper.GetHubSpotId(result);
                            if (!string.IsNullOrWhiteSpace(objectId))
                            {
                                // Try to extract association metadata from result
                                string? label = null;
                                int? typeId = null;
                                string? category = null;

                                // Check if result has association type information (v4 format)
                                if (result.TryGetProperty("associationTypes", out var typesArray) && typesArray.ValueKind == JsonValueKind.Array)
                                {
                                    foreach (var typeItem in typesArray.EnumerateArray())
                                    {
                                        // Extract label
                                        if (typeItem.TryGetProperty("label", out var labelProp) && labelProp.ValueKind == JsonValueKind.String)
                                        {
                                            label = labelProp.GetString();
                                        }
                                        
                                        // Extract typeId
                                        if (typeItem.TryGetProperty("associationTypeId", out var typeIdProp))
                                        {
                                            if (typeIdProp.ValueKind == JsonValueKind.Number)
                                            {
                                                typeId = typeIdProp.GetInt32();
                                            }
                                        }
                                        else if (typeItem.TryGetProperty("typeId", out var typeIdAlt))
                                        {
                                            if (typeIdAlt.ValueKind == JsonValueKind.Number)
                                            {
                                                typeId = typeIdAlt.GetInt32();
                                            }
                                        }
                                        
                                        // Extract category
                                        if (typeItem.TryGetProperty("category", out var categoryProp) && categoryProp.ValueKind == JsonValueKind.String)
                                        {
                                            category = categoryProp.GetString();
                                        }
                                        
                                        // Use first non-null label found
                                        if (!string.IsNullOrWhiteSpace(label))
                                        {
                                            break;
                                        }
                                    }
                                }
                                // Check for alternative format with "types" array
                                else if (result.TryGetProperty("types", out var typesArrayAlt) && typesArrayAlt.ValueKind == JsonValueKind.Array)
                                {
                                    foreach (var typeItem in typesArrayAlt.EnumerateArray())
                                    {
                                        if (typeItem.TryGetProperty("label", out var labelProp))
                                        {
                                            label = labelProp.GetString();
                                        }
                                        if (typeItem.TryGetProperty("associationTypeId", out var typeIdProp) && typeIdProp.ValueKind == JsonValueKind.Number)
                                        {
                                            typeId = typeIdProp.GetInt32();
                                        }
                                        if (typeItem.TryGetProperty("category", out var categoryProp))
                                        {
                                            category = categoryProp.GetString();
                                        }
                                        if (!string.IsNullOrWhiteSpace(label))
                                        {
                                            break;
                                        }
                                    }
                                }
                                // Check for simple "type" property (v3 format fallback)
                                else if (result.TryGetProperty("type", out var typeProp))
                                {
                                    label = typeProp.GetString();
                                }

                                associations.Add((normalizedType, objectId, label, typeId, category));
                            }
                        }
                    }
                }
            }

            return associations;
        }

        /// <summary>
        /// Determines activity status (upcoming, due, overdue, or completed) based on activity type and HubSpot properties.
        /// Uses HubSpot's official CRM schema fields for accurate status determination.
        /// </summary>
        private static string? DetermineActivityStatus(string activityType, JsonElement properties, DateTime? activityDate)
        {
            var now = DateTime.UtcNow;

            switch (activityType.ToUpperInvariant())
            {
                case "TASK":
                    // Tasks are the ONLY activity type with true "upcoming", "due", "overdue", and "completed" concepts
                    // Check completion status first
                    var taskStatus = HubSpotEntityHelper.GetStringProperty(properties, "hs_task_status");
                    var taskCompletionTimestamp = HubSpotEntityHelper.GetDateTimeProperty(properties, "hs_task_completion_timestamp");
                    var taskIsOverdue = HubSpotEntityHelper.GetStringProperty(properties, "hs_task_is_overdue");
                    
                    // If task has completion timestamp, it's completed
                    if (taskCompletionTimestamp.HasValue)
                    {
                        return "completed";
                    }
                    
                    // Check status field
                    if (!string.IsNullOrWhiteSpace(taskStatus))
                    {
                        var statusUpper = taskStatus.ToUpperInvariant();
                        if (statusUpper == "COMPLETE" || statusUpper == "COMPLETED" || statusUpper == "END")
                        {
                            return "completed";
                        }
                    }
                    
                    // Check due date to determine upcoming/overdue/due
                    var taskDueDate = HubSpotEntityHelper.GetDateTimeProperty(properties, "hs_task_due_date");
                    if (taskDueDate.HasValue)
                    {
                        // Check if overdue (HubSpot's calculated field or due date passed)
                        if (!string.IsNullOrWhiteSpace(taskIsOverdue) && 
                            (taskIsOverdue.Equals("true", StringComparison.OrdinalIgnoreCase) || 
                             taskIsOverdue == "1"))
                        {
                            return "overdue";
                        }
                        
                        // If due date has passed and not completed, it's overdue
                        if (taskDueDate.Value < now)
                        {
                            return "overdue";
                        }
                        
                        // If due date is today or very soon (within 24 hours), it's due
                        var timeUntilDue = taskDueDate.Value - now;
                        if (timeUntilDue.TotalHours <= 24 && timeUntilDue.TotalHours >= 0)
                        {
                            return "due";
                        }
                        
                        // If due date is in the future, it's upcoming
                        if (taskDueDate.Value > now)
                        {
                            return "upcoming";
                        }
                    }
                    
                    // Check start date for future-planned tasks
                    var taskStartDate = HubSpotEntityHelper.GetDateTimeProperty(properties, "hs_task_start_date");
                    if (taskStartDate.HasValue && taskStartDate.Value > now)
                    {
                        return "upcoming";
                    }
                    
                    // Fallback: check status for non-completed states
                    if (!string.IsNullOrWhiteSpace(taskStatus))
                    {
                        var statusUpper = taskStatus.ToUpperInvariant();
                        if (statusUpper == "WAITING" || statusUpper == "NOT_STARTED" || statusUpper == "DEFERRED")
                        {
                            // If no due date, check activity date
                            if (activityDate.HasValue && activityDate.Value > now)
                            {
                                return "upcoming";
                            }
                            return "due"; // Default to "due" if no time context
                        }
                        if (statusUpper == "IN_PROGRESS")
                        {
                            return "due"; // In progress tasks are considered "due"
                        }
                    }
                    
                    // Final fallback: compare timestamp
                    if (activityDate.HasValue)
                    {
                        return activityDate.Value <= now ? "completed" : "upcoming";
                    }
                    
                    return "due"; // Default for tasks without clear status

                case "MEETING":
                    // Meetings: future meetings = upcoming, past meetings = completed
                    // HubSpot meetings don't have "completion" - they are simply past or future
                    var meetingStartTime = HubSpotEntityHelper.GetDateTimeProperty(properties, "hs_meeting_start_time");
                    if (meetingStartTime.HasValue)
                    {
                        if (meetingStartTime.Value > now)
                        {
                            return "upcoming";
                        }
                        // Past meetings are considered completed
                        return "completed";
                    }
                    
                    // Fallback: use activityDate
                    if (activityDate.HasValue)
                    {
                        return activityDate.Value > now ? "upcoming" : "completed";
                    }
                    
                    return "completed"; // Default for meetings without start time

                case "CALL":
                    // Calls: Check hs_call_status for scheduled calls, otherwise completed
                    var callStatus = HubSpotEntityHelper.GetStringProperty(properties, "hs_call_status");
                    if (!string.IsNullOrWhiteSpace(callStatus))
                    {
                        var statusUpper = callStatus.ToUpperInvariant();
                        if (statusUpper == "SCHEDULED")
                        {
                            // Check if scheduled time is in the future
                            if (activityDate.HasValue && activityDate.Value > now)
                            {
                                return "upcoming";
                            }
                            return "due"; // Scheduled but time unclear
                        }
                        // COMPLETED, NO_ANSWER, BUSY, etc. are all completed
                        return "completed";
                    }
                    
                    // Fallback: compare timestamp (calls are logged events, usually past)
                    if (activityDate.HasValue)
                    {
                        // If timestamp is in future, might be scheduled (rare)
                        if (activityDate.Value > now)
                        {
                            return "upcoming";
                        }
                        return "completed";
                    }
                    
                    return "completed"; // Default for calls without status

                case "EMAIL":
                    // Emails are always completed (they are logged events, not scheduled)
                    // They use hs_timestamp which represents when the email was sent/received
                    return "completed";

                case "NOTE":
                    // Notes are always completed (created at a moment, not scheduled)
                    return "completed";

                case "SMS":
                    // SMS are always completed (sent/received at a moment, not scheduled)
                    return "completed";

                default:
                    // For unknown types, compare timestamp if available
                    if (activityDate.HasValue)
                    {
                        return activityDate.Value > now ? "upcoming" : "completed";
                    }
                    return "completed"; // Default to completed
            }
        }

        private static (string? Type, string? Id) DetermineAssociation(JsonElement hubspotData)
        {
            string? sourceObjectType = null;
            string? sourceObjectId = null;

            if (hubspotData.TryGetProperty("associations", out var associations))
            {
                var preferredOrder = new[] { "contacts", "deals", "tickets", "companies" };

                foreach (var preferred in preferredOrder)
                {
                    if (associations.TryGetProperty(preferred, out var associationType) &&
                        associationType.TryGetProperty("results", out var preferredResults) &&
                        preferredResults.GetArrayLength() > 0 &&
                        preferredResults[0].TryGetProperty("id", out var preferredIdElement))
                    {
                        sourceObjectType = preferred;
                        sourceObjectId = HubSpotEntityHelper.GetHubSpotId(preferredResults[0]);
                        return (sourceObjectType, sourceObjectId);
                    }
                }

                foreach (var associationType in associations.EnumerateObject())
                {
                    if (associationType.Value.TryGetProperty("results", out var results) &&
                        results.GetArrayLength() > 0 &&
                        results[0].TryGetProperty("id", out var idElement))
                    {
                        sourceObjectType = associationType.Name;
                        sourceObjectId = HubSpotEntityHelper.GetHubSpotId(results[0]);
                        break;
                    }
                }
            }

            return (sourceObjectType, sourceObjectId);
        }

        private static string? GetSmsProperty(JsonElement properties, params string[] names)
        {
            foreach (var name in names)
            {
                var value = HubSpotEntityHelper.GetStringProperty(properties, name);
                if (!string.IsNullOrEmpty(value))
                {
                    return value;
                }
            }

            return null;
        }
    }
}

