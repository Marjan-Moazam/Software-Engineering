using ETL.HubspotService.Domain.Entities;
using Microsoft.EntityFrameworkCore;

namespace ETL.HubspotService.Infrastructure.Data
{
    public class ETLHubspotDbContext : DbContext
    {
        public const string SCHEMA = "Hubspot";

        public ETLHubspotDbContext(DbContextOptions<ETLHubspotDbContext> options) : base(options)
        {
        }

        public DbSet<Contact> Contacts { get; set; }
        public DbSet<Company> Companies { get; set; }
        public DbSet<Deal> Deals { get; set; }
        public DbSet<Ticket> Tickets { get; set; }
        public DbSet<Communication> Communications { get; set; }
        public DbSet<Email> Emails { get; set; }
        public DbSet<Note> Notes { get; set; }
        public DbSet<Activity> Activities { get; set; }
        public DbSet<ActivityCallDetail> ActivityCalls { get; set; }
        public DbSet<ActivityEmailDetail> ActivityEmails { get; set; }
        public DbSet<ActivityMeetingDetail> ActivityMeetings { get; set; }
        public DbSet<ActivityNoteDetail> ActivityNotes { get; set; }
        public DbSet<ActivitySmsDetail> ActivitySmsDetails { get; set; }
        public DbSet<ActivityTaskDetail> ActivityTasks { get; set; }
        public DbSet<ContactCompanyAssociation> ContactCompanyAssociations { get; set; }
        public DbSet<ActivityAssociation> ActivityAssociations { get; set; }
        public DbSet<PropertyHistory> PropertyHistories { get; set; }
        public DbSet<ContactActivityTimeline> ContactActivityTimelines { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.HasDefaultSchema(SCHEMA);

            // Contact configuration - simplified to only required fields
            modelBuilder.Entity<Contact>(entity =>
            {
                entity.ToTable("Contacts");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).ValueGeneratedOnAdd();
                entity.HasIndex(e => e.HubSpotId).IsUnique();
                entity.HasIndex(e => e.RecordId);
                entity.HasIndex(e => e.Email).IsUnique().HasFilter("[Email] IS NOT NULL");
                entity.HasIndex(e => e.ETLDate);
                entity.Property(e => e.HubSpotId).IsRequired().HasMaxLength(50);
                entity.Property(e => e.RecordId).HasMaxLength(50);
                entity.Property(e => e.FullName).HasMaxLength(255);
                entity.Property(e => e.Email).HasMaxLength(255);
                entity.Property(e => e.Phone).HasMaxLength(50);
                entity.Property(e => e.ContactOwner).HasMaxLength(255);
                entity.Property(e => e.Company).HasMaxLength(255);
                entity.Property(e => e.LeadStatus).HasMaxLength(100);
                entity.Property(e => e.LifecycleStage).HasMaxLength(100);
                entity.Property(e => e.PostalCode).HasMaxLength(50);
                entity.Property(e => e.ContactType).HasMaxLength(100);
                entity.Property(e => e.InverterBrand).HasMaxLength(255);
                entity.Property(e => e.LastContacted);
                entity.Property(e => e.AnalyticsSource).HasMaxLength(100);
                entity.Property(e => e.AnalyticsSourceData1).HasMaxLength(255);
                entity.Property(e => e.AnalyticsSourceData2).HasMaxLength(500);
            });

            // Company configuration - simplified to only required fields
            modelBuilder.Entity<Company>(entity =>
            {
                entity.ToTable("Companies");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).ValueGeneratedOnAdd();
                entity.HasIndex(e => e.HubSpotId).IsUnique();
                entity.HasIndex(e => e.RecordId);
                entity.HasIndex(e => e.ETLDate);
                entity.Property(e => e.HubSpotId).IsRequired().HasMaxLength(50);
                entity.Property(e => e.RecordId).HasMaxLength(50);
                entity.Property(e => e.Name).HasMaxLength(255);
                entity.Property(e => e.CompanyOwner).HasMaxLength(255);
                entity.Property(e => e.Phone).HasMaxLength(50);
                entity.Property(e => e.City).HasMaxLength(100);
                entity.Property(e => e.Country).HasMaxLength(100);
                entity.Property(e => e.Cvr).HasMaxLength(50);
                entity.Property(e => e.PostalCode).HasMaxLength(50);
                entity.Property(e => e.Type).HasMaxLength(100);
            });

            // Deal configuration - simplified to only required fields
            modelBuilder.Entity<Deal>(entity =>
            {
                entity.ToTable("Deals");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).ValueGeneratedOnAdd();
                entity.HasIndex(e => e.HubSpotId).IsUnique();
                entity.HasIndex(e => e.RecordId);
                entity.HasIndex(e => e.ETLDate);
                entity.Property(e => e.HubSpotId).IsRequired().HasMaxLength(50);
                entity.Property(e => e.RecordId).HasMaxLength(50);
                entity.Property(e => e.DealName).HasMaxLength(255);
                entity.Property(e => e.DealStage).HasMaxLength(100);
                entity.Property(e => e.Pipeline).HasMaxLength(100);
                entity.Property(e => e.DealOwner).HasMaxLength(255);
                entity.Property(e => e.Amount).HasColumnType("decimal(18,2)");
                entity.Property(e => e.DealType).HasMaxLength(255);
                entity.Property(e => e.CreatedAt).HasColumnType("datetime2");
            });

            // Ticket configuration - simplified to only required fields
            modelBuilder.Entity<Ticket>(entity =>
            {
                entity.ToTable("Tickets");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).ValueGeneratedOnAdd();
                entity.HasIndex(e => e.HubSpotId).IsUnique();
                entity.HasIndex(e => e.RecordId);
                entity.HasIndex(e => e.ETLDate);
                entity.Property(e => e.HubSpotId).IsRequired().HasMaxLength(50);
                entity.Property(e => e.RecordId).HasMaxLength(50);
                entity.Property(e => e.TicketName).HasMaxLength(500);
                entity.Property(e => e.Pipeline).HasMaxLength(100);
                entity.Property(e => e.TicketStatus).HasMaxLength(100);
                entity.Property(e => e.Priority).HasMaxLength(50);
                entity.Property(e => e.TicketOwner).HasMaxLength(255);
                entity.Property(e => e.Source).HasMaxLength(100);
            });

            // Communication configuration - simplified to only required fields
            modelBuilder.Entity<Communication>(entity =>
            {
                entity.ToTable("Communications");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).ValueGeneratedOnAdd();
                entity.HasIndex(e => e.HubSpotId).IsUnique();
                entity.HasIndex(e => e.RecordId);
                entity.HasIndex(e => e.ETLDate);
                entity.HasIndex(e => e.AssociatedContactId);
                entity.Property(e => e.HubSpotId).IsRequired().HasMaxLength(50);
                entity.Property(e => e.RecordId).HasMaxLength(50);
                entity.Property(e => e.ChannelType).HasMaxLength(100);
                entity.Property(e => e.CommunicationBody).HasColumnType("nvarchar(max)");
                entity.Property(e => e.AssociatedContactId).HasMaxLength(50);
                entity.Property(e => e.AssociatedContactName).HasMaxLength(255);
                entity.Property(e => e.AssociatedContactEmail).HasMaxLength(255);
                entity.Property(e => e.ActivityAssignedTo).HasMaxLength(255);
            });

            // Email configuration - simplified to only required fields
            modelBuilder.Entity<Email>(entity =>
            {
                entity.ToTable("Emails");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).ValueGeneratedOnAdd();
                entity.HasIndex(e => e.HubSpotId).IsUnique();
                entity.HasIndex(e => e.RecordId);
                entity.HasIndex(e => e.ETLDate);
                entity.HasIndex(e => e.AssociatedContactId);
                entity.Property(e => e.HubSpotId).IsRequired().HasMaxLength(50);
                entity.Property(e => e.RecordId).HasMaxLength(50);
                entity.Property(e => e.EmailSubject).HasMaxLength(500);
                entity.Property(e => e.EmailBody).HasColumnType("nvarchar(max)");
                entity.Property(e => e.EmailSendStatus).HasMaxLength(50);
                entity.Property(e => e.AssociatedContactId).HasMaxLength(50);
                entity.Property(e => e.AssociatedContactName).HasMaxLength(255);
                entity.Property(e => e.AssociatedContactEmail).HasMaxLength(255);
                entity.Property(e => e.ActivityAssignedTo).HasMaxLength(255);
            });

            // Note configuration - simplified to only required fields
            modelBuilder.Entity<Note>(entity =>
            {
                entity.ToTable("Notes");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).ValueGeneratedOnAdd();
                entity.HasIndex(e => e.HubSpotId).IsUnique();
                entity.HasIndex(e => e.RecordId);
                entity.HasIndex(e => e.ETLDate);
                entity.Property(e => e.HubSpotId).IsRequired().HasMaxLength(50);
                entity.Property(e => e.RecordId).HasMaxLength(50);
                entity.Property(e => e.BodyPreview).HasColumnType("nvarchar(max)");
                entity.Property(e => e.ActivityAssignedTo).HasMaxLength(255);
            });

            modelBuilder.Entity<Activity>(entity =>
            {
                entity.ToTable("Activities");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).ValueGeneratedOnAdd();
                entity.HasIndex(e => e.HubSpotId).IsUnique();
                entity.HasIndex(e => e.RecordId);
                entity.HasIndex(e => e.ActivityType);
                entity.HasIndex(e => e.ActivityDate);
                entity.Property(e => e.HubSpotId).IsRequired().HasMaxLength(50);
                entity.Property(e => e.RecordId).HasMaxLength(50);
                entity.Property(e => e.ActivityType).HasMaxLength(50);
                entity.Property(e => e.Subject).HasMaxLength(500);
                entity.Property(e => e.Body).HasColumnType("nvarchar(max)");
                entity.Property(e => e.ActivityOwner).HasMaxLength(255);
                entity.Property(e => e.SourceObjectType).HasMaxLength(50);
                entity.Property(e => e.SourceObjectId).HasMaxLength(50);
                entity.Property(e => e.SourceObjectName).HasMaxLength(255);
                entity.Property(e => e.SourceObjectEmail).HasMaxLength(255);
                entity.Property(e => e.Status).HasMaxLength(50); // Values: "upcoming", "due", "overdue", "completed"
                entity.HasIndex(e => e.Status);
                entity.HasOne(e => e.CallDetail)
                    .WithOne(d => d.Activity)
                    .HasForeignKey<ActivityCallDetail>(d => d.ActivityId)
                    .OnDelete(DeleteBehavior.Cascade);
                entity.HasOne(e => e.EmailDetail)
                    .WithOne(d => d.Activity)
                    .HasForeignKey<ActivityEmailDetail>(d => d.ActivityId)
                    .OnDelete(DeleteBehavior.Cascade);
                entity.HasOne(e => e.MeetingDetail)
                    .WithOne(d => d.Activity)
                    .HasForeignKey<ActivityMeetingDetail>(d => d.ActivityId)
                    .OnDelete(DeleteBehavior.Cascade);
                entity.HasOne(e => e.NoteDetail)
                    .WithOne(d => d.Activity)
                    .HasForeignKey<ActivityNoteDetail>(d => d.ActivityId)
                    .OnDelete(DeleteBehavior.Cascade);
                entity.HasOne(e => e.SmsDetail)
                    .WithOne(d => d.Activity)
                    .HasForeignKey<ActivitySmsDetail>(d => d.ActivityId)
                    .OnDelete(DeleteBehavior.Cascade);
                entity.HasOne(e => e.TaskDetail)
                    .WithOne(d => d.Activity)
                    .HasForeignKey<ActivityTaskDetail>(d => d.ActivityId)
                    .OnDelete(DeleteBehavior.Cascade);
            });

            modelBuilder.Entity<ActivityCallDetail>(entity =>
            {
                entity.ToTable("ActivityCalls");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).ValueGeneratedOnAdd();
                entity.HasIndex(e => e.ActivityId).IsUnique();
                entity.Property(e => e.ActivityId).IsRequired();
                entity.Property(e => e.Direction).HasMaxLength(50);
                entity.Property(e => e.Status).HasMaxLength(50);
                entity.Property(e => e.RawPropertiesJson).HasColumnType("nvarchar(max)");
                entity.Property(e => e.CallTitle).HasMaxLength(500);
                entity.Property(e => e.CallDirection).HasMaxLength(255);
                entity.Property(e => e.CreatedByUserId).HasMaxLength(255);
            });

            modelBuilder.Entity<ActivityEmailDetail>(entity =>
            {
                entity.ToTable("ActivityEmails");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).ValueGeneratedOnAdd();
                entity.HasIndex(e => e.ActivityId).IsUnique();
                entity.Property(e => e.ActivityId).IsRequired();
                entity.Property(e => e.Status).HasMaxLength(50);
                entity.Property(e => e.TextBody).HasColumnType("nvarchar(max)");
                entity.Property(e => e.HtmlBody).HasColumnType("nvarchar(max)");
                entity.Property(e => e.RawPropertiesJson).HasColumnType("nvarchar(max)");
                entity.Property(e => e.CreatedByUserId).HasMaxLength(255);
                entity.Property(e => e.EmailClickRate).HasMaxLength(255);
                entity.Property(e => e.EmailDirection).HasMaxLength(255);
                entity.Property(e => e.EmailOpenRate).HasMaxLength(255);
                entity.Property(e => e.EmailReplyRate).HasMaxLength(255);
                entity.Property(e => e.NumberOfEmailClicks).HasMaxLength(255);
                entity.Property(e => e.NumberOfEmailOpens).HasMaxLength(255);
                entity.Property(e => e.UpdatedByUserId).HasMaxLength(255);
            });

            modelBuilder.Entity<ActivityMeetingDetail>(entity =>
            {
                entity.ToTable("ActivityMeetings");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).ValueGeneratedOnAdd();
                entity.HasIndex(e => e.ActivityId).IsUnique();
                entity.Property(e => e.ActivityId).IsRequired();
                entity.Property(e => e.RawPropertiesJson).HasColumnType("nvarchar(max)");
                entity.Property(e => e.CreatedByUserId).HasMaxLength(255);
                entity.Property(e => e.HubSpotTeam).HasMaxLength(255);
                entity.Property(e => e.HubSpotAttendeeOwnerIds).HasMaxLength(500);
                entity.Property(e => e.LocationType).HasMaxLength(255);
                entity.Property(e => e.MeetingLocation).HasMaxLength(500);
                entity.Property(e => e.MeetingName).HasMaxLength(500);
                entity.Property(e => e.MeetingSource).HasMaxLength(255);
                entity.Property(e => e.TimeToBookFromFirstContact).HasMaxLength(255);
            });

            modelBuilder.Entity<ActivityNoteDetail>(entity =>
            {
                entity.ToTable("ActivityNotes");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).ValueGeneratedOnAdd();
                entity.HasIndex(e => e.ActivityId).IsUnique();
                entity.Property(e => e.ActivityId).IsRequired();
                entity.Property(e => e.RawPropertiesJson).HasColumnType("nvarchar(max)");
                entity.Property(e => e.CreatedByUserId).HasMaxLength(255);
            });

            modelBuilder.Entity<ActivitySmsDetail>(entity =>
            {
                entity.ToTable("ActivitySms");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).ValueGeneratedOnAdd();
                entity.HasIndex(e => e.ActivityId).IsUnique();
                entity.Property(e => e.ActivityId).IsRequired();
                entity.Property(e => e.Direction).HasMaxLength(50);
                entity.Property(e => e.Status).HasMaxLength(50);
                entity.Property(e => e.ChannelAccountName).HasMaxLength(255);
                entity.Property(e => e.ChannelName).HasMaxLength(255);
                entity.Property(e => e.MessageBody).HasColumnType("nvarchar(max)");
                entity.Property(e => e.RawPropertiesJson).HasColumnType("nvarchar(max)");
                entity.Property(e => e.ActivityAssignedTo).HasMaxLength(255);
                entity.Property(e => e.ChannelType).HasMaxLength(255);
                entity.Property(e => e.CommunicationBody).HasColumnType("nvarchar(max)");
                entity.Property(e => e.CreatedByUserId).HasMaxLength(255);
                entity.Property(e => e.HubSpotTeam).HasMaxLength(255);
                entity.Property(e => e.LoggedFrom).HasMaxLength(255);
                entity.Property(e => e.RecordSource).HasMaxLength(255);
                entity.Property(e => e.RecordSourceDetail1).HasMaxLength(255);
                entity.Property(e => e.UpdatedByUserId).HasMaxLength(255);
            });

            modelBuilder.Entity<ActivityTaskDetail>(entity =>
            {
                entity.ToTable("ActivityTasks");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).ValueGeneratedOnAdd();
                entity.HasIndex(e => e.ActivityId).IsUnique();
                entity.Property(e => e.ActivityId).IsRequired();
                entity.Property(e => e.Priority).HasMaxLength(50);
                entity.Property(e => e.Status).HasMaxLength(50);
                entity.Property(e => e.RawPropertiesJson).HasColumnType("nvarchar(max)");
                entity.Property(e => e.CommunicationBody).HasColumnType("nvarchar(max)");
                entity.Property(e => e.TaskType).HasMaxLength(255);
                entity.Property(e => e.UpdatedByUserId).HasMaxLength(255);
            });

            // ContactCompanyAssociation configuration - many-to-many relationship
            modelBuilder.Entity<ContactCompanyAssociation>(entity =>
            {
                entity.ToTable("ContactCompanyAssociations");
                entity.Property(e => e.AssociationLabelsJson).HasColumnType("nvarchar(max)");
                entity.Property(e => e.AssociationSource).HasMaxLength(100);
                entity.Property(e => e.AssociationSourceId).HasMaxLength(255);
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).ValueGeneratedOnAdd();
                
                // Composite unique index on ContactHubSpotId + CompanyHubSpotId to prevent duplicates
                entity.HasIndex(e => new { e.ContactHubSpotId, e.CompanyHubSpotId }).IsUnique();
                entity.HasIndex(e => e.ContactHubSpotId);
                entity.HasIndex(e => e.CompanyHubSpotId);
                entity.HasIndex(e => e.ETLDate);
                
                entity.Property(e => e.ContactHubSpotId).IsRequired().HasMaxLength(50);
                entity.Property(e => e.CompanyHubSpotId).IsRequired().HasMaxLength(50);
                entity.Property(e => e.AssociationType).HasMaxLength(100);
                
                // Foreign key relationships (optional, for easier querying)
                entity.HasOne(e => e.Contact)
                    .WithMany()
                    .HasForeignKey(e => e.ContactId)
                    .OnDelete(DeleteBehavior.SetNull);
                
                entity.HasOne(e => e.Company)
                    .WithMany()
                    .HasForeignKey(e => e.CompanyId)
                    .OnDelete(DeleteBehavior.SetNull);
            });

            // ActivityAssociation configuration - many-to-many relationships for activities
            modelBuilder.Entity<ActivityAssociation>(entity =>
            {
                entity.ToTable("ActivityAssociations");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).ValueGeneratedOnAdd();
                
                // Composite unique index on ActivityHubSpotId + AssociatedObjectType + AssociatedObjectId to prevent duplicates
                entity.HasIndex(e => new { e.ActivityHubSpotId, e.AssociatedObjectType, e.AssociatedObjectId }).IsUnique();
                entity.HasIndex(e => e.ActivityHubSpotId);
                entity.HasIndex(e => e.AssociatedObjectType);
                entity.HasIndex(e => e.AssociatedObjectId);
                entity.HasIndex(e => e.AssociationTypeId);
                entity.HasIndex(e => e.ETLDate);
                
                entity.Property(e => e.ActivityHubSpotId).IsRequired().HasMaxLength(50);
                entity.Property(e => e.AssociatedObjectType).IsRequired().HasMaxLength(50);
                entity.Property(e => e.AssociatedObjectId).IsRequired().HasMaxLength(50);
                entity.Property(e => e.AssociationLabel).HasMaxLength(200);
                entity.Property(e => e.AssociationCategory).HasMaxLength(50);
                
                // Foreign key relationship to Activity (optional, for easier querying)
                entity.HasOne(e => e.Activity)
                    .WithMany()
                    .HasForeignKey(e => e.ActivityId)
                    .OnDelete(DeleteBehavior.SetNull);
            });

            // ObjectAssociation configuration - generic many-to-many relationships for all object types
            modelBuilder.Entity<ObjectAssociation>(entity =>
            {
                entity.ToTable("ObjectAssociations");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).ValueGeneratedOnAdd();
                
                // Composite unique index on SourceObjectType + SourceObjectId + TargetObjectType + TargetObjectId to prevent duplicates
                entity.HasIndex(e => new { e.SourceObjectType, e.SourceObjectId, e.TargetObjectType, e.TargetObjectId }).IsUnique();
                entity.HasIndex(e => e.SourceObjectType);
                entity.HasIndex(e => e.SourceObjectId);
                entity.HasIndex(e => e.TargetObjectType);
                entity.HasIndex(e => e.TargetObjectId);
                entity.HasIndex(e => e.AssociationTypeId);
                entity.HasIndex(e => e.ETLDate);
                
                entity.Property(e => e.SourceObjectType).IsRequired().HasMaxLength(50);
                entity.Property(e => e.SourceObjectId).IsRequired().HasMaxLength(50);
                entity.Property(e => e.TargetObjectType).IsRequired().HasMaxLength(50);
                entity.Property(e => e.TargetObjectId).IsRequired().HasMaxLength(50);
                entity.Property(e => e.AssociationLabel).HasMaxLength(200);
                entity.Property(e => e.AssociationLabelsJson).HasColumnType("nvarchar(max)");
                entity.Property(e => e.AssociationCategory).HasMaxLength(50);
                entity.Property(e => e.AssociationSource).HasMaxLength(100);
                entity.Property(e => e.AssociationSourceId).HasMaxLength(255);
            });

            // PropertyHistory configuration - tracks historical changes to properties
            modelBuilder.Entity<PropertyHistory>(entity =>
            {
                entity.ToTable("PropertyHistories");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).ValueGeneratedOnAdd();
                
                // Indexes for efficient querying
                entity.HasIndex(e => new { e.ObjectType, e.ObjectId, e.PropertyName, e.ChangeDate });
                entity.HasIndex(e => e.ObjectType);
                entity.HasIndex(e => e.ObjectId);
                entity.HasIndex(e => e.PropertyName);
                entity.HasIndex(e => e.ChangeDate);
                entity.HasIndex(e => e.ETLDate);
                
                entity.Property(e => e.ObjectType).IsRequired().HasMaxLength(50);
                entity.Property(e => e.ObjectId).IsRequired().HasMaxLength(50);
                entity.Property(e => e.PropertyName).IsRequired().HasMaxLength(100);
                entity.Property(e => e.OldValue).HasMaxLength(500);
                entity.Property(e => e.NewValue).HasMaxLength(500);
                entity.Property(e => e.Source).HasMaxLength(200);
                entity.Property(e => e.SourceId).HasMaxLength(255);
            });

            // ContactActivityTimeline configuration - tracks all activities and events related to contacts
            modelBuilder.Entity<ContactActivityTimeline>(entity =>
            {
                entity.ToTable("ContactActivityTimelines");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).ValueGeneratedOnAdd();
                
                // Indexes for efficient querying
                entity.HasIndex(e => e.ContactHubSpotId);
                entity.HasIndex(e => e.EventType);
                entity.HasIndex(e => e.EventDate);
                entity.HasIndex(e => new { e.ContactHubSpotId, e.EventDate });
                entity.HasIndex(e => e.ETLDate);
                
                entity.Property(e => e.ContactHubSpotId).IsRequired().HasMaxLength(50);
                entity.Property(e => e.EventType).IsRequired().HasMaxLength(100);
                entity.Property(e => e.Description).IsRequired().HasMaxLength(2000);
                entity.Property(e => e.RelatedObjectType).HasMaxLength(50);
                entity.Property(e => e.RelatedObjectId).HasMaxLength(50);
                entity.Property(e => e.RelatedObjectName).HasMaxLength(500);
                entity.Property(e => e.ActorId).HasMaxLength(255);
                entity.Property(e => e.ActorName).HasMaxLength(255);
                entity.Property(e => e.Metadata).HasMaxLength(2000);
            });

            base.OnModelCreating(modelBuilder);
        }
    }
}
