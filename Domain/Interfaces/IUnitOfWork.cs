using ETL.HubspotService.Domain.Entities;
using ETL.HubspotService.Infrastructure.Data;

namespace ETL.HubspotService.Domain.Interfaces
{
    public interface IUnitOfWork : IDisposable
    {
        IRepository<Contact> ContactRepository { get; }
        IRepository<Company> CompanyRepository { get; }
        IRepository<Deal> DealRepository { get; }
        IRepository<Ticket> TicketRepository { get; }
        IRepository<Communication> CommunicationRepository { get; }
        IRepository<Email> EmailRepository { get; }
        IRepository<Note> NoteRepository { get; }
        IActivityRepository ActivityRepository { get; }
        IRepository<ContactCompanyAssociation> ContactCompanyAssociationRepository { get; }
        IRepository<ActivityAssociation> ActivityAssociationRepository { get; }
        IRepository<ObjectAssociation> ObjectAssociationRepository { get; }
        IRepository<PropertyHistory> PropertyHistoryRepository { get; }
        IRepository<ContactActivityTimeline> ContactActivityTimelineRepository { get; }

        ETLHubspotDbContext GetDbContext();
        Task<int> SaveChangesAsync();
        Task BeginTransactionAsync();
        Task CommitTransactionAsync();
        Task RollbackTransactionAsync();
    }
}


