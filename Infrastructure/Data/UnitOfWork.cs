using ETL.HubspotService.Domain.Entities;
using ETL.HubspotService.Domain.Interfaces;
using Microsoft.EntityFrameworkCore.Storage;

namespace ETL.HubspotService.Infrastructure.Data
{
    public class UnitOfWork : IUnitOfWork
    {
        private readonly ETLHubspotDbContext _context;
        private IDbContextTransaction? _transaction;

        public UnitOfWork(ETLHubspotDbContext context)
        {
            _context = context;
            ContactRepository = new Repository<Contact>(_context);
            CompanyRepository = new Repository<Company>(_context);
            DealRepository = new Repository<Deal>(_context);
            TicketRepository = new Repository<Ticket>(_context);
            CommunicationRepository = new Repository<Communication>(_context);
            EmailRepository = new Repository<Email>(_context);
            NoteRepository = new Repository<Note>(_context);
            ActivityRepository = new ActivityRepository(_context);
            ContactCompanyAssociationRepository = new Repository<ContactCompanyAssociation>(_context);
            ActivityAssociationRepository = new Repository<ActivityAssociation>(_context);
            ObjectAssociationRepository = new Repository<ObjectAssociation>(_context);
            PropertyHistoryRepository = new Repository<PropertyHistory>(_context);
            ContactActivityTimelineRepository = new Repository<ContactActivityTimeline>(_context);
        }

        public IRepository<Contact> ContactRepository { get; }
        public IRepository<Company> CompanyRepository { get; }
        public IRepository<Deal> DealRepository { get; }
        public IRepository<Ticket> TicketRepository { get; }
        public IRepository<Communication> CommunicationRepository { get; }
        public IRepository<Email> EmailRepository { get; }
        public IRepository<Note> NoteRepository { get; }
        public IActivityRepository ActivityRepository { get; }
        public IRepository<ContactCompanyAssociation> ContactCompanyAssociationRepository { get; }
        public IRepository<ActivityAssociation> ActivityAssociationRepository { get; }
        public IRepository<ObjectAssociation> ObjectAssociationRepository { get; }
        public IRepository<PropertyHistory> PropertyHistoryRepository { get; }
        public IRepository<ContactActivityTimeline> ContactActivityTimelineRepository { get; }

        public ETLHubspotDbContext GetDbContext() => _context;

        public async Task<int> SaveChangesAsync()
        {
            return await _context.SaveChangesAsync();
        }

        public async Task BeginTransactionAsync()
        {
            _transaction = await _context.Database.BeginTransactionAsync();
        }

        public async Task CommitTransactionAsync()
        {
            if (_transaction != null)
            {
                try
                {
                    await _transaction.CommitAsync();
                }
                catch (InvalidOperationException ex) when (ex.Message.Contains("completed") || ex.Message.Contains("no longer usable"))
                {
                    // Transaction was corrupted (e.g., connection timeout)
                    // Try to rollback instead
                    try
                    {
                        await _transaction.RollbackAsync();
                    }
                    catch
                    {
                        // Ignore rollback errors
                    }
                    throw new InvalidOperationException("Transaction was corrupted and cannot be committed. All changes have been rolled back.", ex);
                }
                finally
                {
                    try
                    {
                        await _transaction.DisposeAsync();
                    }
                    catch
                    {
                        // Ignore disposal errors
                    }
                    _transaction = null;
                }
            }
        }

        public async Task RollbackTransactionAsync()
        {
            if (_transaction != null)
            {
                try
                {
                    await _transaction.RollbackAsync();
                }
                catch (Exception)
                {
                    // Transaction may already be in a bad state (e.g., connection broken)
                    // Just dispose it and continue
                }
                finally
                {
                    try
                    {
                        await _transaction.DisposeAsync();
                    }
                    catch
                    {
                        // Ignore disposal errors
                    }
                    _transaction = null;
                }
            }
        }

        public void Dispose()
        {
            _transaction?.Dispose();
            _context.Dispose();
        }
    }
}


