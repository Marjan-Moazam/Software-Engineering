    using CSharpFunctionalExtensions;
    using ETL.HubspotService.Domain.Entities;
    using ETL.HubspotService.Domain.Interfaces;
    using ETL.HubspotService.Infrastructure.Data;
    using Microsoft.EntityFrameworkCore;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Text.Json;

    namespace ETL.HubspotService.Infrastructure.Services
    {
        public interface IETLService
        {
            Task<Result> ExtractAndLoadContactsAsync();
            Task<Result> ExtractAndLoadCompaniesAsync();
            Task<Result> ExtractAndLoadContactCompanyAssociationsAsync();
            Task<Result> ExtractAndLoadDealsAsync();
            Task<Result> ExtractAndLoadTicketsAsync();
            Task<Result> ExtractAndLoadCommunicationsAsync();
            Task<Result> ExtractAndLoadEmailsAsync();
            Task<Result> ExtractAndLoadNotesAsync();
            Task<Result> ExtractAndLoadActivitiesAsync();
            Task<Result> ExtractAndLoadPropertyHistoryAsync();
            Task<Result> ExtractAndLoadContactActivityTimelineAsync();
            Task<Result> RunFullETLAsync();
        }

        public class ETLService : IETLService
        {
            private readonly IHubSpotApiService _hubSpotApiService;
            private readonly IUnitOfWork _unitOfWork;
            private readonly ILogger<ETLService> _logger;
            private const int BATCH_SIZE = 100; // Process 100 records per API call
            private const int SAVE_BATCH_SIZE = 500; // Save to DB in batches of 500
            private Dictionary<string, string> _ownerNames = new(StringComparer.OrdinalIgnoreCase);
            private readonly Dictionary<string, ContactInfo> _contactInfos = new(StringComparer.OrdinalIgnoreCase);
            private Dictionary<string, string> _ticketPipelineLabels = new(StringComparer.OrdinalIgnoreCase);
            private Dictionary<string, string> _ticketStageLabels = new(StringComparer.OrdinalIgnoreCase);
            private Dictionary<string, List<string>> _contactAssociationsFromFetch = new(StringComparer.OrdinalIgnoreCase);
            private static readonly Dictionary<string, string> SupportPipelineStageOverrides = new(StringComparer.OrdinalIgnoreCase)
            {
                ["1"] = "Ny ticket (Support Pipeline)",
                ["2"] = "Venter pÃ¥ kunde (Support Pipeline)",
                ["3"] = "Venter pÃ¥ os (Support Pipeline)",
                ["4"] = "Lukket (Support Pipeline)",
                ["861927400"] = "Venter pÃ¥ montage (Support Pipeline)"
            };

            public ETLService(
                IHubSpotApiService hubSpotApiService,
                IUnitOfWork unitOfWork,
                ILogger<ETLService> logger)
            {
                _hubSpotApiService = hubSpotApiService;
                _unitOfWork = unitOfWork;
                _logger = logger;
            }

            // Generic upsert helper method - uses HubSpotId for lookups
            private async Task UpsertEntitiesAsync<T>(List<T> entities, IRepository<T> repository, string entityName) 
                where T : Domain.Entities.Entity<long>
            {
                int inserted = 0;
                int updated = 0;
                int batchCount = 0;

                foreach (var entity in entities)
                {
                    // Lookup by HubSpotId instead of Id - use reflection to get HubSpotId property
                    T? existing = null;
                    var hubSpotIdProperty = typeof(T).GetProperty("HubSpotId");
                    if (hubSpotIdProperty != null)
                    {
                        var hubSpotIdValue = hubSpotIdProperty.GetValue(entity)?.ToString();
                        if (!string.IsNullOrEmpty(hubSpotIdValue))
                        {
                            if (repository is IActivityRepository activityRepository)
                            {
                                var existingActivity = await activityRepository.GetByHubSpotIdWithDetailsAsync(hubSpotIdValue);
                                existing = existingActivity as T;
                            }
                            else
                            {
                                // Build expression: e => e.HubSpotId == hubSpotIdValue
                                var parameter = System.Linq.Expressions.Expression.Parameter(typeof(T), "e");
                                var property = System.Linq.Expressions.Expression.Property(parameter, "HubSpotId");
                                var constant = System.Linq.Expressions.Expression.Constant(hubSpotIdValue);
                                var equals = System.Linq.Expressions.Expression.Equal(property, constant);
                                var lambda = System.Linq.Expressions.Expression.Lambda<Func<T, bool>>(equals, parameter);

                                existing = await repository.FirstOrDefaultAsync(lambda);
                            }
                        }
                    }
                    
                    if (existing != null)
                    {
                        // Update existing entity using UpdateFrom method
                        if (entity is Contact contact && existing is Contact existingContact)
                        {
                            existingContact.UpdateFrom(contact);
                            await repository.UpdateAsync(existing);
                            updated++;
                        }
                        else if (entity is Company company && existing is Company existingCompany)
                        {
                            existingCompany.UpdateFrom(company);
                            await repository.UpdateAsync(existing);
                            updated++;
                        }
                        else if (entity is Deal deal && existing is Deal existingDeal)
                        {
                            existingDeal.UpdateFrom(deal);
                            await repository.UpdateAsync(existing);
                            updated++;
                        }
                        else if (entity is Ticket ticket && existing is Ticket existingTicket)
                        {
                            existingTicket.UpdateFrom(ticket);
                            await repository.UpdateAsync(existing);
                            updated++;
                        }
                        else if (entity is Communication comm && existing is Communication existingComm)
                        {
                            existingComm.UpdateFrom(comm);
                            await repository.UpdateAsync(existing);
                            updated++;
                        }
                        else if (entity is Email email && existing is Email existingEmail)
                        {
                            existingEmail.UpdateFrom(email);
                            await repository.UpdateAsync(existing);
                            updated++;
                        }
                        else if (entity is Note note && existing is Note existingNote)
                        {
                            existingNote.UpdateFrom(note);
                            await repository.UpdateAsync(existing);
                            updated++;
                        }
                        else if (entity is Activity activity && existing is Activity existingActivity)
                        {
                            existingActivity.UpdateFrom(activity);
                            await repository.UpdateAsync(existing);
                            updated++;
                        }
                    }
                    else
                    {
                        // Insert new entity
                        await repository.AddAsync(entity);
                        inserted++;
                    }

                    batchCount++;
                    // Save in batches to avoid memory issues
                    if (batchCount >= SAVE_BATCH_SIZE)
                    {
                        await _unitOfWork.SaveChangesAsync();
                        _logger.LogInformation("{EntityName}: Saved batch of {Count} records (Inserted: {Inserted}, Updated: {Updated})", 
                            entityName, batchCount, inserted, updated);
                        batchCount = 0;
                    }
                }

                // Save remaining records
                if (batchCount > 0)
                {
                    await _unitOfWork.SaveChangesAsync();
                    _logger.LogInformation("{EntityName}: Saved final batch of {Count} records (Inserted: {Inserted}, Updated: {Updated})", 
                        entityName, batchCount, inserted, updated);
                }

                _logger.LogInformation("{EntityName}: Upsert completed - Total: {Total}, Inserted: {Inserted}, Updated: {Updated}", 
                    entityName, entities.Count, inserted, updated);
            }

            public async Task<Result> ExtractAndLoadContactsAsync()
            {
                try
                {
                    _logger.LogInformation("Starting ETL process for Contacts with UPSERT logic");

                    var allContacts = new List<Contact>();
                    string? after = null;
                    int totalFetched = 0;
                    _contactAssociationsFromFetch.Clear(); // Reset associations cache

                    do
                    {
                        var result = await _hubSpotApiService.GetContactsAsync(BATCH_SIZE, after);
                        if (result.IsFailure)
                        {
                            _logger.LogError("Failed to fetch contacts from HubSpot: {Error}", result.Error);
                            return Result.Failure($"Failed to fetch contacts: {result.Error}");
                        }

                        // Extract associations from contact response if available (v3 API with associations=companies)
                        foreach (var contactJson in result.Value.Results)
                        {
                            if (contactJson.TryGetProperty("associations", out var associationsJson))
                            {
                                if (associationsJson.TryGetProperty("companies", out var companiesJson))
                                {
                                    if (companiesJson.TryGetProperty("results", out var companyResults))
                                    {
                                        var contactId = HubSpotEntityHelper.GetHubSpotId(contactJson);
                                        if (!string.IsNullOrWhiteSpace(contactId))
                                        {
                                            var companyIds = new List<string>();
                                            foreach (var companyResult in companyResults.EnumerateArray())
                                            {
                                                var companyId = HubSpotEntityHelper.GetHubSpotId(companyResult);
                                                if (!string.IsNullOrWhiteSpace(companyId))
                                                {
                                                    companyIds.Add(companyId);
                                                }
                                            }
                                            if (companyIds.Count > 0)
                                            {
                                                _contactAssociationsFromFetch[contactId] = companyIds;
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        var contacts = result.Value.Results
                            .Select(Contact.CreateFromHubSpotData)
                            .Where(r => r.IsSuccess)
                            .Select(r => r.Value)
                            .ToList();

                        foreach (var contact in contacts)
                        {
                            contact.ContactOwner = ResolveOwnerName(contact.ContactOwner);
                            RememberContactInfo(contact);
                        }

                        allContacts.AddRange(contacts);
                        totalFetched += contacts.Count;

                        _logger.LogInformation("Fetched {Count} contacts from HubSpot, total so far: {Total}", 
                            contacts.Count, totalFetched);

                        after = result.Value.Paging?.Next?.After;
                    } while (!string.IsNullOrEmpty(after));
                    
                    if (_contactAssociationsFromFetch.Count > 0)
                    {
                        _logger.LogInformation("Found {Count} contact-company associations embedded in contact fetch response", 
                            _contactAssociationsFromFetch.Count);
                    }

                    _logger.LogInformation("Total contacts fetched: {Total}. Starting upsert process...", totalFetched);
                    await UpsertEntitiesAsync(allContacts, _unitOfWork.ContactRepository, "Contacts");

                    _logger.LogInformation("Successfully completed ETL for Contacts. Total processed: {Total}", totalFetched);
                    return Result.Success();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error occurred during Contacts ETL process");
                    return Result.Failure($"Contacts ETL failed: {ex.Message}");
                }
            }

            public async Task<Result> ExtractAndLoadCompaniesAsync()
            {
                try
                {
                    _logger.LogInformation("Starting ETL process for Companies with UPSERT logic");

                    var allCompanies = new List<Company>();
                    string? after = null;
                    int totalFetched = 0;

                    do
                    {
                        var result = await _hubSpotApiService.GetCompaniesAsync(BATCH_SIZE, after);
                        if (result.IsFailure)
                        {
                            _logger.LogError("Failed to fetch companies from HubSpot: {Error}", result.Error);
                            return Result.Failure($"Failed to fetch companies: {result.Error}");
                        }

                        var companies = result.Value.Results
                            .Select(Company.CreateFromHubSpotData)
                            .Where(r => r.IsSuccess)
                            .Select(r => r.Value)
                            .ToList();

                        foreach (var company in companies)
                        {
                            company.CompanyOwner = ResolveOwnerName(company.CompanyOwner);
                        }

                        allCompanies.AddRange(companies);
                        totalFetched += companies.Count;

                        _logger.LogInformation("Fetched {Count} companies from HubSpot, total so far: {Total}", 
                            companies.Count, totalFetched);

                        after = result.Value.Paging?.Next?.After;
                    } while (!string.IsNullOrEmpty(after));

                    _logger.LogInformation("Total companies fetched: {Total}. Starting upsert process...", totalFetched);
                    await UpsertEntitiesAsync(allCompanies, _unitOfWork.CompanyRepository, "Companies");

                    _logger.LogInformation("Successfully completed ETL for Companies. Total processed: {Total}", totalFetched);
                    return Result.Success();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error occurred during Companies ETL process");
                    return Result.Failure($"Companies ETL failed: {ex.Message}");
                }
            }

        public async Task<Result> ExtractAndLoadContactCompanyAssociationsAsync()
        {
            try
            {
                _logger.LogInformation("Starting ETL process for Contact-Company Associations using HubSpot v4 Associations API");

                // Get all contacts and companies from the database to map HubSpot IDs to internal IDs
                var allContacts = (await _unitOfWork.ContactRepository.GetAllAsync()).ToList();
                var allCompanies = (await _unitOfWork.CompanyRepository.GetAllAsync()).ToList();

                var contactIdMap = allContacts.ToDictionary(c => c.HubSpotId, c => c.Id, StringComparer.OrdinalIgnoreCase);
                var companyIdMap = allCompanies.ToDictionary(c => c.HubSpotId, c => c.Id, StringComparer.OrdinalIgnoreCase);

                // Get all contact HubSpot IDs - THIS IS CORRECTLY PLACED
                var contactHubSpotIds = allContacts
                    .Select(c => c.HubSpotId)
                    .Where(id => !string.IsNullOrWhiteSpace(id))
                    .ToList();

                if (contactHubSpotIds.Count == 0)
                {
                    _logger.LogWarning("No contacts found in database. Skipping association extraction.");
                    return Result.Success();
                }

                _logger.LogInformation("Fetching company associations for {Count} contacts using v4 batch/read API", contactHubSpotIds.Count);
                
                // VALIDATION: Test with a small sample first to verify API connectivity and response format
                if (contactHubSpotIds.Count > 0)
                {
                    var sampleIds = contactHubSpotIds.Take(5).ToList();
                    _logger.LogInformation("VALIDATION: Testing with sample contact IDs: {Ids}", string.Join(", ", sampleIds));
                    var testResult = await _hubSpotApiService.GetContactCompanyAssociationsBatchAsync(sampleIds);
                    if (testResult.IsSuccess && testResult.Value.Count > 0)
                    {
                        _logger.LogInformation("VALIDATION SUCCESS: Found {Count} associations in sample. Proceeding with full batch.", testResult.Value.Count);
                    }
                    else if (testResult.IsSuccess && testResult.Value.Count == 0)
                    {
                        _logger.LogWarning("VALIDATION WARNING: Sample test returned ZERO associations. " +
                            "This indicates HubSpot has no contact-company associations. " +
                            "Please verify in HubSpot UI: Open a Contact â†’ Right Sidebar â†’ 'Associated Companies'. " +
                            "If empty, you need to create associations in HubSpot first.");
                    }
                    else
                    {
                        _logger.LogError("VALIDATION ERROR: Sample test failed: {Error}", testResult.Error);
                    }
                }

                    // Method 1: Use HubSpot v4 Associations API batch/read endpoint (recommended method)
                    var associationsResult = await _hubSpotApiService.GetContactCompanyAssociationsBatchAsync(contactHubSpotIds);
                    
                    if (associationsResult.IsFailure)
                    {
                        _logger.LogError("Failed to fetch contact-company associations: {Error}", associationsResult.Error);
                        return Result.Failure($"Failed to fetch associations: {associationsResult.Error}");
                    }

                    var associations = associationsResult.Value;
                    
                    // Method 2: Merge associations found during contact fetch (v3 API with associations=companies)
                    if (_contactAssociationsFromFetch.Count > 0)
                    {
                        _logger.LogInformation("Merging {Count} associations found during contact fetch...", _contactAssociationsFromFetch.Count);
                        foreach (var kvp in _contactAssociationsFromFetch)
                        {
                            if (!associations.ContainsKey(kvp.Key))
                            {
                                associations[kvp.Key] = new List<string>();
                            }
                            foreach (var companyId in kvp.Value)
                            {
                                if (!associations[kvp.Key].Contains(companyId))
                                {
                                    associations[kvp.Key].Add(companyId);
                                }
                            }
                        }
                    }
                    
                    // Method 3: Fallback - Try fetching associations from company side (reverse direction)
                    if (associations.Count == 0)
                    {
                        _logger.LogInformation("No associations found via contact->company direction. Trying reverse direction (company->contact)...");
                        var reverseAssociationsResult = await _hubSpotApiService.GetCompanyContactAssociationsBatchAsync(
                            allCompanies.Select(c => c.HubSpotId).Where(id => !string.IsNullOrWhiteSpace(id)).ToList());
                        
                        if (reverseAssociationsResult.IsSuccess)
                        {
                            // Reverse the mapping: company->contact becomes contact->company
                            var reverseAssociations = reverseAssociationsResult.Value;
                            foreach (var kvp in reverseAssociations)
                            {
                                var companyId = kvp.Key;
                                foreach (var contactId in kvp.Value)
                                {
                                    if (!associations.ContainsKey(contactId))
                                    {
                                        associations[contactId] = new List<string>();
                                    }
                                    if (!associations[contactId].Contains(companyId))
                                    {
                                        associations[contactId].Add(companyId);
                                    }
                                }
                            }
                            _logger.LogInformation("Found {Count} associations via reverse direction", associations.Count);
                        }
                    }
                    
                    // Log comprehensive diagnostic warning if still no associations found
                    if (associations.Count == 0)
                    {
                        _logger.LogWarning("==========================================");
                        _logger.LogWarning("NO ASSOCIATIONS FOUND - DIAGNOSTIC INFO:");
                        _logger.LogWarning("==========================================");
                        _logger.LogWarning("1. Checked {ContactCount} contacts via v4 batch API", contactHubSpotIds.Count);
                        _logger.LogWarning("2. Checked {CompanyCount} companies via reverse direction", allCompanies.Count);
                        if (_contactAssociationsFromFetch.Count > 0)
                        {
                            _logger.LogWarning("3. Found {Count} associations in v3 contact fetch response", _contactAssociationsFromFetch.Count);
                        }
                        else
                        {
                            _logger.LogWarning("3. Found 0 associations in v3 contact fetch response");
                        }
                        _logger.LogWarning("4. FINAL RESULT: {TotalAssociations} associations found", associations.Count);
                        _logger.LogWarning("");
                        _logger.LogWarning("POSSIBLE REASONS:");
                        _logger.LogWarning("  a) HubSpot has NO contact-company associations (most likely)");
                        _logger.LogWarning("     â†’ Verify in HubSpot UI: Contact â†’ Right Sidebar â†’ 'Associated Companies'");
                        _logger.LogWarning("  b) Private app token missing required scopes:");
                        _logger.LogWarning("     â†’ crm.objects.contacts.read");
                        _logger.LogWarning("     â†’ crm.objects.companies.read");
                        _logger.LogWarning("     â†’ crm.objects.contacts.associations.read");
                        _logger.LogWarning("     â†’ crm.objects.companies.associations.read");
                        _logger.LogWarning("  c) Using wrong ID format (should be HubSpot internal ID, not RecordId)");
                        _logger.LogWarning("  d) Custom association types require specific typeId parameter");
                        _logger.LogWarning("");
                        _logger.LogWarning("NEXT STEPS:");
                        _logger.LogWarning("  1. Open HubSpot portal and check if ANY contact has an associated company");
                        _logger.LogWarning("  2. If none exist, create at least one association manually in HubSpot");
                        _logger.LogWarning("  3. Verify your private app has the required scopes");
                        _logger.LogWarning("  4. Re-run ETL after associations exist in HubSpot");
                        _logger.LogWarning("==========================================");
                    }
                    var allAssociations = new List<ContactCompanyAssociation>();
                    int totalFetched = 0;

                    // Get detailed associations from HubSpotApiService (includes labels, timestamps, source)
                    var detailedAssociations = _hubSpotApiService.GetLastDetailedAssociations();

                    // Process the associations
                    foreach (var kvp in associations)
                    {
                        var contactHubSpotId = kvp.Key;
                        var companyHubSpotIds = kvp.Value;

                        foreach (var companyHubSpotId in companyHubSpotIds)
                        {
                            // Get detailed information if available
                            string? associationLabelsJson = null;
                            string? primaryLabel = null;
                            DateTime? createdAt = null;
                            DateTime? updatedAt = null;
                            string? source = null;
                            string? sourceId = null;

                            if (detailedAssociations != null && detailedAssociations.TryGetValue(contactHubSpotId, out var detailsList))
                            {
                                var details = detailsList.FirstOrDefault(d => d.TargetObjectId == companyHubSpotId);
                                if (details != null)
                                {
                                    primaryLabel = details.PrimaryLabel;
                                    if (details.AllLabels.Count > 0)
                                    {
                                        // Store all labels as JSON array
                                        associationLabelsJson = JsonSerializer.Serialize(details.AllLabels);
                                    }
                                    createdAt = details.CreatedAt;
                                    updatedAt = details.UpdatedAt;
                                    source = details.Source;
                                    sourceId = details.SourceId;
                                }
                            }

                            var associationResult = ContactCompanyAssociation.CreateFromHubSpotData(
                                contactHubSpotId, 
                                companyHubSpotId,
                                associationType: primaryLabel,
                                associationLabelsJson: associationLabelsJson,
                                associationCreatedAt: createdAt,
                                associationUpdatedAt: updatedAt,
                                associationSource: source,
                                associationSourceId: sourceId);
                            
                            if (associationResult.IsSuccess)
                            {
                                var association = associationResult.Value;
                                
                                // Set foreign key IDs if available
                                if (contactIdMap.TryGetValue(contactHubSpotId, out var contactId))
                                {
                                    association.ContactId = contactId;
                                }
                                
                                if (companyIdMap.TryGetValue(companyHubSpotId, out var companyId))
                                {
                                    association.CompanyId = companyId;
                                }
                                
                                allAssociations.Add(association);
                                totalFetched++;
                            }
                        }
                    }

                    _logger.LogInformation("Total associations extracted: {Total}. Starting upsert process...", totalFetched);

                    if (allAssociations.Count == 0)
                    {
                        _logger.LogWarning("No associations to save. ContactCompanyAssociation table will remain empty.");
                        return Result.Success();
                    }

                    // Upsert associations with proper error handling
                    int inserted = 0;
                    int updated = 0;
                    int failed = 0;
                    int batchCount = 0;

                    foreach (var association in allAssociations)
                    {
                        try
                        {
                            // Lookup by composite key (ContactHubSpotId + CompanyHubSpotId)
                            var existing = await _unitOfWork.ContactCompanyAssociationRepository.FirstOrDefaultAsync(
                                a => a.ContactHubSpotId == association.ContactHubSpotId && 
                                    a.CompanyHubSpotId == association.CompanyHubSpotId);

                            if (existing != null)
                            {
                                existing.UpdateFrom(association);
                                await _unitOfWork.ContactCompanyAssociationRepository.UpdateAsync(existing);
                                updated++;
                            }
                            else
                            {
                                await _unitOfWork.ContactCompanyAssociationRepository.AddAsync(association);
                                inserted++;
                            }

                            batchCount++;
                            if (batchCount >= SAVE_BATCH_SIZE)
                            {
                                await _unitOfWork.SaveChangesAsync();
                                _logger.LogInformation("Saved batch of {Count} associations. Inserted: {Inserted}, Updated: {Updated}", 
                                    batchCount, inserted, updated);
                                batchCount = 0;
                            }
                        }
                        catch (Exception ex)
                        {
                            failed++;
                            _logger.LogError(ex, "Failed to upsert association: Contact={ContactId}, Company={CompanyId}. Error: {Error}", 
                                association.ContactHubSpotId, association.CompanyHubSpotId, ex.Message);
                            // Continue with next association instead of failing completely
                        }
                    }

                    if (batchCount > 0)
                    {
                        try
                        {
                            await _unitOfWork.SaveChangesAsync();
                            _logger.LogInformation("Saved final batch of {Count} associations. Inserted: {Inserted}, Updated: {Updated}", 
                                batchCount, inserted, updated);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Failed to save final batch of associations. Error: {Error}", ex.Message);
                            failed += batchCount;
                        }
                    }

                    _logger.LogInformation("Successfully completed ETL for Contact-Company Associations. " +
                        "Total processed: {Total}, Inserted: {Inserted}, Updated: {Updated}, Failed: {Failed}", 
                        totalFetched, inserted, updated, failed);

                    if (failed > 0)
                    {
                        _logger.LogWarning("{Failed} associations failed to save. Check logs for details.", failed);
                    }
                    
                    return Result.Success();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error occurred during Contact-Company Associations ETL process");
                    return Result.Failure($"Contact-Company Associations ETL failed: {ex.Message}");
                }
            }

            public async Task<Result> ExtractAndLoadDealsAsync()
            {
                try
                {
                    _logger.LogInformation("Starting ETL process for Deals with UPSERT logic");

                    var allDeals = new List<Deal>();
                    string? after = null;
                    int totalFetched = 0;

                    do
                    {
                        var result = await _hubSpotApiService.GetDealsAsync(BATCH_SIZE, after);
                        if (result.IsFailure)
                        {
                            _logger.LogError("Failed to fetch deals from HubSpot: {Error}", result.Error);
                            return Result.Failure($"Failed to fetch deals: {result.Error}");
                        }

                        var deals = result.Value.Results
                            .Select(Deal.CreateFromHubSpotData)
                            .Where(r => r.IsSuccess)
                            .Select(r => r.Value)
                            .ToList();

                        foreach (var deal in deals)
                        {
                            deal.DealOwner = ResolveOwnerName(deal.DealOwner);
                        }

                        allDeals.AddRange(deals);
                        totalFetched += deals.Count;

                        _logger.LogInformation("Fetched {Count} deals from HubSpot, total so far: {Total}", 
                            deals.Count, totalFetched);

                        after = result.Value.Paging?.Next?.After;
                    } while (!string.IsNullOrEmpty(after));

                    _logger.LogInformation("Total deals fetched: {Total}. Starting upsert process...", totalFetched);
                    await UpsertEntitiesAsync(allDeals, _unitOfWork.DealRepository, "Deals");

                    _logger.LogInformation("Successfully completed ETL for Deals. Total processed: {Total}", totalFetched);
                    return Result.Success();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error occurred during Deals ETL process");
                    return Result.Failure($"Deals ETL failed: {ex.Message}");
                }
            }

            public async Task<Result> ExtractAndLoadTicketsAsync()
            {
                try
                {
                    _logger.LogInformation("Starting ETL process for Tickets with UPSERT logic");

                    var allTickets = new List<Ticket>();
                    string? after = null;
                    int totalFetched = 0;

                    do
                    {
                        var result = await _hubSpotApiService.GetTicketsAsync(BATCH_SIZE, after);
                        if (result.IsFailure)
                        {
                            _logger.LogError("Failed to fetch tickets from HubSpot: {Error}", result.Error);
                            return Result.Failure($"Failed to fetch tickets: {result.Error}");
                        }

                        var tickets = result.Value.Results
                            .Select(Ticket.CreateFromHubSpotData)
                            .Where(r => r.IsSuccess)
                            .Select(r => r.Value)
                            .ToList();

                        foreach (var ticket in tickets)
                        {
                            ticket.TicketOwner = ResolveOwnerName(ticket.TicketOwner);
                            ticket.Pipeline = NormalizeTicketPipeline(ResolveTicketPipeline(ticket.Pipeline));
                            ticket.TicketStatus = NormalizeTicketStage(ResolveTicketStage(ticket.TicketStatus));
                        }

                        allTickets.AddRange(tickets);
                        totalFetched += tickets.Count;

                        _logger.LogInformation("Fetched {Count} tickets from HubSpot, total so far: {Total}", 
                            tickets.Count, totalFetched);

                        after = result.Value.Paging?.Next?.After;
                    } while (!string.IsNullOrEmpty(after));

                    _logger.LogInformation("Total tickets fetched: {Total}. Starting upsert process...", totalFetched);
                    await UpsertEntitiesAsync(allTickets, _unitOfWork.TicketRepository, "Tickets");

                    _logger.LogInformation("Successfully completed ETL for Tickets. Total processed: {Total}", totalFetched);
                    return Result.Success();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error occurred during Tickets ETL process");
                    return Result.Failure($"Tickets ETL failed: {ex.Message}");
                }
            }

            public async Task<Result> ExtractAndLoadCommunicationsAsync()
            {
                try
                {
                    _logger.LogInformation("Starting ETL process for Communications with UPSERT logic");

                    var allCommunications = new List<Communication>();
                    string? after = null;
                    int totalFetched = 0;

                    do
                    {
                        var result = await _hubSpotApiService.GetCommunicationsAsync(BATCH_SIZE, after);
                        if (result.IsFailure)
                        {
                            _logger.LogError("Failed to fetch communications from HubSpot: {Error}", result.Error);
                            return Result.Failure($"Failed to fetch communications: {result.Error}");
                        }

                        var communications = result.Value.Results
                            .Select(Communication.CreateFromHubSpotData)
                            .Where(r => r.IsSuccess)
                            .Select(r => r.Value)
                            .ToList();

                        foreach (var communication in communications)
                        {
                            communication.ActivityAssignedTo = ResolveOwnerName(communication.ActivityAssignedTo);
                            var commContact = await ResolveContactInfoAsync(communication.AssociatedContactId);
                            if (commContact != null)
                            {
                                communication.AssociatedContactName = commContact.Name;
                                communication.AssociatedContactEmail = commContact.Email;
                            }
                        }

                        allCommunications.AddRange(communications);
                        totalFetched += communications.Count;

                        _logger.LogInformation("Fetched {Count} communications from HubSpot, total so far: {Total}", 
                            communications.Count, totalFetched);

                        after = result.Value.Paging?.Next?.After;
                    } while (!string.IsNullOrEmpty(after));

                    _logger.LogInformation("Total communications fetched: {Total}. Starting upsert process...", totalFetched);
                    await UpsertEntitiesAsync(allCommunications, _unitOfWork.CommunicationRepository, "Communications");

                    _logger.LogInformation("Successfully completed ETL for Communications. Total processed: {Total}", totalFetched);
                    return Result.Success();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error occurred during Communications ETL process");
                    return Result.Failure($"Communications ETL failed: {ex.Message}");
                }
            }

            public async Task<Result> ExtractAndLoadEmailsAsync()
            {
                try
                {
                    _logger.LogInformation("Starting ETL process for Emails with UPSERT logic");

                    var allEmails = new List<Email>();
                    string? after = null;
                    int totalFetched = 0;

                    do
                    {
                        var result = await _hubSpotApiService.GetEmailsAsync(BATCH_SIZE, after);
                        if (result.IsFailure)
                        {
                            _logger.LogError("Failed to fetch emails from HubSpot: {Error}", result.Error);
                            return Result.Failure($"Failed to fetch emails: {result.Error}");
                        }

                        var emails = result.Value.Results
                            .Select(Email.CreateFromHubSpotData)
                            .Where(r => r.IsSuccess)
                            .Select(r => r.Value)
                            .ToList();

                        foreach (var email in emails)
                        {
                            email.ActivityAssignedTo = ResolveOwnerName(email.ActivityAssignedTo);
                            if (TryGetContactInfo(email.AssociatedContactId, out var emailContact))
                            {
                                email.AssociatedContactName = emailContact.Name;
                                email.AssociatedContactEmail = emailContact.Email;
                            }
                        }

                        allEmails.AddRange(emails);
                        totalFetched += emails.Count;

                        _logger.LogInformation("Fetched {Count} emails from HubSpot, total so far: {Total}", 
                            emails.Count, totalFetched);

                        after = result.Value.Paging?.Next?.After;
                    } while (!string.IsNullOrEmpty(after));

                    _logger.LogInformation("Total emails fetched: {Total}. Starting upsert process...", totalFetched);
                    await UpsertEntitiesAsync(allEmails, _unitOfWork.EmailRepository, "Emails");

                    _logger.LogInformation("Successfully completed ETL for Emails. Total processed: {Total}", totalFetched);
                    return Result.Success();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error occurred during Emails ETL process");
                    return Result.Failure($"Emails ETL failed: {ex.Message}");
                }
            }

            public async Task<Result> ExtractAndLoadNotesAsync()
            {
                try
                {
                    _logger.LogInformation("Starting ETL process for Notes with UPSERT logic");

                    var allNotes = new List<Note>();
                    string? after = null;
                    int totalFetched = 0;

                    do
                    {
                        var result = await _hubSpotApiService.GetNotesAsync(BATCH_SIZE, after);
                        if (result.IsFailure)
                        {
                            _logger.LogError("Failed to fetch notes from HubSpot: {Error}", result.Error);
                            return Result.Failure($"Failed to fetch notes: {result.Error}");
                        }

                        var notes = result.Value.Results
                            .Select(Note.CreateFromHubSpotData)
                            .Where(r => r.IsSuccess)
                            .Select(r => r.Value)
                            .ToList();

                        foreach (var note in notes)
                        {
                            note.ActivityAssignedTo = ResolveOwnerName(note.ActivityAssignedTo);
                        }

                        allNotes.AddRange(notes);
                        totalFetched += notes.Count;

                        _logger.LogInformation("Fetched {Count} notes from HubSpot, total so far: {Total}", 
                            notes.Count, totalFetched);

                        after = result.Value.Paging?.Next?.After;
                    } while (!string.IsNullOrEmpty(after));

                    _logger.LogInformation("Total notes fetched: {Total}. Starting upsert process...", totalFetched);
                    await UpsertEntitiesAsync(allNotes, _unitOfWork.NoteRepository, "Notes");

                    _logger.LogInformation("Successfully completed ETL for Notes. Total processed: {Total}", totalFetched);
                    return Result.Success();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error occurred during Notes ETL process");
                    return Result.Failure($"Notes ETL failed: {ex.Message}");
                }
            }

            public async Task<Result> ExtractAndLoadActivitiesAsync()
            {
                try
                {
                    _logger.LogInformation("Starting ETL process for Activities");

                    var allActivities = new List<Activity>();
                    var allAssociations = new List<(string ActivityHubSpotId, string ObjectType, string ObjectId, string? Label, int? TypeId, string? Category)>();

                    var activitySources = new List<(string Type, Func<int, string?, Task<Result<HubSpotApiResponse<JsonElement>>>>)>
                    {
                        ("CALL", _hubSpotApiService.GetCallsAsync),
                        ("EMAIL", _hubSpotApiService.GetEmailsAsync),
                        ("MEETING", _hubSpotApiService.GetMeetingsAsync),
                        ("TASK", _hubSpotApiService.GetTasksAsync),
                        ("NOTE", _hubSpotApiService.GetNotesAsync),
                        ("SMS", _hubSpotApiService.GetSmsAsync)
                    };

                    foreach (var (type, fetcher) in activitySources)
                    {
                        try
                        {
                            string? after = null;
                            int totalFetched = 0;

                            do
                            {
                                var result = await fetcher(BATCH_SIZE, after);
                                if (result.IsFailure)
                                {
                                    _logger.LogWarning("Failed to fetch {Type} activities from HubSpot (skipping this type): {Error}", type, result.Error);
                                    break; // Skip this activity type and continue with next
                                }

                                var activities = result.Value.Results
                                    .Select(r => Activity.CreateFromHubSpotData(r, type))
                                    .Where(r => r.IsSuccess)
                                    .Select(r => r.Value)
                                    .ToList();

                                // Extract associations from raw HubSpot data
                                foreach (var (activity, rawData) in activities.Zip(result.Value.Results, (a, r) => (a, r)))
                                {
                                    activity.ActivityOwner = ResolveOwnerName(activity.ActivityOwner);

                                    // Extract all associations for this activity (with detailed metadata)
                                    var associations = Activity.ExtractAllAssociations(rawData);
                                    foreach (var (objectType, objectId, label, typeId, category) in associations)
                                    {
                                        allAssociations.Add((activity.HubSpotId, objectType, objectId, label, typeId, category));
                                    }

                                    if (string.Equals(activity.SourceObjectType, "contacts", StringComparison.OrdinalIgnoreCase))
                                    {
                                        if (TryGetContactInfo(activity.SourceObjectId, out var contactInfo))
                                        {
                                            activity.SourceObjectName = contactInfo.Name;
                                            activity.SourceObjectEmail = contactInfo.Email;
                                        }
                                    }
                                }

                                allActivities.AddRange(activities);
                                totalFetched += activities.Count;

                                _logger.LogInformation("Fetched {Count} {Type} activities from HubSpot, total so far: {Total}",
                                    activities.Count, type, totalFetched);

                                after = result.Value.Paging?.Next?.After;
                            } while (!string.IsNullOrEmpty(after));
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning(ex, "Error fetching {Type} activities (skipping this type): {Error}", type, ex.Message);
                            // Continue with next activity type
                        }
                    }

                    _logger.LogInformation("Total activities fetched: {Total}. Starting upsert process...", allActivities.Count);
                    await UpsertEntitiesAsync(allActivities, _unitOfWork.ActivityRepository, "Activities");

                    // Now store associations
                    _logger.LogInformation("Total associations extracted: {Total}. Starting upsert process...", allAssociations.Count);
                    await UpsertActivityAssociationsAsync(allAssociations);

                    _logger.LogInformation("Successfully completed ETL for Activities. Total processed: {Total}, Associations: {Associations}", 
                        allActivities.Count, allAssociations.Count);
                    return Result.Success();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error occurred during Activities ETL process");
                    return Result.Failure($"Activities ETL failed: {ex.Message}");
                }
            }

            private async Task UpsertActivityAssociationsAsync(List<(string ActivityHubSpotId, string ObjectType, string ObjectId, string? Label, int? TypeId, string? Category)> associations)
            {
                int inserted = 0;
                int updated = 0;
                int batchCount = 0;

                // Get all activities to map HubSpot IDs to internal IDs
                var allActivities = (await _unitOfWork.ActivityRepository.GetAllAsync()).ToList();
                var activityIdMap = allActivities.ToDictionary(a => a.HubSpotId, a => a.Id, StringComparer.OrdinalIgnoreCase);

                foreach (var (activityHubSpotId, objectType, objectId, label, typeId, category) in associations)
                {
                    // Lookup by composite key (ActivityHubSpotId + AssociatedObjectType + AssociatedObjectId)
                    var existing = await _unitOfWork.ActivityAssociationRepository.FirstOrDefaultAsync(
                        a => a.ActivityHubSpotId == activityHubSpotId &&
                            a.AssociatedObjectType == objectType &&
                            a.AssociatedObjectId == objectId);

                    if (existing != null)
                    {
                        // Update association metadata (label, typeId, category can change)
                        existing.ETLDate = DateTime.UtcNow;
                        if (!string.IsNullOrWhiteSpace(label))
                        {
                            existing.AssociationLabel = label;
                        }
                        if (typeId.HasValue)
                        {
                            existing.AssociationTypeId = typeId;
                        }
                        if (!string.IsNullOrWhiteSpace(category))
                        {
                            existing.AssociationCategory = category;
                        }
                        await _unitOfWork.ActivityAssociationRepository.UpdateAsync(existing);
                        updated++;
                    }
                    else
                    {
                        var associationResult = ActivityAssociation.CreateFromHubSpotData(
                            activityHubSpotId, 
                            objectType, 
                            objectId,
                            label,
                            typeId,
                            category);
                        
                        if (associationResult.IsSuccess)
                        {
                            var association = associationResult.Value;
                            
                            // Set foreign key ID if available
                            if (activityIdMap.TryGetValue(activityHubSpotId, out var activityId))
                            {
                                association.ActivityId = activityId;
                            }
                            
                            await _unitOfWork.ActivityAssociationRepository.AddAsync(association);
                            inserted++;
                        }
                    }

                    batchCount++;
                    if (batchCount >= SAVE_BATCH_SIZE)
                    {
                        await _unitOfWork.SaveChangesAsync();
                        _logger.LogInformation("Saved batch of {Count} activity associations. Inserted: {Inserted}, Updated: {Updated}",
                            batchCount, inserted, updated);
                        batchCount = 0;
                    }
                }

                if (batchCount > 0)
                {
                    await _unitOfWork.SaveChangesAsync();
                    _logger.LogInformation("Saved final batch of {Count} activity associations. Inserted: {Inserted}, Updated: {Updated}",
                        batchCount, inserted, updated);
                }

                _logger.LogInformation("Completed upsert of activity associations. Total: {Total}, Inserted: {Inserted}, Updated: {Updated}",
                    associations.Count, inserted, updated);
            }


            public async Task<Result> RunFullETLAsync()
            {
                try
                {
                    _logger.LogInformation("Starting full ETL process for all HubSpot data with UPSERT logic");

                    await LoadOwnerNamesAsync();
                    await LoadTicketPropertyLabelsAsync();
                    _contactInfos.Clear();

                    // Use execution strategy to wrap the transaction (required when EnableRetryOnFailure is enabled)
                    var dbContext = _unitOfWork.GetDbContext();
                    var strategy = dbContext.Database.CreateExecutionStrategy();
                    var transactionResult = await strategy.ExecuteAsync(
                        dbContext,
                        async (context, state, ct) =>
                        {
                            await _unitOfWork.BeginTransactionAsync();

                        try
                        {
                            // Run ETL for each entity type
                        var contactsResult = await ExtractAndLoadContactsAsync();
                        if (contactsResult.IsFailure)
                        {
                            await _unitOfWork.RollbackTransactionAsync();
                            return Result.Failure($"Contacts ETL failed: {contactsResult.Error}");
                        }

                        var companiesResult = await ExtractAndLoadCompaniesAsync();
                        if (companiesResult.IsFailure)
                        {
                            await _unitOfWork.RollbackTransactionAsync();
                            return Result.Failure($"Companies ETL failed: {companiesResult.Error}");
                        }

                        // Extract associations after both contacts and companies are loaded
                        var associationsResult = await ExtractAndLoadContactCompanyAssociationsAsync();
                        
                        // Extract all object associations
                        await ExtractAndLoadObjectAssociationsAsync("contact", "deal", 
                            async () => Result.Success((await _unitOfWork.ContactRepository.GetAllAsync()).Select(c => c.HubSpotId).ToList()));
                        await ExtractAndLoadObjectAssociationsAsync("contact", "ticket", 
                            async () => Result.Success((await _unitOfWork.ContactRepository.GetAllAsync()).Select(c => c.HubSpotId).ToList()));
                        await ExtractAndLoadObjectAssociationsAsync("company", "deal", 
                            async () => Result.Success((await _unitOfWork.CompanyRepository.GetAllAsync()).Select(c => c.HubSpotId).ToList()));
                        await ExtractAndLoadObjectAssociationsAsync("company", "ticket", 
                            async () => Result.Success((await _unitOfWork.CompanyRepository.GetAllAsync()).Select(c => c.HubSpotId).ToList()));
                        await ExtractAndLoadObjectAssociationsAsync("deal", "ticket", 
                            async () => Result.Success((await _unitOfWork.DealRepository.GetAllAsync()).Select(d => d.HubSpotId).ToList()));
                        
                        // Ticket â†’ Activities associations
                        await ExtractAndLoadObjectAssociationsAsync("ticket", "call", 
                            async () => Result.Success((await _unitOfWork.TicketRepository.GetAllAsync()).Select(t => t.HubSpotId).ToList()));
                        await ExtractAndLoadObjectAssociationsAsync("ticket", "email", 
                            async () => Result.Success((await _unitOfWork.TicketRepository.GetAllAsync()).Select(t => t.HubSpotId).ToList()));
                        await ExtractAndLoadObjectAssociationsAsync("ticket", "note", 
                            async () => Result.Success((await _unitOfWork.TicketRepository.GetAllAsync()).Select(t => t.HubSpotId).ToList()));
                        await ExtractAndLoadObjectAssociationsAsync("ticket", "task", 
                            async () => Result.Success((await _unitOfWork.TicketRepository.GetAllAsync()).Select(t => t.HubSpotId).ToList()));
                        await ExtractAndLoadObjectAssociationsAsync("ticket", "meeting", 
                            async () => Result.Success((await _unitOfWork.TicketRepository.GetAllAsync()).Select(t => t.HubSpotId).ToList()));
                        
                        // Email â†’ Deals/Companies/Tickets associations
                        await ExtractAndLoadObjectAssociationsAsync("email", "deal", 
                            async () => Result.Success((await _unitOfWork.EmailRepository.GetAllAsync()).Select(e => e.HubSpotId).ToList()));
                        await ExtractAndLoadObjectAssociationsAsync("email", "company", 
                            async () => Result.Success((await _unitOfWork.EmailRepository.GetAllAsync()).Select(e => e.HubSpotId).ToList()));
                        await ExtractAndLoadObjectAssociationsAsync("email", "ticket", 
                            async () => Result.Success((await _unitOfWork.EmailRepository.GetAllAsync()).Select(e => e.HubSpotId).ToList()));
                        
                        // Meeting â†’ Deals/Companies/Tickets associations
                        await ExtractAndLoadObjectAssociationsAsync("meeting", "deal", 
                            async () => Result.Success((await _unitOfWork.ActivityRepository.GetAllAsync()).Where(a => a.ActivityType == "MEETING").Select(a => a.HubSpotId).ToList()));
                        await ExtractAndLoadObjectAssociationsAsync("meeting", "company", 
                            async () => Result.Success((await _unitOfWork.ActivityRepository.GetAllAsync()).Where(a => a.ActivityType == "MEETING").Select(a => a.HubSpotId).ToList()));
                        await ExtractAndLoadObjectAssociationsAsync("meeting", "ticket", 
                            async () => Result.Success((await _unitOfWork.ActivityRepository.GetAllAsync()).Where(a => a.ActivityType == "MEETING").Select(a => a.HubSpotId).ToList()));
                        
                        // Task â†’ Deals/Companies/Tickets associations
                        await ExtractAndLoadObjectAssociationsAsync("task", "deal", 
                            async () => Result.Success((await _unitOfWork.ActivityRepository.GetAllAsync()).Where(a => a.ActivityType == "TASK").Select(a => a.HubSpotId).ToList()));
                        await ExtractAndLoadObjectAssociationsAsync("task", "company", 
                            async () => Result.Success((await _unitOfWork.ActivityRepository.GetAllAsync()).Where(a => a.ActivityType == "TASK").Select(a => a.HubSpotId).ToList()));
                        await ExtractAndLoadObjectAssociationsAsync("task", "ticket", 
                            async () => Result.Success((await _unitOfWork.ActivityRepository.GetAllAsync()).Where(a => a.ActivityType == "TASK").Select(a => a.HubSpotId).ToList()));
                        if (associationsResult.IsFailure)
                        {
                            _logger.LogWarning("Contact-Company Associations ETL failed: {Error}", associationsResult.Error);
                            // Continue with other entities - associations are not critical
                        }

                        var dealsResult = await ExtractAndLoadDealsAsync();
                        if (dealsResult.IsFailure)
                        {
                            await _unitOfWork.RollbackTransactionAsync();
                            return Result.Failure($"Deals ETL failed: {dealsResult.Error}");
                        }

                        var ticketsResult = await ExtractAndLoadTicketsAsync();
                        if (ticketsResult.IsFailure)
                        {
                            await _unitOfWork.RollbackTransactionAsync();
                            return Result.Failure($"Tickets ETL failed: {ticketsResult.Error}");
                        }

                        var communicationsResult = await ExtractAndLoadCommunicationsAsync();
                        if (communicationsResult.IsFailure)
                        {
                            _logger.LogWarning("Communications ETL failed: {Error}", communicationsResult.Error);
                            // Continue with other entities
                        }

                        var emailsResult = await ExtractAndLoadEmailsAsync();
                        if (emailsResult.IsFailure)
                        {
                            _logger.LogWarning("Emails ETL failed: {Error}", emailsResult.Error);
                            // Continue with other entities
                        }

                        var notesResult = await ExtractAndLoadNotesAsync();
                        if (notesResult.IsFailure)
                        {
                            _logger.LogWarning("Notes ETL failed: {Error}", notesResult.Error);
                            // Continue with other entities
                        }

                            // Commit transaction BEFORE Activities ETL (Activities is non-critical and causes connection timeouts)
                            await _unitOfWork.CommitTransactionAsync();
                            _logger.LogInformation("Successfully completed main ETL process. Committed transaction.");
                            
                            // Now run Activities ETL outside the transaction (it's non-critical)
                            var activitiesResult = await ExtractAndLoadActivitiesAsync();
                            if (activitiesResult.IsFailure)
                            {
                                _logger.LogWarning("Activities ETL failed (non-critical): {Error}", activitiesResult.Error);
                                // Activities failure doesn't affect the main ETL since it's already committed
                            }
                            
                            _logger.LogInformation("Successfully completed full ETL process for all HubSpot data");
                            return Result.Success();
                        }
                        catch (Exception)
                        {
                            await _unitOfWork.RollbackTransactionAsync();
                            throw;
                        }
                        },
                        null,
                        CancellationToken.None);

                    if (transactionResult.IsFailure)
                    {
                        return transactionResult;
                    }
                    
                    // Extract Property History AFTER transaction commit (it needs to read from committed data)
                    var propertyHistoryResult = await ExtractAndLoadPropertyHistoryAsync();
                    if (propertyHistoryResult.IsFailure)
                    {
                        _logger.LogWarning("Property History ETL failed: {Error}", propertyHistoryResult.Error);
                        // Continue - property history is not critical for main ETL
                    }

                    // Extract Contact Activity Timeline AFTER property history (it uses property history data)
                    var timelineResult = await ExtractAndLoadContactActivityTimelineAsync();
                    if (timelineResult.IsFailure)
                    {
                        _logger.LogWarning("Contact Activity Timeline ETL failed: {Error}", timelineResult.Error);
                        // Continue - timeline is not critical for main ETL
                    }
                    
                    return Result.Success();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error occurred during full ETL process");
                    return Result.Failure($"Full ETL failed: {ex.Message}");
                }
            }

            private async Task LoadOwnerNamesAsync()
            {
                try
                {
                    var ownerResult = await _hubSpotApiService.GetOwnersAsync();
                    if (ownerResult.IsSuccess)
                    {
                        _ownerNames = ownerResult.Value;
                        _logger.LogInformation("Cached {Count} HubSpot owners", _ownerNames.Count);
                    }
                    else
                    {
                        _ownerNames = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                        _logger.LogWarning("Unable to load HubSpot owners: {Error}", ownerResult.Error);
                    }
                }
                catch (Exception ex)
                {
                    _ownerNames = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                    _logger.LogWarning(ex, "Unexpected error loading HubSpot owners");
                }
            }

            private async Task LoadTicketPropertyLabelsAsync()
            {
                try
                {
                    var pipelineResult = await _hubSpotApiService.GetTicketPropertyOptionsAsync("hs_pipeline");
                    if (pipelineResult.IsSuccess)
                    {
                        _ticketPipelineLabels = pipelineResult.Value;
                        _logger.LogInformation("Cached {Count} ticket pipeline labels", _ticketPipelineLabels.Count);
                    }
                    else
                    {
                        _ticketPipelineLabels = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                        _logger.LogWarning("Unable to load ticket pipeline labels: {Error}", pipelineResult.Error);
                    }

                    var stageResult = await _hubSpotApiService.GetTicketPropertyOptionsAsync("hs_pipeline_stage");
                    if (stageResult.IsSuccess)
                    {
                        _ticketStageLabels = stageResult.Value;
                        _logger.LogInformation("Cached {Count} ticket stage labels", _ticketStageLabels.Count);
                    }
                    else
                    {
                        _ticketStageLabels = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                        _logger.LogWarning("Unable to load ticket stage labels: {Error}", stageResult.Error);
                    }
                }
                catch (Exception ex)
                {
                    _ticketPipelineLabels = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                    _ticketStageLabels = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                    _logger.LogWarning(ex, "Unexpected error loading ticket property labels");
                }
            }

            private string? ResolveOwnerName(string? ownerId)
            {
                if (string.IsNullOrWhiteSpace(ownerId))
                {
                    return ownerId;
                }

                if (_ownerNames.TryGetValue(ownerId, out var name))
                {
                    return name;
                }

                return ManualOwnerResolver.Resolve(ownerId);
            }

            private string? ResolveTicketPipeline(string? pipelineId)
            {
                if (string.IsNullOrWhiteSpace(pipelineId))
                {
                    return pipelineId;
                }

                return _ticketPipelineLabels.TryGetValue(pipelineId, out var label) ? label : pipelineId;
            }

            private string? ResolveTicketStage(string? stageId)
            {
                if (string.IsNullOrWhiteSpace(stageId))
                {
                    return stageId;
                }

                return _ticketStageLabels.TryGetValue(stageId, out var label) ? label : stageId;
            }

            private static string? NormalizeTicketPipeline(string? pipeline)
            {
                if (string.IsNullOrWhiteSpace(pipeline))
                {
                    return pipeline;
                }

                return pipeline == "0" ? "Support Pipeline" : pipeline;
            }

            private static string? NormalizeTicketStage(string? stage)
            {
                if (string.IsNullOrWhiteSpace(stage))
                {
                    return stage;
                }

                return SupportPipelineStageOverrides.TryGetValue(stage, out var label) ? label : stage;
            }

            private async Task<ContactInfo?> ResolveContactInfoAsync(string? contactId)
            {
                if (string.IsNullOrWhiteSpace(contactId))
                {
                    return null;
                }

                if (TryGetContactInfo(contactId, out var info))
                {
                    return info;
                }

                var contactFromDb = await _unitOfWork.ContactRepository.FirstOrDefaultAsync(
                    c => c.HubSpotId == contactId || c.RecordId == contactId);

                if (contactFromDb != null)
                {
                    RememberContactInfo(contactFromDb);
                    return new ContactInfo(GetDisplayName(contactFromDb.FullName, contactFromDb.Email, contactId), contactFromDb.Email);
                }

                var apiResult = await _hubSpotApiService.GetContactByIdAsync(contactId);
                if (apiResult.IsFailure)
                {
                    _logger.LogWarning("Unable to fetch contact {ContactId} from HubSpot: {Error}", contactId, apiResult.Error);
                    return null;
                }

                var contactResult = Contact.CreateFromHubSpotData(apiResult.Value);
                if (contactResult.IsFailure)
                {
                    _logger.LogWarning("Unable to parse contact data for {ContactId}: {Error}", contactId, contactResult.Error);
                    return null;
                }

                var contact = contactResult.Value;
                RememberContactInfo(contact);
                return new ContactInfo(GetDisplayName(contact.FullName, contact.Email, contactId), contact.Email);
            }

            private void RememberContactInfo(Contact contact)
            {
                if (string.IsNullOrWhiteSpace(contact.HubSpotId))
                {
                    return;
                }

                var displayName = string.IsNullOrWhiteSpace(contact.FullName)
                    ? contact.Email
                    : contact.FullName;

                if (string.IsNullOrWhiteSpace(displayName))
                {
                    displayName = contact.HubSpotId;
                }

                var info = new ContactInfo(displayName, contact.Email);
                _contactInfos[contact.HubSpotId] = info;

                if (!string.IsNullOrWhiteSpace(contact.RecordId))
                {
                    _contactInfos[contact.RecordId] = info;
                }
            }

            private string? ResolveContactName(string? contactId)
            {
                return TryGetContactInfo(contactId, out var info) ? info.Name : contactId;
            }

            private bool TryGetContactInfo(string? contactId, out ContactInfo info)
            {
                if (string.IsNullOrWhiteSpace(contactId))
                {
                    info = default!;
                    return false;
                }

                return _contactInfos.TryGetValue(contactId, out info!);
            }

            private static string? GetDisplayName(string? fullName, string? email, string fallback)
            {
                if (!string.IsNullOrWhiteSpace(fullName))
                {
                    return fullName;
                }

                if (!string.IsNullOrWhiteSpace(email))
                {
                    return email;
                }

                return fallback;
            }

            private record ContactInfo(string? Name, string? Email);

            private async Task<Result> ExtractAndLoadObjectAssociationsAsync(string sourceObjectType, string targetObjectType, Func<Task<Result<List<string>>>> getSourceIdsFunc)
            {
                try
                {
                    _logger.LogInformation("Starting ETL process for {SourceType} â†’ {TargetType} Associations", sourceObjectType, targetObjectType);
                    var sourceIdsResult = await getSourceIdsFunc();
                    if (sourceIdsResult.IsFailure) return Result.Failure($"Failed to get {sourceObjectType} IDs: {sourceIdsResult.Error}");
                    var sourceIds = sourceIdsResult.Value;
                    if (sourceIds == null || sourceIds.Count == 0) { _logger.LogInformation("No {SourceType} objects found, skipping associations", sourceObjectType); return Result.Success(); }
                    _logger.LogInformation("Found {Count} {SourceType} objects to process associations for", sourceIds.Count, sourceObjectType);
                    var associationsResult = await _hubSpotApiService.GetObjectAssociationsBatchAsync(sourceObjectType, targetObjectType, sourceIds);
                    if (associationsResult.IsFailure) { _logger.LogWarning("Failed to fetch {SourceType} â†’ {TargetType} associations: {Error}", sourceObjectType, targetObjectType, associationsResult.Error); return Result.Success(); }
                    var associations = associationsResult.Value;
                    if (associations == null || associations.Count == 0) { _logger.LogInformation("No {SourceType} â†’ {TargetType} associations found", sourceObjectType, targetObjectType); return Result.Success(); }
                    var allAssociations = new List<ObjectAssociation>();
                    foreach (var kvp in associations)
                    {
                        foreach (var details in kvp.Value)
                        {
                            var labels = new List<string>();
                            if (!string.IsNullOrWhiteSpace(details.PrimaryLabel)) labels.Add(details.PrimaryLabel);
                            labels.AddRange(details.AllLabels.Where(l => !labels.Contains(l, StringComparer.OrdinalIgnoreCase)));
                            string? labelsJson = labels.Count > 0 ? JsonSerializer.Serialize(labels) : null;
                            var associationResult = ObjectAssociation.CreateFromHubSpotData(sourceObjectType, kvp.Key, targetObjectType, details.TargetObjectId, details.PrimaryLabel, labelsJson, details.AssociationTypeId, details.AssociationCategory, details.CreatedAt, details.UpdatedAt, details.Source, details.SourceId);
                            if (associationResult.IsSuccess) allAssociations.Add(associationResult.Value);
                        }
                    }
                    if (allAssociations.Count == 0) { _logger.LogInformation("No {SourceType} â†’ {TargetType} associations to save", sourceObjectType, targetObjectType); return Result.Success(); }
                    await UpsertObjectAssociationsAsync(allAssociations);
                    _logger.LogInformation("Successfully completed ETL for {SourceType} â†’ {TargetType} Associations. Total processed: {Total}", sourceObjectType, targetObjectType, allAssociations.Count);
                    return Result.Success();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error occurred during {SourceType} â†’ {TargetType} Associations ETL process", sourceObjectType, targetObjectType);
                    return Result.Failure($"{sourceObjectType} â†’ {targetObjectType} Associations ETL failed: {ex.Message}");
                }
            }

            private async Task UpsertObjectAssociationsAsync(List<ObjectAssociation> associations)
            {
                int inserted = 0, updated = 0, batchCount = 0;
                foreach (var association in associations)
                {
                    var existing = await _unitOfWork.ObjectAssociationRepository.FirstOrDefaultAsync(a => a.SourceObjectType == association.SourceObjectType && a.SourceObjectId == association.SourceObjectId && a.TargetObjectType == association.TargetObjectType && a.TargetObjectId == association.TargetObjectId);
                    if (existing != null)
                    {
                        existing.ETLDate = DateTime.UtcNow;
                        if (!string.IsNullOrWhiteSpace(association.AssociationLabel)) existing.AssociationLabel = association.AssociationLabel;
                        if (!string.IsNullOrWhiteSpace(association.AssociationLabelsJson)) existing.AssociationLabelsJson = association.AssociationLabelsJson;
                        if (association.AssociationTypeId.HasValue) existing.AssociationTypeId = association.AssociationTypeId;
                        if (!string.IsNullOrWhiteSpace(association.AssociationCategory)) existing.AssociationCategory = association.AssociationCategory;
                        if (association.AssociationCreatedAt.HasValue) existing.AssociationCreatedAt = association.AssociationCreatedAt;
                        if (association.AssociationUpdatedAt.HasValue) existing.AssociationUpdatedAt = association.AssociationUpdatedAt;
                        if (!string.IsNullOrWhiteSpace(association.AssociationSource)) existing.AssociationSource = association.AssociationSource;
                        if (!string.IsNullOrWhiteSpace(association.AssociationSourceId)) existing.AssociationSourceId = association.AssociationSourceId;
                        existing.IsPrimary = association.IsPrimary;
                        await _unitOfWork.ObjectAssociationRepository.UpdateAsync(existing);
                        updated++;
                    }
                    else
                    {
                        await _unitOfWork.ObjectAssociationRepository.AddAsync(association);
                        inserted++;
                    }
                    batchCount++;
                    if (batchCount >= SAVE_BATCH_SIZE) { await _unitOfWork.SaveChangesAsync(); _logger.LogInformation("Saved batch of object associations. Inserted: {Inserted}, Updated: {Updated}", inserted, updated); batchCount = 0; }
                }
                if (batchCount > 0) await _unitOfWork.SaveChangesAsync();
                _logger.LogInformation("Completed upsert of object associations. Inserted: {Inserted}, Updated: {Updated}", inserted, updated);
            }

            public async Task<Result> ExtractAndLoadPropertyHistoryAsync()
            {
                try
                {
                    _logger.LogInformation("Starting ETL process for Property History (Contacts: hs_lead_status, Deals: dealstage, Tickets: hs_pipeline_stage, Companies: meeting_invite)");

                    var allPropertyHistories = new List<PropertyHistory>();
                    int totalProcessed = 0;
                    int totalErrors = 0;

                    // Use execution strategy to handle connection retries
                    var dbContext = _unitOfWork.GetDbContext();
                    var strategy = dbContext.Database.CreateExecutionStrategy();
                    
                    // Process Contacts - track hs_lead_status changes
                    _logger.LogInformation("Extracting property history for Contacts (hs_lead_status)...");
                    List<Contact> allContacts;
                    try
                    {
                        allContacts = await strategy.ExecuteAsync(
                            dbContext,
                            async (context, state, ct) => (await ((ETLHubspotDbContext)context).Contacts.ToListAsync(ct)),
                            null,
                            CancellationToken.None);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to load contacts for property history extraction");
                        return Result.Failure($"Failed to load contacts: {ex.Message}");
                    }
                    foreach (var contact in allContacts)
                    {
                        try
                        {
                            var historyResult = await _hubSpotApiService.GetObjectPropertyHistoryAsync("contact", contact.HubSpotId, "hs_lead_status");
                            if (historyResult.IsSuccess)
                            {
                                var historyJson = historyResult.Value;
                                if (historyJson.TryGetProperty("propertiesWithHistory", out var propertiesWithHistory))
                                {
                                    if (propertiesWithHistory.TryGetProperty("hs_lead_status", out var leadStatusHistory))
                                    {
                                        var histories = PropertyHistory.CreateFromHubSpotHistory("contact", contact.HubSpotId, "hs_lead_status", leadStatusHistory);
                                        allPropertyHistories.AddRange(histories);
                                        totalProcessed++;
                                    }
                                }
                            }
                            else
                            {
                                totalErrors++;
                                _logger.LogWarning("Failed to fetch hs_lead_status history for contact {ContactId}: {Error}", contact.HubSpotId, historyResult.Error);
                            }
                        }
                        catch (Exception ex)
                        {
                            totalErrors++;
                            _logger.LogWarning(ex, "Error fetching hs_lead_status history for contact {ContactId}", contact.HubSpotId);
                        }
                    }

                    // Process Deals - track dealstage changes
                    _logger.LogInformation("Extracting property history for Deals (dealstage)...");
                    List<Deal> allDeals;
                    try
                    {
                        allDeals = await strategy.ExecuteAsync(
                            dbContext,
                            async (context, state, ct) => (await ((ETLHubspotDbContext)context).Deals.ToListAsync(ct)),
                            null,
                            CancellationToken.None);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to load deals for property history extraction");
                        return Result.Failure($"Failed to load deals: {ex.Message}");
                    }
                    foreach (var deal in allDeals)
                    {
                        try
                        {
                            var historyResult = await _hubSpotApiService.GetObjectPropertyHistoryAsync("deal", deal.HubSpotId, "dealstage");
                            if (historyResult.IsSuccess)
                            {
                                var historyJson = historyResult.Value;
                                if (historyJson.TryGetProperty("propertiesWithHistory", out var propertiesWithHistory))
                                {
                                    if (propertiesWithHistory.TryGetProperty("dealstage", out var dealstageHistory))
                                    {
                                        var histories = PropertyHistory.CreateFromHubSpotHistory("deal", deal.HubSpotId, "dealstage", dealstageHistory);
                                        allPropertyHistories.AddRange(histories);
                                        totalProcessed++;
                                    }
                                }
                            }
                            else
                            {
                                totalErrors++;
                                _logger.LogWarning("Failed to fetch dealstage history for deal {DealId}: {Error}", deal.HubSpotId, historyResult.Error);
                            }
                        }
                        catch (Exception ex)
                        {
                            totalErrors++;
                            _logger.LogWarning(ex, "Error fetching dealstage history for deal {DealId}", deal.HubSpotId);
                        }
                    }

                    // Process Tickets - track hs_pipeline_stage changes
                    _logger.LogInformation("Extracting property history for Tickets (hs_pipeline_stage)...");
                    var allTickets = (await _unitOfWork.TicketRepository.GetAllAsync()).ToList();
                    foreach (var ticket in allTickets)
                    {
                        try
                        {
                            var historyResult = await _hubSpotApiService.GetObjectPropertyHistoryAsync("ticket", ticket.HubSpotId, "hs_pipeline_stage");
                            if (historyResult.IsSuccess)
                            {
                                var historyJson = historyResult.Value;
                                if (historyJson.TryGetProperty("propertiesWithHistory", out var propertiesWithHistory))
                                {
                                    if (propertiesWithHistory.TryGetProperty("hs_pipeline_stage", out var stageHistory))
                                    {
                                        var histories = PropertyHistory.CreateFromHubSpotHistory("ticket", ticket.HubSpotId, "hs_pipeline_stage", stageHistory);
                                        allPropertyHistories.AddRange(histories);
                                        totalProcessed++;
                                    }
                                }
                            }
                            else
                            {
                                totalErrors++;
                                _logger.LogWarning("Failed to fetch hs_pipeline_stage history for ticket {TicketId}: {Error}", ticket.HubSpotId, historyResult.Error);
                            }
                        }
                        catch (Exception ex)
                        {
                            totalErrors++;
                            _logger.LogWarning(ex, "Error fetching hs_pipeline_stage history for ticket {TicketId}", ticket.HubSpotId);
                        }
                    }

                    // Process Companies - track meeting_invite changes
                    _logger.LogInformation("Extracting property history for Companies (meeting_invite)...");
                    List<Company> allCompanies;
                    try
                    {
                        allCompanies = await strategy.ExecuteAsync(
                            dbContext,
                            async (context, state, ct) => (await ((ETLHubspotDbContext)context).Companies.ToListAsync(ct)),
                            null,
                            CancellationToken.None);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to load companies for property history extraction");
                        return Result.Failure($"Failed to load companies: {ex.Message}");
                    }
                    foreach (var company in allCompanies)
                    {
                        try
                        {
                            var historyResult = await _hubSpotApiService.GetObjectPropertyHistoryAsync("company", company.HubSpotId, "meeting_invite");
                            if (historyResult.IsSuccess)
                            {
                                var historyJson = historyResult.Value;
                                if (historyJson.TryGetProperty("propertiesWithHistory", out var propertiesWithHistory))
                                {
                                    if (propertiesWithHistory.TryGetProperty("meeting_invite", out var meetingInviteHistory))
                                    {
                                        var histories = PropertyHistory.CreateFromHubSpotHistory("company", company.HubSpotId, "meeting_invite", meetingInviteHistory);
                                        allPropertyHistories.AddRange(histories);
                                        totalProcessed++;
                                    }
                                }
                            }
                            else
                            {
                                totalErrors++;
                                _logger.LogWarning("Failed to fetch meeting_invite history for company {CompanyId}: {Error}", company.HubSpotId, historyResult.Error);
                            }
                        }
                        catch (Exception ex)
                        {
                            totalErrors++;
                            _logger.LogWarning(ex, "Error fetching meeting_invite history for company {CompanyId}", company.HubSpotId);
                        }
                    }

                    _logger.LogInformation("Total property history records extracted: {Total}. Processing {Count} objects, {Errors} errors.", 
                        allPropertyHistories.Count, totalProcessed, totalErrors);

                    // Upsert property histories (avoid duplicates based on ObjectType + ObjectId + PropertyName + ChangeDate)
                    int inserted = 0;
                    int updated = 0;
                    int batchCount = 0;

                    foreach (var history in allPropertyHistories)
                    {
                        var existing = await _unitOfWork.PropertyHistoryRepository.FirstOrDefaultAsync(
                            h => h.ObjectType == history.ObjectType &&
                                 h.ObjectId == history.ObjectId &&
                                 h.PropertyName == history.PropertyName &&
                                 h.ChangeDate == history.ChangeDate);

                        if (existing != null)
                        {
                            // Update existing record
                            existing.NewValue = history.NewValue ?? existing.NewValue;  // SWAPPED: was OldValue
                            existing.OldValue = history.OldValue ?? existing.OldValue;  // SWAPPED: was NewValue
                            existing.Source = history.Source ?? existing.Source;
                            existing.SourceId = history.SourceId ?? existing.SourceId;
                            existing.ETLDate = DateTime.UtcNow;
                            await _unitOfWork.PropertyHistoryRepository.UpdateAsync(existing);
                            updated++;
                        }
                        else
                        {
                            // Insert new record
                            await _unitOfWork.PropertyHistoryRepository.AddAsync(history);
                            inserted++;
                        }

                        batchCount++;
                        if (batchCount >= SAVE_BATCH_SIZE)
                        {
                            await _unitOfWork.SaveChangesAsync();
                            _logger.LogInformation("Saved batch of property histories. Inserted: {Inserted}, Updated: {Updated}", 
                                inserted, updated);
                            batchCount = 0;
                        }
                    }

                    if (batchCount > 0)
                    {
                        await _unitOfWork.SaveChangesAsync();
                        _logger.LogInformation("Saved final batch of property histories. Inserted: {Inserted}, Updated: {Updated}", 
                            inserted, updated);
                    }

                    _logger.LogInformation("Successfully completed ETL for Property History. Total records: {Total}, Inserted: {Inserted}, Updated: {Updated}", 
                        allPropertyHistories.Count, inserted, updated);
                    return Result.Success();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error occurred during Property History ETL process");
                    return Result.Failure($"Property History ETL failed: {ex.Message}");
                }
            }

            /// <summary>
            /// Extracts and loads Contact Activity Timeline - tracks all activities and events related to contacts
            /// </summary>
            public async Task<Result> ExtractAndLoadContactActivityTimelineAsync()
            {
                try
                {
                    _logger.LogInformation("Starting ETL process for Contact Activity Timeline...");

                    var allTimelineEvents = new List<ContactActivityTimeline>();
                    var dbContext = _unitOfWork.GetDbContext();
                    var strategy = dbContext.Database.CreateExecutionStrategy();

                    // 1. Process Email Activities (sent, opened, clicked)
                    _logger.LogInformation("Processing email activities for contacts...");
                    try
                    {
                        var allActivities = await strategy.ExecuteAsync(
                            dbContext,
                            async (context, state, ct) => await ((ETLHubspotDbContext)context).Activities
                                .Include(a => a.EmailDetail)
                                .Where(a => a.ActivityType == "EMAIL")
                                .ToListAsync(ct),
                            null,
                            CancellationToken.None);

                        _logger.LogInformation("Found {Count} email activities", allActivities.Count);

                        // Get all activity associations for these activities
                        var activityIds = allActivities.Select(a => a.HubSpotId).ToList();
                        var allActivityAssociations = await strategy.ExecuteAsync(
                            dbContext,
                            async (context, state, ct) => await ((ETLHubspotDbContext)context).ActivityAssociations
                                .Where(aa => activityIds.Contains(aa.ActivityHubSpotId))
                                .ToListAsync(ct),
                            null,
                            CancellationToken.None);

                        _logger.LogInformation("Found {Count} activity associations for emails", allActivityAssociations.Count);

                        var associationsByActivity = allActivityAssociations
                            .GroupBy(aa => aa.ActivityHubSpotId)
                            .ToDictionary(g => g.Key, g => g.ToList(), StringComparer.OrdinalIgnoreCase);

                        int emailEventsAdded = 0;
                        foreach (var activity in allActivities)
                        {
                            // Find contacts associated with this email activity
                            var contactAssociations = associationsByActivity.TryGetValue(activity.HubSpotId, out var assocs)
                                ? assocs.Where(aa => aa.AssociatedObjectType.Equals("contact", StringComparison.OrdinalIgnoreCase)).ToList()
                                : new List<ActivityAssociation>();

                            if (!contactAssociations.Any() && !string.IsNullOrEmpty(activity.SourceObjectId) && 
                                activity.SourceObjectType?.Equals("contact", StringComparison.OrdinalIgnoreCase) == true)
                            {
                                // Use source object if no associations found
                                contactAssociations = new List<ActivityAssociation> 
                                { 
                                    new ActivityAssociation(activity.HubSpotId, "contact", activity.SourceObjectId)
                                };
                            }

                            if (!contactAssociations.Any())
                            {
                                _logger.LogDebug("Email activity {ActivityId} has no contact associations, skipping", activity.HubSpotId);
                                continue;
                            }

                            foreach (var contactAssoc in contactAssociations)
                            {
                                var contactId = contactAssoc.AssociatedObjectId;
                                var emailDetail = activity.EmailDetail;
                                var eventDate = activity.ActivityDate ?? DateTime.UtcNow;

                                // Determine if email is sent or received based on direction
                                var emailDirection = emailDetail?.EmailDirection?.ToUpperInvariant();
                                var isReceived = emailDirection == "INCOMING" || emailDirection == "RECEIVED" || 
                                                emailDirection == "INBOUND" || emailDirection == "IN";
                                var eventType = isReceived ? "email_received" : "email_sent";
                                
                                var emailDescription = isReceived ? "Email received from contact" : "Email sent to contact";
                                if (!string.IsNullOrEmpty(activity.Subject))
                                {
                                    emailDescription += $": {activity.Subject}";
                                }
                                if (!string.IsNullOrEmpty(activity.ActivityOwner))
                                {
                                    emailDescription += $" by {activity.ActivityOwner}";
                                }

                                var metadata = new Dictionary<string, object>();
                                if (emailDetail != null)
                                {
                                    if (!string.IsNullOrEmpty(emailDetail.EmailDirection))
                                        metadata["direction"] = emailDetail.EmailDirection;
                                    if (!string.IsNullOrEmpty(emailDetail.Status))
                                        metadata["status"] = emailDetail.Status;
                                }

                                allTimelineEvents.Add(new ContactActivityTimeline(
                                    contactId,
                                    eventType,
                                    eventDate,
                                    emailDescription,
                                    "email",
                                    activity.HubSpotId,
                                    activity.Subject,
                                    activity.ActivityOwner,
                                    ResolveOwnerName(activity.ActivityOwner),
                                    metadata.Any() ? System.Text.Json.JsonSerializer.Serialize(metadata) : null));
                                emailEventsAdded++;

                                // Email opened events (if tracked)
                                if (emailDetail != null && !string.IsNullOrEmpty(emailDetail.NumberOfEmailOpens))
                                {
                                    if (int.TryParse(emailDetail.NumberOfEmailOpens, out var openCount) && openCount > 0)
                                    {
                                        var openDescription = $"Email opened {openCount} time(s)";
                                        if (!string.IsNullOrEmpty(activity.Subject))
                                        {
                                            openDescription += $": {activity.Subject}";
                                        }
                                        if (!string.IsNullOrEmpty(emailDetail.EmailOpenRate))
                                        {
                                            openDescription += $" (Open rate: {emailDetail.EmailOpenRate})";
                                        }

                                        allTimelineEvents.Add(new ContactActivityTimeline(
                                            contactId,
                                            "email_opened",
                                            eventDate,
                                            openDescription,
                                            "email",
                                            activity.HubSpotId,
                                            activity.Subject,
                                            null,
                                            null,
                                            $"{{\"openCount\": {openCount}, \"openRate\": \"{emailDetail.EmailOpenRate}\"}}"));
                                    }
                                }

                                // Email clicked events (if tracked)
                                if (emailDetail != null && !string.IsNullOrEmpty(emailDetail.NumberOfEmailClicks))
                                {
                                    if (int.TryParse(emailDetail.NumberOfEmailClicks, out var clickCount) && clickCount > 0)
                                    {
                                        var clickDescription = $"Email clicked {clickCount} time(s)";
                                        if (!string.IsNullOrEmpty(activity.Subject))
                                        {
                                            clickDescription += $": {activity.Subject}";
                                        }
                                        if (!string.IsNullOrEmpty(emailDetail.EmailClickRate))
                                        {
                                            clickDescription += $" (Click rate: {emailDetail.EmailClickRate})";
                                        }

                                        allTimelineEvents.Add(new ContactActivityTimeline(
                                            contactId,
                                            "email_clicked",
                                            eventDate,
                                            clickDescription,
                                            "email",
                                            activity.HubSpotId,
                                            activity.Subject,
                                            null,
                                            null,
                                            $"{{\"clickCount\": {clickCount}, \"clickRate\": \"{emailDetail.EmailClickRate}\"}}"));
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error processing email activities");
                    }

                    // 2. Process Ticket Activities
                    _logger.LogInformation("Processing ticket activities for contacts...");
                    try
                    {
                        var allTickets = await strategy.ExecuteAsync(
                            dbContext,
                            async (context, state, ct) => await ((ETLHubspotDbContext)context).Tickets.ToListAsync(ct),
                            null,
                            CancellationToken.None);

                        // Get ticket associations to contacts
                        var ticketAssociations = (await _unitOfWork.ObjectAssociationRepository.FindAsync(
                            oa => oa.SourceObjectType == "ticket" && oa.TargetObjectType == "contact")).ToList();

                        var ticketAssocDict = ticketAssociations.GroupBy(ta => ta.SourceObjectId)
                            .ToDictionary(g => g.Key, g => g.Select(ta => ta.TargetObjectId).ToList(), StringComparer.OrdinalIgnoreCase);

                        foreach (var ticket in allTickets)
                        {
                            if (ticketAssocDict.TryGetValue(ticket.HubSpotId, out var contactIds))
                            {
                                foreach (var contactId in contactIds)
                                {
                                    var ticketCreatedDate = ticket.CreatedAt ?? ticket.ETLDate;
                                    var ticketDescription = $"Ticket created";
                                    if (!string.IsNullOrEmpty(ticket.TicketName))
                                    {
                                        ticketDescription += $": {ticket.TicketName}";
                                    }
                                    if (!string.IsNullOrEmpty(ticket.TicketStatus))
                                    {
                                        ticketDescription += $" (Status: {ticket.TicketStatus})";
                                    }
                                    if (!string.IsNullOrEmpty(ticket.TicketOwner))
                                    {
                                        ticketDescription += $" - Assigned to {ticket.TicketOwner}";
                                    }

                                    allTimelineEvents.Add(new ContactActivityTimeline(
                                        contactId,
                                        "ticket_created",
                                        ticketCreatedDate,
                                        ticketDescription,
                                        "ticket",
                                        ticket.HubSpotId,
                                        ticket.TicketName,
                                        ticket.TicketOwner,
                                        ResolveOwnerName(ticket.TicketOwner),
                                        null));

                                    // Ticket status changes from PropertyHistory
                                    var ticketStatusHistory = await _unitOfWork.PropertyHistoryRepository.FindAsync(
                                        ph => ph.ObjectType == "ticket" && 
                                              ph.ObjectId == ticket.HubSpotId && 
                                              ph.PropertyName == "hs_pipeline_stage");

                                    foreach (var statusChange in ticketStatusHistory.OrderBy(ph => ph.ChangeDate))
                                    {
                                        var statusDescription = $"Ticket status changed";
                                        if (!string.IsNullOrEmpty(statusChange.OldValue))
                                        {
                                            statusDescription += $" from {statusChange.OldValue}";
                                        }
                                        if (!string.IsNullOrEmpty(statusChange.NewValue))
                                        {
                                            statusDescription += $" to {statusChange.NewValue}";
                                        }
                                        if (!string.IsNullOrEmpty(ticket.TicketName))
                                        {
                                            statusDescription += $": {ticket.TicketName}";
                                        }

                                        allTimelineEvents.Add(new ContactActivityTimeline(
                                            contactId,
                                            "ticket_status_changed",
                                            statusChange.ChangeDate,
                                            statusDescription,
                                            "ticket",
                                            ticket.HubSpotId,
                                            ticket.TicketName,
                                            null,
                                            null,
                                            $"{{\"oldStatus\": \"{statusChange.OldValue}\", \"newStatus\": \"{statusChange.NewValue}\"}}"));
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error processing ticket activities");
                    }

                    // 3. Process Lifecycle Stage Changes
                    _logger.LogInformation("Processing lifecycle stage changes for contacts...");
                    try
                    {
                        var lifecycleChanges = await _unitOfWork.PropertyHistoryRepository.FindAsync(
                            ph => ph.ObjectType == "contact" && ph.PropertyName == "lifecyclestage");

                        foreach (var change in lifecycleChanges)
                        {
                            var description = $"Lifecycle stage changed";
                            if (!string.IsNullOrEmpty(change.OldValue))
                            {
                                description += $" from {change.OldValue}";
                            }
                            if (!string.IsNullOrEmpty(change.NewValue))
                            {
                                description += $" to {change.NewValue}";
                            }

                            allTimelineEvents.Add(new ContactActivityTimeline(
                                change.ObjectId,
                                "lifecycle_changed",
                                change.ChangeDate,
                                description,
                                "contact",
                                change.ObjectId,
                                null,
                                change.Source,
                                null,
                                $"{{\"oldValue\": \"{change.OldValue}\", \"newValue\": \"{change.NewValue}\"}}"));
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error processing lifecycle changes");
                    }

                    // 4. Process Lead Status Changes
                    _logger.LogInformation("Processing lead status changes for contacts...");
                    try
                    {
                        var leadStatusChanges = await _unitOfWork.PropertyHistoryRepository.FindAsync(
                            ph => ph.ObjectType == "contact" && ph.PropertyName == "hs_lead_status");

                        foreach (var change in leadStatusChanges)
                        {
                            var description = $"Lead status changed";
                            if (!string.IsNullOrEmpty(change.OldValue))
                            {
                                description += $" from {change.OldValue}";
                            }
                            if (!string.IsNullOrEmpty(change.NewValue))
                            {
                                description += $" to {change.NewValue}";
                            }

                            allTimelineEvents.Add(new ContactActivityTimeline(
                                change.ObjectId,
                                "lead_status_changed",
                                change.ChangeDate,
                                description,
                                "contact",
                                change.ObjectId,
                                null,
                                change.Source,
                                null,
                                $"{{\"oldValue\": \"{change.OldValue}\", \"newValue\": \"{change.NewValue}\"}}"));
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error processing lead status changes");
                    }

                    // 5. Process Other Activities (Calls, Meetings, Tasks, Notes, SMS)
                    _logger.LogInformation("Processing other activities for contacts...");
                    try
                    {
                        var otherActivities = await strategy.ExecuteAsync(
                            dbContext,
                            async (context, state, ct) => await ((ETLHubspotDbContext)context).Activities
                                .Include(a => a.CallDetail)
                                .Include(a => a.MeetingDetail)
                                .Include(a => a.TaskDetail)
                                .Include(a => a.NoteDetail)
                                .Where(a => a.ActivityType != "EMAIL")
                                .ToListAsync(ct),
                            null,
                            CancellationToken.None);

                        // Get all activity associations for these activities
                        var otherActivityIds = otherActivities.Select(a => a.HubSpotId).ToList();
                        var otherActivityAssociations = await strategy.ExecuteAsync(
                            dbContext,
                            async (context, state, ct) => await ((ETLHubspotDbContext)context).ActivityAssociations
                                .Where(aa => otherActivityIds.Contains(aa.ActivityHubSpotId))
                                .ToListAsync(ct),
                            null,
                            CancellationToken.None);

                        var otherAssociationsByActivity = otherActivityAssociations
                            .GroupBy(aa => aa.ActivityHubSpotId)
                            .ToDictionary(g => g.Key, g => g.ToList(), StringComparer.OrdinalIgnoreCase);

                        foreach (var activity in otherActivities)
                        {
                            var contactAssociations = otherAssociationsByActivity.TryGetValue(activity.HubSpotId, out var assocs)
                                ? assocs.Where(aa => aa.AssociatedObjectType.Equals("contact", StringComparison.OrdinalIgnoreCase)).ToList()
                                : new List<ActivityAssociation>();

                            if (!contactAssociations.Any() && !string.IsNullOrEmpty(activity.SourceObjectId) && 
                                activity.SourceObjectType?.Equals("contact", StringComparison.OrdinalIgnoreCase) == true)
                            {
                                contactAssociations = new List<ActivityAssociation> 
                                { 
                                    new ActivityAssociation(activity.HubSpotId, "contact", activity.SourceObjectId)
                                };
                            }

                            foreach (var contactAssoc in contactAssociations)
                            {
                                var contactId = contactAssoc.AssociatedObjectId;
                                var eventDate = activity.ActivityDate ?? DateTime.UtcNow;
                                var activityType = activity.ActivityType?.ToUpperInvariant();
                                
                                switch (activityType)
                                {
                                    case "CALL":
                                        var callDetail = activity.CallDetail;
                                        var callDescription = "Call";
                                        if (!string.IsNullOrEmpty(callDetail?.CallTitle))
                                        {
                                            callDescription += $": {callDetail.CallTitle}";
                                        }
                                        else if (!string.IsNullOrEmpty(activity.Subject))
                                        {
                                            callDescription += $": {activity.Subject}";
                                        }
                                        
                                        if (!string.IsNullOrEmpty(callDetail?.CallDirection))
                                        {
                                            callDescription += $" ({callDetail.CallDirection})";
                                        }
                                        if (!string.IsNullOrEmpty(callDetail?.Status))
                                        {
                                            callDescription += $" - Status: {callDetail.Status}";
                                        }
                                        if (!string.IsNullOrEmpty(activity.ActivityOwner))
                                        {
                                            callDescription += $" by {activity.ActivityOwner}";
                                        }

                                        var callMetadata = new Dictionary<string, object>();
                                        if (callDetail != null)
                                        {
                                            if (!string.IsNullOrEmpty(callDetail.Direction))
                                                callMetadata["direction"] = callDetail.Direction;
                                            if (!string.IsNullOrEmpty(callDetail.Status))
                                                callMetadata["status"] = callDetail.Status;
                                            if (!string.IsNullOrEmpty(callDetail.CallDirection))
                                                callMetadata["callDirection"] = callDetail.CallDirection;
                                        }

                                        allTimelineEvents.Add(new ContactActivityTimeline(
                                            contactId,
                                            "call_created",
                                            eventDate,
                                            callDescription,
                                            "call",
                                            activity.HubSpotId,
                                            activity.Subject ?? callDetail?.CallTitle,
                                            activity.ActivityOwner,
                                            ResolveOwnerName(activity.ActivityOwner),
                                            callMetadata.Any() ? System.Text.Json.JsonSerializer.Serialize(callMetadata) : null));
                                        break;

                                    case "MEETING":
                                        var meetingDetail = activity.MeetingDetail;
                                        var meetingDescription = "Meeting";
                                        if (!string.IsNullOrEmpty(meetingDetail?.MeetingName))
                                        {
                                            meetingDescription += $": {meetingDetail.MeetingName}";
                                        }
                                        else if (!string.IsNullOrEmpty(activity.Subject))
                                        {
                                            meetingDescription += $": {activity.Subject}";
                                        }
                                        
                                        if (!string.IsNullOrEmpty(activity.Status))
                                        {
                                            meetingDescription += $" - Status: {activity.Status}";
                                        }
                                        if (meetingDetail?.StartTime.HasValue == true)
                                        {
                                            meetingDescription += $" (Start: {meetingDetail.StartTime.Value:yyyy-MM-dd HH:mm})";
                                        }
                                        if (!string.IsNullOrEmpty(meetingDetail?.MeetingLocation))
                                        {
                                            meetingDescription += $" - Location: {meetingDetail.MeetingLocation}";
                                        }
                                        if (!string.IsNullOrEmpty(activity.ActivityOwner))
                                        {
                                            meetingDescription += $" by {activity.ActivityOwner}";
                                        }

                                        var meetingMetadata = new Dictionary<string, object>();
                                        if (meetingDetail != null)
                                        {
                                            if (meetingDetail.StartTime.HasValue)
                                                meetingMetadata["startTime"] = meetingDetail.StartTime.Value;
                                            if (meetingDetail.EndTime.HasValue)
                                                meetingMetadata["endTime"] = meetingDetail.EndTime.Value;
                                            if (!string.IsNullOrEmpty(meetingDetail.LocationType))
                                                meetingMetadata["locationType"] = meetingDetail.LocationType;
                                            if (!string.IsNullOrEmpty(meetingDetail.MeetingLocation))
                                                meetingMetadata["location"] = meetingDetail.MeetingLocation;
                                        }
                                        if (!string.IsNullOrEmpty(activity.Status))
                                            meetingMetadata["status"] = activity.Status;

                                        allTimelineEvents.Add(new ContactActivityTimeline(
                                            contactId,
                                            "meeting_created",
                                            eventDate,
                                            meetingDescription,
                                            "meeting",
                                            activity.HubSpotId,
                                            activity.Subject ?? meetingDetail?.MeetingName,
                                            activity.ActivityOwner,
                                            ResolveOwnerName(activity.ActivityOwner),
                                            meetingMetadata.Any() ? System.Text.Json.JsonSerializer.Serialize(meetingMetadata) : null));
                                        break;

                                    case "TASK":
                                        var taskDetail = activity.TaskDetail;
                                        var taskDescription = "Task";
                                        if (!string.IsNullOrEmpty(activity.Subject))
                                        {
                                            taskDescription += $": {activity.Subject}";
                                        }
                                        
                                        if (!string.IsNullOrEmpty(taskDetail?.TaskType))
                                        {
                                            taskDescription += $" (Type: {taskDetail.TaskType})";
                                        }
                                        if (!string.IsNullOrEmpty(taskDetail?.Status))
                                        {
                                            taskDescription += $" - Status: {taskDetail.Status}";
                                        }
                                        if (!string.IsNullOrEmpty(taskDetail?.Priority))
                                        {
                                            taskDescription += $" - Priority: {taskDetail.Priority}";
                                        }
                                        if (!string.IsNullOrEmpty(activity.ActivityOwner))
                                        {
                                            taskDescription += $" by {activity.ActivityOwner}";
                                        }

                                        var taskMetadata = new Dictionary<string, object>();
                                        if (taskDetail != null)
                                        {
                                            if (!string.IsNullOrEmpty(taskDetail.TaskType))
                                                taskMetadata["taskType"] = taskDetail.TaskType;
                                            if (!string.IsNullOrEmpty(taskDetail.Status))
                                                taskMetadata["status"] = taskDetail.Status;
                                            if (!string.IsNullOrEmpty(taskDetail.Priority))
                                                taskMetadata["priority"] = taskDetail.Priority;
                                        }
                                        if (!string.IsNullOrEmpty(activity.Status))
                                            taskMetadata["activityStatus"] = activity.Status;

                                        allTimelineEvents.Add(new ContactActivityTimeline(
                                            contactId,
                                            "task_created",
                                            eventDate,
                                            taskDescription,
                                            "task",
                                            activity.HubSpotId,
                                            activity.Subject,
                                            activity.ActivityOwner,
                                            ResolveOwnerName(activity.ActivityOwner),
                                            taskMetadata.Any() ? System.Text.Json.JsonSerializer.Serialize(taskMetadata) : null));
                                        break;

                                    case "NOTE":
                                        var noteDescription = "Note";
                                        if (!string.IsNullOrEmpty(activity.Subject))
                                        {
                                            noteDescription += $": {activity.Subject}";
                                        }
                                        else if (!string.IsNullOrEmpty(activity.Body))
                                        {
                                            var bodyPreview = activity.Body.Length > 100 
                                                ? activity.Body.Substring(0, 100) + "..." 
                                                : activity.Body;
                                            noteDescription += $": {bodyPreview}";
                                        }
                                        if (!string.IsNullOrEmpty(activity.ActivityOwner))
                                        {
                                            noteDescription += $" by {activity.ActivityOwner}";
                                        }

                                        var noteMetadata = new Dictionary<string, object>();
                                        if (!string.IsNullOrEmpty(activity.Status))
                                            noteMetadata["status"] = activity.Status;

                                        allTimelineEvents.Add(new ContactActivityTimeline(
                                            contactId,
                                            "note_created",
                                            eventDate,
                                            noteDescription,
                                            "note",
                                            activity.HubSpotId,
                                            activity.Subject,
                                            activity.ActivityOwner,
                                            ResolveOwnerName(activity.ActivityOwner),
                                            noteMetadata.Any() ? System.Text.Json.JsonSerializer.Serialize(noteMetadata) : null));
                                        break;

                                    default:
                                        // For SMS or other activity types
                                        var defaultDescription = $"{activity.ActivityType} activity";
                                        if (!string.IsNullOrEmpty(activity.Subject))
                                        {
                                            defaultDescription += $": {activity.Subject}";
                                        }
                                        if (!string.IsNullOrEmpty(activity.ActivityOwner))
                                        {
                                            defaultDescription += $" by {activity.ActivityOwner}";
                                        }

                                        var defaultMetadata = new Dictionary<string, object>();
                                        if (!string.IsNullOrEmpty(activity.Status))
                                            defaultMetadata["status"] = activity.Status;

                                        allTimelineEvents.Add(new ContactActivityTimeline(
                                            contactId,
                                            $"{activity.ActivityType?.ToLowerInvariant()}_created" ?? "activity_created",
                                            eventDate,
                                            defaultDescription,
                                            activity.ActivityType?.ToLowerInvariant() ?? "activity",
                                            activity.HubSpotId,
                                            activity.Subject,
                                            activity.ActivityOwner,
                                            ResolveOwnerName(activity.ActivityOwner),
                                            defaultMetadata.Any() ? System.Text.Json.JsonSerializer.Serialize(defaultMetadata) : null));
                                        break;
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error processing other activities");
                    }

                    _logger.LogInformation("Total timeline events extracted: {Count}", allTimelineEvents.Count);

                    if (allTimelineEvents.Count == 0)
                    {
                        _logger.LogWarning("No timeline events were extracted. This might indicate:");
                        _logger.LogWarning("  - Activities table is empty");
                        _logger.LogWarning("  - ActivityAssociations table is empty or has no contact associations");
                        _logger.LogWarning("  - No contacts exist in the database");
                        _logger.LogWarning("  - PropertyHistory table is empty (for lifecycle/lead status changes)");
                        return Result.Success(); // Return success but log the warning
                    }

                    // Upsert timeline events (avoid duplicates based on ContactHubSpotId + EventType + EventDate + RelatedObjectId)
                    int inserted = 0;
                    int updated = 0;
                    int batchCount = 0;

                    foreach (var timelineEvent in allTimelineEvents)
                    {
                        var existing = await _unitOfWork.ContactActivityTimelineRepository.FirstOrDefaultAsync(
                            t => t.ContactHubSpotId == timelineEvent.ContactHubSpotId &&
                                 t.EventType == timelineEvent.EventType &&
                                 t.EventDate == timelineEvent.EventDate &&
                                 t.RelatedObjectId == timelineEvent.RelatedObjectId);

                        if (existing != null)
                        {
                            existing.UpdateFrom(timelineEvent);
                            await _unitOfWork.ContactActivityTimelineRepository.UpdateAsync(existing);
                            updated++;
                        }
                        else
                        {
                            await _unitOfWork.ContactActivityTimelineRepository.AddAsync(timelineEvent);
                            inserted++;
                        }

                        batchCount++;
                        if (batchCount >= SAVE_BATCH_SIZE)
                        {
                            await _unitOfWork.SaveChangesAsync();
                            _logger.LogInformation("Saved batch of timeline events. Inserted: {Inserted}, Updated: {Updated}", inserted, updated);
                            batchCount = 0;
                        }
                    }

                    if (batchCount > 0)
                    {
                        await _unitOfWork.SaveChangesAsync();
                        _logger.LogInformation("Saved final batch of timeline events. Inserted: {Inserted}, Updated: {Updated}", inserted, updated);
                    }

                    _logger.LogInformation("Successfully completed ETL for Contact Activity Timeline. Total records: {Total}, Inserted: {Inserted}, Updated: {Updated}", 
                        allTimelineEvents.Count, inserted, updated);
                    return Result.Success();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error occurred during Contact Activity Timeline ETL process");
                    return Result.Failure($"Contact Activity Timeline ETL failed: {ex.Message}");
                }
            }
        }
    }
