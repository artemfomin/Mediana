using Mediana.Outbox;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Diagnostics;

namespace Mediana.Outbox.EFCore;

/// <summary>EF Core-провайдер outbox (net10.0-only, D13): SaveChanges-interceptor + SKIP LOCKED relay.</summary>
public sealed class OutboxEntry
{
    public long Sequence { get; set; }

    public Guid MessageId { get; set; }

    public string Destination { get; set; } = "";

    public string? Transport { get; set; }

    public byte[] EnvelopeBytes { get; set; } = [];

    public DateTimeOffset CreatedAt { get; set; }

    public long LeaseUntil { get; set; }

    public int DeliveryAttempts { get; set; }

    public DateTimeOffset? DeliveredAt { get; set; }

    public string? LastError { get; set; }

    public bool Parked { get; set; }
}

/// <summary>DbContext-расширение: ToTable + interceptor регистрации конвертов.</summary>
public static class OutboxModelBuilderExtensions
{
    public static ModelBuilder AddMedianaOutbox(this ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<OutboxEntry>(e =>
        {
            e.ToTable("mediana_outbox");
            e.HasKey(x => x.Sequence);
            e.Property(x => x.Destination).HasMaxLength(400).IsRequired();
            e.Property(x => x.Transport).HasMaxLength(100);
            e.Property(x => x.EnvelopeBytes).IsRequired();
            e.HasIndex(x => new { x.DeliveredAt, x.LeaseUntil });
        });
        return modelBuilder;
    }
}

/// <summary>
/// Interceptor: исходящие конверты OutboxCollector записываются в ту же транзакцию SaveChanges (§9.4).
/// </summary>
public sealed class OutboxSaveChangesInterceptor(OutboxCollector collector) : SaveChangesInterceptor
{
    public override InterceptionResult<int> SavingChanges(
        DbContextEventData eventData,
        InterceptionResult<int> result)
    {
        var pending = collector.TakePending();
        if (pending.Count > 0 && eventData.Context is { } context)
        {
            foreach (var message in pending)
            {
                context.Set<OutboxEntry>().Add(new OutboxEntry
                {
                    MessageId = message.MessageId,
                    Destination = message.Destination,
                    Transport = message.Transport,
                    EnvelopeBytes = message.EnvelopeBytes,
                    CreatedAt = message.CreatedAt,
                    LeaseUntil = message.LeaseUntil,
                });
            }
        }

        return base.SavingChanges(eventData, result);
    }

    public override ValueTask<InterceptionResult<int>> SavingChangesAsync(
        DbContextEventData eventData,
        InterceptionResult<int> result,
        CancellationToken cancellationToken = default)
    {
        var pending = collector.TakePending();
        if (pending.Count > 0 && eventData.Context is { } context)
        {
            foreach (var message in pending)
            {
                context.Set<OutboxEntry>().Add(new OutboxEntry
                {
                    MessageId = message.MessageId,
                    Destination = message.Destination,
                    Transport = message.Transport,
                    EnvelopeBytes = message.EnvelopeBytes,
                    CreatedAt = message.CreatedAt,
                    LeaseUntil = message.LeaseUntil,
                });
            }
        }

        return base.SavingChangesAsync(eventData, result, cancellationToken);
    }
}

/// <summary>Хранилище поверх EF Core.</summary>
public sealed class EfOutboxStore(Func<DbContext> contextFactory) : IOutboxStore
{
    public async ValueTask AddRange(IEnumerable<OutboxMessage> messages, CancellationToken cancellationToken)
    {
        await using var context = contextFactory();
        context.Set<OutboxEntry>().AddRange(messages.Select(m => new OutboxEntry
        {
            MessageId = m.MessageId,
            Destination = m.Destination,
            Transport = m.Transport,
            EnvelopeBytes = m.EnvelopeBytes,
            CreatedAt = m.CreatedAt,
        }));
        await context.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<IReadOnlyList<OutboxMessage>> LeaseBatch(int batchSize, long leaseUnixMs, CancellationToken cancellationToken)
    {
        await using var context = contextFactory();
        await using var transaction = await context.Database.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var rows = await context.Set<OutboxEntry>()
            .FromSqlRaw(
                "SELECT * FROM mediana_outbox WHERE delivered_at IS NULL AND parked = false AND lease_until < {0} ORDER BY sequence LIMIT {1} FOR UPDATE SKIP LOCKED",
                now, batchSize)
            .ToListAsync(cancellationToken).ConfigureAwait(false);

        foreach (var row in rows)
        {
            row.LeaseUntil = leaseUnixMs;
        }

        await context.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return rows.Select(ToMessage).ToList();
    }

    public async ValueTask MarkDelivered(OutboxMessage message, CancellationToken cancellationToken)
    {
        await using var context = contextFactory();
        await context.Set<OutboxEntry>()
            .Where(e => e.Sequence == message.Sequence)
            .ExecuteUpdateAsync(s => s.SetProperty(e => e.DeliveredAt, DateTimeOffset.UtcNow), cancellationToken)
            .ConfigureAwait(false);
    }

    public async ValueTask MarkFailed(OutboxMessage message, string error, CancellationToken cancellationToken)
    {
        // OB-08 fix: экспоненциальный backoff; OB-02 fix: парковка при исчерпании попыток
        var truncatedError = error is { Length: > 4000 } ? error[..4000] : error;
        var backoffMs = Math.Min(Math.Pow(2, message.DeliveryAttempts) * 1000, 300_000);
        var leaseUntil = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + (long)backoffMs;
        var parked = message.DeliveryAttempts >= 10;

        await using var context = contextFactory();
        await context.Set<OutboxEntry>()
            .Where(e => e.Sequence == message.Sequence)
            .ExecuteUpdateAsync(s => s
                .SetProperty(e => e.DeliveryAttempts, e => e.DeliveryAttempts + 1)
                .SetProperty(e => e.LastError, truncatedError)
                .SetProperty(e => e.LeaseUntil, leaseUntil)
                .SetProperty(e => e.Parked, parked), cancellationToken)
            .ConfigureAwait(false);
    }

    public async ValueTask<int> CleanupOlderThan(TimeSpan age, CancellationToken cancellationToken)
    {
        await using var context = contextFactory();
        return await context.Set<OutboxEntry>()
            .Where(e => e.DeliveredAt != null && e.DeliveredAt < DateTimeOffset.UtcNow - age)
            .ExecuteDeleteAsync(cancellationToken)
            .ConfigureAwait(false);
    }

    private static OutboxMessage ToMessage(OutboxEntry entry)
        => new()
        {
            Sequence = entry.Sequence,
            MessageId = entry.MessageId,
            Destination = entry.Destination,
            Transport = entry.Transport,
            EnvelopeBytes = entry.EnvelopeBytes,
            CreatedAt = entry.CreatedAt,
            LeaseUntil = entry.LeaseUntil,
            DeliveryAttempts = entry.DeliveryAttempts,
            LastError = entry.LastError,
        };
}
