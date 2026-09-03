using Mediana.Messaging;
using Microsoft.Extensions.DependencyInjection;
using Mediana.Transports;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Mediana.Outbox;

/// <summary>outbox: , relay.</summary>
public sealed record OutboxMessage
{
    public long Sequence { get; init; }

    /// <summary>(ObjectId MongoDB, sequence SQL). R1 fix.</summary>
    public string? DocumentId { get; init; }

    public Guid MessageId { get; init; }

    public string Destination { get; init; } = "";

    public string? Transport { get; init; }

    public byte[] EnvelopeBytes { get; init; } = [];

    public DateTimeOffset CreatedAt { get; init; }

    /// <summary>0 = ; >0 = lease (unix-ms) relay.</summary>
    public long LeaseUntil { get; init; }

    public int DeliveryAttempts { get; init; }

    public string? LastError { get; init; }

    public bool Parked { get; init; }
}

/// <summary>outbox: (EF Core/Dapper/Mongo).</summary>
public interface IOutboxStore
{
    /// <summary>(interceptor').</summary>
    ValueTask AddRange(IEnumerable<OutboxMessage> messages, CancellationToken cancellationToken);

    /// <summary>: FOR UPDATE SKIP LOCKED (SQL) / lease (Mongo).</summary>
    ValueTask<IReadOnlyList<OutboxMessage>> LeaseBatch(int batchSize, long leaseUnixMs, CancellationToken cancellationToken);

    /// <summary>.</summary>
    ValueTask MarkDelivered(OutboxMessage message, CancellationToken cancellationToken);

    /// <summary>lease (): backoff + maxAttempts (R3).</summary>
    ValueTask MarkFailed(OutboxMessage message, string error, int maxDeliveryAttempts, CancellationToken cancellationToken);

    /// <summary>(cleanup-).</summary>
    ValueTask<int> CleanupOlderThan(TimeSpan age, CancellationToken cancellationToken);
}

/// <summary>(per-scope).</summary>
[System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec reflection-based JSON.")]
public sealed class OutboxCollector
{
    private readonly List<OutboxMessage> _pending = [];

    public void Add(Envelope envelope, string destination, string? transport = null)
    {
        _pending.Add(new OutboxMessage
        {
            MessageId = envelope.MessageId,
            Destination = destination,
            Transport = transport,
            EnvelopeBytes = EnvelopeCodec.Encode(envelope),
            CreatedAt = DateTimeOffset.UtcNow,
        });
    }

    public IReadOnlyList<OutboxMessage> TakePending()
    {
        var taken = _pending.ToArray();
        _pending.Clear();
        return taken;
    }

    public int Count => _pending.Count;
}


/// <summary>relay.</summary>
public sealed record OutboxRelayOptions
{
    public int BatchSize { get; init; } = 100;

    /// <summary>.</summary>
    public TimeSpan PollInterval { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>Lease (relay).</summary>
    public TimeSpan LeaseDuration { get; init; } = TimeSpan.FromMinutes(2);

    /// <summary>parking (store LastError).</summary>
    public int MaxDeliveryAttempts { get; init; } = 10;

    /// <summary>Backoff .</summary>
    public TimeSpan FailureBackoff { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>Cleanup ; null — .</summary>
    public TimeSpan? CleanupAge { get; init; } = TimeSpan.FromDays(7);
}

/// <summary>
/// relay: → → mark-delivered
/// backoff , lease-(§9.4)
/// </summary>
[System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec reflection-based JSON.")]
public sealed class OutboxRelay(
    IOutboxStore store,
    Func<CancellationToken, ValueTask<ITransportPublisher>> publisherFactory,
    OutboxRelayOptions? options = null,
    ILogger<OutboxRelay>? logger = null) : BackgroundService
{
    private readonly OutboxRelayOptions _options = options ?? new OutboxRelayOptions();
    private int _cycleCount;
    private const int _cleanupEveryCycles = 10; // cleanup every 10 empty cycles

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var backoff = _options.FailureBackoff;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var leased = await store.LeaseBatch(
                    _options.BatchSize,
                    DateTimeOffset.UtcNow.Add(_options.LeaseDuration).ToUnixTimeMilliseconds(),
                    stoppingToken).ConfigureAwait(false);

                if (leased.Count == 0)
                {
                    // OB-05 fix: periodic cleanup of delivered messages
                    if (_options.CleanupAge is { } cleanupAge)
                    {
                        _cycleCount++;
                        if (_cycleCount >= _cleanupEveryCycles)
                        {
                            _cycleCount = 0;
                            try
                            {
                                var removed = await store.CleanupOlderThan(cleanupAge, stoppingToken).ConfigureAwait(false);
                                if (removed > 0)
                                {
                                    logger?.LogInformation("Outbox cleanup removed {Count} delivered messages older than {Age}", removed, cleanupAge);
                                }
                            }
                            catch (Exception cleanupEx)
                            {
                                logger?.LogWarning(cleanupEx, "Outbox cleanup failed (non-fatal)");
                            }
                        }
                    }

                    await Task.Delay(_options.PollInterval, stoppingToken).ConfigureAwait(false);
                    continue;
                }

                var publisher = await publisherFactory(stoppingToken).ConfigureAwait(false);
                foreach (var message in leased)
                {
                    await Deliver(publisher, message, stoppingToken).ConfigureAwait(false);
                }

                backoff = _options.FailureBackoff;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Outbox relay cycle failed; backing off {Backoff}", backoff);
                try
                {
                    await Task.Delay(backoff, stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                backoff = TimeSpan.FromTicks(Math.Min(backoff.Ticks * 2, TimeSpan.FromMinutes(1).Ticks));
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec reflection-based JSON.")]
    private async ValueTask Deliver(ITransportPublisher publisher, OutboxMessage message, CancellationToken cancellationToken)
    {
        try
        {
            var envelope = EnvelopeCodec.Decode(message.EnvelopeBytes);
            await publisher.Publish(
                envelope,
                new PublishOptions { DestinationOverride = message.Destination, ConfirmDelivery = true },
                cancellationToken).ConfigureAwait(false);
            await store.MarkDelivered(message, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // OB-04 fix: OCE during shutdown — rethrow, do not count as delivery failure
            throw;
        }
        catch (Exception ex)
        {
            // OB-04 fix: use CancellationToken.None for MarkFailed (token may be cancelled)
            await store.MarkFailed(message, ex.Message, _options.MaxDeliveryAttempts, CancellationToken.None).ConfigureAwait(false);
        }
    }
}

/// <summary>DI opt-in outbox (D4: ).</summary>
public static class OutboxServiceCollectionExtensions
{
    public static IServiceCollection AddMedianaOutbox(
        this IServiceCollection services,
        Func<OutboxRelayOptions, OutboxRelayOptions>? configure = null)
    {
        var options = configure?.Invoke(new OutboxRelayOptions()) ?? new OutboxRelayOptions();
        services.AddSingleton(options);
        services.AddSingleton<OutboxCollector>();
        return services;
    }
}
