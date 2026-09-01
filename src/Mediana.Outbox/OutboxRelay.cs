using Mediana.Messaging;
using Microsoft.Extensions.DependencyInjection;
using Mediana.Transports;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Mediana.Outbox;

/// <summary>Запись outbox: атомарно с бизнес-транзакцией, доставляется relay.</summary>
public sealed record OutboxMessage
{
    public long Sequence { get; init; }

    public Guid MessageId { get; init; }

    public string Destination { get; init; } = "";

    public string? Transport { get; init; }

    public byte[] EnvelopeBytes { get; init; } = [];

    public DateTimeOffset CreatedAt { get; init; }

    /// <summary>0 = не доставлено; >0 = lease до (unix-ms) для конкурентных relay.</summary>
    public long LeaseUntil { get; init; }

    public int DeliveryAttempts { get; init; }

    public string? LastError { get; init; }
}

/// <summary>Хранилище outbox: реализуют спутниковые пакеты (EF Core/Dapper/Mongo).</summary>
public interface IOutboxStore
{
    /// <summary>Пакетная вставка в бизнес-транзакцию (вызывается из interceptor'а провайдера).</summary>
    ValueTask AddRange(IEnumerable<OutboxMessage> messages, CancellationToken cancellationToken);

    /// <summary>Взять батч к доставке: FOR UPDATE SKIP LOCKED (SQL) / lease (Mongo).</summary>
    ValueTask<IReadOnlyList<OutboxMessage>> LeaseBatch(int batchSize, long leaseUnixMs, CancellationToken cancellationToken);

    /// <summary>Пометить доставленным.</summary>
    ValueTask MarkDelivered(OutboxMessage message, CancellationToken cancellationToken);

    /// <summary>Вернуть lease (ошибка доставки): счётчик попыток + последняя ошибка.</summary>
    ValueTask MarkFailed(OutboxMessage message, string error, CancellationToken cancellationToken);

    /// <summary>Удалить доставленные старше возраста (cleanup-политика).</summary>
    ValueTask<int> CleanupOlderThan(TimeSpan age, CancellationToken cancellationToken);
}

/// <summary>Сборщик исходящих конвертов внутри бизнес-операции (per-scope).</summary>
[System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec использует reflection-based JSON.")]
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

/// <summary>Кодирование конверта.</summary>
public static class EnvelopeCodec
{
    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Reflection-based JSON; для AOT — source-gen.")]
    public static byte[] Encode(Envelope envelope)
        => System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(envelope);

    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Reflection-based JSON; для AOT — source-gen.")]
    public static Envelope Decode(byte[] body)
        => System.Text.Json.JsonSerializer.Deserialize<Envelope>(body)
           ?? throw new Mediana.Messaging.SerializationException("Empty envelope body.");
}

/// <summary>Опции relay.</summary>
public sealed record OutboxRelayOptions
{
    public int BatchSize { get; init; } = 100;

    /// <summary>Интервал опроса при пустом батче.</summary>
    public TimeSpan PollInterval { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>Lease на запись при доставке (конкурентные relay).</summary>
    public TimeSpan LeaseDuration { get; init; } = TimeSpan.FromMinutes(2);

    /// <summary>Максимум попыток доставки до parking (ошибка остаётся в store с LastError).</summary>
    public int MaxDeliveryAttempts { get; init; } = 10;

    /// <summary>Backoff повтора опроса при недоступности транспорта.</summary>
    public TimeSpan FailureBackoff { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>Cleanup доставленных старше возраста; null — не чистить.</summary>
    public TimeSpan? CleanupAge { get; init; } = TimeSpan.FromDays(7);
}

/// <summary>
/// Фоновый relay: батч-выборка → издатель транспорта → mark-delivered;
/// экспоненциальный backoff при недоступности, lease-конкурентность (§9.4).
/// </summary>
[System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec использует reflection-based JSON.")]
public sealed class OutboxRelay(
    IOutboxStore store,
    Func<CancellationToken, ValueTask<ITransportPublisher>> publisherFactory,
    OutboxRelayOptions? options = null,
    ILogger<OutboxRelay>? logger = null) : BackgroundService
{
    private readonly OutboxRelayOptions _options = options ?? new OutboxRelayOptions();

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

    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec использует reflection-based JSON.")]
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
        catch (Exception ex)
        {
            await store.MarkFailed(message, ex.Message, cancellationToken).ConfigureAwait(false);
        }
    }
}

/// <summary>Расширения DI для opt-in outbox (D4: без пакета ядро не знает о нём).</summary>
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
