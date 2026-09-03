using Mediana.Messaging;
using System.Diagnostics.CodeAnalysis;

namespace Mediana.Transports;

/// <summary>Возможности транспорта — провайдер декларирует, конфигурация проверяет.</summary>
public sealed record TransportCapabilities
{
    public required string Name { get; init; }

    public bool SupportsRequestReply { get; init; }

    public bool SupportsStreaming { get; init; }

    public bool SupportsDelayedRedelivery { get; init; }

    public bool SupportsPartitioning { get; init; }

    public bool SupportsFanOut { get; init; }
}

/// <summary>Точка потребления: очередь/топик + параллельность.</summary>
public sealed record ConsumerEndpoint
{
    public required string Name { get; init; }

    /// <summary>Максимальная параллельная обработка (prefetch/concurrency).</summary>
    public int MaxConcurrency { get; init; } = 1;

    /// <summary>Типы сообщений, ожидаемые на endpoint (для топологии bindings/подписок).</summary>
    public IReadOnlyList<string> MessageTypes { get; init; } = [];
}

/// <summary>Манифест топологии: идемпотентно декларируется транспортом на старте.</summary>
public sealed record TopologyManifest
{
    public required string Transport { get; init; }

    public IReadOnlyList<ConsumerEndpoint> Endpoints { get; init; } = [];

    /// <summary>Очереди/топики для публикации (без консьюмеров).</summary>
    public IReadOnlyList<string> PublishDestinations { get; init; } = [];

    public IReadOnlyList<(string Queue, TimeSpan Delay)> RetryDestinations { get; init; } = [];

    public IReadOnlyList<string> DeadLetterDestinations { get; init; } = [];
}

/// <summary>Опции публикации.</summary>
public sealed record PublishOptions
{
    /// <summary>Ждать подтверждения брокера (обязательно в outbox-режиме).</summary>
    public bool ConfirmDelivery { get; init; }

    public string? PartitionKey { get; init; }

    /// <summary>Destination override (очередь/топик), если отличается от политики роутинга.</summary>
    public string? DestinationOverride { get; init; }

    public static readonly PublishOptions Default = new();
}

/// <summary>Издатель в транспорт.</summary>
public interface ITransportPublisher
{
    ValueTask Publish(Envelope envelope, PublishOptions options, CancellationToken cancellationToken);
}

/// <summary>Доставленное сообщение + подтверждение.</summary>
public interface ITransportDelivery
{
    Envelope Envelope { get; }

    ValueTask Ack();

    ValueTask Nack(bool requeue, TimeSpan? redeliveryDelay);
}

/// <summary>Фабрика хостов консьюмеров.</summary>
public interface IConsumerHostFactory
{
    IConsumerHost Create(ConsumerEndpoint endpoint, Func<ITransportDelivery, CancellationToken, ValueTask> handler);
}

/// <summary>Хост консьюмеров: start/stop с graceful drain.</summary>
public interface IConsumerHost : IAsyncDisposable
{
    Task Start();

    Task Stop();
}

/// <summary>Транспортный провайдер (SPI, §8 спеки).</summary>
public interface ITransport
{
    TransportCapabilities Capabilities { get; }

    /// <summary>Идемпотентный declare топологии из манифеста.</summary>
    ValueTask BuildTopology(TopologyManifest manifest, CancellationToken cancellationToken);

    [RequiresDynamicCode("Реализация может использовать reflection-based JSON; для NativeAOT подключите source-gen сериализатор.")]
    ValueTask<ITransportPublisher> CreatePublisher(CancellationToken cancellationToken);

    [RequiresDynamicCode("Реализация может использовать reflection-based JSON; для NativeAOT подключите source-gen сериализатор.")]
    IConsumerHostFactory CreateConsumerHosts();
}

/// <summary>Недоступность транспорта.</summary>
public class TransportException : Exception
{
    public TransportException(string message)
        : base(message)
    {
    }

    public TransportException(string message, Exception inner)
        : base(message, inner)
    {
    }
}
