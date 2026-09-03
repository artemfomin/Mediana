using Mediana.Messaging;
using System.Diagnostics.CodeAnalysis;

namespace Mediana.Transports;

/// <summary>Transport capabilities declared by the provider, checked by configuration.</summary>
public sealed record TransportCapabilities
{
    public required string Name { get; init; }

    public bool SupportsRequestReply { get; init; }

    public bool SupportsStreaming { get; init; }

    public bool SupportsDelayedRedelivery { get; init; }

    public bool SupportsPartitioning { get; init; }

    public bool SupportsFanOut { get; init; }
}

/// <summary>Consumer endpoint: queue/topic + concurrency.</summary>
public sealed record ConsumerEndpoint
{
    public required string Name { get; init; }

    /// <summary>Maximum concurrent processing (prefetch/concurrency).</summary>
    public int MaxConcurrency { get; init; } = 1;

    /// <summary>Message types expected on the endpoint (for topology bindings/subscriptions).</summary>
    public IReadOnlyList<string> MessageTypes { get; init; } = [];
}

/// <summary>Topology manifest: declared idempotently by the transport on startup.</summary>
public sealed record TopologyManifest
{
    public required string Transport { get; init; }

    public IReadOnlyList<ConsumerEndpoint> Endpoints { get; init; } = [];

    /// <summary>Queues/topics for publishing (without consumers).</summary>
    public IReadOnlyList<string> PublishDestinations { get; init; } = [];

    public IReadOnlyList<(string Queue, TimeSpan Delay)> RetryDestinations { get; init; } = [];

    public IReadOnlyList<string> DeadLetterDestinations { get; init; } = [];
}

/// <summary>Publish options.</summary>
public sealed record PublishOptions
{
    /// <summary> byinand (but in outbox-and).</summary>
    public bool ConfirmDelivery { get; init; }

    public string? PartitionKey { get; init; }

    /// <summary>Destination override (/thenand), if fromand from byandandand and.</summary>
    public string? DestinationOverride { get; init; }

    public static readonly PublishOptions Default = new();
}

/// <summary>Transport publisher.</summary>
public interface ITransportPublisher
{
    ValueTask Publish(Envelope envelope, PublishOptions options, CancellationToken cancellationToken);
}

/// <summary>Delivered message + acknowledgement.</summary>
public interface ITransportDelivery
{
    Envelope Envelope { get; }

    ValueTask Ack();

    ValueTask Nack(bool requeue, TimeSpan? redeliveryDelay);
}

/// <summary>Consumer host factory.</summary>
public interface IConsumerHostFactory
{
    IConsumerHost Create(ConsumerEndpoint endpoint, Func<ITransportDelivery, CancellationToken, ValueTask> handler);
}

/// <summary>Consumer host: start/stop with graceful drain.</summary>
public interface IConsumerHost : IAsyncDisposable
{
    Task Start();

    Task Stop();
}

/// <summary>Transport provider (SPI, spec §8).</summary>
public interface ITransport
{
    TransportCapabilities Capabilities { get; }

    /// <summary>Idempotent topology declaration from the manifest.</summary>
    ValueTask BuildTopology(TopologyManifest manifest, CancellationToken cancellationToken);

    [RequiresDynamicCode("The implementation may use reflection-based JSON; for NativeAOT register a source-gen serializer.")]
    ValueTask<ITransportPublisher> CreatePublisher(CancellationToken cancellationToken);

    [RequiresDynamicCode("The implementation may use reflection-based JSON; for NativeAOT register a source-gen serializer.")]
    IConsumerHostFactory CreateConsumerHosts();
}

/// <summary>Transport unavailability.</summary>
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
