using Mediana.Messaging;
using System.Diagnostics.CodeAnalysis;

namespace Mediana.Transports;

/// <summary>, .</summary>
public sealed record TransportCapabilities
{
    public required string Name { get; init; }

    public bool SupportsRequestReply { get; init; }

    public bool SupportsStreaming { get; init; }

    public bool SupportsDelayedRedelivery { get; init; }

    public bool SupportsPartitioning { get; init; }

    public bool SupportsFanOut { get; init; }
}

/// <summary>: /+ .</summary>
public sealed record ConsumerEndpoint
{
    public required string Name { get; init; }

    /// <summary>(prefetch/concurrency).</summary>
    public int MaxConcurrency { get; init; } = 1;

    /// <summary>, endpoint (bindings/).</summary>
    public IReadOnlyList<string> MessageTypes { get; init; } = [];
}

/// <summary>: .</summary>
public sealed record TopologyManifest
{
    public required string Transport { get; init; }

    public IReadOnlyList<ConsumerEndpoint> Endpoints { get; init; } = [];

    /// <summary>/().</summary>
    public IReadOnlyList<string> PublishDestinations { get; init; } = [];

    public IReadOnlyList<(string Queue, TimeSpan Delay)> RetryDestinations { get; init; } = [];

    public IReadOnlyList<string> DeadLetterDestinations { get; init; } = [];
}

/// <summary>.</summary>
public sealed record PublishOptions
{
    /// <summary>(outbox-).</summary>
    public bool ConfirmDelivery { get; init; }

    public string? PartitionKey { get; init; }

    /// <summary>Destination override (/), .</summary>
    public string? DestinationOverride { get; init; }

    public static readonly PublishOptions Default = new();
}

/// <summary>.</summary>
public interface ITransportPublisher
{
    ValueTask Publish(Envelope envelope, PublishOptions options, CancellationToken cancellationToken);
}

/// <summary>+ .</summary>
public interface ITransportDelivery
{
    Envelope Envelope { get; }

    ValueTask Ack();

    ValueTask Nack(bool requeue, TimeSpan? redeliveryDelay);
}

/// <summary>.</summary>
public interface IConsumerHostFactory
{
    IConsumerHost Create(ConsumerEndpoint endpoint, Func<ITransportDelivery, CancellationToken, ValueTask> handler);
}

/// <summary>: start/stop graceful drain.</summary>
public interface IConsumerHost : IAsyncDisposable
{
    Task Start();

    Task Stop();
}

/// <summary>(SPI, §8 ).</summary>
public interface ITransport
{
    TransportCapabilities Capabilities { get; }

    /// <summary>declare .</summary>
    ValueTask BuildTopology(TopologyManifest manifest, CancellationToken cancellationToken);

 [RequiresDynamicCode(" reflection-based JSON; NativeAOT source-gen ")]
    ValueTask<ITransportPublisher> CreatePublisher(CancellationToken cancellationToken);

 [RequiresDynamicCode(" reflection-based JSON; NativeAOT source-gen ")]
    IConsumerHostFactory CreateConsumerHosts();
}

/// <summary>.</summary>
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
