using MassTransit;
using Mediana.Consuming;
using Mediana.Messaging;
using Mediana.Transports;

namespace Mediana.MassTransit;

/// <summary>
/// 1 — MassTransit : Mediana-IBus
/// saga-MassTransit (§8.3)
/// </summary>
public sealed class MassTransitTransport(IBus bus) : ITransport
{
    public TransportCapabilities Capabilities { get; } = new()
    {
        Name = "masstransit",
        SupportsRequestReply = true,
        SupportsStreaming = true,
        SupportsDelayedRedelivery = true,
        SupportsPartitioning = true,
        SupportsFanOut = true,
    };

    public ValueTask BuildTopology(TopologyManifest manifest, CancellationToken cancellationToken)
        => default; // MassTransit ()

 [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec reflection-based JSON.")]
    public async ValueTask<ITransportPublisher> CreatePublisher(CancellationToken cancellationToken)
        => await Task.FromResult<ITransportPublisher>(new MassTransitPublisher(bus)).ConfigureAwait(false);

 [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec reflection-based JSON.")]
    public IConsumerHostFactory CreateConsumerHosts()
        => new MassTransitConsumerHostFactory();

 [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec reflection-based JSON.")]
    public static async ValueTask PublishEnvelope(IBus bus, Envelope envelope, string? destination, CancellationToken cancellationToken)
    {
        var message = new MedianaWireMessage
        {
            MessageId = envelope.MessageId,
            Destination = destination,
            Body = EnvelopeCodec.Encode(envelope),
        };
        await bus.Publish(message, cancellationToken).ConfigureAwait(false);
    }

}

/// <summary>Wire-MassTransit ().</summary>
public sealed record MedianaWireMessage
{
    public Guid MessageId { get; init; }

    public string? Destination { get; init; }

    public byte[] Body { get; init; } = [];
}

/// <summary>MassTransit IBus.</summary>
[System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec reflection-based JSON.")]
public sealed class MassTransitPublisher(IBus bus) : ITransportPublisher
{
    public async ValueTask Publish(Envelope envelope, PublishOptions options, CancellationToken cancellationToken)
        => await MassTransitTransport.PublishEnvelope(bus, envelope, options.DestinationOverride, cancellationToken)
            .ConfigureAwait(false);
}

/// <summary>(2 — ): MassTransit Mediana-.</summary>
public sealed class MassTransitConsumerHostFactory : IConsumerHostFactory
{
    public IConsumerHost Create(ConsumerEndpoint endpoint, Func<ITransportDelivery, CancellationToken, ValueTask> handler)
        => new InProcessConsumerHost(handler);
}

/// <summary>, MassTransit receive-endpoint'(MassTransit).</summary>
public sealed class InProcessConsumerHost(
    Func<ITransportDelivery, CancellationToken, ValueTask> handler) : IConsumerHost
{
    private volatile bool _started;

    /// <summary>MassTransit-: Mediana-(2).</summary>
    public async ValueTask Deliver(ITransportDelivery delivery, CancellationToken cancellationToken)
    {
        if (!_started)
        {
            throw new Mediana.Transports.TransportException("Consumer host is not started.");
        }

        await handler(delivery, cancellationToken).ConfigureAwait(false);
    }

    public Task Start()
    {
        _started = true;
        return Task.CompletedTask;
    }

    public Task Stop()
    {
        _started = false;
        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync() => default;
}

/// <summary>3 — MassTransit-envelope : Fault/.</summary>
public static class MassTransitEnvelopeMapper
{
    /// <summary>MassTransit-fault-(§8.3).</summary>
    public static Dictionary<string, object> ToMassTransitFault(Envelope envelope, Exception exception)
    {
        return new Dictionary<string, object>
        {
            ["faultId"] = Guid.NewGuid(),
            ["faultedMessageId"] = envelope.MessageId,
            ["faultMessageType"] = envelope.MessageType.FullName,
            ["exceptions"] = new[]
            {
                new Dictionary<string, object>
                {
                    ["exceptionType"] = exception.GetType().Name,
                    ["errorCode"] = exception.HResult.ToString(),
                },
            },
            ["host"] = new Dictionary<string, object>
            {
                // T-14 fix: removed MachineName
            },
        };
    }

    /// <summary>Mediana-wire-.</summary>
 [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec reflection-based JSON.")]
    public static Envelope FromWireMessage(MedianaWireMessage message)
        => EnvelopeCodec.Decode(message.Body);
}


/// <summary>
/// 2 — : MassTransit-Mediana-, Mediana
/// receive endpoint: cfg.ReceiveEndpoint("orders", e => e.Consumer(() => new MedianaDispatchBridge(...)))
/// </summary>
[System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec reflection-based JSON.")]
public sealed class MedianaDispatchBridge(Func<Envelope, CancellationToken, ValueTask> dispatch) : IConsumer<MedianaWireMessage>
{
    public async Task Consume(ConsumeContext<MedianaWireMessage> context)
    {
        var envelope = EnvelopeCodec.Decode(context.Message.Body);
        await dispatch(envelope, context.CancellationToken).ConfigureAwait(false);
    }
}
