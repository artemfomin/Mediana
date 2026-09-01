using MassTransit;
using Mediana.Consuming;
using Mediana.Messaging;
using Mediana.Transports;

namespace Mediana.MassTransit;

/// <summary>
/// Режим 1 — MassTransit как транспорт: Mediana-конверты публикуются через IBus.
/// Пользователь получает saga-экосистему и конфигурацию MassTransit (§8.3).
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
        => default; // топологией управляет MassTransit (конфигурация эндпоинтов потребителя)

    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec использует reflection-based JSON.")]
    public async ValueTask<ITransportPublisher> CreatePublisher(CancellationToken cancellationToken)
        => await Task.FromResult<ITransportPublisher>(new MassTransitPublisher(bus)).ConfigureAwait(false);

    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec использует reflection-based JSON.")]
    public IConsumerHostFactory CreateConsumerHosts()
        => new MassTransitConsumerHostFactory();

    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec использует reflection-based JSON.")]
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

/// <summary>Wire-сообщение для публикации через MassTransit (конверт внутри).</summary>
public sealed record MedianaWireMessage
{
    public Guid MessageId { get; init; }

    public string? Destination { get; init; }

    public byte[] Body { get; init; } = [];
}

/// <summary>Издатель через MassTransit IBus.</summary>
[System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec использует reflection-based JSON.")]
public sealed class MassTransitPublisher(IBus bus) : ITransportPublisher
{
    public async ValueTask Publish(Envelope envelope, PublishOptions options, CancellationToken cancellationToken)
        => await MassTransitTransport.PublishEnvelope(bus, envelope, options.DestinationOverride, cancellationToken)
            .ConfigureAwait(false);
}

/// <summary>Фабрика хостов (режим 2 — мост): консьюмеры MassTransit диспатчат в Mediana-пайплайн.</summary>
public sealed class MassTransitConsumerHostFactory : IConsumerHostFactory
{
    public IConsumerHost Create(ConsumerEndpoint endpoint, Func<ITransportDelivery, CancellationToken, ValueTask> handler)
        => new InProcessConsumerHost(handler);
}

/// <summary>Хост, управляемый MassTransit receive-endpoint'ами (стартует вместе с хостом MassTransit).</summary>
public sealed class InProcessConsumerHost(
    Func<ITransportDelivery, CancellationToken, ValueTask> handler) : IConsumerHost
{
    private volatile bool _started;

    /// <summary>Мост для MassTransit-консюмеров: доставка в Mediana-пайплайн (режим 2).</summary>
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

/// <summary>Режим 3 — MassTransit-envelope совместимость: нативный формат Fault/конвертов.</summary>
public static class MassTransitEnvelopeMapper
{
    /// <summary>Собрать MassTransit-совместимый fault-конверт для события-ошибки (§8.3).</summary>
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
                    ["exceptionType"] = exception.GetType().FullName ?? "unknown",
                    ["message"] = exception.Message,
                },
            },
            ["host"] = new Dictionary<string, object>
            {
                ["machineName"] = Environment.MachineName,
            },
        };
    }

    /// <summary>Декодировать Mediana-конверт из wire-сообщения.</summary>
    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec использует reflection-based JSON.")]
    public static Envelope FromWireMessage(MedianaWireMessage message)
        => EnvelopeCodec.Decode(message.Body);
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
           ?? throw new SerializationException("Empty envelope body.");
}

/// <summary>
/// Режим 2 — мост: MassTransit-консюмер Mediana-сообщений, диспатчит в локальный пайплайн Mediana.
/// Регистрируется на receive endpoint: cfg.ReceiveEndpoint("orders", e => e.Consumer(() => new MedianaDispatchBridge(...))).
/// </summary>
[System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec использует reflection-based JSON.")]
public sealed class MedianaDispatchBridge(Func<Envelope, CancellationToken, ValueTask> dispatch) : IConsumer<MedianaWireMessage>
{
    public async Task Consume(ConsumeContext<MedianaWireMessage> context)
    {
        var envelope = EnvelopeCodec.Decode(context.Message.Body);
        await dispatch(envelope, context.CancellationToken).ConfigureAwait(false);
    }
}
