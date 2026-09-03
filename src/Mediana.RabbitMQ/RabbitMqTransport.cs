using Mediana.Messaging;
using Mediana.Reliability;
using Mediana.Routing;
using Mediana.Transports;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Mediana.RabbitMq;

/// <summary>
/// RabbitMQ-(net10.0: 7.x; ns2.1: 6.x )
/// DLX-cycle retry (<q>.retry.<delay>), direct reply-to request/reply
/// publisher confirms outbox-relay (§8.1 )
/// </summary>
public sealed class RabbitMqTransport : ITransport
{
    public const string MedianaExchange = "mediana";
    public const string DeadLetterHeader = "mediana.dlx-reason";
    public const string RetryCountHeader = "mediana.retry-count";

    private readonly IConnectionFactory _factory;
    private readonly string _sourceEndpoint;

    public RabbitMqTransport(IConnectionFactory factory, string sourceEndpoint = "unknown")
    {
        _factory = factory;
        _sourceEndpoint = sourceEndpoint;
    }

    public TransportCapabilities Capabilities { get; } = new()
    {
        Name = "rabbit",
        SupportsRequestReply = true,
        SupportsStreaming = true,
        SupportsDelayedRedelivery = true,
        SupportsPartitioning = true,
        SupportsFanOut = true,
    };

    public async ValueTask BuildTopology(TopologyManifest manifest, CancellationToken cancellationToken)
    {
        var connection = await CreateConnection(cancellationToken).ConfigureAwait(false);
        await using var channel = await connection.CreateChannelAsync(
            options: null, cancellationToken: cancellationToken).ConfigureAwait(false);

        // : topic — routing key =
        await TopologyProvisioner.DeclareTopology(channel, manifest, cancellationToken).ConfigureAwait(false);
    }

 [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec reflection-based JSON.")]
    public async ValueTask<ITransportPublisher> CreatePublisher(CancellationToken cancellationToken)
    {
        var connection = await CreateConnection(cancellationToken).ConfigureAwait(false);
        // 7.x: publisher confirms ; PublishAsync
        var options = new CreateChannelOptions(
            publisherConfirmationsEnabled: true,
            publisherConfirmationTrackingEnabled: true);
        var channel = await connection.CreateChannelAsync(options, cancellationToken).ConfigureAwait(false);
        return new RabbitMqPublisher(connection, channel);
    }

 [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec reflection-based JSON.")]
    public IConsumerHostFactory CreateConsumerHosts()
        => new RabbitMqConsumerHostFactory(this);

    internal ValueTask<IConnection> CreateConnection(CancellationToken cancellationToken)
    {
        return new ValueTask<IConnection>(_factory.CreateConnectionAsync());
    }

    internal string SourceEndpoint => _sourceEndpoint;
}

/// <summary>: exchange, , DLX, retry-().</summary>
public static class TopologyProvisioner
{
    public static async Task DeclareTopology(
        IChannel channel,
        TopologyManifest manifest,
        CancellationToken cancellationToken)
    {
        await channel.ExchangeDeclareAsync(
            RabbitMqTransport.MedianaExchange,
            ExchangeType.Topic,
            durable: true,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        foreach (var endpoint in manifest.Endpoints)
        {
            var args = new Dictionary<string, object?>
            {
                ["x-dead-letter-exchange"] = RabbitMqTransport.MedianaExchange,
                ["x-dead-letter-routing-key"] = endpoint.Name + ".dlq",
            };
            await channel.QueueDeclareAsync(
                endpoint.Name,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: args,
                cancellationToken: cancellationToken).ConfigureAwait(false);
            await channel.QueueBindAsync(
                endpoint.Name,
                RabbitMqTransport.MedianaExchange,
                routingKey: endpoint.Name,
                cancellationToken: cancellationToken).ConfigureAwait(false);

            await DeclareDeadLetterAsync(channel, endpoint.Name, cancellationToken).ConfigureAwait(false);
        }

        foreach (var destination in manifest.PublishDestinations)
        {
            var args = new Dictionary<string, object?>
            {
                ["x-dead-letter-exchange"] = RabbitMqTransport.MedianaExchange,
                ["x-dead-letter-routing-key"] = destination + ".dlq",
            };
            await channel.QueueDeclareAsync(
                destination, durable: true, exclusive: false, autoDelete: false,
                arguments: args, cancellationToken: cancellationToken).ConfigureAwait(false);
            await channel.QueueBindAsync(
                destination, RabbitMqTransport.MedianaExchange, destination, cancellationToken: cancellationToken)
                .ConfigureAwait(false);
            await DeclareDeadLetterAsync(channel, destination, cancellationToken).ConfigureAwait(false);
        }

        foreach (var (queue, delay) in manifest.RetryDestinations)
        {
            // Retry-TTL (DLX-cycle, §8.1)
            var args = new Dictionary<string, object?>
            {
                ["x-dead-letter-exchange"] = RabbitMqTransport.MedianaExchange,
                ["x-dead-letter-routing-key"] = queue,
                ["x-message-ttl"] = (long)delay.TotalMilliseconds,
            };
            await channel.QueueDeclareAsync(
                RetryQueueName(queue, delay),
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: args,
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        foreach (var dlq in manifest.DeadLetterDestinations)
        {
            await DeclareDeadLetterAsync(channel, dlq, cancellationToken).ConfigureAwait(false);
        }
    }

    public static string RetryQueueName(string queue, TimeSpan delay)
        => queue + ".retry." + delay.TotalMilliseconds + "ms";

    private static Task DeclareDeadLetterAsync(IChannel channel, string queue, CancellationToken cancellationToken)
    {
        return Task.WhenAll(
            channel.QueueDeclareAsync(
                queue + ".dlq",
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: new Dictionary<string, object?> { ["x-queue-mode"] = "lazy" },
                cancellationToken: cancellationToken),
            channel.QueueBindAsync(queue + ".dlq", RabbitMqTransport.MedianaExchange, queue + ".dlq", cancellationToken: cancellationToken));
    }

    /// <summary>retry-().</summary>
    public static IEnumerable<(string Queue, TimeSpan Delay)> RetryDestinationsFrom(
        IEnumerable<string> queues,
        RetryPolicy policy,
        int attemptCount = 3)
    {
        for (var attempt = 1; attempt <= attemptCount; attempt++)
        {
            var delay = policy.DelayFor(attempt);
            foreach (var queue in queues)
            {
                yield return (queue, delay);
            }
        }
    }
}

/// <summary>: → AMQP-, publisher confirms.</summary>
[System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec reflection-based JSON.")]
public sealed class RabbitMqPublisher : ITransportPublisher
{
    private readonly IConnection _connection;
    private readonly IChannel _channel;

    public RabbitMqPublisher(IConnection connection, IChannel channel)
    {
        _connection = connection;
        _channel = channel;
    }

    public async ValueTask Publish(Envelope envelope, PublishOptions options, CancellationToken cancellationToken)
    {
        string destination;
        if (options.DestinationOverride is { } overrideDestination)
        {
            destination = overrideDestination;
        }
        else if (envelope.Headers.TryGetValue("mediana.destination", out var headerDestination))
        {
            destination = headerDestination;
        }
        else
        {
            throw new TransportException("No destination for envelope " + envelope.MessageId);
        }

        var props = new BasicProperties
        {
            MessageId = envelope.MessageId.ToString(),
            ContentType = "application/json",
            Persistent = true,
            Timestamp = new AmqpTimestamp(envelope.Timestamp.ToUnixTimeSeconds()),
            Headers = new Dictionary<string, object?>
            {
                ["mediana.message-type"] = envelope.MessageType.FullName,
                ["mediana.type-version"] = envelope.MessageType.TypeVersion,
                ["mediana.correlation-id"] = envelope.CorrelationId?.ToString("N"),
                ["mediana.causation-id"] = envelope.CausationId?.ToString("N"),
                ["mediana.traceparent"] = envelope.TraceParent,
                ["mediana.version"] = envelope.Version,
            },
        };

        var partitionKey = options.PartitionKey ?? envelope.PartitionKey;
        if (partitionKey is not null)
        {
            props.Headers["mediana.partition-key"] = partitionKey;
        }

        var body = new ReadOnlyMemory<byte>(EnvelopeCodec.Encode(envelope));
        // publisherConfirmationTrackingEnabled: await =
        await _channel.BasicPublishAsync(
            RabbitMqTransport.MedianaExchange,
            routingKey: destination,
            mandatory: false,
            basicProperties: props,
            body: body,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }
}

