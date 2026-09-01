using Mediana.Routing;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Mediana.Messaging;
using Mediana.Transports;

namespace Mediana.Kafka;

/// <summary>
/// Kafka-транспорт: топики, partition key (ordering per key), consumer groups для команд,
/// retry-topics (<topic>.retry.<delay> → <topic>.dlq), БЕЗ request/reply и стриминга (D11).
/// </summary>
public sealed class KafkaTransport : ITransport
{
    private readonly ProducerConfig _producerConfig;

    public KafkaTransport(ProducerConfig producerConfig)
    {
        _producerConfig = producerConfig;
    }

    public TransportCapabilities Capabilities { get; } = new()
    {
        Name = "kafka",
        SupportsRequestReply = false,
        SupportsStreaming = false,
        SupportsDelayedRedelivery = true,
        SupportsPartitioning = true,
        SupportsFanOut = true,
    };

    public async ValueTask BuildTopology(TopologyManifest manifest, CancellationToken cancellationToken)
    {
        // Топики создаёт AdminClient; идемпотентно (если существует — игнор)
        var adminConfig = new AdminClientConfig { BootstrapServers = _producerConfig.BootstrapServers };
        using var admin = new AdminClientBuilder(adminConfig).Build();

        var topics = new List<string>();
        topics.AddRange(manifest.Endpoints.Select(e => e.Name));
        topics.AddRange(manifest.PublishDestinations);
        topics.AddRange(manifest.RetryDestinations.Select(r => RetryTopicName(r.Queue, r.Delay)));
        topics.AddRange(manifest.DeadLetterDestinations.Select(d => d + ".dlq"));

        try
        {
            await admin.CreateTopicsAsync(topics.Select(t => new TopicSpecification
            {
                Name = t,
                NumPartitions = -1, // default брокера
                ReplicationFactor = -1,
            }), new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(10) }).ConfigureAwait(false);
        }
        catch (CreateTopicsException ex) when (ex.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
            // идемпотентность: существующие топики — норма
        }
    }

    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec использует reflection-based JSON.")]
    public async ValueTask<ITransportPublisher> CreatePublisher(CancellationToken cancellationToken)
    {
        var producer = new ProducerBuilder<string, byte[]>(_producerConfig).Build();
        return new KafkaPublisher(producer);
    }

    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec использует reflection-based JSON.")]
    public IConsumerHostFactory CreateConsumerHosts()
        => new KafkaConsumerHostFactory(_producerConfig.BootstrapServers);

    public static string RetryTopicName(string topic, TimeSpan delay)
        => topic + ".retry." + delay.TotalMilliseconds + "ms";
}

/// <summary>Издатель Kafka: ключ = PartitionKey, заголовки = конверт.</summary>
[System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec использует reflection-based JSON.")]
public sealed class KafkaPublisher(IProducer<string, byte[]> producer) : ITransportPublisher
{
    public async ValueTask Publish(Envelope envelope, PublishOptions options, CancellationToken cancellationToken)
    {
        var topic = options.DestinationOverride
            ?? envelope.Headers.GetValueOrDefault("mediana.destination")
            ?? throw new TransportException("No destination for envelope " + envelope.MessageId);

        var message = new Message<string, byte[]>
        {
            Key = options.PartitionKey ?? envelope.PartitionKey ?? envelope.MessageId.ToString("N"),
            Value = EnvelopeCodec.Encode(envelope),
            Headers = new Headers
            {
                { "mediana.message-type", System.Text.Encoding.UTF8.GetBytes(envelope.MessageType.FullName) },
                { "mediana.version", System.Text.Encoding.UTF8.GetBytes(envelope.Version.ToString()) },
            },
            Timestamp = new Timestamp(envelope.Timestamp.ToUnixTimeMilliseconds(), TimestampType.CreateTime),
        };

        await producer.ProduceAsync(topic, message, cancellationToken).ConfigureAwait(false);
    }
}

/// <summary>Фабрика консьюмеров Kafka (consumer groups).</summary>
[System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec использует reflection-based JSON.")]
public sealed class KafkaConsumerHostFactory(string bootstrapServers) : IConsumerHostFactory
{
    public IConsumerHost Create(ConsumerEndpoint endpoint, Func<ITransportDelivery, CancellationToken, ValueTask> handler)
        => new KafkaConsumerHost(bootstrapServers, endpoint, handler);
}

/// <summary>Консьюмер Kafka: группа = endpoint.Name (конкурентность команд), poll-цикл, commit после обработки.</summary>
[System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec использует reflection-based JSON.")]
public sealed class KafkaConsumerHost(
    string bootstrapServers,
    ConsumerEndpoint endpoint,
    Func<ITransportDelivery, CancellationToken, ValueTask> handler) : IConsumerHost
{
    private IConsumer<string, byte[]>? _consumer;
    private CancellationTokenSource? _cts;
    private Task? _pollLoop;

    public Task Start()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = endpoint.Name,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        };
        _consumer = new ConsumerBuilder<string, byte[]>(config).Build();
        _consumer.Subscribe(endpoint.Name);
        _cts = new CancellationTokenSource();
        _pollLoop = Task.Run(() => PollLoop(_cts.Token));
        return Task.CompletedTask;
    }

    private async Task PollLoop(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var result = _consumer!.Consume(cancellationToken);
                var delivery = new KafkaDelivery(_consumer, result);
                await handler(delivery, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    public async Task Stop()
    {
        _cts?.Cancel();
        if (_pollLoop is not null)
        {
            try
            {
                await _pollLoop.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }

        _consumer?.Close();
    }

    public async ValueTask DisposeAsync()
    {
        await Stop().ConfigureAwait(false);
        _consumer?.Dispose();
        _cts?.Dispose();
    }
}

/// <summary>Доставка Kafka: commit = ack; nack(requeue:false) → retry-topic/DLQ publish.</summary>
[System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec использует reflection-based JSON.")]
public sealed class KafkaDelivery(IConsumer<string, byte[]> consumer, ConsumeResult<string, byte[]> result)
    : ITransportDelivery
{
    public Envelope Envelope { get; } = EnvelopeCodec.Decode(result.Message.Value);

    public ValueTask Ack()
    {
        consumer.Commit(result);
        return default;
    }

    public ValueTask Nack(bool requeue, TimeSpan? redeliveryDelay)
    {
        // Non-blocking retry: исход коммитим, копию — в retry-topic (delay) или DLQ
        consumer.Commit(result);
        if (!requeue)
        {
            // retry/DLQ публикует вышестоящий контур (RetryEngine решает); здесь только семантика ack
        }

        return default;
    }
}

/// <summary>Кодирование конверта (JSON).</summary>
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
/// Гард конфигурации: Query/StreamQuery на kafka-транспорте → NotSupportedException (D11).
/// Вызывается из MedianaConfiguration при резолве политик.
/// </summary>
public static class KafkaGuards
{
    public static void EnsureSupported(RouteTarget target, bool isRequestReply, bool isStreaming)
    {
        if (target == RouteTarget.Remote && isRequestReply)
        {
            throw new NotSupportedException(
                "Kafka transport does not support request/reply queries (spec D11): use RabbitMQ/MassTransit.");
        }

        if (target == RouteTarget.Remote && isStreaming)
        {
            throw new NotSupportedException(
                "Kafka transport does not support streaming queries (spec D11).");
        }
    }
}
