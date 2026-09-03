using Mediana.Routing;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Mediana.Messaging;
using Mediana.Transports;

namespace Mediana.Kafka;

/// <summary>
/// Kafka-: , partition key (ordering per key), consumer groups
/// retry-topics (<topic>.retry.<delay> → <topic>.dlq), request/reply (D11)
/// ClientConfig (SASL/SSL) (T-04 fix)
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
        // T-04 fix: ClientConfig (SASL/SSL/etc.) — BootstrapServers
        var adminConfig = new AdminClientConfig(_producerConfig);
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
                NumPartitions = -1, // default
                ReplicationFactor = -1,
            }), new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(10) }).ConfigureAwait(false);
        }
        catch (CreateTopicsException ex) when (ex.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
            // :
        }
    }

 [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec reflection-based JSON.")]
    public async ValueTask<ITransportPublisher> CreatePublisher(CancellationToken cancellationToken)
    {
        var producer = new ProducerBuilder<string, byte[]>(_producerConfig).Build();
        return new KafkaPublisher(producer);
    }

 [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec reflection-based JSON.")]
    public IConsumerHostFactory CreateConsumerHosts()
        => new KafkaConsumerHostFactory(new ConsumerConfig(_producerConfig));

    public static string RetryTopicName(string topic, TimeSpan delay)
        => topic + ".retry." + delay.TotalMilliseconds + "ms";
}

/// <summary>Kafka: = PartitionKey, = .</summary>
[System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec reflection-based JSON.")]
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

/// <summary>Kafka (consumer groups).</summary>
[System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec reflection-based JSON.")]
public sealed class KafkaConsumerHostFactory(ConsumerConfig baseConfig) : IConsumerHostFactory
{
    public IConsumerHost Create(ConsumerEndpoint endpoint, Func<ITransportDelivery, CancellationToken, ValueTask> handler)
        => new KafkaConsumerHost(baseConfig, endpoint, handler);
}

/// <summary>
/// Kafka: = endpoint.Name, poll-catch-all (T-01/T-06 fix)
/// poison → DLQ + commit (T-01), Nack(requeue:false) → DLQ (T-05), fault-health-monitoring.
/// </summary>
[System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec reflection-based JSON.")]
public sealed class KafkaConsumerHost(
    ConsumerConfig baseConfig,
    ConsumerEndpoint endpoint,
    Func<ITransportDelivery, CancellationToken, ValueTask> handler) : IConsumerHost
{
    private IConsumer<string, byte[]>? _consumer;
    private IProducer<string, byte[]>? _dlqProducer;
    private CancellationTokenSource? _cts;
    private Task? _pollLoop;

    /// <summary>Health-probe: true poll loop faulted-(T-06).</summary>
    public bool IsHealthy => _pollLoop is not null && !_pollLoop.IsFaulted;

    public Task Start()
    {
        // T-04: (SASL/SSL) + consumer-
        var config = new ConsumerConfig(baseConfig)
        {
            GroupId = endpoint.Name,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        };
        _consumer = new ConsumerBuilder<string, byte[]>(config).Build();
        _consumer.Subscribe(endpoint.Name);
        _dlqProducer = new ProducerBuilder<string, byte[]>(new ProducerConfig(baseConfig)).Build();
        _cts = new CancellationTokenSource();
        _pollLoop = Task.Run(() => PollLoop(_cts.Token));
        return Task.CompletedTask;
    }

    private async Task PollLoop(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            ConsumeResult<string, byte[]>? result = null;
            try
            {
                result = _consumer!.Consume(cancellationToken);
                var delivery = new KafkaDelivery(_consumer, _dlqProducer!, result);
                await handler(delivery, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                // NEW-R2-1 fix: best-effort DLQ produce, then commit.
                // If DLQ produce fails — Seek back (do NOT commit, message is not lost).
                if (result is not null)
                {
                    var dlqOk = await TryProduceToDlq(result, "handler-exception", ex, cancellationToken).ConfigureAwait(false);
                    if (dlqOk)
                    {
                        try { _consumer!.Commit(result); }
                        catch (Exception commitEx)
                        {
                            Console.Error.WriteLine($"Kafka commit-after-DLQ failed for {endpoint.Name}: {commitEx.Message}");
                        }
                    }
                    else
                    {
                        // DLQ unavailable — Seek back so message is retried on next cycle
                        Console.Error.WriteLine($"Kafka DLQ unavailable for {endpoint.Name}, seeking back to offset {result.Offset}");
                        try
                        {
                            _consumer!.Seek(new TopicPartitionOffset(result.TopicPartition, result.Offset));
                            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken).ConfigureAwait(false);
                        }
                        catch (Exception seekEx)
                        {
                            Console.Error.WriteLine($"Kafka seek-back failed: {seekEx.Message}");
                        }
                    }
                }
            }
        }
    }

    /// <summary>Best-effort poison-report produce to DLQ topic. Returns false if DLQ is unavailable.</summary>
    private async Task<bool> TryProduceToDlq(
        ConsumeResult<string, byte[]> result, string reason, Exception? originalException, CancellationToken ct)
    {
        try
        {
            var dlqTopic = result.Topic + ".dlq";
            var headers = new Headers
            {
                { "mediana.dlx-reason", System.Text.Encoding.UTF8.GetBytes(reason) },
                { "mediana.original-topic", System.Text.Encoding.UTF8.GetBytes(result.Topic) },
                { "mediana.original-partition", System.Text.Encoding.UTF8.GetBytes(result.Partition.Value.ToString(System.Globalization.CultureInfo.InvariantCulture)) },
                { "mediana.original-offset", System.Text.Encoding.UTF8.GetBytes(result.Offset.Value.ToString(System.Globalization.CultureInfo.InvariantCulture)) },
            };
            // T-05 fix: preserve original headers
            if (result.Message.Headers is { } originalHeaders)
            {
                foreach (var h in originalHeaders)
                {
                    if (!h.Key.StartsWith("mediana.", StringComparison.Ordinal))
                    {
                        headers.Add(h.Key, h.GetValueBytes());
                    }
                }
            }
            if (originalException is not null)
            {
                headers.Add("mediana.error-type", System.Text.Encoding.UTF8.GetBytes(originalException.GetType().Name));
            }

            var message = new Message<string, byte[]>
            {
                Key = result.Message.Key,
                Value = result.Message.Value,
                Headers = headers,
            };
            await _dlqProducer!.ProduceAsync(dlqTopic, message, ct).ConfigureAwait(false);
            return true;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Kafka DLQ produce failed for {endpoint.Name}: {ex.Message}");
            return false;
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
        _dlqProducer?.Flush(CancellationToken.None);
    }

    public async ValueTask DisposeAsync()
    {
        await Stop().ConfigureAwait(false);
        _consumer?.Dispose();
        _dlqProducer?.Dispose();
        _cts?.Dispose();
    }
}

/// <summary>
/// Kafka: commit = ack; Nack(requeue:false) → publish <topic>.dlq + commit (T-05 fix)
/// Envelope poison consumer (T-01 fix)
/// </summary>
[System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec reflection-based JSON.")]
public sealed class KafkaDelivery(
    IConsumer<string, byte[]> consumer,
    IProducer<string, byte[]> dlqProducer,
    ConsumeResult<string, byte[]> result) : ITransportDelivery
{
    private Envelope? _envelope;

    public Envelope Envelope => _envelope ??= DecodeSafe();

    private Envelope DecodeSafe()
    {
        try
        {
            return EnvelopeCodec.Decode(result.Message.Value);
        }
        catch
        {
            // N-4/NEW-R2-2 fix: deterministic poison-id from (topic, partition, offset)
            var seed = $"{result.Topic}:{result.Partition.Value}:{result.Offset.Value}";
            using var sha = System.Security.Cryptography.SHA256.Create();
            var hash = sha.ComputeHash(System.Text.Encoding.UTF8.GetBytes(seed));
            var guidBytes = new byte[16];
            Array.Copy(hash, guidBytes, 16);
            guidBytes[7] = (byte)((guidBytes[7] & 0x0F) | 0x70); // version 7
            guidBytes[8] = (byte)((guidBytes[8] & 0x3F) | 0x80); // variant 10xx
            return new Envelope
            {
                MessageId = new Guid(guidBytes),
                MessageType = new MessageTypeDescriptor
                {
                    FullName = "mediana.poison",
                    TypeVersion = "0",
                },
                Timestamp = DateTimeOffset.UtcNow,
                Payload = [],
            };
        }
    }

    public ValueTask Ack()
    {
        consumer.Commit(result);
        return default;
    }

    public async ValueTask Nack(bool requeue, TimeSpan? redeliveryDelay)
    {
        // T-05 fix: full DLQ produce with CT, original headers, retry-topic support
        var targetTopic = redeliveryDelay is { } delay
            ? Mediana.Kafka.KafkaTransport.RetryTopicName(result.Topic, delay)   // retry-topic for delayed redelivery
            : result.Topic + ".dlq";               // DLQ for permanent failure

        var headers = new Headers
        {
            { "mediana.dlx-reason", System.Text.Encoding.UTF8.GetBytes(requeue ? "nack-requeue" : "nack-no-requeue") },
            { "mediana.original-topic", System.Text.Encoding.UTF8.GetBytes(result.Topic) },
            { "mediana.original-partition", System.Text.Encoding.UTF8.GetBytes(result.Partition.Value.ToString(System.Globalization.CultureInfo.InvariantCulture)) },
            { "mediana.original-offset", System.Text.Encoding.UTF8.GetBytes(result.Offset.Value.ToString(System.Globalization.CultureInfo.InvariantCulture)) },
        };
        // T-05 fix: preserve original headers
        if (result.Message.Headers is { } originalHeaders)
        {
            foreach (var h in originalHeaders)
            {
                if (!h.Key.StartsWith("mediana.", StringComparison.Ordinal))
                {
                    headers.Add(h.Key, h.GetValueBytes());
                }
            }
        }

        var message = new Message<string, byte[]>
        {
            Key = result.Message.Key,
            Value = result.Message.Value,
            Headers = headers,
        };
        await dlqProducer.ProduceAsync(targetTopic, message).ConfigureAwait(false);
        consumer.Commit(result);
    }
}


/// <summary>
/// : Query/StreamQuery kafka-→ NotSupportedException (D11)
/// MedianaConfiguration
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
