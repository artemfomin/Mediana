using Mediana.Messaging;
using Mediana.Consuming;
using Mediana.Reliability;
using Mediana.Transports;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Mediana.RabbitMq;

/// <summary>RabbitMQ.</summary>
[System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec reflection-based JSON.")]
public sealed class RabbitMqConsumerHostFactory(RabbitMqTransport transport) : IConsumerHostFactory
{
    public IConsumerHost Create(ConsumerEndpoint endpoint, Func<ITransportDelivery, CancellationToken, ValueTask> handler)
        => new RabbitMqConsumerHost(transport, endpoint, handler);
}

/// <summary>
/// : prefetch = MaxConcurrency, AsyncEventingBasicConsumer
/// ConsumerPipeline (inbox-+ retry + poison), ack/nack
/// </summary>
[System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec reflection-based JSON.")]
public sealed class RabbitMqConsumerHost(
    RabbitMqTransport transport,
    ConsumerEndpoint endpoint,
    Func<ITransportDelivery, CancellationToken, ValueTask> handler) : IConsumerHost
{
    private IConnection? _connection;
    private IChannel? _channel;
    private AsyncEventingBasicConsumer? _consumer;
    private string? _consumerTag;
    private readonly SemaphoreSlim _handlerLimiter = new(endpoint.MaxConcurrency, endpoint.MaxConcurrency);

    public async Task Start()
    {
        _connection = await transport.CreateConnection(CancellationToken.None).ConfigureAwait(false);
        _channel = await _connection.CreateChannelAsync(options: null, CancellationToken.None).ConfigureAwait(false);
        await _channel.BasicQosAsync(0, (ushort)endpoint.MaxConcurrency, global: false, CancellationToken.None)
            .ConfigureAwait(false);

        _consumer = new AsyncEventingBasicConsumer(_channel);
        _consumer.ReceivedAsync += OnReceived;
        _consumerTag = await _channel.BasicConsumeAsync(
            endpoint.Name,
            autoAck: false,
            _consumer,
            CancellationToken.None).ConfigureAwait(false);
    }

    private async Task OnReceived(object sender, BasicDeliverEventArgs @event)
    {
        await _handlerLimiter.WaitAsync().ConfigureAwait(false);
        try
        {
            var delivery = new RabbitMqDelivery(_channel!, @event);
            await handler(delivery, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // T-02 fix: poison → nack requeue → DLX (<queue>.dlq), unacked-
            try
            {
                await _channel!.BasicNackAsync(@event.DeliveryTag, multiple: false, requeue: false, CancellationToken.None)
                    .ConfigureAwait(false);
            }
            catch (Exception nackEx)
            {
                // nack
                System.Diagnostics.Debug.WriteLine($"Mediana: nack-after-poison failed: {nackEx.Message}");
            }
        }
        finally
        {
            _handlerLimiter.Release();
        }
    }

    public async Task Stop()
    {
        // Graceful drain: → in-flight →
        if (_channel is not null && _consumerTag is not null)
        {
            await _channel.BasicCancelAsync(_consumerTag, noWait: false, CancellationToken.None).ConfigureAwait(false);
        }

        // T-13 fix: drain — permit'timeout
        var drainTimeout = TimeSpan.FromSeconds(30);
        using var drainCts = new CancellationTokenSource(drainTimeout);
        var acquired = 0;
        try
        {
            for (var i = 0; i < endpoint.MaxConcurrency; i++)
            {
                await _handlerLimiter.WaitAsync(drainCts.Token).ConfigureAwait(false);
                acquired++;
            }
        }
        catch (OperationCanceledException)
        {
            // drain timeout — in-flight requeue'
        }

        if (_channel is not null)
        {
            await _channel.CloseAsync(CancellationToken.None).ConfigureAwait(false);
        }

        if (_connection is not null)
        {
            await _connection.CloseAsync().ConfigureAwait(false);
        }

        for (var i = 0; i < acquired; i++)
        {
            _handlerLimiter.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await Stop().ConfigureAwait(false);
        _channel?.Dispose();
        if (_connection is not null)
        {
            await _connection.DisposeAsync().ConfigureAwait(false);
        }

        _handlerLimiter.Dispose();
    }
}

/// <summary>RabbitMQ: ack/nack retry-count .</summary>
[System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec reflection-based JSON.")]
public sealed class RabbitMqDelivery(IChannel channel, BasicDeliverEventArgs args) : ITransportDelivery
{

    public Envelope Envelope { get; } = EnvelopeCodec.Decode(args.Body.ToArray());

    public async ValueTask Ack()
        => await channel.BasicAckAsync(args.DeliveryTag, multiple: false, CancellationToken.None).ConfigureAwait(false);

    public async ValueTask Nack(bool requeue, TimeSpan? redeliveryDelay)
    {
        if (requeue)
        {
            await channel.BasicNackAsync(args.DeliveryTag, multiple: false, requeue: true, CancellationToken.None)
                .ConfigureAwait(false);
            return;
        }

        if (redeliveryDelay is { } delay)
        {
            // DLX-cycle: retry-TTL, ack'
            var retryCount = GetRetryCount() + 1;
            var props = new BasicProperties
            {
                MessageId = args.BasicProperties.MessageId,
                Persistent = true,
                Headers = new Dictionary<string, object?>(args.BasicProperties.Headers ?? new Dictionary<string, object?>())
                {
                    [RabbitMqTransport.RetryCountHeader] = retryCount,
                },
                Expiration = ((int)delay.TotalMilliseconds).ToString(),
            };
            var queue = args.RoutingKey;
            await channel.BasicPublishAsync(
                string.Empty,
                TopologyProvisioner.RetryQueueName(queue, delay),
                mandatory: false,
                basicProperties: props,
                body: args.Body,
                CancellationToken.None).ConfigureAwait(false);
            await channel.BasicAckAsync(args.DeliveryTag, multiple: false, CancellationToken.None).ConfigureAwait(false);
            return;
        }

        // requeue: nack → DLX <queue>.dlq (fingerprint )
        await channel.BasicNackAsync(args.DeliveryTag, multiple: false, requeue: false, CancellationToken.None)
            .ConfigureAwait(false);
    }

    private int GetRetryCount()
        => args.BasicProperties.Headers is { } headers
            && headers.TryGetValue(RabbitMqTransport.RetryCountHeader, out var value)
            && value is int count
            ? count
            : 0;
}

/// <summary>request/reply direct reply-to (§8.1): .</summary>
public sealed class RabbitMqRequestClient(IConnection connection)
{
    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec reflection-based JSON.")]
    public async ValueTask<Envelope> Request(
        Envelope request,
        string destination,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        var channel = await connection.CreateChannelAsync(options: null, cancellationToken).ConfigureAwait(false);
        await using var _ = channel.ConfigureAwait(false);

        var completion = new TaskCompletionSource<Envelope>(TaskCreationOptions.RunContinuationsAsynchronously);
        var correlationId = request.MessageId.ToString();

        // T-07 fix: linked CTS instead of Task.Delay race — clean cancellation
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(timeout);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += (_, received) =>
        {
            // T-07 fix: validate both CorrelationId AND MessageId to prevent reply-spoofing by co-consumer
            if (received.BasicProperties.CorrelationId == correlationId
                && (received.BasicProperties.MessageId is null || received.BasicProperties.MessageId == correlationId))
            {
                try
                {
                    completion.TrySetResult(EnvelopeCodec.Decode(received.Body.ToArray()));
                }
                catch (Exception ex)
                {
                    completion.TrySetException(ex);
                }
            }

            return Task.CompletedTask;
        };

        await channel.BasicConsumeAsync(
            queue: "amq.rabbitmq.reply-to",
            autoAck: true,
            consumer,
            timeoutCts.Token).ConfigureAwait(false);

        var props = new BasicProperties
        {
            MessageId = correlationId,
            CorrelationId = correlationId,
            ReplyTo = "amq.rabbitmq.reply-to",
            Persistent = false,
            ContentType = "application/json",
        };
        await channel.BasicPublishAsync(
            RabbitMqTransport.MedianaExchange,
            destination,
            mandatory: false,
            basicProperties: props,
            body: new ReadOnlyMemory<byte>(EnvelopeCodec.Encode(request)),
            timeoutCts.Token).ConfigureAwait(false);

        // T-07 fix: await completion with linked CT — no Task.Delay race, no leak
        try
        {
#if NET10_0
            return await completion.Task.WaitAsync(timeoutCts.Token).ConfigureAwait(false);
#else
            // netstandard2.1: WaitAsync not available — use WhenAny with CTS registration
            var cancelTask = Task.Delay(Timeout.Infinite, timeoutCts.Token);
            var done = await Task.WhenAny(completion.Task, cancelTask).ConfigureAwait(false);
            if (done == cancelTask)
            {
                throw new RemoteTimeoutException("Request to " + destination + " timed out after " + timeout + ".");
            }
            return await completion.Task.ConfigureAwait(false);
#endif
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw new RemoteTimeoutException("Request to " + destination + " timed out after " + timeout + ".");
        }
    }
}
