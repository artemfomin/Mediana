using Mediana.Messaging;
using Mediana.Consuming;
using Mediana.Reliability;
using Mediana.Transports;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Mediana.RabbitMq;

/// <summary>Фабрика хостов консьюмеров RabbitMQ.</summary>
[System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec использует reflection-based JSON.")]
public sealed class RabbitMqConsumerHostFactory(RabbitMqTransport transport) : IConsumerHostFactory
{
    public IConsumerHost Create(ConsumerEndpoint endpoint, Func<ITransportDelivery, CancellationToken, ValueTask> handler)
        => new RabbitMqConsumerHost(transport, endpoint, handler);
}

/// <summary>
/// Хост консьюмера: prefetch = MaxConcurrency, AsyncEventingBasicConsumer,
/// обработка через ConsumerPipeline (inbox-дедуп + retry + poison), ack/nack.
/// </summary>
[System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec использует reflection-based JSON.")]
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
        finally
        {
            _handlerLimiter.Release();
        }
    }

    public async Task Stop()
    {
        // Graceful drain: отписка → in-flight завершаются (семафор) → закрытие канала
        if (_channel is not null && _consumerTag is not null)
        {
            await _channel.BasicCancelAsync(_consumerTag, noWait: false, CancellationToken.None).ConfigureAwait(false);
        }

        await _handlerLimiter.WaitAsync(endpoint.MaxConcurrency).ConfigureAwait(false);
        if (_channel is not null)
        {
            await _channel.CloseAsync(CancellationToken.None).ConfigureAwait(false);
        }

        _connection?.CloseAsync();
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

/// <summary>Доставка RabbitMQ: ack/nack с retry-count заголовком.</summary>
[System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec использует reflection-based JSON.")]
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
            // DLX-cycle: публикуем копию в retry-очередь с TTL, исход ack'аем
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

        // Без requeue: nack → DLX переносит в <queue>.dlq (fingerprint в заголовках)
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

/// <summary>Клиент request/reply поверх direct reply-to (§8.1): без временных очередей.</summary>
public sealed class RabbitMqRequestClient(IConnection connection)
{
    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("EnvelopeCodec использует reflection-based JSON.")]
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
        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += (_, received) =>
        {
            if (received.BasicProperties.CorrelationId == correlationId)
            {
                completion.TrySetResult(EnvelopeCodec.Decode(received.Body.ToArray()));
            }

            return Task.CompletedTask;
        };

        await channel.BasicConsumeAsync(
            queue: "amq.rabbitmq.reply-to",
            autoAck: true,
            consumer,
            cancellationToken).ConfigureAwait(false);

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
            cancellationToken).ConfigureAwait(false);

        var delayTask = Task.Delay(timeout, cancellationToken);
        var done = await Task.WhenAny(completion.Task, delayTask).ConfigureAwait(false);
        if (done == delayTask)
        {
            throw new RemoteTimeoutException("Request to " + destination + " timed out after " + timeout + ".");
        }

        return await completion.Task.ConfigureAwait(false);
    }
}
