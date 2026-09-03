using Mediana.Inbox;
using Mediana.Messaging;
using Mediana.Reliability;
using Mediana.Transports;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Mediana.Consuming;

/// <summary>
/// : inbox-→ retry-→
/// (§9)
/// </summary>
public sealed class ConsumerPipeline
{
    private readonly IInboxStore _inbox;
    private readonly ILogger? _logger;

    public ConsumerPipeline(IInboxStore inbox, ILogger? logger = null)
    {
        _inbox = inbox;
        _logger = logger;
    }

    /// <summary>: → retry → handler → ack/nack/DLQ.</summary>
    public async ValueTask Process(
        ITransportDelivery delivery,
        string handlerIdentity,
        Func<Envelope, CancellationToken, ValueTask> handler,
        RetryPolicy? policy = null,
        Func<Exception, bool>? isRetryable = null,
        CancellationToken cancellationToken = default)
    {
        policy ??= RetryPolicy.Default;
        isRetryable ??= static ex => !PoisonDetector.IsPoison(ex);

        if (!await _inbox.TryBegin(delivery.Envelope.MessageId.ToString("N"), handlerIdentity).ConfigureAwait(false))
        {
            // : skip + ack (at-least-once → effectively-once, §9.4)
            _logger?.LogDebug("Duplicate delivery {MessageId} skipped", delivery.Envelope.MessageId);
            await delivery.Ack().ConfigureAwait(false);
            return;
        }

        try
        {
            await RetryEngine.Execute(
                (attempt, ct) => handler(delivery.Envelope, ct),
                policy,
                isRetryable,
                #if NET10_0
                random: Random.Shared,
#else
                random: JitterRandom.Shared,
#endif
                cancellationToken: cancellationToken).ConfigureAwait(false);
            await delivery.Ack().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Handler failed for {MessageId} after retries", delivery.Envelope.MessageId);
            // nack requeue → dead-letter (fingerprint )
            await delivery.Nack(requeue: false, redeliveryDelay: null).ConfigureAwait(false);
        }
    }
}

/// <summary>
/// BackgroundService: bounded Channel backpressure
/// graceful drain (§5.3)
/// </summary>
public sealed class ConsumerHostService : BackgroundService
{
    private readonly IConsumerHost _host;

    public ConsumerHostService(IConsumerHost host)
    {
        _host = host;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await _host.Start().ConfigureAwait(false);
        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // : graceful drain Stop
        }

        await _host.Stop().ConfigureAwait(false);
    }
}

#if !NET10_0
/// <summary>ns2.1 fallback Random.Shared (T-12): thread-safe jitter-.</summary>
internal static class JitterRandom
{
    // R7 fix: thread-safe ThreadLocal (System.Random ns2.1)
    [ThreadStatic]
    private static Random? _random;
    public static Random Shared => _random ??= new Random();
}
#endif
