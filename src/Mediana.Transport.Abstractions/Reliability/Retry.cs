namespace Mediana.Reliability;

/// <summary>(, D14 — Polly).</summary>
public enum BackoffStrategy
{
    Fixed,
    Incremental,
    Exponential,
}

/// <summary>per message type (§9.2). : Exponential 50ms→5s, 5 .</summary>
public sealed record RetryPolicy
{
    public required BackoffStrategy Strategy { get; init; }

    /// <summary>.</summary>
    public required TimeSpan BaseDelay { get; init; }

    /// <summary>(cap).</summary>
    public TimeSpan MaxDelay { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>in-process /DLQ.</summary>
    public int MaxAttempts { get; init; } = 5;

    /// <summary>Jitter (0..1]: , thundering herd.</summary>
    public double Jitter { get; init; }

    public static RetryPolicy Default { get; } = new()
    {
        Strategy = BackoffStrategy.Exponential,
        BaseDelay = TimeSpan.FromMilliseconds(50),
        MaxDelay = TimeSpan.FromSeconds(5),
        MaxAttempts = 5,
        Jitter = 0.2,
    };

    /// <summary>attempt (1-based) — seed.</summary>
    public TimeSpan DelayFor(int attempt, Random? random = null)
    {
        if (attempt < 1)
        {
            attempt = 1;
        }

        double multiplier = Strategy switch
        {
            BackoffStrategy.Fixed => 1.0,
            BackoffStrategy.Incremental => attempt,
            BackoffStrategy.Exponential => Math.Pow(2, attempt - 1),
            _ => 1.0,
        };

        var delay = TimeSpan.FromMilliseconds(BaseDelay.TotalMilliseconds * multiplier);
        if (delay > MaxDelay)
        {
            delay = MaxDelay;
        }

        if (Jitter > 0 && random is not null)
        {
            var spread = delay.TotalMilliseconds * Jitter;
            delay = TimeSpan.FromMilliseconds(Math.Max(0, delay.TotalMilliseconds - random.NextDouble() * spread));
        }

        return delay;
    }
}

/// <summary>: (in-process ).</summary>
public static class RetryEngine
{
    /// <summary>
    /// in-process
    /// <paramref name="isRetryable"/>-(non-retryable)
    /// </summary>
    public static async ValueTask<RetryOutcome> Execute(
        Func<int, CancellationToken, ValueTask> handler,
        RetryPolicy policy,
        Func<Exception, bool>? isRetryable = null,
        Random? random = null,
        CancellationToken cancellationToken = default)
    {
        isRetryable ??= static _ => true;
        var attempt = 0;
        while (true)
        {
            attempt++;
            try
            {
                await handler(attempt, cancellationToken).ConfigureAwait(false);
                return RetryOutcome.Succeeded;
            }
            catch (Exception ex) when (isRetryable(ex) && attempt < policy.MaxAttempts && !cancellationToken.IsCancellationRequested)
            {
                var delay = policy.DelayFor(attempt, random);
                if (delay > TimeSpan.Zero)
                {
                    await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                }
            }
        }
    }
}

/// <summary>retry-.</summary>
public enum RetryOutcome
{
    Succeeded,
    Exhausted,
}

/// <summary>
/// Poison detection (§9.3): /ismatch non-retryable —
/// DLQ
/// </summary>
public static class PoisonDetector
{
    /// <summary>: poison (DLQ ) retryable.</summary>
    public static bool IsPoison(Exception exception)
    {
        return exception is Mediana.Messaging.SerializationException
            or FormatException
            // T-11 fix: removed volatile classifications
            or MediatorConfigurationException;
    }
}
