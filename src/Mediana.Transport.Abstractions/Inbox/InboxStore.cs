using Mediana.Transports;

namespace Mediana.Inbox;

/// <summary>
/// Inbox: (MessageId, HandlerIdentity) — remote-(§9.1)
/// In-memory default (; DB-outbox-)
/// </summary>
public interface IInboxStore
{
    /// <summary>: true — (), false — .</summary>
    ValueTask<bool> TryBegin(string messageId, string handlerIdentity);

    /// <summary>(/).</summary>
    ValueTask Complete(string messageId, string handlerIdentity);
}

/// <summary>
/// in-memory inbox ()
/// TryAdd
/// </summary>
public sealed class InMemoryInboxStore : IInboxStore
{
    private readonly object _lock = new();
    private readonly HashSet<string> _completed = [];
    private readonly Queue<string> _eviction = [];
    private readonly int _capacity;

    public InMemoryInboxStore(int capacity = 100_000)
    {
        _capacity = capacity;
    }

    public ValueTask<bool> TryBegin(string messageId, string handlerIdentity)
    {
        lock (_lock)
        {
            var ok = _completed.Add(Key(messageId, handlerIdentity));
            if (ok)
            {
                _eviction.Enqueue(Key(messageId, handlerIdentity));
                while (_eviction.Count > _capacity)
                {
                    _completed.Remove(_eviction.Dequeue());
                }
            }

            return new ValueTask<bool>(ok);
        }
    }

    public ValueTask Complete(string messageId, string handlerIdentity)
    {
        lock (_lock)
        {
            // _completed TryBegin
        }

        return default;
    }

    private static string Key(string messageId, string handlerIdentity)
        => messageId + "|" + handlerIdentity;
}
