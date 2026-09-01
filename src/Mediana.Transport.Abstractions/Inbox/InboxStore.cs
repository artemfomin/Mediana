using Mediana.Transports;

namespace Mediana.Inbox;

/// <summary>
/// Inbox: дедупликация по (MessageId, HandlerIdentity) — всегда включён для remote-консюмеров (§9.1).
/// In-memory реализация — default (не переживает рестарт; DB-реализации — в outbox-пакетах).
/// </summary>
public interface IInboxStore
{
    /// <summary>Попытаться начать обработку: true — первый раз (можно выполнять), false — дубликат.</summary>
    ValueTask<bool> TryBegin(string messageId, string handlerIdentity);

    /// <summary>Пометить обработку завершённой (для диагностики/очистки).</summary>
    ValueTask Complete(string messageId, string handlerIdentity);
}

/// <summary>
/// Потокобезопасный in-memory inbox с вытеснением по размеру (защита от роста).
/// Гонки двойной доставки побеждаются атомарным TryAdd.
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
            // запись уже в _completed после TryBegin; метод оставлен для симметрии контракта
        }

        return default;
    }

    private static string Key(string messageId, string handlerIdentity)
        => messageId + "|" + handlerIdentity;
}
