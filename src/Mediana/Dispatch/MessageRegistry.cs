using System.Runtime.CompilerServices;
#if NET10_0
using System.Collections.Frozen;
#endif

namespace Mediana.Dispatch;

/// <summary>
/// Иммутабельный реестр сообщений: RuntimeTypeHandle → <see cref="MessageEntry"/>.
/// Чисто функциональный copy-on-write: <see cref="Add"/> НЕ мутирует этот экземпляр —
/// возвращает новую версию на основе его содержимого (последовательность Adds накапливает типы).
/// Конкурентные добавления из одной версии — под внешней синхронизацией вызывающего (документировано);
/// чтение всегда без локов и аллокаций.
/// net10.0 — FrozenDictionary; netstandard2.1 — собственный immutable bucket-массив (D2).
/// </summary>
public sealed class MessageRegistry
{
    public static MessageRegistry Empty { get; } = Build(
        Array.Empty<KeyValuePair<RuntimeTypeHandle, MessageEntry>>());

#if NET10_0
    private readonly FrozenDictionary<RuntimeTypeHandle, MessageEntry> _map;
#else
    private readonly Bucket[] _buckets;
#endif
    private readonly KeyValuePair<RuntimeTypeHandle, MessageEntry>[] _items;

#if NET10_0
    private MessageRegistry(FrozenDictionary<RuntimeTypeHandle, MessageEntry> map, KeyValuePair<RuntimeTypeHandle, MessageEntry>[] items)
    {
        _map = map;
        _items = items;
    }
#else
    private MessageRegistry(Bucket[] buckets, KeyValuePair<RuntimeTypeHandle, MessageEntry>[] items)
    {
        _buckets = buckets;
        _items = items;
    }

    private const int InitialBuckets = 16;

    private sealed class Bucket
    {
        public RuntimeTypeHandle Handle;
        public MessageEntry Entry = null!;
        public Bucket? Next;
    }
#endif

    /// <summary>Чтение без аллокаций и локов. null — тип не зарегистрирован.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MessageEntry? TryGet(Type messageType)
    {
#if NET10_0
        return _map.TryGetValue(messageType.TypeHandle, out var entry) ? entry : null;
#else
        var buckets = _buckets;
        var bucket = buckets[(uint)messageType.TypeHandle.GetHashCode() % (uint)buckets.Length];
        while (bucket is not null)
        {
            if (bucket.Handle.Equals(messageType.TypeHandle))
            {
                return bucket.Entry;
            }

            bucket = bucket.Next;
        }

        return null;
#endif
    }

    /// <summary>
    /// Copy-on-write добавление: чистая функция — этот экземпляр неизменен, возвращается новая версия
    /// со всеми его типами плюс новый. Дубликат — ошибка конфигурации.
    /// </summary>
    public MessageRegistry Add(Type messageType, MessageEntry entry)
    {
        if (TryGet(messageType) is not null)
        {
            throw new MediatorConfigurationException(
                $"Message type {messageType} is already registered.");
        }

        var items = new KeyValuePair<RuntimeTypeHandle, MessageEntry>[_items.Length + 1];
        for (var i = 0; i < _items.Length; i++)
        {
            if (_items[i].Key.Equals(messageType.TypeHandle))
            {
                throw new MediatorConfigurationException(
                    $"Message type {messageType} is already registered.");
            }

            items[i] = _items[i];
        }

        items[_items.Length] = new KeyValuePair<RuntimeTypeHandle, MessageEntry>(messageType.TypeHandle, entry);
        return Build(items);
    }

    internal static MessageRegistry Build(IEnumerable<KeyValuePair<RuntimeTypeHandle, MessageEntry>> pairs)
    {
#if NET10_0
        var items = pairs.ToArray();
        return new MessageRegistry(items.ToFrozenDictionary(), items);
#else
        var items = pairs.ToArray();
        var bucketCount = InitialBuckets;
        while (bucketCount < items.Length * 2)
        {
            bucketCount <<= 1;
        }

        var buckets = new Bucket[bucketCount];
        foreach (var pair in items)
        {
            var index = (uint)pair.Key.GetHashCode() % (uint)bucketCount;
            buckets[index] = new Bucket { Handle = pair.Key, Entry = pair.Value, Next = buckets[index] };
        }

        return new MessageRegistry(buckets, items);
#endif
    }
}
