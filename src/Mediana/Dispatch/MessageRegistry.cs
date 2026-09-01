using System.Runtime.CompilerServices;
#if NET10_0
using System.Collections.Frozen;
#endif

namespace Mediana.Dispatch;

/// <summary>
/// Иммутабельный реестр сообщений: RuntimeTypeHandle → <see cref="MessageEntry"/>.
/// Copy-on-write: <see cref="Add"/> строит новую карту и публикует через Volatile.Write; читатели не лочатся (§5.2).
/// net10.0 — FrozenDictionary; netstandard2.1 — собственный immutable bucket-массив по RuntimeTypeHandle (D2).
/// </summary>
public sealed class MessageRegistry
{
    public static MessageRegistry Empty { get; } = Build(
        Array.Empty<KeyValuePair<RuntimeTypeHandle, MessageEntry>>());

    private readonly object _writeLock = new();
#if NET10_0
    private readonly System.Collections.Frozen.FrozenDictionary<RuntimeTypeHandle, MessageEntry> _map;
#else
    private readonly Bucket[] _buckets;
#endif
    private volatile MessageRegistry? _latest;

#if NET10_0
    private MessageRegistry(
        System.Collections.Frozen.FrozenDictionary<RuntimeTypeHandle, MessageEntry> map,
        IReadOnlyCollection<KeyValuePair<RuntimeTypeHandle, MessageEntry>> seed)
    {
        _map = map;
        _seed = seed;
    }

    private readonly IReadOnlyCollection<KeyValuePair<RuntimeTypeHandle, MessageEntry>> _seed;
#else
    private MessageRegistry(Bucket[] buckets, IReadOnlyCollection<KeyValuePair<RuntimeTypeHandle, MessageEntry>> seed)
    {
        _buckets = buckets;
        _seed = seed;
    }

    private readonly IReadOnlyCollection<KeyValuePair<RuntimeTypeHandle, MessageEntry>> _seed;

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
    /// Copy-on-write добавление: возвращает НОВУЮ версию реестра (эта остаётся консистентной для читателей).
    /// Однопоточный lock сериализует писателей; публикация — volatile write.
    /// </summary>
    public MessageRegistry Add(Type messageType, MessageEntry entry)
    {
        if (TryGet(messageType) is not null)
        {
            throw new MediatorConfigurationException(
                $"Message type {messageType} is already registered. Use the latest registry version or remove the previous entry first.");
        }

        lock (_writeLock)
        {
            var latest = _latest ?? this;
            var seed = new List<KeyValuePair<RuntimeTypeHandle, MessageEntry>>(latest._seed.Count + 1)
            {
                Capacity = latest._seed.Count + 1,
            };
            foreach (var pair in latest._seed)
            {
                if (pair.Key.Equals(messageType.TypeHandle))
                {
                    throw new MediatorConfigurationException(
                        $"Message type {messageType} is already registered.");
                }

                seed.Add(pair);
            }

            seed.Add(new KeyValuePair<RuntimeTypeHandle, MessageEntry>(messageType.TypeHandle, entry));

            var next = Build(seed);
            _latest = next;
            return next;
        }
    }

    internal static MessageRegistry Build(IEnumerable<KeyValuePair<RuntimeTypeHandle, MessageEntry>> pairs)
    {
#if NET10_0
        return new MessageRegistry(pairs.ToFrozenDictionary(), pairs.ToArray());
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

    /// <summary>Самая свежая версия после copy-on-write добавлений (для диспетчера после runtime-регистрации).</summary>
    public MessageRegistry Latest => _latest ?? this;
}
