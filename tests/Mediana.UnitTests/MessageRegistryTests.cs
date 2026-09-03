using Mediana.Dispatch;
using Mediana.UnitTests.TestMessages;
using Xunit;

namespace Mediana.UnitTests;

public class MessageRegistryTests
{
    private static MessageEntry Entry() =>
        new(HandlerKind.Command, typeof(object), null);

    private static Type[] TestTypes =>
        typeof(MessageRegistryTests).Assembly.GetTypes()
            .Concat(typeof(int).Assembly.GetTypes())
            .ToArray();

    [Fact]
    public void Empty_registry_returns_null_for_any_type()
    {
        Assert.Null(MessageRegistry.Empty.TryGet(typeof(CreateOrder)));
    }

    [Fact]
    public void Add_returns_new_version_and_keeps_original_intact()
    {
        var original = MessageRegistry.Empty;
        var entry = Entry();
        var updated = original.Add(typeof(CreateOrder), entry);

        Assert.Null(original.TryGet(typeof(CreateOrder)));
        Assert.Same(entry, updated.TryGet(typeof(CreateOrder)));
    }

    [Fact]
    public void Add_same_type_twice_throws()
    {
        var registry = MessageRegistry.Empty.Add(typeof(CreateOrder), Entry());
        Assert.Throws<MediatorConfigurationException>(
            () => registry.Add(typeof(CreateOrder), Entry()));
    }

    [Fact]
    public void Chained_adds_accumulate_types()
    {
        var r = MessageRegistry.Empty
            .Add(typeof(CreateOrder), Entry())
            .Add(typeof(GetOrder), Entry())
            .Add(typeof(OrderCreated), Entry());

        Assert.NotNull(r.TryGet(typeof(CreateOrder)));
        Assert.NotNull(r.TryGet(typeof(GetOrder)));
        Assert.NotNull(r.TryGet(typeof(OrderCreated)));
        Assert.Null(r.TryGet(typeof(Ping)));
    }

    [Fact]
    public async Task Concurrent_reads_during_rebuild_never_throw()
    {
        var types = TestTypes.Take(200).ToArray();
        var registry = MessageRegistry.Empty;
        foreach (var type in types)
        {
            registry = registry.Add(type, Entry());
        }

        var stop = 0;
        var reader = Task.Run(() =>
        {
            while (Volatile.Read(ref stop) == 0)
            {
                _ = registry.TryGet(types[100]);
                _ = registry.TryGet(typeof(Ping));
            }
        });

        var extraTypes = TestTypes.Skip(200).Take(80).ToArray();
        await Task.Run(() =>
        {
            foreach (var type in extraTypes)
            {
                registry = registry.Add(type, Entry());
            }
        });

        Interlocked.Exchange(ref stop, 1);
        await reader;

        // финальная версия видит все добавления
        foreach (var type in extraTypes)
        {
            Assert.NotNull(registry.TryGet(type));
        }
    }

    [Fact]
    public async Task Parallel_adds_from_same_base_produce_valid_versions()
    {
        var baseRegistry = MessageRegistry.Empty;
        var types = TestTypes.Take(60).ToArray();
        var versions = new MessageRegistry[types.Length];

        await Task.Run(() =>
        {
            Parallel.For(0, types.Length, i =>
            {
                versions[i] = baseRegistry.Add(types[i], Entry());
            });
        });

        // каждая версия консистентна: содержит свой тип
        for (var i = 0; i < types.Length; i++)
        {
            Assert.NotNull(versions[i].TryGet(types[i]));
        }
    }
}
