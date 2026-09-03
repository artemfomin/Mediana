using Mediana.Transports;
using Mediana.Dispatch;
using Mediana.Messaging;
using Mediana.Outbox;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Mediana.UnitTests;

/// <summary>Closing slice: byand and inand (null-, Diagnostics listener, and-scan, DI outbox).</summary>
public class ClosingBranchTests
{
    private sealed record C2(int V) : ICommand<int>;
    private sealed record Q2(int V) : IQuery<int>;
    private sealed record E2 : IEvent;

    private sealed class C2H : Handlers.ICommandHandler<C2, int>
    {
        public ValueTask<int> Handle(C2 c, CancellationToken ct) => new(c.V + 1);
    }

    private sealed class Q2H : Handlers.IQueryHandler<Q2, int>
    {
        public ValueTask<int> Handle(Q2 q, CancellationToken ct) => new(q.V + 1);
    }

    private sealed class E2H : Handlers.IEventHandler<E2>
    {
        public ValueTask Handle(E2 e, CancellationToken ct) => default;
    }

    // 1) Scoped-and: = null (RefResponse && !singleton) → InvokeAny and in SlowAny generic
    [Fact]
    public async Task Scoped_ref_command_invoke_any_null_bridge()
    {
        var cfg = new MedianaConfiguration()
            .AddCommandHandler<C2, int, C2H>();
        var sc = new ServiceCollection().AddScoped<C2H>();
        sc.AddMediana(_ => { });
        var sp = sc.BuildServiceProvider();
        var registry = cfg.Freeze();
        var mediator = new Mediator(registry, sp);

        var entry = registry.TryGet(typeof(C2))!;
        var any = (IUntypedCallSite)entry.CommandCallSite!;
        Assert.Equal(2, await any.InvokeAny(new C2(1), sp, default));
    }

    // 2) Query scoped InvokeAny null-then
    [Fact]
    public async Task Scoped_query_invoke_any_null_bridge()
    {
        var cfg = new MedianaConfiguration()
            .AddQueryHandler<Q2, int, Q2H>();
        var sc = new ServiceCollection().AddScoped<Q2H>();
        sc.AddMediana(_ => { });
        var sp = sc.BuildServiceProvider();
        var registry = cfg.Freeze();

        var entry = registry.TryGet(typeof(Q2))!;
        var any = (IUntypedCallSite)entry.QueryCallSite!;
        Assert.Equal(2, await any.InvokeAny(new Q2(1), sp, default));
    }

    // 3) Diagnostics: listener-inand all Start*
    [Fact]
    public void Diagnostics_all_spans_with_listener()
    {
        using var listener = new System.Diagnostics.ActivityListener
        {
            ShouldListenTo = s => s.Name == "Mediana",
            SampleUsingParentId = (ref System.Diagnostics.ActivityCreationOptions<string> o) => System.Diagnostics.ActivitySamplingResult.AllData,
            Sample = (ref System.Diagnostics.ActivityCreationOptions<System.Diagnostics.ActivityContext> o) => System.Diagnostics.ActivitySamplingResult.AllData,
        };
        System.Diagnostics.ActivitySource.AddActivityListener(listener);

        using var dispatch = MedianaDiagnostics.StartDispatch("D");
        using var publish = MedianaDiagnostics.StartPublish("P");
        using var consume = MedianaDiagnostics.StartConsume("C");
        Assert.NotNull(dispatch);
        Assert.NotNull(publish);
        Assert.NotNull(consume);
    }

    // 4) Configuration: scan and without Mediana-handlers + byandand without handlers + VoidResponse-
    [Fact]
    public void Configuration_scan_clean_assembly_and_policy_guard()
    {
        var cfg = new MedianaConfiguration()
            .AddHandlersFromAssembly(typeof(object).Assembly); // BCL: handlers no
        var registry = cfg.Freeze();
        Assert.Null(registry.TryGet(typeof(C2)));

        var sc = new ServiceCollection();
        Assert.Throws<MediatorConfigurationException>(() =>
            sc.AddMediana(c => c.AddHandlersFromAssembly(typeof(object).Assembly).SetEventPolicy<E2>(EventDispatchPolicy.Parallel)));
    }

    // 5) DI-andand outbox
    [Fact]
    public void AddMedianaOutbox_registers_collector_and_options()
    {
        var sc = new ServiceCollection();
        sc.AddMedianaOutbox(o => o with { BatchSize = 7 });
        var sp = sc.BuildServiceProvider();

        Assert.NotNull(sp.GetRequiredService<OutboxCollector>());
        Assert.Equal(7, sp.GetRequiredService<OutboxRelayOptions>().BatchSize);
        var collector = sp.GetRequiredService<OutboxCollector>();
        collector.Add(Envelope.Create("X", "1", []), "q");
        Assert.Equal(1, collector.Count);
    }

    // 6) Mediator: notfromininand on command-in and untyped-null fallback
    [Fact]
    public async Task Mediator_untyped_fallback_null_callsite_throws()
    {
        // entry without CommandCallSite → mismatch-andand
        var registry = Mediana.Dispatch.MessageRegistry.Empty.Add(
            typeof(C2), new MessageEntry(HandlerKind.Command, typeof(C2), typeof(int)));
        var mediator = new Mediator(registry, new ServiceCollection().BuildServiceProvider());

        await Assert.ThrowsAsync<MediatorConfigurationException>(
            () => mediator.Send((ICommand<int>)new C2(1)).AsTask());
        await Assert.ThrowsAsync<MediatorConfigurationException>(
            () => mediator.Send((IQuery<int>)System.Runtime.CompilerServices.Unsafe.As<IQuery<int>>(new Q2(1))).AsTask());
    }

    // 7) ChainState: byinthen Take by Return ()
    [Fact]
    public async Task ChainState_pool_reuse()
    {
        await Task.CompletedTask;
        Pipeline.HandlerDelegate<C2, int> terminal = (_, _) => new ValueTask<int>(1);
        var sp = new ServiceCollection().BuildServiceProvider();
        var s1 = ChainState<C2, int>.Take(sp, [], terminal);
        var r1 = s1.Next(new C2(1), default);
        s1.Return();
        var s2 = ChainState<C2, int>.Take(sp, [], terminal);
        var r2 = s2.Next(new C2(2), default);
        s2.Return();
        Assert.Equal(1, await r1.AsTask());
        Assert.Equal(1, await r2.AsTask());
        await Task.CompletedTask;
    }

    // 8) DI outbox without configure (null-in)
    [Fact]
    public void AddMedianaOutbox_without_configure()
    {
        var sc = new ServiceCollection();
        sc.AddMedianaOutbox();
        var sp = sc.BuildServiceProvider();
        Assert.Equal(100, sp.GetRequiredService<OutboxRelayOptions>().BatchSize);
    }

    // 9) Relay StopAsync to Start (graceful no-op inand)
    [Fact]
    public async Task Relay_stop_before_start()
    {
        var store = new OutboxTestHelpers.FakeOutboxStoreProxy();
        var relay = new OutboxRelay(store, _ => new ValueTask<ITransportPublisher>(new OutboxTestHelpers.NullPublisher()));
        await relay.StopAsync(CancellationToken.None);
        relay.Dispose();
    }

    // 10) Mediator.Stream notandandinbut
    [Fact]
    public async Task Stream_unregistered_throws()
    {
        var mediator = new Mediator(Mediana.Dispatch.MessageRegistry.Empty, new ServiceCollection().BuildServiceProvider());
        await Assert.ThrowsAsync<MediatorConfigurationException>(async () =>
        {
            await foreach (var r in mediator.Stream((IStreamQuery<int>)new NsStreamStub()))
            {
            }
        });
    }

    private sealed record NsStreamStub() : IStreamQuery<int>;
}
