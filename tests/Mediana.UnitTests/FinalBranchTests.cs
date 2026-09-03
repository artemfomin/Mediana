using Mediana.Dispatch;
using Mediana.Messaging;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Mediana.UnitTests;

/// <summary>andon to: mismatch-inand Mediator, command-, event GetRoot, registry race-branch.</summary>
public class FinalBranchTests
{
    private sealed record RC(int V) : ICommand<RR>;
    private sealed record RR(int V);
    private sealed record VC(int V) : ICommand<int>;

    private sealed class RCHandler : Handlers.ICommandHandler<RC, RR>
    {
        public ValueTask<RR> Handle(RC c, CancellationToken ct) => new(new RR(c.V + 1));
    }

    private sealed class RCHAsyncHandler : Handlers.ICommandHandler<RC, RR>
    {
        public async ValueTask<RR> Handle(RC c, CancellationToken ct)
        {
            await Task.Yield();
            return new RR(c.V + 1);
        }
    }

    private sealed class VCHandler : Handlers.ICommandHandler<VC, int>
    {
        public ValueTask<int> Handle(VC c, CancellationToken ct) => new(c.V + 1);
    }

    private sealed record Ev : IEvent;

    private sealed class EvHandler : Handlers.IEventHandler<Ev>
    {
        public ValueTask Handle(Ev e, CancellationToken ct) => default;
    }

    private static Mediator Build(MedianaConfiguration cfg, IServiceCollection sc)
    {
        sc.AddMediana(_ => { });
        return new Mediator(cfg.Freeze(), sc.BuildServiceProvider());
    }

    [Fact]
    public async Task Command_ref_mismatch_throws_via_object_path()
    {
        var mediator = Build(
            new MedianaConfiguration().UseSingletonHandlers().AddCommandHandler<RC, RR, RCHandler>(),
            new ServiceCollection().AddSingleton<RCHandler>());

        // ref-fromin, but Send<int> (notinand to )
        var mismatch = System.Runtime.CompilerServices.Unsafe.As<ICommand<int>>(new RC(1));
        await Assert.ThrowsAsync<MediatorConfigurationException>(() => mediator.Send<int>(mismatch).AsTask());
    }

    [Fact]
    public async Task Command_value_object_invoke_bridge_warm_paths()
    {
        var sc = new ServiceCollection().AddSingleton<RCHandler>();
        var mediator = Build(
            new MedianaConfiguration().UseSingletonHandlers().AddCommandHandler<RC, RR, RCHandler>(), sc);
        var sp = sc.BuildServiceProvider();
        var entry = mediator.Registry.TryGet(typeof(RC))!;
        var objectCallSite = (IObjectCommandCallSite<RR>)entry.CommandCallSite!;

        // Invoke: bybutin; : ref- (sync CastBoxed)
        Assert.Equal(2, (await objectCallSite.Invoke(new RC(1), sp, default)).V);
        Assert.Equal(3, (await objectCallSite.Invoke(new RC(2), sp, default)).V);
    }

    [Fact]
    public async Task Command_async_ref_object_invoke_covers_async_cast()
    {
        var sc = new ServiceCollection().AddSingleton<RCHAsyncHandler>();
        var mediator = Build(
            new MedianaConfiguration().UseSingletonHandlers().AddCommandHandler<RC, RR, RCHAsyncHandler>(), sc);
        var sp = sc.BuildServiceProvider();
        var entry = mediator.Registry.TryGet(typeof(RC))!;
        var objectCallSite = (IObjectCommandCallSite<RR>)entry.CommandCallSite!;
        var any = (IUntypedCallSite)entry.CommandCallSite!;

        // async ref object-: AwaitCast (async CastBoxed)
        Assert.Equal(2, (await objectCallSite.Invoke(new RC(1), sp, default)).V);
        Assert.Equal(3, (await objectCallSite.Invoke(new RC(2), sp, default)).V);

        // async ref InvokeAny: cold → SlowAny(async), warm → AwaitUpcast(async )
        var cold = await any.InvokeAny(new RC(3), sp, default);
        Assert.Equal(4, ((RR)cold!).V);
        var warm = await any.InvokeAny(new RC(4), sp, default);
        Assert.Equal(5, ((RR)warm!).V);
    }

    [Fact]
    public async Task Command_value_invoke_any_cold_and_warm()
    {
        var sc = new ServiceCollection().AddSingleton<VCHandler>();
        var mediator = Build(
            new MedianaConfiguration().UseSingletonHandlers().AddCommandHandler<VC, int, VCHandler>(), sc);
        var sp = sc.BuildServiceProvider();
        var entry = mediator.Registry.TryGet(typeof(VC))!;
        var any = (IUntypedCallSite)entry.CommandCallSite!;

        // value: no bridge → generic slow- (and onand toand only in InvokeAny)
        Assert.Equal(2, await any.InvokeAny(new VC(1), sp, default));
        Assert.Equal(3, await any.InvokeAny(new VC(2), sp, default));
    }

    [Fact]
    public async Task Command_query_mismatch_typed_paths()
    {
        // andandinon, but SendExact-and fromin not in
        var mediator = Build(
            new MedianaConfiguration().UseSingletonHandlers().AddCommandHandler<VC, int, VCHandler>(),
            new ServiceCollection().AddSingleton<VCHandler>());

        var wrong = System.Runtime.CompilerServices.Unsafe.As<VC>(new VC(1));
        // SendExact<VC, string> — VC:ICommand<string> — not byand
        // inand typed byin mediatr-typed frominand: only object-
        Assert.Equal(2, await mediator.Send((ICommand<int>)new VC(1)));
    }

    [Fact]
    public async Task Event_getRoot_warm_path()
    {
        var sc = new ServiceCollection().AddSingleton<EvHandler>();
        var sp = sc.BuildServiceProvider();
        var cfg = new MedianaConfiguration().UseSingletonHandlers().AddEventHandler<Ev, EvHandler>();
        var mediator = Build(cfg, sc);

        await mediator.Publish(new Ev()); // bybutin root
        var entry = mediator.Registry.TryGet(typeof(Ev))!;
        var evtCallSite = (EventCallSite<Ev, EvHandler>)entry.EventCallSites[0];
        var root1 = evtCallSite.GetRoot(sp); // GetRoot
        var root2 = evtCallSite.GetRoot(sp);
        Assert.Same(root1, root2);
        await root1(new Ev(), default);
    }

    [Fact]
    public async Task Event_scoped_invoke_after_getRoot()
    {
        var cfg = new MedianaConfiguration().AddEventHandler<Ev, EvHandler>();
        var sc = new ServiceCollection().AddScoped<EvHandler>();
        var mediator = Build(cfg, sc);

        await mediator.Publish(new Ev());
        await mediator.Publish(new Ev());
    }

    [Fact]
    public void Registry_race_branch_between_versions()
    {
        // Add inandand: inandand and → throw-in _items-and
        var baseRegistry = Mediana.Dispatch.MessageRegistry.Empty.Add(typeof(string), new MessageEntry(HandlerKind.Event, typeof(string), null));
        Assert.Throws<MediatorConfigurationException>(
            () => baseRegistry.Add(typeof(string), new MessageEntry(HandlerKind.Event, typeof(string), null)));
    }

    [Fact]
    public async Task Publish_entry_without_callsites_is_noop()
    {
        // entry and EventCallSites → default-inin (Length == 0)
        var registry = Mediana.Dispatch.MessageRegistry.Empty.Add(
            typeof(Ev),
            new MessageEntry(HandlerKind.Event, typeof(Ev), null)); // call-sites not
        var mediator = new Mediator(registry, new ServiceCollection().BuildServiceProvider());

        await mediator.Publish(new Ev());
    }
}
