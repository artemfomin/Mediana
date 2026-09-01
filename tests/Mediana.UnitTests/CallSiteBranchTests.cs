using Mediana.Dispatch;
using Mediana.Messaging;
using Mediana.Pipeline;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Mediana.UnitTests;

/// <summary>Систематический обход веток Command/Query/Event call-site'ов: все режимы и пути.</summary>
public class CallSiteBranchTests
{
    private sealed record VCmd(int V) : ICommand<int>;
    private sealed record RCmd(int V) : ICommand<RResp>;
    private sealed record RResp(int V);
    private sealed record VQuery(int V) : IQuery<int>;
    private sealed record RQuery(int V) : IQuery<RResp>;

    private sealed class VCmdHandler : Handlers.ICommandHandler<VCmd, int>
    {
        public ValueTask<int> Handle(VCmd c, CancellationToken ct) => new(c.V + 1);
    }

    private sealed class VCmdAsyncHandler : Handlers.ICommandHandler<VCmd, int>
    {
        public async ValueTask<int> Handle(VCmd c, CancellationToken ct)
        {
            await Task.Yield();
            return c.V + 1;
        }
    }

    private sealed class RCmdHandler : Handlers.ICommandHandler<RCmd, RResp>
    {
        public ValueTask<RResp> Handle(RCmd c, CancellationToken ct) => new(new RResp(c.V + 1));
    }

    private sealed class RCmdAsyncHandler : Handlers.ICommandHandler<RCmd, RResp>
    {
        public async ValueTask<RResp> Handle(RCmd c, CancellationToken ct)
        {
            await Task.Yield();
            return new RResp(c.V + 1);
        }
    }

    private sealed class VQueryHandler : Handlers.IQueryHandler<VQuery, int>
    {
        public ValueTask<int> Handle(VQuery q, CancellationToken ct) => new(q.V + 1);
    }

    private sealed class VQueryAsyncHandler : Handlers.IQueryHandler<VQuery, int>
    {
        public async ValueTask<int> Handle(VQuery q, CancellationToken ct)
        {
            await Task.Yield();
            return q.V + 1;
        }
    }

    private sealed class RQueryHandler : Handlers.IQueryHandler<RQuery, RResp>
    {
        public ValueTask<RResp> Handle(RQuery q, CancellationToken ct) => new(new RResp(q.V + 1));
    }

    private sealed class RQueryAsyncHandler : Handlers.IQueryHandler<RQuery, RResp>
    {
        public async ValueTask<RResp> Handle(RQuery q, CancellationToken ct)
        {
            await Task.Yield();
            return new RResp(q.V + 1);
        }
    }

    private sealed class Beh1 : IPipelineBehavior<VCmd, int>
    {
        public ValueTask<int> Handle(VCmd r, RequestHandlerDelegate<VCmd, int> next, CancellationToken ct) => next(r, ct);
    }

    private sealed class Beh2 : IPipelineBehavior<VCmd, int>
    {
        public ValueTask<int> Handle(VCmd r, RequestHandlerDelegate<VCmd, int> next, CancellationToken ct) => next(r, ct);
    }

    private sealed class RBeh : IPipelineBehavior<RCmd, RResp>
    {
        public ValueTask<RResp> Handle(RCmd r, RequestHandlerDelegate<RCmd, RResp> next, CancellationToken ct) => next(r, ct);
    }

    private static Mediator BuildMediator(MedianaConfiguration cfg, IServiceCollection services)
    {
        services.AddMediana(_ => { });
        var sp = services.BuildServiceProvider();
        var registry = cfg.Freeze();
        return new Mediator(registry, sp);
    }

    // ── Command: матрица путей ───────────────────────────────────────────────

    [Fact]
    public async Task Command_value_singleton_with_behaviors_all_paths()
    {
        var cfg = new MedianaConfiguration().UseSingletonHandlers()
            .AddCommandHandler<VCmd, int, VCmdHandler>()
            .AddCommandHandler<RCmd, RResp, RCmdHandler>()
            .AddBehavior<VCmd, int, Beh1>()
            .AddBehavior<VCmd, int, Beh2>()
            .AddBehavior<RCmd, RResp, RBeh>();
        var services = new ServiceCollection()
            .AddSingleton<VCmdHandler>()
            .AddSingleton<RCmdHandler>()
            .AddSingleton<Beh1>()
            .AddSingleton<Beh2>()
            .AddSingleton<RBeh>();
        var mediator = BuildMediator(cfg, services);

        // первый вызов: Slow-компоновка; повторные: мост/typed fast-path (инкрементные входы)
        Assert.Equal(2, await mediator.Send((ICommand<int>)new VCmd(1)));
        Assert.Equal(3, await mediator.Send((ICommand<int>)new VCmd(2)));
        Assert.Equal(3, (await mediator.Send((ICommand<RResp>)new RCmd(2))).V);
        Assert.Equal(4, (await mediator.Send((ICommand<RResp>)new RCmd(3))).V);
        Assert.Equal(3, await mediator.SendExact<VCmd, int>(new VCmd(2)));
        Assert.Equal(5, (await mediator.Send((ICommand<RResp>)new RCmd(4))).V);
    }

    [Fact]
    public async Task Command_value_scoped_with_behaviors()
    {
        var cfg = new MedianaConfiguration()
            .AddCommandHandler<VCmd, int, VCmdHandler>()
            .AddBehavior<VCmd, int, Beh1>()
            .AddBehavior<VCmd, int, Beh2>();
        var services = new ServiceCollection()
            .AddScoped<VCmdHandler>()
            .AddScoped<Beh1>()
            .AddScoped<Beh2>();
        var mediator = BuildMediator(cfg, services);

        var vcmd = (ICommand<int>)new VCmd(4);
        Assert.Equal(5, await mediator.Send(vcmd));
        Assert.Equal(6, await mediator.Send((ICommand<int>)new VCmd(5)));
        Assert.Equal(7, await mediator.SendExact<VCmd, int>(new VCmd(6)));
    }

    [Fact]
    public async Task Command_ref_scoped_with_behavior()
    {
        var cfg = new MedianaConfiguration()
            .AddCommandHandler<RCmd, RResp, RCmdHandler>()
            .AddBehavior<RCmd, RResp, RBeh>();
        var services = new ServiceCollection()
            .AddScoped<RCmdHandler>()
            .AddScoped<RBeh>();
        var mediator = BuildMediator(cfg, services);

        Assert.Equal(3, (await mediator.Send((ICommand<RResp>)new RCmd(2))).V);
    }

    [Fact]
    public async Task Command_async_handlers_through_pooled_states()
    {
        var cfg = new MedianaConfiguration().UseSingletonHandlers()
            .AddCommandHandler<VCmd, int, VCmdAsyncHandler>()
            .AddCommandHandler<RCmd, RResp, RCmdAsyncHandler>();
        var services = new ServiceCollection()
            .AddSingleton<VCmdAsyncHandler>()
            .AddSingleton<RCmdAsyncHandler>();
        var mediator = BuildMediator(cfg, services);

        Assert.Equal(11, await mediator.Send((ICommand<int>)new VCmd(10)));
        Assert.Equal(12, (await mediator.Send((ICommand<RResp>)new RCmd(11))).V);
    }

    [Fact]
    public async Task Command_async_scoped()
    {
        var cfg = new MedianaConfiguration()
            .AddCommandHandler<VCmd, int, VCmdAsyncHandler>()
            .AddBehavior<VCmd, int, Beh1>();
        var services = new ServiceCollection()
            .AddScoped<VCmdAsyncHandler>()
            .AddScoped<Beh1>();
        var mediator = BuildMediator(cfg, services);

        Assert.Equal(21, await mediator.Send((ICommand<int>)new VCmd(20)));
    }

    [Fact]
    public async Task Command_untyped_invoke_any_paths()
    {
        var cfg = new MedianaConfiguration().UseSingletonHandlers()
            .AddCommandHandler<VCmd, int, VCmdHandler>()
            .AddCommandHandler<RCmd, RResp, RCmdHandler>();
        var services = new ServiceCollection()
            .AddSingleton<VCmdHandler>()
            .AddSingleton<RCmdHandler>();
        var sp = services.BuildServiceProvider();
        var registry = cfg.Freeze();

        var vEntry = registry.TryGet(typeof(VCmd))!;
        var vAny = (IUntypedCallSite)vEntry.CommandCallSite!;
        var rEntry = registry.TryGet(typeof(RCmd))!;
        var rAny = (IUntypedCallSite)rEntry.CommandCallSite!;

        // value-ответ: InvokeAny уходит в generic slow-путь с боксингом
        var boxed = await vAny.InvokeAny(new VCmd(5), sp, default);
        Assert.Equal(6, boxed);

        // ref-ответ: до компоновки корня — slow; после — мост
        var first = await rAny.InvokeAny(new RCmd(5), sp, default);
        Assert.Equal(6, ((RResp)first!).V);
        var second = await rAny.InvokeAny(new RCmd(6), sp, default);
        Assert.Equal(7, ((RResp)second!).V);
    }

    // ── Query: та же матрица ────────────────────────────────────────────────

    [Fact]
    public async Task Query_all_paths()
    {
        var cfg = new MedianaConfiguration().UseSingletonHandlers()
            .AddQueryHandler<VQuery, int, VQueryHandler>()
            .AddQueryHandler<RQuery, RResp, RQueryHandler>();
        var services = new ServiceCollection()
            .AddSingleton<VQueryHandler>()
            .AddSingleton<RQueryHandler>();
        var mediator = BuildMediator(cfg, services);

        Assert.Equal(2, await mediator.Send((IQuery<int>)new VQuery(1)));
        Assert.Equal(3, await mediator.Send((IQuery<int>)new VQuery(2)));
        Assert.Equal(3, (await mediator.Send((IQuery<RResp>)new RQuery(2))).V);
        Assert.Equal(4, await mediator.SendExact<VQuery, int>(new VQuery(3)));
    }

    [Fact]
    public async Task Query_scoped_and_async()
    {
        var cfgSync = new MedianaConfiguration()
            .AddQueryHandler<VQuery, int, VQueryHandler>()
            .AddQueryHandler<RQuery, RResp, RQueryHandler>();
        var services = new ServiceCollection()
            .AddScoped<VQueryHandler>()
            .AddScoped<RQueryHandler>();
        var mediator = BuildMediator(cfgSync, services);

        Assert.Equal(2, await mediator.Send((IQuery<int>)new VQuery(1)));
        Assert.Equal(3, (await mediator.Send((IQuery<RResp>)new RQuery(2))).V);

        var cfgAsync = new MedianaConfiguration().UseSingletonHandlers()
            .AddQueryHandler<VQuery, int, VQueryAsyncHandler>()
            .AddQueryHandler<RQuery, RResp, RQueryAsyncHandler>();
        var asyncServices = new ServiceCollection()
            .AddSingleton<VQueryAsyncHandler>()
            .AddSingleton<RQueryAsyncHandler>();
        var asyncMediator = BuildMediator(cfgAsync, asyncServices);

        Assert.Equal(4, await asyncMediator.Send((IQuery<int>)new VQuery(3)));
        Assert.Equal(5, (await asyncMediator.Send((IQuery<RResp>)new RQuery(4))).V);
    }

    [Fact]
    public async Task Query_untyped_invoke_any()
    {
        var cfg = new MedianaConfiguration().UseSingletonHandlers()
            .AddQueryHandler<VQuery, int, VQueryHandler>()
            .AddQueryHandler<RQuery, RResp, RQueryHandler>();
        var services = new ServiceCollection()
            .AddSingleton<VQueryHandler>()
            .AddSingleton<RQueryHandler>();
        var sp = services.BuildServiceProvider();
        var registry = cfg.Freeze();

        var vAny = (IUntypedCallSite)registry.TryGet(typeof(VQuery))!.QueryCallSite!;
        var rAny = (IUntypedCallSite)registry.TryGet(typeof(RQuery))!.QueryCallSite!;

        Assert.Equal(6, await vAny.InvokeAny(new VQuery(5), sp, default));
        var slow = await rAny.InvokeAny(new RQuery(5), sp, default);
        Assert.Equal(6, ((RResp)slow!).V);
        var fast = await rAny.InvokeAny(new RQuery(6), sp, default);
        Assert.Equal(7, ((RResp)fast!).V);
    }

    // ── Композитор: 0-behavior fast-path ────────────────────────────────────

    [Fact]
    public async Task Compositor_zero_behaviors_returns_terminal()
    {
        var cfg = new MedianaConfiguration().UseSingletonHandlers()
            .AddCommandHandler<VCmd, int, VCmdHandler>(); // behaviors не зарегистрированы
        var services = new ServiceCollection().AddSingleton<VCmdHandler>();
        var mediator = BuildMediator(cfg, services);

        Assert.Equal(2, await mediator.Send((ICommand<int>)new VCmd(1)));
    }

    // ── EventCallSite: обе ветки behaviors ──────────────────────────────────

    private sealed record Evt : IEvent;

    private sealed class EH1 : Handlers.IEventHandler<Evt>
    {
        public ValueTask Handle(Evt e, CancellationToken ct) => default;
    }

    private sealed class EB : IEventPipelineBehavior<Evt>
    {
        public ValueTask Handle(Evt e, EventHandlerDelegate<Evt> next, CancellationToken ct) => next(e, ct);
    }

    [Fact]
    public async Task Event_singleton_without_behaviors()
    {
        var cfg = new MedianaConfiguration().UseSingletonHandlers()
            .AddEventHandler<Evt, EH1>();
        var services = new ServiceCollection().AddSingleton<EH1>();
        var mediator = BuildMediator(cfg, services);

        await mediator.Publish(new Evt());
        await mediator.Publish(new Evt());
    }

    [Fact]
    public async Task Event_scoped_with_behavior_and_singleton_with_behavior()
    {
        var scopedCfg = new MedianaConfiguration()
            .AddEventHandler<Evt, EH1>()
            .AddEventBehavior<Evt, EB>();
        var scopedServices = new ServiceCollection()
            .AddScoped<EH1>()
            .AddScoped<EB>();
        var scopedMediator = BuildMediator(scopedCfg, scopedServices);
        await scopedMediator.Publish(new Evt());

        var singletonCfg = new MedianaConfiguration().UseSingletonHandlers()
            .AddEventHandler<Evt, EH1>()
            .AddEventBehavior<Evt, EB>();
        var singletonServices = new ServiceCollection()
            .AddSingleton<EH1>()
            .AddSingleton<EB>();
        var singletonMediator = BuildMediator(singletonCfg, singletonServices);
        await singletonMediator.Publish(new Evt());
        await singletonMediator.Publish(new Evt());
    }
}
