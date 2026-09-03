using Mediana.Dispatch;
using Mediana.Messaging;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Mediana.UnitTests;

/// <summary>QueryCallSite: ref-, typed-ref, async-cast, InvokeAny /.</summary>
public class QueryFinalBranchTests
{
    private sealed record RQ(int V) : IQuery<RR>;
    private sealed record RR(int V);

    private sealed class RQHandler : Handlers.IQueryHandler<RQ, RR>
    {
        public ValueTask<RR> Handle(RQ q, CancellationToken ct) => new(new RR(q.V + 1));
    }

    private sealed class RQAsyncHandler : Handlers.IQueryHandler<RQ, RR>
    {
        public async ValueTask<RR> Handle(RQ q, CancellationToken ct)
        {
            await Task.Yield();
            return new RR(q.V + 1);
        }
    }

    private sealed record VQ(int V) : IQuery<int>;

    private sealed class VQHandler : Handlers.IQueryHandler<VQ, int>
    {
        public ValueTask<int> Handle(VQ q, CancellationToken ct) => new(q.V + 1);
    }

    private sealed class VQAsyncHandler : Handlers.IQueryHandler<VQ, int>
    {
        public async ValueTask<int> Handle(VQ q, CancellationToken ct)
        {
            await Task.Yield();
            return q.V + 1;
        }
    }

    private static Mediator Build(MedianaConfiguration cfg, IServiceCollection sc)
    {
        sc.AddMediana(_ => { });
        return new Mediator(cfg.Freeze(), sc.BuildServiceProvider());
    }

    [Fact]
    public async Task Ref_query_object_invoke_uses_bridge_after_warmup()
    {
        var cfg = new MedianaConfiguration().UseSingletonHandlers()
            .AddQueryHandler<RQ, RR, RQHandler>();
        var sc = new ServiceCollection().AddSingleton<RQHandler>();
        var mediator = Build(cfg, sc);

        var entry = mediator.Registry.TryGet(typeof(RQ))!;
        var objectCallSite = (IObjectQueryCallSite<RR>)entry.QueryCallSite!;
        var sp = sc.BuildServiceProvider();

        // : → Slow-
        Assert.Equal(2, (await objectCallSite.Invoke(new RQ(1), sp, default)).V);
        // : ref-
        Assert.Equal(3, (await objectCallSite.Invoke(new RQ(2), sp, default)).V);
    }

    private static IServiceProvider BuildProvider(ServiceCollection sc) => sc.BuildServiceProvider();

    [Fact]
    public async Task Ref_query_typed_after_warmup()
    {
        var cfg = new MedianaConfiguration().UseSingletonHandlers()
            .AddQueryHandler<RQ, RR, RQHandler>();
        var sc = new ServiceCollection().AddSingleton<RQHandler>();
        var mediator = Build(cfg, sc);

        // object-
        Assert.Equal(2, (await mediator.Send((IQuery<RR>)new RQ(1))).V);
        // typed fast-path
        Assert.Equal(3, (await mediator.SendExact<RQ, RR>(new RQ(2))).V);
    }

    [Fact]
    public async Task Async_ref_query_covers_async_upcast_and_cast()
    {
        var cfg = new MedianaConfiguration().UseSingletonHandlers()
            .AddQueryHandler<RQ, RR, RQAsyncHandler>()
            .AddQueryHandler<VQ, int, VQAsyncHandler>();
        var sc = new ServiceCollection()
            .AddSingleton<RQAsyncHandler>()
            .AddSingleton<VQAsyncHandler>();
        var mediator = Build(cfg, sc);

        // async ref object-: AwaitCast (CastBoxed async)
        Assert.Equal(2, (await mediator.Send((IQuery<RR>)new RQ(1))).V);
        Assert.Equal(3, (await mediator.Send((IQuery<RR>)new RQ(2))).V);

        // async value typed: AwaitAndReturn
        Assert.Equal(4, await mediator.Send((IQuery<int>)new VQ(3)));

        // async ref InvokeAny: AwaitUpcast
        var entry = mediator.Registry.TryGet(typeof(RQ))!;
        var any = (IUntypedCallSite)entry.QueryCallSite!;
        var sp = sc.BuildServiceProvider();
        var slowAsync = await any.InvokeAny(new RQ(4), sp, default);
        Assert.Equal(5, ((RR)slowAsync!).V);
        var fastAsync = await any.InvokeAny(new RQ(5), sp, default);
        Assert.Equal(6, ((RR)fastAsync!).V);
    }

    [Fact]
    public async Task Value_query_invoke_any_and_async_typed()
    {
        var cfg = new MedianaConfiguration().UseSingletonHandlers()
            .AddQueryHandler<VQ, int, VQAsyncHandler>();
        var sc = new ServiceCollection().AddSingleton<VQAsyncHandler>();
        var mediator = Build(cfg, sc);

        var entry = mediator.Registry.TryGet(typeof(VQ))!;
        var any = (IUntypedCallSite)entry.QueryCallSite!;
        var sp = sc.BuildServiceProvider();

        // value: → generic slow-(async)
        Assert.Equal(6, await any.InvokeAny(new VQ(5), sp, default));

        // typed async
        Assert.Equal(7, await mediator.SendExact<VQ, int>(new VQ(6)));
    }
}
