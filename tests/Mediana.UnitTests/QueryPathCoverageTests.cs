using Mediana.Dispatch;
using Mediana.UnitTests.TestMessages;
using Mediana.Messaging;
using Mediana.Pipeline;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Mediana.UnitTests;

/// <summary>Добор: query-пути всех режимов, parallel-async ошибки, async-cast, scoped query.</summary>
public class QueryPathCoverageTests
{
    internal sealed record Q1(int V) : IQuery<int>;
    internal sealed record QRef(int V) : IQuery<QRef>;

    internal sealed class Q1Handler : Handlers.IQueryHandler<Q1, int>
    {
        public ValueTask<int> Handle(Q1 q, CancellationToken ct) => new(q.V + 1);
    }

    internal sealed class QRefHandler : Handlers.IQueryHandler<QRef, QRef>
    {
        public ValueTask<QRef> Handle(QRef q, CancellationToken ct) => new(q);
    }

    internal sealed class QAsyncHandler : Handlers.IQueryHandler<Q1, int>
    {
        public async ValueTask<int> Handle(Q1 q, CancellationToken ct)
        {
            await Task.Yield();
            return q.V + 1;
        }
    }

    internal sealed class QThrowAfterAwaitHandler : Handlers.IQueryHandler<Q1, int>
    {
        public async ValueTask<int> Handle(Q1 q, CancellationToken ct)
        {
            await Task.Yield();
            throw new InvalidOperationException("after-await");
        }
    }

    [Fact]
    public async Task Query_scoped_mode_sync()
    {
        var cfg = new MedianaConfiguration().AddQueryHandler<Q1, int, Q1Handler>();
        var sc = new ServiceCollection().AddScoped<Q1Handler>();
        sc.AddMediana(c => { });
        var mediator = new Mediator(cfg.Freeze(), sc.BuildServiceProvider());

        var result = await mediator.Send((IQuery<int>)new Q1(1));
        Assert.Equal(2, result);
    }

    [Fact]
    public async Task Query_singleton_mode_typed_and_object()
    {
        var cfg = new MedianaConfiguration().UseSingletonHandlers().AddQueryHandler<Q1, int, Q1Handler>();
        var sc = new ServiceCollection().AddSingleton<Q1Handler>();
        var sp = sc.BuildServiceProvider();
        var mediator = new Mediator(cfg.Freeze(), sp);

        Assert.Equal(5, await mediator.Send((IQuery<int>)new Q1(4)));
        Assert.Equal(6, await mediator.SendExact<Q1, int>(new Q1(5)));
    }

    [Fact]
    public async Task Query_ref_response_object_path()
    {
        var cfg = new MedianaConfiguration().UseSingletonHandlers().AddQueryHandler<QRef, QRef, QRefHandler>();
        var sc = new ServiceCollection().AddSingleton<QRefHandler>();
        var mediator = new Mediator(cfg.Freeze(), sc.BuildServiceProvider());

        var result = await mediator.Send((IQuery<QRef>)new QRef(7));
        Assert.Equal(7, result.V);
    }

    [Fact]
    public async Task Query_async_handler_scoped()
    {
        var cfg = new MedianaConfiguration().AddQueryHandler<Q1, int, QAsyncHandler>();
        var sc = new ServiceCollection().AddScoped<QAsyncHandler>();
        var mediator = new Mediator(cfg.Freeze(), sc.BuildServiceProvider());

        Assert.Equal(11, await mediator.Send((IQuery<int>)new Q1(10)));
    }

    [Fact]
    public async Task Query_untyped_mismatch_throws()
    {
        var cfg = new MedianaConfiguration().UseSingletonHandlers().AddQueryHandler<QRef, QRef, QRefHandler>();
        var sc = new ServiceCollection().AddSingleton<QRefHandler>();
        var mediator = new Mediator(cfg.Freeze(), sc.BuildServiceProvider());

        var q = System.Runtime.CompilerServices.Unsafe.As<IQuery<int>>(new QRef(1));
        await Assert.ThrowsAsync<MediatorConfigurationException>(
            () => mediator.Send<int>(q).AsTask());
    }

    [Fact]
    public async Task Publish_parallel_async_error_aggregated()
    {
        var sc = new ServiceCollection()
            .AddSingleton<ParallelThrowingHandler>()
            .AddSingleton<CountingHandler1>()
            .AddMediana(c => c
                .AddEventHandler<CountedEvent, ParallelThrowingHandler>()
                .AddEventHandler<CountedEvent, CountingHandler1>()
                .SetEventPolicy<CountedEvent>(EventDispatchPolicy.Parallel));
        var sp = sc.BuildServiceProvider();
        var mediator = sp.GetRequiredService<IMediator>();

        var ex = await Assert.ThrowsAsync<AggregateException>(
            () => mediator.Publish(new CountedEvent()).AsTask());
        Assert.Equal("parallel-after-await", ex.InnerExceptions[0].Message);
    }

    internal sealed class CountedEventBehavior : Pipeline.IEventMiddleware<CountedEvent>
    {
        public ValueTask Handle(CountedEvent @event, Pipeline.EventHandlerDelegate<CountedEvent> next, CancellationToken ct)
            => next(@event, ct);
    }

    internal sealed class SyncRowsFilter : Pipeline.IStreamMiddleware<SyncRows, int>
    {
        public IAsyncEnumerable<int> Handle(SyncRows query, Pipeline.StreamHandlerDelegate<SyncRows, int> next, CancellationToken ct)
            => next(query, ct);
    }

    internal sealed class ParallelThrowingHandler : Handlers.IEventHandler<CountedEvent>
    {
        public async ValueTask Handle(CountedEvent @event, CancellationToken ct)
        {
            await Task.Yield();
            throw new InvalidOperationException("parallel-after-await");
        }
    }

    [Fact]
    public async Task Publish_event_middlewares_applied_twice()
    {
        var sc = new ServiceCollection()
            .AddSingleton<CountingHandler1>()
            .AddSingleton<CountedEventBehavior>()
            .AddMediana(c => c
                .AddEventHandler<CountedEvent, CountingHandler1>()
                .AddEventMiddleware<CountedEvent, CountedEventBehavior>()
                .AddEventMiddleware<CountedEvent, CountedEventBehavior>());
        var sp = sc.BuildServiceProvider();
        var mediator = sp.GetRequiredService<IMediator>();

        await mediator.Publish(new CountedEvent());
        Assert.Equal(1, sp.GetRequiredService<CountingHandler1>().Count);
    }

    [Fact]
    public async Task Command_singleton_first_call_composes_and_dispatches()
    {
        // первый вызов через Slow: ленивая компоновка корня + моста (ref-ответ)
        var cfg = new MedianaConfiguration().UseSingletonHandlers()
            .AddCommandHandler<AllocCommand, int, AllocCommandHandler>();
        var sc = new ServiceCollection().AddSingleton<AllocCommandHandler>();
        var mediator = new Mediator(cfg.Freeze(), sc.BuildServiceProvider());

        Assert.Equal(8, await mediator.Send((ICommand<int>)new AllocCommand(7)));
        Assert.Equal(9, await mediator.Send((ICommand<int>)new AllocCommand(8)));
    }

    [Fact]
    public async Task Command_ref_response_async_handler_through_bridge()
    {
        var sc = new ServiceCollection()
            .AddSingleton<RefAsyncHandler>()
            .AddMediana(c => c.UseSingletonHandlers().AddCommandHandler<LocalRefCmd, LocalRefResp, RefAsyncHandler>());
        var sp = sc.BuildServiceProvider();
        var mediator = sp.GetRequiredService<IMediator>();

        var result = await mediator.Send((ICommand<LocalRefResp>)new LocalRefCmd());
        Assert.NotNull(result);
    }

    internal sealed record LocalRefCmd() : ICommand<LocalRefResp>;
    internal sealed record LocalRefResp();

    internal sealed class RefAsyncHandler : Handlers.ICommandHandler<LocalRefCmd, LocalRefResp>
    {
        public async ValueTask<LocalRefResp> Handle(LocalRefCmd command, CancellationToken ct)
        {
            await Task.Yield();
            return new LocalRefResp();
        }
    }

    [Fact]
    public async Task Stream_with_behavior_wraps_enumerable()
    {
        var sc = new ServiceCollection()
            .AddSingleton<SyncRowsStreamHandler>()
            .AddSingleton<SyncRowsFilter>()
            .AddMediana(c => c
                .UseSingletonHandlers()
                .AddStreamHandler<SyncRows, int, SyncRowsStreamHandler>()
                .AddStreamMiddleware<SyncRows, int, SyncRowsFilter>());
        var sp = sc.BuildServiceProvider();
        var mediator = sp.GetRequiredService<IMediator>();

        var sum = 0;
        await foreach (var r in mediator.Stream((IStreamQuery<int>)new SyncRows()))
        {
            sum += r;
        }

        Assert.Equal(6, sum);
    }

    [Fact]
    public void Registry_build_empty_and_single_bucket_paths()
    {
        var registry = Mediana.Dispatch.MessageRegistry.Empty;
        Assert.Null(registry.TryGet(typeof(object)));

        // множество добавлений — рост корзин (ns2.1) / rebuild (net10)
        // 40 РАЗЛИЧНЫХ типов — рост корзин (ns2.1) / rebuild (net10)
        var types = typeof(string).Assembly.GetTypes().Where(t => t.IsVisible).Take(40).ToArray();
        var r = registry;
        foreach (var t in types)
        {
            r = r.Add(t, new Mediana.Dispatch.MessageEntry(Mediana.Dispatch.HandlerKind.Event, t, null));
        }

        Assert.NotNull(r.TryGet(types[0]));
        // дубликат
        Assert.Throws<MediatorConfigurationException>(
            () => r.Add(types[0], new Mediana.Dispatch.MessageEntry(Mediana.Dispatch.HandlerKind.Event, types[0], null)));
    }
}
