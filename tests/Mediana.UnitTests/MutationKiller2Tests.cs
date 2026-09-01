using Mediana.Dispatch;
using Mediana.Messaging;
using Mediana.Pipeline;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Mediana.UnitTests;

/// <summary>
/// Мутационные killer-тесты II: агрегация ошибок параллельного publish, event-цепочки
/// с наблюдаемым порядком, DI-порядок регистраций, registry-рост.
/// </summary>
public class MutationKiller2Tests
{
    private sealed record PE : IEvent;

    private sealed class Throwing1 : Handlers.IEventHandler<PE>
    {
        public ValueTask Handle(PE e, CancellationToken ct) => throw new InvalidOperationException("e1");
    }

    private sealed class Throwing2 : Handlers.IEventHandler<PE>
    {
        public async ValueTask Handle(PE e, CancellationToken ct)
        {
            await Task.Yield();
            throw new InvalidOperationException("e2");
    }
    }

    private sealed class Throwing3 : Handlers.IEventHandler<PE>
    {
        public ValueTask Handle(PE e, CancellationToken ct) => throw new InvalidOperationException("e3");
    }

    // ── Mediator.PublishParallel: все ошибки агрегируются (coalesce-мутации) ──

    [Fact]
    public async Task Parallel_publish_aggregates_ALL_errors_sync_and_async()
    {
        var sc = new ServiceCollection()
            .AddSingleton<Throwing1>()
            .AddSingleton<Throwing2>()
            .AddSingleton<Throwing3>()
            .AddMediana(c => c
                .AddEventHandler<PE, Throwing1>()   // sync-бросок (первый контур)
                .AddEventHandler<PE, Throwing2>()   // async-бросок (второй контур)
                .AddEventHandler<PE, Throwing3>()   // sync-бросок
                .SetEventPolicy<PE>(EventDispatchPolicy.Parallel));
        var sp = sc.BuildServiceProvider();
        var mediator = sp.GetRequiredService<IMediator>();

        var ex = await Assert.ThrowsAsync<AggregateException>(() => mediator.Publish(new PE()).AsTask());
        // ВСЕ ТРИ ошибки должны быть в агрегате (coalesce-накопление не теряет)
        Assert.Equal(3, ex.InnerExceptions.Count);
        Assert.Contains(ex.InnerExceptions, x => x.Message == "e1");
        Assert.Contains(ex.InnerExceptions, x => x.Message == "e2");
        Assert.Contains(ex.InnerExceptions, x => x.Message == "e3");
    }

    [Fact]
    public async Task Parallel_publish_two_sync_errors_both_aggregated()
    {
        var sc = new ServiceCollection()
            .AddSingleton<Throwing1>()
            .AddSingleton<Throwing3>()
            .AddMediana(c => c
                .AddEventHandler<PE, Throwing1>()
                .AddEventHandler<PE, Throwing3>()
                .SetEventPolicy<PE>(EventDispatchPolicy.Parallel));
        var sp = sc.BuildServiceProvider();
        var mediator = sp.GetRequiredService<IMediator>();

        var ex = await Assert.ThrowsAsync<AggregateException>(() => mediator.Publish(new PE()).AsTask());
        Assert.Equal(2, ex.InnerExceptions.Count);
    }

    // ── EventCallSite: порядок behaviors наблюдаем (negate/block-мутации) ──

    private sealed record OE : IEvent;
    private sealed class OETrace { public static List<string> Log = []; }

    private sealed class OEBehaviorA : IEventPipelineBehavior<OE>
    {
        public ValueTask Handle(OE e, EventHandlerDelegate<OE> next, CancellationToken ct)
        {
            OETrace.Log.Add("A:before");
            var r = next(e, ct);
            OETrace.Log.Add("A:after");
            return r;
        }
    }

    private sealed class OEBehaviorB : IEventPipelineBehavior<OE>
    {
        public ValueTask Handle(OE e, EventHandlerDelegate<OE> next, CancellationToken ct)
        {
            OETrace.Log.Add("B:before");
            var r = next(e, ct);
            OETrace.Log.Add("B:after");
            return r;
        }
    }

    private sealed class OECounting : Handlers.IEventHandler<OE>
    {
        public static int Calls;

        public ValueTask Handle(OE e, CancellationToken ct)
        {
            Calls++;
            return default;
        }
    }

    [Fact]
    public async Task Event_singleton_behaviors_execute_in_registration_order()
    {
        OETrace.Log = [];
        OECounting.Calls = 0;
        var sc = new ServiceCollection()
            .AddSingleton<OEBehaviorA>()
            .AddSingleton<OEBehaviorB>()
            .AddSingleton<Handlers.IEventHandler<OE>>(new OECounting())
            .AddMediana(c => c
                .UseSingletonHandlers()
                .AddEventHandler<OE, OECounting>()
                .AddEventBehavior<OE, OEBehaviorA>()
                .AddEventBehavior<OE, OEBehaviorB>());
        var sp = sc.BuildServiceProvider();
        var mediator = sp.GetRequiredService<IMediator>();

        await mediator.Publish(new OE());

        // строго вложенный порядок: A → B → handler → B → A
        Assert.Equal(
        [
            "A:before",
            "B:before",
            "B:after",
            "A:after",
        ], OETrace.Log);
        Assert.Equal(1, OECounting.Calls);
    }

    [Fact]
    public async Task Event_scoped_behaviors_execute_in_registration_order()
    {
        OETrace.Log = [];
        OECounting.Calls = 0;
        var sc = new ServiceCollection()
            .AddScoped<OEBehaviorA>()
            .AddScoped<OEBehaviorB>()
            .AddScoped<OECounting>()
            .AddMediana(c => c
                .AddEventHandler<OE, OECounting>()
                .AddEventBehavior<OE, OEBehaviorA>()
                .AddEventBehavior<OE, OEBehaviorB>());
        sc.AddScoped<OECounting>();
        var sp = sc.BuildServiceProvider();
        var mediator = sp.GetRequiredService<IMediator>();

        await mediator.Publish(new OE());
        await mediator.Publish(new OE());

        Assert.Equal(8, OETrace.Log.Count);
        Assert.Equal(2, OECounting.Calls);
        Assert.Equal("A:before", OETrace.Log[0]);
        Assert.Equal("B:before", OETrace.Log[1]);
    }

    // ── DI-порядок: регистрации хендлеров именно через TryAdd + точные lifetime ──

    [Fact]
    public void AddMediana_does_not_duplicate_existing_registrations()
    {
        var sc = new ServiceCollection();
        sc.AddSingleton<MK2H>(); // пред-регистрация юзера
        sc.AddMediana(c => c.AddCommandHandler<MK2, int, MK2H>());

        Assert.Single(sc, d => d.ServiceType == typeof(MK2H));
        Assert.Single(sc, d => d.ServiceType == typeof(IMediator));
    }

    private sealed record MK2(int V) : ICommand<int>;

    private sealed class MK2H : Handlers.ICommandHandler<MK2, int>
    {
        public ValueTask<int> Handle(MK2 c, CancellationToken ct) => new(c.V + 1);
    }

    // ── Registry: цикл копирования не теряет элементы (statement-мутации) ──

    [Fact]
    public void Registry_add_copies_all_previous_items()
    {
        var r = Mediana.Dispatch.MessageRegistry.Empty;
        var types = new[] { typeof(string), typeof(int), typeof(bool) };
        foreach (var t in types)
        {
            r = r.Add(t, new Mediana.Dispatch.MessageEntry(Mediana.Dispatch.HandlerKind.Event, t, null));
        }

        // ВСЕ предыдущие типы присутствуют после каждого добавления
        Assert.NotNull(r.TryGet(typeof(string)));
        Assert.NotNull(r.TryGet(typeof(int)));
        Assert.NotNull(r.TryGet(typeof(bool)));
        Assert.Null(r.TryGet(typeof(object)));
    }
}
