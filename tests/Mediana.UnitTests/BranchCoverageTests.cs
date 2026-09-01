using Mediana.UnitTests.TestMessages;
using System.Diagnostics;
using Mediana.Dispatch;
using Mediana.Messaging;
using Mediana.Pipeline;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Mediana.UnitTests;

/// <summary>Добор веточного покрытия: ошибки конфигурации, scoped-пути, scan, диагностика, исключения.</summary>
public class BranchCoverageTests
{
    // ── Исключения ─────────────────────────────────────────────────────────────

    [Fact]
    public void RemoteExecutionException_carries_details()
    {
        var details = new Dictionary<string, string?> { ["code"] = "500" };
        var ex = new RemoteExecutionException("boom", "System.InvalidOperationException", details);

        Assert.Equal("boom", ex.Message);
        Assert.Equal("System.InvalidOperationException", ex.RemoteErrorType);
        Assert.Same(details, ex.Details);

        var empty = new RemoteExecutionException("x", null, null);
        Assert.NotNull(empty.Details);
        Assert.Empty(empty.Details);
        Assert.Null(empty.RemoteErrorType);

        var inner = new InvalidOperationException();
        var withInner = new MediatorConfigurationException("cfg", inner);
        Assert.Same(inner, withInner.InnerException);
        Assert.Equal("cfg", withInner.Message);

        var timeout = new RemoteTimeoutException("t");
        Assert.Equal("t", timeout.Message);
    }

    // ── Диагностика: no-op без слушателей ──────────────────────────────────────

    [Fact]
    public void Diagnostics_noop_without_listeners()
    {
        Assert.Null(MedianaDiagnostics.StartDispatch("X"));
        Assert.Null(MedianaDiagnostics.StartPublish("X"));
        Assert.Null(MedianaDiagnostics.StartConsume("X"));
        MedianaDiagnostics.Enrich(null, "k", "v"); // null-tolerant

        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "Mediana",
            SampleUsingParentId = (ref ActivityCreationOptions<string> o) => ActivitySamplingResult.AllData,
            Sample = (ref ActivityCreationOptions<ActivityContext> o) => ActivitySamplingResult.AllData,
        };
        ActivitySource.AddActivityListener(listener);

        using var activity = MedianaDiagnostics.StartDispatch("Y");
        Assert.NotNull(activity);
        MedianaDiagnostics.Enrich(activity, "messaging.message.id", "42");
        Assert.Equal("42", activity.GetTagItem("messaging.message.id"));
    }

    // ── Конфигурация: ошибки и scan ────────────────────────────────────────────

    [Fact]
    public void Duplicate_query_handler_rejected()
    {
        var sc = new ServiceCollection();
        Assert.Throws<MediatorConfigurationException>(() => sc.AddMediana(c => c
            .AddQueryHandler<GetOrder, OrderDto, GetOrderHandler>()
            .AddQueryHandler<GetOrder, OrderDto, ThrowingQueryHandler>()));
    }

    [Fact]
    public void Duplicate_stream_handler_rejected()
    {
        var sc = new ServiceCollection();
        Assert.Throws<MediatorConfigurationException>(() => sc.AddMediana(c => c
            .AddStreamHandler<SearchOrders, OrderDto, SearchOrdersHandler>()
            .AddStreamHandler<SearchOrders, OrderDto, SearchOrdersHandler>()));
    }

    [Fact]
    public void AddHandlersFromAssembly_rejects_duplicate_command_handlers()
    {
        // тестовая сборка содержит CreateOrderHandler и AsyncCreateOrderHandler для CreateOrder —
        // scan обязан поймать дубликат (в реальных проектах дубликат ловит генератор через MED001)
        var sc = new ServiceCollection();
        Assert.Throws<MediatorConfigurationException>(() =>
            sc.AddMediana(c => c.AddHandlersFromAssembly(typeof(CreateOrderHandler).Assembly)));
    }

    [Fact]
    public void AddHandlersFromAssembly_ignores_generic_definitions()
    {
        var sc = new ServiceCollection();
        sc.AddMediana(c => c.AddHandlersFromAssembly(typeof(List<>).Assembly));
        // BCL не содержит хендлеров — freeze проходит без регистраций
        var sp = sc.BuildServiceProvider();
        Assert.Throws<MediatorConfigurationException>(
            () => sp.GetRequiredService<IMediator>().Send((IQuery<OrderDto>)new GetOrder(1)).AsTask().Result);
    }

    [Fact]
    public async Task Stream_behavior_applies_when_matching()
    {
        var sc = new ServiceCollection()
            .AddSingleton<SearchOrdersHandler>()
            .AddSingleton<StreamFilterBehavior>()
            .AddMediana(c => c
                .AddStreamHandler<SearchOrders, OrderDto, SearchOrdersHandler>()
                .AddStreamBehavior<SearchOrders, OrderDto, StreamFilterBehavior>()
                .AddBehavior<CreateOrder, OrderCreated, OrderingBehavior>()); // не применим к стриму
        var sp = sc.BuildServiceProvider();
        var mediator = sp.GetRequiredService<IMediator>();

        var rows = new List<OrderDto>();
        var handler = sp.GetRequiredService<SearchOrdersHandler>();
        handler.Rows = 2;
        await foreach (var r in mediator.Stream((IStreamQuery<OrderDto>)new SearchOrders("q")))
        {
            rows.Add(r);
        }

        Assert.Equal(2, rows.Count);
    }

    // ── Mediator: несоответствия и параллельный успех ──────────────────────────

    [Fact]
    public async Task Send_wrong_response_type_throws()
    {
        var sc = new ServiceCollection()
            .AddSingleton<GetOrderHandler>()
            .AddSingleton<CreateOrderHandler>()
            .AddMediana(c => c.AddQueryHandler<GetOrder, OrderDto, GetOrderHandler>());
        var sp = sc.BuildServiceProvider();
        var mediator = sp.GetRequiredService<IMediator>();

        // GetOrder зарегистрирован как IQuery<OrderDto>, но шлём как IQuery<string> через Unsafe.As
        var query = System.Runtime.CompilerServices.Unsafe.As<IQuery<string>>(new GetOrder(1));
        await Assert.ThrowsAsync<MediatorConfigurationException>(
            () => mediator.Send<string>(query).AsTask());
    }

    [Fact]
    public async Task Send_unregistered_query_via_typed_path_throws()
    {
        var sc = new ServiceCollection().AddMediana(c => { });
        var sp = sc.BuildServiceProvider();
        var mediator = (Mediator)sp.GetRequiredService<IMediator>();

        await Assert.ThrowsAsync<MediatorConfigurationException>(
            () => mediator.Send((IQuery<OrderDto>)new GetOrder(1)).AsTask());
        await Assert.ThrowsAsync<MediatorConfigurationException>(
            () => mediator.Send((ICommand<OrderCreated>)new CreateOrder(1)).AsTask());
        await Assert.ThrowsAsync<MediatorConfigurationException>(
            () => mediator.SendExact<GetOrder, OrderDto>(new GetOrder(1)).AsTask());
        await Assert.ThrowsAsync<ArgumentNullException>(
            () => mediator.Send((IQuery<OrderDto>)null!).AsTask());
        await Assert.ThrowsAsync<ArgumentNullException>(
            () => mediator.Publish<IEvent>(null!).AsTask());
        Assert.Throws<ArgumentNullException>(
            () => mediator.Stream((IStreamQuery<OrderDto>)null!));
        await Assert.ThrowsAsync<ArgumentNullException>(
            () => mediator.SendExact<GetOrder, OrderDto>(null!).AsTask());
    }

    internal sealed record IntStreamQuery(int N) : IStreamQuery<int>;

    internal sealed class IntStreamHandler : Handlers.IStreamHandler<IntStreamQuery, int>
    {
        public IAsyncEnumerable<int> Handle(IntStreamQuery q, CancellationToken ct) => new Ints(q.N);
    }

    private sealed class Ints(int n) : IAsyncEnumerable<int>
    {
        public IAsyncEnumerator<int> GetAsyncEnumerator(CancellationToken ct) => new Enum(n);

        private sealed class Enum(int n) : IAsyncEnumerator<int>
        {
            private int _current;

            public int Current => _current;

            public ValueTask<bool> MoveNextAsync()
            {
                if (_current < n)
                {
                    _current++;
                    return new ValueTask<bool>(true);
                }

                return new ValueTask<bool>(false);
            }

            public ValueTask DisposeAsync() => default;
        }
    }

    [Fact]
    public async Task Stream_row_type_mismatch_throws()
    {
        var sc = new ServiceCollection()
            .AddSingleton<IntStreamHandler>()
            .AddMediana(c => c.AddStreamHandler<IntStreamQuery, int, IntStreamHandler>());
        var sp = sc.BuildServiceProvider();
        var mediator = (Mediator)sp.GetRequiredService<IMediator>();

        // зарегистрирован row=int; запрашиваем как Stream<OrderDto> через Unsafe.As
        var query = System.Runtime.CompilerServices.Unsafe.As<IStreamQuery<OrderDto>>(new IntStreamQuery(3));
        await Assert.ThrowsAsync<MediatorConfigurationException>(async () =>
        {
            await foreach (var r in mediator.Stream(query))
            {
            }
        });
    }

    [Fact]
    public async Task Publish_parallel_success_path()
    {
        var sc = new ServiceCollection()
            .AddSingleton<CountingHandler1>()
            .AddSingleton<CountingHandler2>()
            .AddMediana(c => c
                .AddEventHandler<CountedEvent, CountingHandler1>()
                .AddEventHandler<CountedEvent, CountingHandler2>()
                .SetEventPolicy<CountedEvent>(EventDispatchPolicy.Parallel));
        var sp = sc.BuildServiceProvider();
        var mediator = sp.GetRequiredService<IMediator>();

        await mediator.Publish(new CountedEvent());
        Assert.Equal(1, sp.GetRequiredService<CountingHandler1>().Count);
        Assert.Equal(1, sp.GetRequiredService<CountingHandler2>().Count);
    }

    // ── Scoped-пути и ChainState ───────────────────────────────────────────────

    private static IServiceProvider BuildScoped(
        Action<MedianaConfiguration> cfg, Action<ServiceCollection> services)
    {
        var sc = new ServiceCollection();
        services(sc);
        sc.AddMediana(cfg);
        return sc.BuildServiceProvider();
    }

    [Fact]
    public async Task Scoped_command_with_behaviors_uses_chain_state()
    {
        var sc = new ServiceCollection()
            .AddScoped<ScopedCounterHandler>()
            .AddScoped<AllocBehavior1>()
            .AddMediana(c => c
                .AddCommandHandler<AllocCommand, int, AllocCommandHandler>()
                .AddBehavior<AllocCommand, int, AllocBehavior1>());
        sc.AddScoped<AllocCommandHandler>();
        var sp = sc.BuildServiceProvider();
        using var scope = sp.CreateScope();
        var mediator = scope.ServiceProvider.GetRequiredService<IMediator>();

        var result = await mediator.Send((ICommand<int>)new AllocCommand(5));
        Assert.Equal(6, result);
    }

    [Fact]
    public async Task Scoped_async_command_returns_via_pooled_state()
    {
        var sc = new ServiceCollection()
            .AddScoped<AsyncCommandHandler>()
            .AddMediana(c => c.AddCommandHandler<AsyncCommand, int, AsyncCommandHandler>());
        var sp = sc.BuildServiceProvider();
        using var scope = sp.CreateScope();
        var mediator = scope.ServiceProvider.GetRequiredService<IMediator>();

        var result = await mediator.Send((ICommand<int>)new AsyncCommand(5));
        Assert.Equal(6, result);
    }

    internal sealed record AsyncCommand(int V) : ICommand<int>;

    internal sealed class AsyncCommandHandler : Handlers.ICommandHandler<AsyncCommand, int>
    {
        public async ValueTask<int> Handle(AsyncCommand c, CancellationToken ct)
        {
            await Task.Yield();
            return c.V + 1;
        }
    }

    [Fact]
    public async Task Missing_handler_registration_throws_at_invoke()
    {
        // AddMediana регистрирует хендлеры сам; для проверки пропуска строим медиатор вручную
        var cfg = new MedianaConfiguration().AddCommandHandler<AllocCommand, int, AllocCommandHandler>();
        var mediator = new Mediator(cfg.Freeze(), new ServiceCollection().BuildServiceProvider());

        await Assert.ThrowsAsync<MediatorConfigurationException>(
            () => mediator.Send((ICommand<int>)new AllocCommand(1)).AsTask());
    }

    [Fact]
    public async Task Missing_behavior_registration_throws_at_invoke()
    {
        var cfg = new MedianaConfiguration()
            .AddCommandHandler<AllocCommand, int, AllocCommandHandler>()
            .AddBehavior<AllocCommand, int, AllocBehavior1>(); // behavior не в DI
        var mediator = new Mediator(cfg.Freeze(), new ServiceCollection().BuildServiceProvider());

        await Assert.ThrowsAsync<MediatorConfigurationException>(
            () => mediator.Send((ICommand<int>)new AllocCommand(1)).AsTask());
    }

    [Fact]
    public async Task Missing_event_handler_registration_throws()
    {
        var cfg = new MedianaConfiguration().AddEventHandler<CountedEvent, CountingHandler1>();
        var mediator = new Mediator(cfg.Freeze(), new ServiceCollection().BuildServiceProvider());

        await Assert.ThrowsAsync<MediatorConfigurationException>(
            () => mediator.Publish(new CountedEvent()).AsTask());
    }

    [Fact]
    public async Task Missing_stream_handler_registration_throws()
    {
        var cfg = new MedianaConfiguration().AddStreamHandler<SyncRows, int, SyncRowsStreamHandler>();
        var mediator = new Mediator(cfg.Freeze(), new ServiceCollection().BuildServiceProvider());

        await Assert.ThrowsAsync<MediatorConfigurationException>(async () =>
        {
            await foreach (var r in mediator.Stream((IStreamQuery<int>)new SyncRows()))
            {
            }
        });
    }

    // ── ChainState защита ──────────────────────────────────────────────────────

    [Fact]
    public async Task ChainState_double_next_after_terminal_throws()
    {
        var terminalCalls = 0;
        RequestHandlerDelegate<AllocCommand, int> terminal = (_, _) =>
        {
            terminalCalls++;
            return new ValueTask<int>(1);
        };
        var state = ChainState<AllocCommand, int>.Take(new EmptyServiceProvider(), [], terminal);

        var first = state.Next(new AllocCommand(1), default);
        Assert.True(first.IsCompletedSuccessfully);
        Assert.Throws<InvalidOperationException>(() => state.Next(new AllocCommand(1), default));

        state.Return();
        Assert.Equal(1, terminalCalls);
        await Task.CompletedTask;
    }

    private sealed class EmptyServiceProvider : IServiceProvider
    {
        public object? GetService(Type serviceType) => null;
    }

    // ── Событийные behaviors ───────────────────────────────────────────────────

    [Fact]
    public async Task Event_behaviors_scoped_mode_applied_in_order()
    {
        EventOrderingBehavior.Trace = [];
        var sc = new ServiceCollection()
            .AddScoped<OrderCreatedAuditHandler>()
            .AddScoped<EventOrderingBehavior>()
            .AddMediana(c => c
                .AddEventHandler<OrderCreated, OrderCreatedAuditHandler>()
                .AddEventBehavior<OrderCreated, EventOrderingBehavior>());
        var sp = sc.BuildServiceProvider();
        var mediator = sp.GetRequiredService<IMediator>();

        await mediator.Publish(new OrderCreated(1, "x"));
        Assert.Equal(["event-behavior:before", "event-behavior:after"], EventOrderingBehavior.Trace);
    }

    [Fact]
    public async Task Event_behavior_missing_registration_throws()
    {
        var sc = new ServiceCollection()
            .AddScoped<OrderCreatedAuditHandler>()
            .AddMediana(c => c
                .AddEventHandler<OrderCreated, OrderCreatedAuditHandler>()
                .AddEventBehavior<OrderCreated, EventOrderingBehavior>()); // не в DI
        var sp = sc.BuildServiceProvider();
        var mediator = sp.GetRequiredService<IMediator>();

        await Assert.ThrowsAsync<MediatorConfigurationException>(
            () => mediator.Publish(new OrderCreated(1, "x")).AsTask());
    }

    [Fact]
    public async Task Event_handler_exception_through_behavior_propagates()
    {
        var sc = new ServiceCollection()
            .AddScoped<ThrowingEventHandler>()
            .AddScoped<EventOrderingBehavior>()
            .AddMediana(c => c
                .AddEventHandler<OrderCreated, ThrowingEventHandler>()
                .AddEventBehavior<OrderCreated, EventOrderingBehavior>());
        var sp = sc.BuildServiceProvider();
        var mediator = sp.GetRequiredService<IMediator>();

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => mediator.Publish(new OrderCreated(1, "x")).AsTask());
    }

    // ── Singleton event с behaviors ────────────────────────────────────────────

    [Fact]
    public async Task Singleton_event_with_behaviors_zero_di()
    {
        EventOrderingBehavior.Trace = [];
        var sc = new ServiceCollection()
            .AddSingleton<OrderCreatedAuditHandler>()
            .AddSingleton<EventOrderingBehavior>()
            .AddMediana(c => c
                .UseSingletonHandlers()
                .AddEventHandler<OrderCreated, OrderCreatedAuditHandler>()
                .AddEventBehavior<OrderCreated, EventOrderingBehavior>());
        var sp = sc.BuildServiceProvider();
        var mediator = sp.GetRequiredService<IMediator>();

        await mediator.Publish(new OrderCreated(9, "x"));
        await mediator.Publish(new OrderCreated(9, "x"));
        Assert.Equal(4, EventOrderingBehavior.Trace.Count);
    }

    // ── WithRegistry (runtime-расширение) ──────────────────────────────────────

    [Fact]
    public async Task WithRegistry_extends_dispatch()
    {
        var sc = new ServiceCollection()
            .AddSingleton<GetOrderHandler>()
            .AddSingleton<CreateOrderHandler>()
            .AddMediana(c => c.AddQueryHandler<GetOrder, OrderDto, GetOrderHandler>());
        var sp = sc.BuildServiceProvider();
        var mediator = (Mediator)sp.GetRequiredService<IMediator>();

        await Assert.ThrowsAsync<MediatorConfigurationException>(
            () => mediator.Send((ICommand<OrderCreated>)new CreateOrder(1)).AsTask());

        // runtime-добавление команды в реестр через copy-on-write
        var extended = mediator.Registry.Add(
            typeof(CreateOrder),
            BuildCommandEntry(sp));
        var extendedMediator = mediator.WithRegistry(extended);

        var result = await extendedMediator.Send((ICommand<OrderCreated>)new CreateOrder(42));
        Assert.Equal(42, result.OrderId);
        // исходный медиатор не изменился
        await Assert.ThrowsAsync<MediatorConfigurationException>(
            () => mediator.Send((ICommand<OrderCreated>)new CreateOrder(1)).AsTask());
    }

    private static Mediana.Dispatch.MessageEntry BuildCommandEntry(IServiceProvider sp)
    {
        var callSite = new CommandCallSite<CreateOrder, OrderCreated, CreateOrderHandler>([], singleton: true);
        return new Mediana.Dispatch.MessageEntry(HandlerKind.Command, typeof(CreateOrder), typeof(OrderCreated))
        {
            CommandCallSite = callSite,
        };
    }
}
