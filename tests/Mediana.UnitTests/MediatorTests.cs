using Mediana.Handlers;
using Mediana.Dispatch;
using Mediana.Messaging;
using Mediana.UnitTests.TestMessages;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Mediana.UnitTests;

public class MediatorTests
{
    private static IServiceProvider Build(Action<MedianaConfiguration> configure, Action<IServiceCollection>? services = null)
    {
        var sc = new ServiceCollection();
        services?.Invoke(sc);
        sc.AddMediana(configure);
        return sc.BuildServiceProvider();
    }

    // ── Send: ───────────────────────────────────────────────

    [Fact]
    public async Task Send_command_dispatches_to_handler()
    {
        var handler = new CreateOrderHandler();
        var sp = Build(c => c.AddCommandHandler<CreateOrder, OrderCreated, CreateOrderHandler>(),
            s => s.AddSingleton(handler));

        var mediator = sp.GetRequiredService<IMediator>();
        var result = await mediator.Send((ICommand<OrderCreated>)new CreateOrder(42));

        Assert.Equal(42, result.OrderId);
        Assert.Equal(1, handler.Calls);
    }

    [Fact]
    public async Task Send_query_dispatches_to_handler()
    {
        var sp = Build(c => c.AddQueryHandler<GetOrder, OrderDto, GetOrderHandler>(),
            s => s.AddSingleton<GetOrderHandler>());

        var mediator = sp.GetRequiredService<IMediator>();
        var result = await mediator.Send((IQuery<OrderDto>)new GetOrder(7));

        Assert.Equal(7, result.OrderId);
    }

    [Fact]
    public async Task Send_async_handler_awaited_correctly()
    {
        var sp = Build(c => c.AddCommandHandler<CreateOrder, OrderCreated, AsyncCreateOrderHandler>(),
            s => s.AddSingleton<AsyncCreateOrderHandler>());

        var mediator = sp.GetRequiredService<IMediator>();
        var result = await mediator.Send((ICommand<OrderCreated>)new CreateOrder(1));

        Assert.Equal("CreatedAsync", result.Status);
    }

    [Fact]
    public async Task Send_handler_exception_propagates_as_is()
    {
        var sp = Build(c => c.AddQueryHandler<GetOrder, OrderDto, ThrowingQueryHandler>(),
            s => s.AddSingleton<ThrowingQueryHandler>());

        var mediator = sp.GetRequiredService<IMediator>();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => mediator.Send((IQuery<OrderDto>)new GetOrder(1)).AsTask());
        Assert.Equal("handler-failure", ex.Message);
    }

    [Fact]
    public async Task Send_unregistered_message_throws_configuration_exception()
    {
        var sp = Build(c => { });
        var mediator = sp.GetRequiredService<IMediator>();

        await Assert.ThrowsAsync<MediatorConfigurationException>(
            () => mediator.Send((IQuery<OrderDto>)new GetOrder(1)).AsTask());
    }

    [Fact]
    public async Task Send_duplicate_command_handler_rejected_at_freeze()
    {
        var sc = new ServiceCollection();
        var ex = Assert.Throws<MediatorConfigurationException>(() =>
            sc.AddMediana(c => c
                .AddCommandHandler<CreateOrder, OrderCreated, CreateOrderHandler>()
                .AddCommandHandler<CreateOrder, OrderCreated, AsyncCreateOrderHandler>()));
        Assert.Contains("exactly one handler", ex.Message);
    }

    [Fact]
    public async Task Send_null_command_throws()
    {
        var sp = Build(c => c.AddCommandHandler<CreateOrder, OrderCreated, CreateOrderHandler>(),
            s => s.AddSingleton<CreateOrderHandler>());
        var mediator = sp.GetRequiredService<IMediator>();

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => mediator.Send<OrderCreated>((ICommand<OrderCreated>)null!).AsTask());
    }

    // ── SendExact: zero-boxing struct-─────────────────────

    [Fact]
    public async Task SendExact_struct_command_without_boxing()
    {
        var sp = Build(c => c.AddCommandHandler<IncrementCommand, int, IncrementHandler>(),
            s => s.AddSingleton<IncrementHandler>());
        var mediator = sp.GetRequiredService<IMediator>();

        var result = await mediator.SendExact<IncrementCommand, int>(new IncrementCommand(41));

        Assert.Equal(42, result);
    }

    // ── Publish: ─────────────────────────────────────────────────────

    [Fact]
    public async Task Publish_dispatches_to_all_handlers_sequentially()
    {
        var audit = new OrderCreatedAuditHandler();
        var metrics = new OrderCreatedMetricsHandler();
        var sp = Build(
            c => c.AddEventHandler<OrderCreated, OrderCreatedAuditHandler>()
                  .AddEventHandler<OrderCreated, OrderCreatedMetricsHandler>(),
            s =>
            {
                s.AddSingleton(audit);
                s.AddSingleton(metrics);
            });

        var mediator = sp.GetRequiredService<IMediator>();
        await mediator.Publish(new OrderCreated(5, "Created"));

        Assert.Single(audit.Seen);
        Assert.Equal(1, metrics.Count);
    }

    [Fact]
    public async Task Publish_without_subscribers_is_noop()
    {
        var sp = Build(c => { });
        var mediator = sp.GetRequiredService<IMediator>();

        await mediator.Publish(new OrderCreated(1, "x"));
    }

    [Fact]
    public async Task Publish_sequential_first_exception_interrupts()
    {
        var sp = Build(
            c => c.AddEventHandler<OrderCreated, ThrowingEventHandler>()
                  .AddEventHandler<OrderCreated, OrderCreatedMetricsHandler>(),
            s =>
            {
                s.AddSingleton<ThrowingEventHandler>();
                s.AddSingleton<OrderCreatedMetricsHandler>();
            });

        var mediator = sp.GetRequiredService<IMediator>();
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => mediator.Publish(new OrderCreated(1, "x")).AsTask());
    }

    [Fact]
    public async Task Publish_parallel_runs_handlers_and_aggregates_errors()
    {
        var slow = new SlowEventHandler();
        var sp = Build(
            c => c.AddEventHandler<OrderCreated, ThrowingEventHandler>()
                  .AddEventHandler<OrderCreated, SlowEventHandler>()
                  .SetEventPolicy<OrderCreated>(EventDispatchPolicy.Parallel),
            s =>
            {
                s.AddSingleton<ThrowingEventHandler>();
                s.AddSingleton(slow);
            });

        var mediator = sp.GetRequiredService<IMediator>();
        var ex = await Assert.ThrowsAsync<AggregateException>(
            () => mediator.Publish(new OrderCreated(3, "x")).AsTask());

        Assert.Single(ex.InnerExceptions);
        Assert.Equal("event-handler-failure", ex.InnerExceptions[0].Message);
        // See English documentation.
        await Task.Delay(80);
        Assert.Single(slow.CompletionOrder);
    }

    [Fact]
    public void SetEventPolicy_without_handlers_rejected()
    {
        var sc = new ServiceCollection();
        Assert.Throws<MediatorConfigurationException>(() =>
            sc.AddMediana(c => c.SetEventPolicy<OrderCreated>(EventDispatchPolicy.Parallel)));
    }

    // ── Stream ───────────────────────────────────────────────────────────────

    [Fact]
    public async Task Stream_yields_all_rows()
    {
        var sp = Build(c => c.AddStreamHandler<SearchOrders, OrderDto, SearchOrdersHandler>(),
            s => s.AddSingleton(new SearchOrdersHandler { Rows = 4 }));
        var mediator = sp.GetRequiredService<IMediator>();

        var rows = new List<OrderDto>();
        await foreach (var row in mediator.Stream((IStreamQuery<OrderDto>)new SearchOrders("f")))
        {
            rows.Add(row);
        }

        Assert.Equal(4, rows.Count);
        Assert.All(rows, r => Assert.Equal("f", r.Status));
    }

    [Fact]
    public async Task Stream_cancellation_stops_source()
    {
        var sp = Build(c => c.AddStreamHandler<SearchOrders, OrderDto, SearchOrdersHandler>(),
            s => s.AddSingleton(new SearchOrdersHandler { Rows = 100 }));
        var mediator = sp.GetRequiredService<IMediator>();

        using var cts = new CancellationTokenSource(100);
        var count = 0;
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var row in mediator.Stream((IStreamQuery<OrderDto>)new SearchOrders("f"), cts.Token))
            {
                count++;
            }
        });
        Assert.True(count < 100, $"expected early exit, got {count} rows");
    }

    // ── Behaviors ────────────────────────────────────────────────────────────

    [Fact]
    public async Task Behaviors_wrap_handler_in_registration_order()
    {
        OrderingBehavior.Trace = [];
        var sp = Build(
            c => c.AddCommandHandler<CreateOrder, OrderCreated, CreateOrderHandler>()
                  .AddMiddleware<CreateOrder, OrderCreated, OrderingBehavior>()
                  .AddMiddleware<CreateOrder, OrderCreated, SecondBehavior>(),
            s =>
            {
                s.AddSingleton<CreateOrderHandler>();
                s.AddSingleton<OrderingBehavior>();
                s.AddSingleton<SecondBehavior>();
            });

        var mediator = sp.GetRequiredService<IMediator>();
        await mediator.Send((ICommand<OrderCreated>)new CreateOrder(1));

        Assert.Equal(
        [
            "behavior:before",
            "second:before",
            "behavior:after-sync",
        ], OrderingBehavior.Trace);
    }

    [Fact]
    public async Task Cancellation_token_reaches_middlewares()
    {
        var sp = Build(
            c => c.AddQueryHandler<GetOrder, OrderDto, GetOrderHandler>()
                  .AddMiddleware<GetOrder, OrderDto, CancellationBehavior>(),
            s =>
            {
                s.AddSingleton<GetOrderHandler>();
                s.AddSingleton<CancellationBehavior>();
            });

        var mediator = sp.GetRequiredService<IMediator>();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => mediator.Send((IQuery<OrderDto>)new GetOrder(1), cts.Token).AsTask());
    }

    [Fact]
    public async Task Event_middlewares_wrap_event_handlers()
    {
        EventOrderingBehavior.Trace = [];
        var sp = Build(
            c => c.AddEventHandler<OrderCreated, OrderCreatedAuditHandler>()
                  .AddEventMiddleware<OrderCreated, EventOrderingBehavior>(),
            s =>
            {
                s.AddSingleton<OrderCreatedAuditHandler>();
                s.AddSingleton<EventOrderingBehavior>();
            });

        var mediator = sp.GetRequiredService<IMediator>();
        await mediator.Publish(new OrderCreated(1, "x"));

        Assert.Equal(["event-behavior:before", "event-behavior:after"], EventOrderingBehavior.Trace);
    }

    // ── Singleton vs Scoped ───────────────────────────────────────────

    [Fact]
    public async Task Singleton_mode_resolves_handler_once_and_reuses()
    {
        var handler = new CreateOrderHandler();
        var sp = Build(
            c => c.AddCommandHandler<CreateOrder, OrderCreated, CreateOrderHandler>().UseSingletonHandlers(),
            s => s.AddSingleton(handler));

        using var scope1 = sp.CreateScope();
        using var scope2 = sp.CreateScope();
        var m1 = scope1.ServiceProvider.GetRequiredService<IMediator>();
        var m2 = scope2.ServiceProvider.GetRequiredService<IMediator>();

        await m1.Send((ICommand<OrderCreated>)new CreateOrder(1));
        await m2.Send((ICommand<OrderCreated>)new CreateOrder(2));

        Assert.Equal(2, handler.Calls);
    }

    [Fact]
    public async Task Scoped_mode_resolves_handler_per_scope()
    {
        var sp = Build(
            c => c.AddCommandHandler<CreateOrder, OrderCreated, ScopedCounterHandler>(),
            s => s.AddScoped<ScopedCounterHandler>());

        using var scope1 = sp.CreateScope();
        using var scope2 = sp.CreateScope();
        var m1 = scope1.ServiceProvider.GetRequiredService<IMediator>();
        var m2 = scope2.ServiceProvider.GetRequiredService<IMediator>();

        await m1.Send((ICommand<OrderCreated>)new CreateOrder(1));
        await m2.Send((ICommand<OrderCreated>)new CreateOrder(2));

        var h1 = scope1.ServiceProvider.GetRequiredService<ScopedCounterHandler>();
        var h2 = scope2.ServiceProvider.GetRequiredService<ScopedCounterHandler>();
        Assert.Same(h1, h1);
        Assert.NotSame(h1, h2);
        Assert.Equal(1, h1.Calls);
        Assert.Equal(1, h2.Calls);
    }
}

public sealed class ScopedCounterHandler : ICommandHandler<CreateOrder, OrderCreated>
{
    public int Calls;

    public ValueTask<OrderCreated> Handle(CreateOrder command, CancellationToken ct)
    {
        Calls++;
        return new ValueTask<OrderCreated>(new OrderCreated(command.OrderId, "Scoped"));
    }
}
