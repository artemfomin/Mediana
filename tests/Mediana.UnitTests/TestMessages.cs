using Mediana.Handlers;
using Mediana.Messaging;
using Mediana.Pipeline;

namespace Mediana.UnitTests.TestMessages;

// ── Команды/запросы ──────────────────────────────────────────────────────────

public sealed record CreateOrder(int OrderId) : ICommand<OrderCreated>;
public sealed record OrderCreated(int OrderId, string Status) : IEvent;
public sealed record GetOrder(int OrderId) : IQuery<OrderDto>;
public sealed record OrderDto(int OrderId, string Status);
public sealed record SearchOrders(string Filter) : IStreamQuery<OrderDto>;
public sealed record Ping(Guid Value) : ICommand<string>;

/// <summary>Struct-команда для zero-boxing тестов SendExact.</summary>
public readonly record struct IncrementCommand(int Delta) : ICommand<int>;

// ── Хендлеры ─────────────────────────────────────────────────────────────────

public sealed class CreateOrderHandler : ICommandHandler<CreateOrder, OrderCreated>
{
    public int Calls;
    public ValueTask<OrderCreated> Handle(CreateOrder command, CancellationToken ct)
    {
        Calls++;
        return new ValueTask<OrderCreated>(new OrderCreated(command.OrderId, "Created"));
    }
}

public sealed class AsyncCreateOrderHandler : ICommandHandler<CreateOrder, OrderCreated>
{
    public ValueTask<OrderCreated> Handle(CreateOrder command, CancellationToken ct)
        => Async(command);

    private static async ValueTask<OrderCreated> Async(CreateOrder command)
    {
        await Task.Yield();
        return new OrderCreated(command.OrderId, "CreatedAsync");
    }
}

public sealed class GetOrderHandler : IQueryHandler<GetOrder, OrderDto>
{
    public ValueTask<OrderDto> Handle(GetOrder query, CancellationToken ct)
        => new(new OrderDto(query.OrderId, "Fetched"));
}

public sealed class ThrowingQueryHandler : IQueryHandler<GetOrder, OrderDto>
{
    public ValueTask<OrderDto> Handle(GetOrder query, CancellationToken ct)
        => throw new InvalidOperationException("handler-failure");
}

public sealed class SearchOrdersHandler : IStreamHandler<SearchOrders, OrderDto>
{
    public int Rows = 3;
    public async IAsyncEnumerable<OrderDto> Handle(SearchOrders query, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        for (var i = 0; i < Rows; i++)
        {
            await Task.Delay(10, ct);
            yield return new OrderDto(i, query.Filter);
        }
    }
}

public sealed class IncrementHandler : ICommandHandler<IncrementCommand, int>
{
    public ValueTask<int> Handle(IncrementCommand command, CancellationToken ct)
        => new(command.Delta + 1);
}

// ── Хендлеры событий ─────────────────────────────────────────────────────────

public sealed class OrderCreatedAuditHandler : IEventHandler<OrderCreated>
{
    public List<OrderCreated> Seen = [];
    public ValueTask Handle(OrderCreated @event, CancellationToken ct)
    {
        Seen.Add(@event);
        return ValueTask.CompletedTask;
    }
}

public sealed class OrderCreatedMetricsHandler : IEventHandler<OrderCreated>
{
    public int Count;
    public ValueTask Handle(OrderCreated @event, CancellationToken ct)
    {
        Count++;
        return ValueTask.CompletedTask;
    }
}

public sealed class ThrowingEventHandler : IEventHandler<OrderCreated>
{
    public ValueTask Handle(OrderCreated @event, CancellationToken ct)
        => throw new InvalidOperationException("event-handler-failure");
}

public sealed class SlowEventHandler : IEventHandler<OrderCreated>
{
    public List<int> CompletionOrder = [];
    public async ValueTask Handle(OrderCreated @event, CancellationToken ct)
    {
        await Task.Delay(50, ct);
        CompletionOrder.Add(@event.OrderId);
    }
}

// ── Behaviors ────────────────────────────────────────────────────────────────

public sealed class OrderingBehavior : IPipelineBehavior<CreateOrder, OrderCreated>
{
    public static List<string> Trace = [];
    public ValueTask<OrderCreated> Handle(CreateOrder request, RequestHandlerDelegate<CreateOrder, OrderCreated> next, CancellationToken ct)
    {
        Trace.Add("behavior:before");
        var result = next(request, ct);
        Trace.Add("behavior:after-sync");
        return result;
    }
}

public sealed class SecondBehavior : IPipelineBehavior<CreateOrder, OrderCreated>
{
    public ValueTask<OrderCreated> Handle(CreateOrder request, RequestHandlerDelegate<CreateOrder, OrderCreated> next, CancellationToken ct)
    {
        OrderingBehavior.Trace.Add("second:before");
        return next(request, ct);
    }
}

public sealed class CancellationBehavior : IPipelineBehavior<GetOrder, OrderDto>
{
    public ValueTask<OrderDto> Handle(GetOrder request, RequestHandlerDelegate<GetOrder, OrderDto> next, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        return next(request, ct);
    }
}

public sealed class EventOrderingBehavior : IEventPipelineBehavior<OrderCreated>
{
    public static List<string> Trace = [];
    public ValueTask Handle(OrderCreated @event, EventHandlerDelegate<OrderCreated> next, CancellationToken ct)
    {
        Trace.Add("event-behavior:before");
        var result = next(@event, ct);
        Trace.Add("event-behavior:after");
        return result;
    }
}

public sealed class StreamFilterBehavior : IStreamPipelineBehavior<SearchOrders, OrderDto>
{
    public ValueTask<IAsyncEnumerable<OrderDto>>? Observed;
    public IAsyncEnumerable<OrderDto> Handle(SearchOrders query, StreamHandlerDelegate<SearchOrders, OrderDto> next, CancellationToken ct)
        => next(query, ct);
}
