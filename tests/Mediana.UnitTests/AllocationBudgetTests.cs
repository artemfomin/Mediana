using Mediana.Messaging;
using Mediana.UnitTests.TestMessages;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Mediana.UnitTests;

/// <summary>
/// §12 (D16):
/// Category=Allocation — CI-. GC.GetAllocatedBytesForCurrentThread:
/// (singleton-), steady state
/// </summary>
[Trait("Category", "Allocation")]
public class AllocationBudgetTests
{
    private static (IMediator Mediator, IServiceProvider Sp) BuildSingletonMediator()
    {
        var sc = new ServiceCollection()
            .AddSingleton<SyncRowsStreamHandler>()
            .AddMediana(c => c
                .UseSingletonHandlers()
                .AddStreamHandler<SyncRows, int, SyncRowsStreamHandler>());

        var sp = sc.BuildServiceProvider();
        return (sp.GetRequiredService<IMediator>(), sp);
    }

    /// <summary>§12.1: Send 2 behaviors, sync-0 steady state.</summary>
    [Fact]
    public async Task Send_sync_with_two_middlewares_zero_alloc()
    {
        var sc = new ServiceCollection()
            .AddSingleton<AllocCommandHandler>()
            .AddSingleton<AllocBehavior1>()
            .AddSingleton<AllocBehavior2>()
            .AddMediana(c => c
                .UseSingletonHandlers()
                .AddCommandHandler<AllocCommand, int, AllocCommandHandler>()
                .AddMiddleware<AllocCommand, int, AllocBehavior1>()
                .AddMiddleware<AllocCommand, int, AllocBehavior2>());
        var sp = sc.BuildServiceProvider();
        var mediator = sp.GetRequiredService<IMediator>();
        var command = (ICommand<int>)new AllocCommand(7);

        // : singleton-
        for (var i = 0; i < 100; i++)
        {
            _ = await mediator.Send(command);
        }

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++)
        {
            _ = await mediator.Send(command);
        }

        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.Equal(0, allocated);
    }

    /// <summary>§12.3: Publish sequential, 2 0 .</summary>
    [Fact]
    public async Task Publish_sequential_two_handlers_zero_alloc()
    {
        var sc = new ServiceCollection()
            .AddSingleton<CountingHandler1>()
            .AddSingleton<CountingHandler2>()
            .AddMediana(c => c
                .UseSingletonHandlers()
                .AddEventHandler<CountedEvent, CountingHandler1>()
                .AddEventHandler<CountedEvent, CountingHandler2>());
        var sp = sc.BuildServiceProvider();
        var mediator = sp.GetRequiredService<IMediator>();
        var @event = new CountedEvent();

        for (var i = 0; i < 100; i++)
        {
            await mediator.Publish(@event);
        }

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++)
        {
            await mediator.Publish(@event);
        }

        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.Equal(0, allocated);
    }

    /// <summary>
    /// §12.5: Stream behaviors — 0
    /// ≤1 (IAsyncEnumerator) —
    /// </summary>
    [Fact]
    public async Task Stream_without_middlewares_zero_alloc_per_cursor_move()
    {
        var (mediator, _) = BuildSingletonMediator();
        var query = (IStreamQuery<int>)new SyncRows();

        for (var warmup = 0; warmup < 10; warmup++)
        {
            await foreach (var row in mediator.Stream(query))
            {
            }
        }

        const int iterations = 100;
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var iter = 0; iter < iterations; iter++)
        {
            await foreach (var row in mediator.Stream(query))
            {
            }
        }

        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        // 3 ; (+
        // await foreach): ≤ 96 /
        Assert.True(allocated <= iterations * 96,
            $"Stream path allocated {allocated} bytes for {iterations} calls ({allocated / (double)iterations:F1}/call), budget 96/call.");
    }

    /// <summary>
    /// §12.2 (async): async-baseline
    /// Task.Yield
    /// SendAsyncAlloc <= YieldBaseline + 300/()
    /// </summary>
    [Fact]
    public async Task Send_async_handler_alloc_normalized_to_yield_baseline()
    {
        var sc = new ServiceCollection()
            .AddSingleton<AsyncCreateOrderHandler>()
            .AddMediana(c => c
                .UseSingletonHandlers()
                .AddCommandHandler<CreateOrder, OrderCreated, AsyncCreateOrderHandler>());
        var sp = sc.BuildServiceProvider();
        var mediator = sp.GetRequiredService<IMediator>();
        var command = (ICommand<OrderCreated>)new CreateOrder(1);

        for (var i = 0; i < 1000; i++)
        {
            _ = await mediator.Send(command);
        }

        // baseline: Task.Yield-
        for (var i = 0; i < 500; i++)
        {
            await Task.Yield();
        }

        var beforeYield = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 2000; i++)
        {
            await Task.Yield();
        }

        var yieldBaseline = GC.GetAllocatedBytesForCurrentThread() - beforeYield;

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 2000; i++)
        {
            #pragma warning disable xUnit1031
            _ = mediator.Send(command).Result;
            #pragma warning restore xUnit1031
        }

        var sendAlloc = GC.GetAllocatedBytesForCurrentThread() - before;

        // : yield. async-
        // record-≈ 860, ~2000(; ~860); ns2.1-. x2 (≥4000)
        var isNs21 = typeof(Mediator).Assembly.GetReferencedAssemblies().Any(a => a.Name == "netstandard");
        var overheadBudget = isNs21 ? 4000 : 2500;
        Assert.True(
            sendAlloc <= yieldBaseline + 2000 * overheadBudget,
            $"Async send {sendAlloc / 2000.0:F0}B/call vs yield baseline {yieldBaseline / 2000.0:F0}B/call; overhead budget {overheadBudget}B/call (asset={(isNs21 ? "ns2.1" : "net10")}).");
    }

    /// <summary>SendExact struct-: .</summary>
    [Fact]
    public async Task SendExact_struct_command_zero_alloc()
    {
        var sc = new ServiceCollection()
            .AddSingleton<IncrementHandler>()
            .AddMediana(c => c
                .UseSingletonHandlers()
                .AddCommandHandler<IncrementCommand, int, IncrementHandler>());
        var sp = sc.BuildServiceProvider();
        var mediator = sp.GetRequiredService<IMediator>();
        var command = new IncrementCommand(1);

        for (var i = 0; i < 100; i++)
        {
            _ = await mediator.SendExact<IncrementCommand, int>(command);
        }

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++)
        {
            _ = await mediator.SendExact<IncrementCommand, int>(command);
        }

        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.Equal(0, allocated);
    }
}

// ── ──────────────────────────

public sealed class AllocBehavior1 : Pipeline.IHandlerMiddleware<AllocCommand, int>
{
    public ValueTask<int> Handle(AllocCommand request, Pipeline.HandlerDelegate<AllocCommand, int> next, CancellationToken ct)
        => next(request, ct);
}

public sealed class AllocBehavior2 : Pipeline.IHandlerMiddleware<AllocCommand, int>
{
    public ValueTask<int> Handle(AllocCommand request, Pipeline.HandlerDelegate<AllocCommand, int> next, CancellationToken ct)
        => next(request, ct);
}

public sealed record AllocCommand(int Delta) : ICommand<int>;

public sealed class AllocCommandHandler : Handlers.ICommandHandler<AllocCommand, int>
{
    public ValueTask<int> Handle(AllocCommand command, CancellationToken ct)
        => new(command.Delta + 1);
}

public sealed record CountedEvent : IEvent;

public sealed class CountingHandler1 : Handlers.IEventHandler<CountedEvent>
{
    public int Count;

    public ValueTask Handle(CountedEvent @event, CancellationToken ct)
    {
        Count++;
        return ValueTask.CompletedTask;
    }
}

public sealed class CountingHandler2 : Handlers.IEventHandler<CountedEvent>
{
    public int Count;

    public ValueTask Handle(CountedEvent @event, CancellationToken ct)
    {
        Count++;
        return ValueTask.CompletedTask;
    }
}

public sealed record SyncRows() : IStreamQuery<int>;

/// <summary>
/// zero-alloc :
/// MoveNextAsync ValueTask (async-)
/// </summary>
public sealed class SyncRowsStreamHandler : Handlers.IStreamHandler<SyncRows, int>
{
    public IAsyncEnumerable<int> Handle(SyncRows query, CancellationToken ct)
        => new SyncRowsEnumerable(3);

    private sealed class SyncRowsEnumerable(int count) : IAsyncEnumerable<int>
    {
        public IAsyncEnumerator<int> GetAsyncEnumerator(CancellationToken cancellationToken)
            => new Enumerator(count);

        private sealed class Enumerator(int count) : IAsyncEnumerator<int>
        {
            private int _current;

            public int Current => _current;

            public ValueTask<bool> MoveNextAsync()
            {
                if (_current < count)
                {
                    _current++;
                    return new ValueTask<bool>(true);
                }

                return new ValueTask<bool>(false);
            }

            public ValueTask DisposeAsync() => default;
        }
    }
}
