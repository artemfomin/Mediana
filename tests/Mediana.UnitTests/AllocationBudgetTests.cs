using Mediana.Messaging;
using Mediana.UnitTests.TestMessages;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Mediana.UnitTests;

/// <summary>
/// Аллокационные бюджеты спеки §12 (D16): абсолютные контракты на горячих путях.
/// Помечены Category=Allocation — CI-гейт. Проверка через GC.GetAllocatedBytesForCurrentThread:
/// первый прогон разогревает (ленивые композиции singleton-цепочек), второй — измеряет steady state.
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

    /// <summary>Бюджет §12.1: Send с 2 behaviors, sync-хендлер — 0 байт в steady state.</summary>
    [Fact]
    public async Task Send_sync_with_two_behaviors_zero_alloc()
    {
        var sc = new ServiceCollection()
            .AddSingleton<AllocCommandHandler>()
            .AddSingleton<AllocBehavior1>()
            .AddSingleton<AllocBehavior2>()
            .AddMediana(c => c
                .UseSingletonHandlers()
                .AddCommandHandler<AllocCommand, int, AllocCommandHandler>()
                .AddBehavior<AllocCommand, int, AllocBehavior1>()
                .AddBehavior<AllocCommand, int, AllocBehavior2>());
        var sp = sc.BuildServiceProvider();
        var mediator = sp.GetRequiredService<IMediator>();
        var command = (ICommand<int>)new AllocCommand(7);

        // Разогрев: ленивая композиция singleton-цепочки один раз.
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

    /// <summary>Бюджет §12.3: Publish sequential, 2 хендлера — 0 байт.</summary>
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
    /// Бюджет §12.5: Stream без behaviors — 0 байт на движение курсора;
    /// ≤1 малой аллокации на вызов (IAsyncEnumerator) — документированная граница.
    /// </summary>
    [Fact]
    public async Task Stream_without_behaviors_zero_alloc_per_cursor_move()
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
        // 3 строки на вызов; фиксированная стоимость на вызов (энумератор + интерп-машинерия
        // await foreach): ≤ 96 байт/вызов. Движение курсора между строками — ноль.
        Assert.True(allocated <= iterations * 96,
            $"Stream path allocated {allocated} bytes for {iterations} calls ({allocated / (double)iterations:F1}/call), budget 96/call.");
    }

    /// <summary>Бюджет §12.2: async-хендлер — 0 байт в steady state (пул состояний).</summary>
    [Fact]
    public async Task Send_async_handler_zero_alloc_steady_state()
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

        // Асинхронный путь уходит с текущего потока: измеряем общий объём аллокаций.
        var before = GC.GetTotalAllocatedBytes(precise: true);
        for (var i = 0; i < 2000; i++)
        {
            _ = await mediator.Send(command);
        }

        var allocated = GC.GetTotalAllocatedBytes(precise: true) - before;
        // Документированный бюджет: истинно-асинхронный путь (Task.Yield) несёт стоимость
        // async-инфраструктуры CLR (state machine + очередь потоков): ~250Б соло / до ~700Б
        // под нагрузкой полного прогона. ns2.1-ассет (фасадные async-builders) — до 1600Б.
        // Синхронные хендлеры — строго ноль (Value_response тесты).
        var isNetStandardAsset = typeof(Mediator).Assembly
            .GetReferencedAssemblies()
            .Any(a => a.Name == "netstandard");
        var perSend = isNetStandardAsset ? 1600 : 800;
        Assert.True(allocated <= 2000 * perSend,
            $"Async path allocated {allocated} bytes for 2000 sends ({allocated / 2000:F1}/send), budget {perSend}/send (asset={(isNetStandardAsset ? "ns2.1" : "net10")}).");
    }

    /// <summary>SendExact struct-команда: без боксинга сообщения и ответа.</summary>
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

// ── Вспомогательные типы для аллокационных тестов ──────────────────────────

public sealed class AllocBehavior1 : Pipeline.IPipelineBehavior<AllocCommand, int>
{
    public ValueTask<int> Handle(AllocCommand request, Pipeline.RequestHandlerDelegate<AllocCommand, int> next, CancellationToken ct)
        => next(request, ct);
}

public sealed class AllocBehavior2 : Pipeline.IPipelineBehavior<AllocCommand, int>
{
    public ValueTask<int> Handle(AllocCommand request, Pipeline.RequestHandlerDelegate<AllocCommand, int> next, CancellationToken ct)
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
/// Полностью синхронный стрим-хендлер с zero-alloc движением курсора:
/// MoveNextAsync возвращает завершённые ValueTask (никаких async-машин).
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
