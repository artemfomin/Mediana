using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Mediana;
using Mediana.Handlers;
using Mediana.Messaging;
using Mediana.Pipeline;
using Mediana.Routing;
using Mediana.Reliability;
using Mediana.Transports;
using Microsoft.Extensions.DependencyInjection;

namespace Mediana.Benchmarks;

public sealed record BenchCommand(int Value) : ICommand<int>;
public sealed record BenchQuery(int Value) : IQuery<int>;
public sealed record BenchEvent(int Value) : IEvent;

public sealed class BenchCommandHandler : ICommandHandler<BenchCommand, int>
{
    public ValueTask<int> Handle(BenchCommand command, CancellationToken ct) => new(command.Value + 1);
}

public sealed class BenchQueryHandler : IQueryHandler<BenchQuery, int>
{
    public ValueTask<int> Handle(BenchQuery query, CancellationToken ct) => new(query.Value + 1);
}

public sealed class BenchEventHandler1 : IEventHandler<BenchEvent>
{
    public ValueTask Handle(BenchEvent @event, CancellationToken ct) => default;
}

public sealed class BenchEventHandler2 : IEventHandler<BenchEvent>
{
    public ValueTask Handle(BenchEvent @event, CancellationToken ct) => default;
}

public sealed class PassBehavior1 : IPipelineBehavior<BenchCommand, int>
{
    public ValueTask<int> Handle(BenchCommand request, RequestHandlerDelegate<BenchCommand, int> next, CancellationToken ct)
        => next(request, ct);
}

public sealed class PassBehavior2 : IPipelineBehavior<BenchCommand, int>
{
    public ValueTask<int> Handle(BenchCommand request, RequestHandlerDelegate<BenchCommand, int> next, CancellationToken ct)
        => next(request, ct);
}

// ── MediatR-эквиваленты ──────────────────────────────────────────────────────

public sealed record MediatRPing(int Value) : global::MediatR.IRequest<int>;
public sealed record MediatRNotification(int Value) : global::MediatR.INotification;

public sealed class MediatRPingHandler : global::MediatR.IRequestHandler<MediatRPing, int>
{
    public Task<int> Handle(MediatRPing request, CancellationToken ct) => Task.FromResult(request.Value + 1);
}

public sealed class MediatRNotificationHandler1 : global::MediatR.INotificationHandler<MediatRNotification>
{
    public Task Handle(MediatRNotification n, CancellationToken ct) => Task.CompletedTask;
}

public sealed class MediatRNotificationHandler2 : global::MediatR.INotificationHandler<MediatRNotification>
{
    public Task Handle(MediatRNotification n, CancellationToken ct) => Task.CompletedTask;
}

public sealed class MediatRPassBehavior1 : global::MediatR.IPipelineBehavior<MediatRPing, int>
{
    public async Task<int> Handle(MediatRPing request, global::MediatR.RequestHandlerDelegate<int> next, CancellationToken ct)
        => await next();
}

public sealed class MediatRPassBehavior2 : global::MediatR.IPipelineBehavior<MediatRPing, int>
{
    public async Task<int> Handle(MediatRPing request, global::MediatR.RequestHandlerDelegate<int> next, CancellationToken ct)
        => await next();
}

[MemoryDiagnoser]
public class DispatchBenchmarks
{
    private IMediator _mediana = default!;
    private global::MediatR.IMediator _mediatr = default!;
    private ICommand<int> _command = default!;
    private IQuery<int> _query = default!;
    private BenchEvent _event = default!;
    private MediatRPing _mediatrRequest = default!;

    [GlobalSetup]
    public void Setup()
    {
        var mediana = new ServiceCollection()
            .AddSingleton<BenchCommandHandler>()
            .AddSingleton<BenchQueryHandler>()
            .AddSingleton<BenchEventHandler1>()
            .AddSingleton<BenchEventHandler2>()
            .AddSingleton<PassBehavior1>()
            .AddSingleton<PassBehavior2>()
            .AddMediana(c => c
                .UseSingletonHandlers()
                .AddCommandHandler<BenchCommand, int, BenchCommandHandler>()
                .AddQueryHandler<BenchQuery, int, BenchQueryHandler>()
                .AddEventHandler<BenchEvent, BenchEventHandler1>()
                .AddEventHandler<BenchEvent, BenchEventHandler2>()
                .AddBehavior<BenchCommand, int, PassBehavior1>()
                .AddBehavior<BenchCommand, int, PassBehavior2>());
        _mediana = mediana.BuildServiceProvider().GetRequiredService<IMediator>();
        _command = new BenchCommand(1);
        _query = new BenchQuery(1);
        _event = new BenchEvent(1);

        var mediatr = new ServiceCollection()
            .AddLogging()
            .AddSingleton<MediatRPingHandler>()
            .AddSingleton<MediatRNotificationHandler1>()
            .AddSingleton<MediatRNotificationHandler2>()
            .AddTransient<global::MediatR.IPipelineBehavior<MediatRPing, int>, MediatRPassBehavior1>()
            .AddTransient<global::MediatR.IPipelineBehavior<MediatRPing, int>, MediatRPassBehavior2>()
            .AddMediatR(cfg => cfg.RegisterServicesFromAssembly(typeof(DispatchBenchmarks).Assembly));
        _mediatr = mediatr.BuildServiceProvider().GetRequiredService<global::MediatR.IMediator>();
        _mediatrRequest = new MediatRPing(1);
    }

    [Benchmark(Baseline = true)]
    public Task<int> MediatR_Send() => _mediatr.Send(_mediatrRequest);

    [Benchmark]
    public ValueTask<int> Mediana_Send() => _mediana.Send(_command);

    [Benchmark]
    public ValueTask<int> Mediana_Query() => _mediana.Send(_query);

    [Benchmark]
    public Task MediatR_Publish() => _mediatr.Publish(new MediatRNotification(1));

    [Benchmark]
    public ValueTask Mediana_Publish() => _mediana.Publish(_event);
}

public static class Program
{
    public static void Main(string[] args)
    {
        if (args.Length > 0 && args[0] == "alloc-check")
        {
            // быстрый аллокационный прогон без BenchmarkDotNet (CI-friendly)
            var benchmarks = new DispatchBenchmarks();
            benchmarks.Setup();

            var command = (ICommand<int>)new BenchCommand(1);
            for (var i = 0; i < 1000; i++)
            {
                _ = benchmarks.Mediana_Send();
            }

            var before = GC.GetAllocatedBytesForCurrentThread();
            for (var i = 0; i < 10_000; i++)
            {
                _ = benchmarks.Mediana_Send();
            }

            var perCall = (GC.GetAllocatedBytesForCurrentThread() - before) / 10_000.0;
            Console.WriteLine($"Mediana Send+2behaviors: {perCall:F2} bytes/call");
            if (perCall > 0.5)
            {
                Console.Error.WriteLine($"ALLOC GATE FAIL: {perCall:F2} > 0.5 bytes/call");
                Environment.Exit(1);
            }

            return;
        }

        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
    }
}
