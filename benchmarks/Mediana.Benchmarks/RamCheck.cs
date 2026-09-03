using System.Diagnostics;
using Mediana;
using Mediana.Handlers;
using Mediana.Messaging;
using Mediana.Pipeline;
using Microsoft.Extensions.DependencyInjection;

namespace Mediana.Benchmarks;

/// <summary>
/// Mediana vs MediatR RAM scenarios: GC-churn (andand on N and)
/// retention (and N notin async-and), footprint (from )
/// </summary>
public static class RamCheck
{
    private const int ChurnOps = 1_000_000;
    private const int RetentionOps = 20_000;

    private sealed record RamCommand(int Value) : ICommand<int>;
    private sealed record RamEvent(int Value) : IEvent;

    private sealed class RamHandler : ICommandHandler<RamCommand, int>
    {
        public ValueTask<int> Handle(RamCommand c, CancellationToken ct) => new(c.Value + 1);
    }

    private sealed class RamAsyncHandler(TaskCompletionSource gate) : ICommandHandler<RamCommand, int>
    {
        public async ValueTask<int> Handle(RamCommand c, CancellationToken ct)
        {
            await gate.Task.ConfigureAwait(false);
            return c.Value + 1;
        }
    }

    private sealed class RamPass1 : IHandlerMiddleware<RamCommand, int>
    {
        public ValueTask<int> Handle(RamCommand r, HandlerDelegate<RamCommand, int> next, CancellationToken ct) => next(r, ct);
    }

    private sealed class RamPass2 : IHandlerMiddleware<RamCommand, int>
    {
        public ValueTask<int> Handle(RamCommand r, HandlerDelegate<RamCommand, int> next, CancellationToken ct) => next(r, ct);
    }

    // ── MediatR mirrors ─────────────────────────────────────────────────────

    private sealed record MediatRRam(int Value) : global::MediatR.IRequest<int>;

    private sealed class MediatRRamHandler : global::MediatR.IRequestHandler<MediatRRam, int>
    {
        public Task<int> Handle(MediatRRam r, CancellationToken ct) => Task.FromResult(r.Value + 1);
    }

    private sealed class MediatRRamAsyncHandler(TaskCompletionSource gate) : global::MediatR.IRequestHandler<MediatRRam, int>
    {
        public async Task<int> Handle(MediatRRam r, CancellationToken ct)
        {
            await gate.Task.ConfigureAwait(false);
            return r.Value + 1;
        }
    }

    private sealed class MediatRPass1 : global::MediatR.IPipelineBehavior<MediatRRam, int>
    {
        public async Task<int> Handle(MediatRRam r, global::MediatR.RequestHandlerDelegate<int> next, CancellationToken ct) => await next();
    }

    private sealed class MediatRPass2 : global::MediatR.IPipelineBehavior<MediatRRam, int>
    {
        public async Task<int> Handle(MediatRRam r, global::MediatR.RequestHandlerDelegate<int> next, CancellationToken ct) => await next();
    }

    private static (IMediator Mediana, global::MediatR.IMediator MediatR) BuildBoth()
    {
        var mediana = new ServiceCollection()
            .AddSingleton<RamHandler>()
            .AddSingleton<RamPass1>()
            .AddSingleton<RamPass2>()
            .AddMediana(c => c
                .UseSingletonHandlers()
                .AddCommandHandler<RamCommand, int, RamHandler>()
                .AddMiddleware<RamCommand, int, RamPass1>()
                .AddMiddleware<RamCommand, int, RamPass2>());
        var medianaMediator = mediana.BuildServiceProvider().GetRequiredService<IMediator>();

        var mediatr = new ServiceCollection()
            .AddLogging()
            .AddSingleton<global::MediatR.IRequestHandler<MediatRRam, int>, MediatRRamHandler>()
            .AddTransient<global::MediatR.IPipelineBehavior<MediatRRam, int>, MediatRPass1>()
            .AddTransient<global::MediatR.IPipelineBehavior<MediatRRam, int>, MediatRPass2>()
            .AddMediatR(cfg => cfg.RegisterServicesFromAssembly(typeof(RamCheck).Assembly));
        var mediatrMediator = mediatr.BuildServiceProvider().GetRequiredService<global::MediatR.IMediator>();

        return (medianaMediator, mediatrMediator);
    }

    private static IMediator BuildMedianaOnly()
    {
        var mediana = new ServiceCollection()
            .AddSingleton<RamHandler>()
            .AddSingleton<RamPass1>()
            .AddSingleton<RamPass2>()
            .AddMediana(c => c
                .UseSingletonHandlers()
                .AddCommandHandler<RamCommand, int, RamHandler>()
                .AddMiddleware<RamCommand, int, RamPass1>()
                .AddMiddleware<RamCommand, int, RamPass2>());
        return mediana.BuildServiceProvider().GetRequiredService<IMediator>();
    }

    private static global::MediatR.IMediator BuildMediatROnly()
    {
        var mediatr = new ServiceCollection()
            .AddLogging()
            .AddSingleton<global::MediatR.IRequestHandler<MediatRRam, int>, MediatRRamHandler>()
            .AddTransient<global::MediatR.IPipelineBehavior<MediatRRam, int>, MediatRPass1>()
            .AddTransient<global::MediatR.IPipelineBehavior<MediatRRam, int>, MediatRPass2>()
            .AddMediatR(cfg => cfg.RegisterServicesFromAssembly(typeof(RamCheck).Assembly));
        return mediatr.BuildServiceProvider().GetRequiredService<global::MediatR.IMediator>();
    }

    private static void FullCollect()
    {
        GC.Collect(2, GCCollectionMode.Forced, blocking: true);
        GC.WaitForPendingFinalizers();
        GC.Collect(2, GCCollectionMode.Forced, blocking: true);
    }

    // ═══ onand 1: churn — GC-andand and andand on ChurnOps and ═══

    public static void Churn()
    {
        var (mediana, mediatr) = BuildBoth();
        var cmd = (ICommand<int>)new RamCommand(1);
        var mcmd = new MediatRRam(1);

        // warmup
        for (var i = 0; i < 20_000; i++)
        {
            _ = mediana.Send(cmd);
            _ = mediatr.Send(mcmd);
        }

 Console.WriteLine($"== RAM churn: {ChurnOps:N0} sync- and (Send + 2 pass-through middlewares) ==\n");


        FullCollect();
        var g0 = GC.CollectionCount(0); var g1 = GC.CollectionCount(1); var g2 = GC.CollectionCount(2);
        var alloc = GC.GetTotalAllocatedBytes(precise: true);
        var sw = Stopwatch.StartNew();
        for (var i = 0; i < ChurnOps; i++)
        {
            _ = mediatr.Send(mcmd);
        }

        sw.Stop();
        var mg0 = GC.CollectionCount(0) - g0; var mg1 = GC.CollectionCount(1) - g1;
        var mAlloc = GC.GetTotalAllocatedBytes(precise: true) - alloc;
 Console.WriteLine($"MediatR : {sw.ElapsedMilliseconds,5} ms | Gen0={mg0,4} Gen1={mg1,3} | allocated {mAlloc / (double)ChurnOps,7:F0} B/ ");


        FullCollect();
        g0 = GC.CollectionCount(0); g1 = GC.CollectionCount(1); g2 = GC.CollectionCount(2);
        alloc = GC.GetTotalAllocatedBytes(precise: true);
        sw.Restart();
        for (var i = 0; i < ChurnOps; i++)
        {
            _ = mediana.Send(cmd);
        }

        sw.Stop();
        var dg0 = GC.CollectionCount(0) - g0; var dg1 = GC.CollectionCount(1) - g1;
        var dAlloc = GC.GetTotalAllocatedBytes(precise: true) - alloc;
 Console.WriteLine($"Mediana : {sw.ElapsedMilliseconds,5} ms | Gen0={dg0,4} Gen1={dg1,3} | allocated {dAlloc / (double)ChurnOps,7:F0} B/ ");

    }

    // ═══ onand 2: retention — and RetentionOps notin async-and ═══

    public static void Retention()
    {
 Console.WriteLine($"\n== RAM retention: {RetentionOps:N0} andin async- and (by by but GC) ==\n");


        // MediatR
        {
            var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var sc = new ServiceCollection()
                .AddLogging()
                .AddSingleton<global::MediatR.IRequestHandler<MediatRRam, int>>(new MediatRRamAsyncHandler(gate))
                .AddTransient<global::MediatR.IPipelineBehavior<MediatRRam, int>, MediatRPass1>()
                .AddTransient<global::MediatR.IPipelineBehavior<MediatRRam, int>, MediatRPass2>()
                .AddMediatR(cfg => cfg.RegisterServicesFromAssembly(typeof(RamCheck).Assembly));
            var mediator = sc.BuildServiceProvider().GetRequiredService<global::MediatR.IMediator>();

            FullCollect();
            var before = GC.GetTotalMemory(true);
            var pending = new Task<int>[RetentionOps];
            for (var i = 0; i < RetentionOps; i++)
            {
                pending[i] = mediator.Send(new MediatRRam(i));
            }

            FullCollect();
            var retained = GC.GetTotalMemory(true) - before;
 Console.WriteLine($"MediatR : retained {retained / 1024.0 / 1024,7:F2} MB ({retained / (double)RetentionOps,6:F0} B/ )");


            gate.SetResult();
            Task.WaitAll(pending);
            pending = null!;
            FullCollect();
        }

        // Mediana
        {
            var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var sc = new ServiceCollection()
                .AddSingleton(new RamAsyncHandler(gate))
                .AddSingleton<RamPass1>()
                .AddSingleton<RamPass2>()
                .AddMediana(c => c
                    .UseSingletonHandlers()
                    .AddCommandHandler<RamCommand, int, RamAsyncHandler>()
                    .AddMiddleware<RamCommand, int, RamPass1>()
                    .AddMiddleware<RamCommand, int, RamPass2>());
            var mediator = (Mediator)sc.BuildServiceProvider().GetRequiredService<IMediator>();

            FullCollect();
            var before = GC.GetTotalMemory(true);
            var pending = new ValueTask<int>[RetentionOps];
            for (var i = 0; i < RetentionOps; i++)
            {
                pending[i] = mediator.Send((ICommand<int>)new RamCommand(i));
            }

            FullCollect();
            var retained = GC.GetTotalMemory(true) - before;
 Console.WriteLine($"Mediana : retained {retained / 1024.0 / 1024,7:F2} MB ({retained / (double)RetentionOps,6:F0} B/ )");


            gate.SetResult();
            for (var i = 0; i < RetentionOps; i++)
            {
                _ = pending[i].Result;
            }

            pending = null!;
            FullCollect();
        }
    }

    // ═══ onand 3: footprint but (JIT + + by churn) ═══

    public static void Footprint(string lib)
    {
        var isMediatR = lib == "mediatr";
 Console.WriteLine($"== RAM footprint ({lib}): warmup + {200_000:N0} and , by on GC ==\n");


        if (isMediatR)
        {
            var mediator = BuildMediatROnly();
            var cmd = new MediatRRam(1);
            for (var i = 0; i < 220_000; i++)
            {
                _ = mediator.Send(cmd);
            }
        }
        else
        {
            var mediator = BuildMedianaOnly();
            var cmd = (ICommand<int>)new RamCommand(1);
            for (var i = 0; i < 220_000; i++)
            {
                _ = mediator.Send(cmd);
            }
        }

        FullCollect();
        var proc = Process.GetCurrentProcess();
        Console.WriteLine($"Managed heap : {GC.GetTotalMemory(true) / 1024.0,8:F1} KB");
        Console.WriteLine($"WorkingSet   : {proc.WorkingSet64 / 1024.0,8:F1} KB");
        Console.WriteLine($"PrivateMem   : {proc.PrivateMemorySize64 / 1024.0,8:F1} KB");
    }
}
