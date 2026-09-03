using System.Diagnostics;
using Mediana;
using Mediana.Handlers;
using Mediana.Messaging;
using Mediana.Pipeline;
using Microsoft.Extensions.DependencyInjection;

namespace Mediana.Benchmarks;

/// <summary>
/// Нагрузочные сценарии in-process: #1 масштабирование throughput по потокам,
/// #2 хвостовые латентности (p50..p99.99) + GC-паузы. Workstation GC зафиксирован
/// в csproj; стороны гоняются последовательно с полной GC между фазами.
/// </summary>
public static class LoadCheck
{
    private static readonly int[] ThreadCounts = [1, 2, 4, 8, 16, 32, 64];
    private static readonly TimeSpan ScalingDuration = TimeSpan.FromSeconds(3);
    private const int TailTotalOps = 5_000_000;

    // ── Сообщения/хендлеры (симметричные для обеих сторон) ──────────────────

    private sealed record LoadCmd(int Value) : ICommand<int>;

    private sealed class LoadHandler : ICommandHandler<LoadCmd, int>
    {
        public ValueTask<int> Handle(LoadCmd c, CancellationToken ct) => new(c.Value + 1);
    }

    private sealed class Pass1 : IHandlerMiddleware<LoadCmd, int>
    {
        public ValueTask<int> Handle(LoadCmd r, HandlerDelegate<LoadCmd, int> next, CancellationToken ct) => next(r, ct);
    }

    private sealed class Pass2 : IHandlerMiddleware<LoadCmd, int>
    {
        public ValueTask<int> Handle(LoadCmd r, HandlerDelegate<LoadCmd, int> next, CancellationToken ct) => next(r, ct);
    }

    private sealed record MediatRLoad(int Value) : global::MediatR.IRequest<int>;

    private sealed class MediatRLoadHandler : global::MediatR.IRequestHandler<MediatRLoad, int>
    {
        public Task<int> Handle(MediatRLoad r, CancellationToken ct) => Task.FromResult(r.Value + 1);
    }

    private sealed class MediatRPass1 : global::MediatR.IPipelineBehavior<MediatRLoad, int>
    {
        public async Task<int> Handle(MediatRLoad r, global::MediatR.RequestHandlerDelegate<int> next, CancellationToken ct) => await next();
    }

    private sealed class MediatRPass2 : global::MediatR.IPipelineBehavior<MediatRLoad, int>
    {
        public async Task<int> Handle(MediatRLoad r, global::MediatR.RequestHandlerDelegate<int> next, CancellationToken ct) => await next();
    }

    private static IMediator BuildMediana()
    {
        var sc = new ServiceCollection()
            .AddSingleton<LoadHandler>()
            .AddSingleton<Pass1>()
            .AddSingleton<Pass2>()
            .AddMediana(c => c
                .UseSingletonHandlers()
                .AddCommandHandler<LoadCmd, int, LoadHandler>()
                .AddMiddleware<LoadCmd, int, Pass1>()
                .AddMiddleware<LoadCmd, int, Pass2>());
        return sc.BuildServiceProvider().GetRequiredService<IMediator>();
    }

    private static global::MediatR.IMediator BuildMediatR()
    {
        var sc = new ServiceCollection()
            .AddLogging()
            .AddSingleton<global::MediatR.IRequestHandler<MediatRLoad, int>, MediatRLoadHandler>()
            .AddTransient<global::MediatR.IPipelineBehavior<MediatRLoad, int>, MediatRPass1>()
            .AddTransient<global::MediatR.IPipelineBehavior<MediatRLoad, int>, MediatRPass2>()
            .AddMediatR(cfg => cfg.RegisterServicesFromAssembly(typeof(LoadCheck).Assembly));
        return sc.BuildServiceProvider().GetRequiredService<global::MediatR.IMediator>();
    }

    private static void FullCollect()
    {
        GC.Collect(2, GCCollectionMode.Forced, blocking: true);
        GC.WaitForPendingFinalizers();
        GC.Collect(2, GCCollectionMode.Forced, blocking: true);
    }

    // ═══ Сценарий 1: масштабирование throughput по потокам ═══

    public static void Scaling()
    {
        Console.WriteLine($"== LOAD scaling: Send+2 middlewares, {ScalingDuration.TotalSeconds:F0}s на конфиг, Workstation GC ==\n");
        Console.WriteLine("Потоки |      MediatR ops/s |      Mediana ops/s |  Mediana×");
        Console.WriteLine("-------|--------------------|--------------------|--------");

        var mediatr = BuildMediatR();
        var mediana = BuildMediana();
        var mCmd = new MediatRLoad(1);
        var dCmd = (ICommand<int>)new LoadCmd(1);
        var csv = new List<string>();

        foreach (var threads in ThreadCounts)
        {
            // прогрев каждой стороны на этом числе потоков
            RunScalingPhase(mediana, dCmd, threads, TimeSpan.FromMilliseconds(300), static (m, c) => m.Send(c).Result);
            RunScalingPhase(mediatr, mCmd, threads, TimeSpan.FromMilliseconds(300), static (m, c) => m.Send(c).Result);
            FullCollect();

            var mediatrOps = RunScalingPhase(mediatr, mCmd, threads, ScalingDuration, static (m, c) => m.Send(c).Result);
            FullCollect();
            var medianaOps = RunScalingPhase(mediana, dCmd, threads, ScalingDuration, static (m, c) => m.Send(c).Result);
            FullCollect();

            var mRate = mediatrOps / ScalingDuration.TotalSeconds;
            var dRate = medianaOps / ScalingDuration.TotalSeconds;
            csv.Add($"{threads},{(long)mRate},{(long)dRate}");
            Console.WriteLine($"{threads,6} | {mRate,18:N0} | {dRate,18:N0} | {dRate / mRate,7:F1}x");
        }

        Console.WriteLine("\nCSV threads,mediatr_ops_s,mediana_ops_s:");
        foreach (var line in csv)
        {
            Console.WriteLine(line);
        }
    }

    private static long RunScalingPhase<TMediator, TCmd>(TMediator mediator, TCmd cmd, int threads, TimeSpan duration, Func<TMediator, TCmd, int> op)
    {
        var deadline = Stopwatch.GetTimestamp() + (long)(duration.TotalSeconds * Stopwatch.Frequency);
        var barrier = new Barrier(threads + 1);
        var counts = new long[threads];
        var workers = new Thread[threads];

        for (var t = 0; t < threads; t++)
        {
            var idx = t;
            workers[t] = new Thread(() =>
            {
                barrier.SignalAndWait();
                var local = 0L;
                while (Stopwatch.GetTimestamp() < deadline)
                {
                    op(mediator, cmd);
                    local++;
                }

                counts[idx] = local;
            });
            workers[t].Start();
        }

        barrier.SignalAndWait(); // одновременный старт
        foreach (var w in workers)
        {
            w.Join();
        }

        return counts.Sum();
    }

    // ═══ Сценарий 2: хвостовые латентности + GC-паузы ═══

    public static void Tails()
    {
        Console.WriteLine($"\n== LOAD tails: {TailTotalOps:N0} операций, пер-оп Stopwatch, Workstation GC ==\n");
        Console.WriteLine("Метрика        |      MediatR |      Mediana");
        Console.WriteLine("---------------|--------------|-------------");

        TailPhase("MediatR", BuildMediatR(), new MediatRLoad(1),
            static (m, c) => m.Send(c).Result,
            static (m, c, samples, i, freq) =>
            {
                var t0 = Stopwatch.GetTimestamp();
                _ = m.Send(c).Result;
                samples[i] = (Stopwatch.GetTimestamp() - t0) * 1_000_000_000 / freq;
            });

        TailPhase("Mediana", BuildMediana(), (ICommand<int>)new LoadCmd(1),
            static (m, c) => m.Send(c).Result,
            static (m, c, samples, i, freq) =>
            {
                var t0 = Stopwatch.GetTimestamp();
                _ = m.Send(c).Result;
                samples[i] = (Stopwatch.GetTimestamp() - t0) * 1_000_000_000 / freq;
            });
    }

    private static void TailPhase<TMediator, TCmd>(
        string label,
        TMediator mediator,
        TCmd cmd,
        Func<TMediator, TCmd, int> warmOp,
        Action<TMediator, TCmd, long[], int, long> timedOp)
    {
        var threads = Math.Min(Environment.ProcessorCount, 8);
        var perThread = TailTotalOps / threads;
        var samples = new long[TailTotalOps];
        var freq = Stopwatch.Frequency;

        // прогрев (JIT + tiering)
        for (var i = 0; i < 200_000; i++)
        {
            warmOp(mediator, cmd);
        }

        FullCollect();
        var g0 = GC.CollectionCount(0);
        var g1 = GC.CollectionCount(1);
        var g2 = GC.CollectionCount(2);
        var pauseBefore = GC.GetTotalPauseDuration();
        var allocBefore = GC.GetTotalAllocatedBytes(precise: true);
        var sw = Stopwatch.StartNew();

        var barrier = new Barrier(threads + 1);
        var workers = new Thread[threads];
        for (var t = 0; t < threads; t++)
        {
            var start = t * perThread;
            workers[t] = new Thread(() =>
            {
                barrier.SignalAndWait();
                for (var i = start; i < start + perThread; i++)
                {
                    timedOp(mediator, cmd, samples, i, freq);
                }
            });
            workers[t].Start();
        }

        barrier.SignalAndWait();
        foreach (var w in workers)
        {
            w.Join();
        }

        sw.Stop();
        var pauseMs = (GC.GetTotalPauseDuration() - pauseBefore).TotalMilliseconds;
        var alloc = GC.GetTotalAllocatedBytes(precise: true) - allocBefore;

        Array.Sort(samples);
        Console.WriteLine($"--- {label} ({threads} потоков, {sw.Elapsed.TotalSeconds:F1}s) ---");
        Console.WriteLine($"  ops/s         | {samples.Length / sw.Elapsed.TotalSeconds,12:N0}");
        Console.WriteLine($"  p50           | {Percentile(samples, 0.50),9:N0} ns");
        Console.WriteLine($"  p95           | {Percentile(samples, 0.95),9:N0} ns");
        Console.WriteLine($"  p99           | {Percentile(samples, 0.99),9:N0} ns");
        Console.WriteLine($"  p99.9         | {Percentile(samples, 0.999),9:N0} ns");
        Console.WriteLine($"  p99.99        | {Percentile(samples, 0.9999),9:N0} ns");
        Console.WriteLine($"  max           | {samples[^1],9:N0} ns");
        Console.WriteLine($"  Gen0/1/2      | {GC.CollectionCount(0) - g0}/{GC.CollectionCount(1) - g1}/{GC.CollectionCount(2) - g2}");
        Console.WriteLine($"  GC pause      | {pauseMs,9:F1} ms ({pauseMs / sw.Elapsed.TotalMilliseconds * 100,5:F2}% времени)");
        Console.WriteLine($"  Аллокировано  | {alloc / (double)samples.Length,9:F0} B/оп");

        FullCollect();
    }

    private static long Percentile(long[] sorted, double p)
    {
        var idx = (int)Math.Clamp(Math.Ceiling(p * sorted.Length), 1, sorted.Length) - 1;
        return sorted[idx];
    }
}
