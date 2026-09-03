using Mediana.Dispatch;
using Mediana.Messaging;
using Mediana.Pipeline;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Mediana.UnitTests;

/// <summary>
/// and in and and by by-
/// andnot : invocation generic- from canon-shared generic-
/// and ~24-32B/inin; byand non-generic and andfromandin
/// See English documentation.
/// </summary>
[Trait("Category", "Allocation")]
public class AllocationBisectTests
{
    internal sealed record Echo(int Value) : ICommand<int>;

    internal sealed class EchoHandler : Handlers.ICommandHandler<Echo, int>
    {
        public ValueTask<int> Handle(Echo command, CancellationToken ct) => new(command.Value * 2);
    }

    internal sealed class PassBehavior : IHandlerMiddleware<Echo, int>
    {
        public ValueTask<int> Handle(Echo request, HandlerDelegate<Echo, int> next, CancellationToken ct)
            => next(request, ct);
    }

    private static long Measure(Action action, int iterations)
    {
        action();
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < iterations; i++)
        {
            action();
        }

        return GC.GetAllocatedBytesForCurrentThread() - before;
    }

    [Fact]
    public void Value_response_paths_are_zero_alloc()
    {
        var sc = new ServiceCollection()
            .AddSingleton<EchoHandler>()
            .AddSingleton<PassBehavior>()
            .AddMediana(c => c.UseSingletonHandlers()
                .AddCommandHandler<Echo, int, EchoHandler>()
                .AddMiddleware<Echo, int, PassBehavior>());
        var sp = sc.BuildServiceProvider();
        var mediator = (Mediator)sp.GetRequiredService<IMediator>();
        var command = (ICommand<int>)new Echo(21);
        var raw = new Echo(1);

        var entry = mediator.Registry.TryGet(typeof(Echo))!;
        var iface = (IObjectCommandCallSite<int>)entry.CommandCallSite!;
        var concrete = (CommandCallSite<Echo, int, EchoHandler>)entry.CommandCallSite!;

        var n = 5000;
        // warmup
        _ = concrete.InvokeTyped(new Echo(1), sp, default);
        _ = concrete.InvokeTyped(new Echo(1), sp, default);

        var a = Measure(() => iface.Invoke(command, sp, default), n);
        var b = Measure(() => concrete.Invoke(command, sp, default), n);
        var c = Measure(() => concrete.InvokeTyped(raw, sp, default), n);
        var e = Measure(() => mediator.Send(command), n);

        Assert.Equal(0, a);
        Assert.Equal(0, b);
        Assert.Equal(0, c);
        Assert.Equal(0, e);
    }

    [Fact]
    public void Event_paths_are_zero_alloc()
    {
        var sc = new ServiceCollection()
            .AddSingleton<CountingHandler1>()
            .AddSingleton<CountingHandler2>()
            .AddMediana(c => c.UseSingletonHandlers()
                .AddEventHandler<CountedEvent, CountingHandler1>()
                .AddEventHandler<CountedEvent, CountingHandler2>());
        var sp = sc.BuildServiceProvider();
        var mediator = (Mediator)sp.GetRequiredService<IMediator>();
        var evt = new CountedEvent();
        var entry = mediator.Registry.TryGet(typeof(CountedEvent))!;
        var sites = entry.EventCallSites;

        var n = 5000;
        var f = Measure(() => sites[0].Invoke(evt, sp, default), n);
        var g = Measure(() => mediator.Publish(evt), n);

        Assert.Equal(0, f);
        Assert.Equal(0, g);
    }

    /// <summary>
    /// andin canon-on: object- Send reference-frominthen from generic- —
    /// to 1 andand (~32). Value-fromin, and and typed- — but ( in)
    /// notthen (Mediana.Generators) on but
    /// </summary>
    [Fact]
    public void Ref_response_send_documented_canon_tax()
    {
        var sc = new ServiceCollection()
            .AddSingleton<RefHandler>()
            .AddMediana(c => c.UseSingletonHandlers()
                .AddCommandHandler<RefCmd, RefResp, RefHandler>());
        var sp = sc.BuildServiceProvider();
        var mediator = (Mediator)sp.GetRequiredService<IMediator>();
        var command = (ICommand<RefResp>)new RefCmd();

        var n = 5000;
        var h = Measure(() => mediator.Send(command), n);

        Assert.True(h <= n * 40L, $"Ref-response object-path allocated {h / (double)n:F2}/call, documented budget 40");
    }

    internal sealed record RefCmd() : ICommand<RefResp>;
    internal sealed record RefResp();

    internal sealed class RefHandler : Handlers.ICommandHandler<RefCmd, RefResp>
    {
        private readonly RefResp _cached = new();

        public ValueTask<RefResp> Handle(RefCmd command, CancellationToken ct) => new(_cached);
    }
}
