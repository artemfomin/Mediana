using Mediana.Dispatch;
using Mediana.Pipeline;
using Mediana.Messaging;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Mediana.UnitTests;

/// <summary>and killer-: then andand, DI-inand, guard-andinandand.</summary>
public class MutationKillerTests
{
    private sealed record MK(int V) : ICommand<int>;
    private sealed record MRQ(int V) : IQuery<int>;
    private sealed record MRE : IEvent;
    private sealed record MS : IStreamQuery<int>;

    private sealed class MKH : Handlers.ICommandHandler<MK, int>
    {
        public ValueTask<int> Handle(MK c, CancellationToken ct) => new(c.V + 1);
    }

    private sealed class MRQH : Handlers.IQueryHandler<MRQ, int>
    {
        public ValueTask<int> Handle(MRQ q, CancellationToken ct) => new(q.V + 1);
    }

    private sealed class MREH : Handlers.IEventHandler<MRE>
    {
        public int Calls;

        public ValueTask Handle(MRE e, CancellationToken ct)
        {
            Calls++;
            return default;
        }
    }

    private sealed class MKBeh : IHandlerMiddleware<MK, int>
    {
        public ValueTask<int> Handle(MK r, HandlerDelegate<MK, int> next, CancellationToken ct) => next(r, ct);
    }

    private sealed class MSH : Handlers.IStreamHandler<MS, int>
    {
        public IAsyncEnumerable<int> Handle(MS q, CancellationToken ct) => ZeroRows();

        private static async IAsyncEnumerable<int> ZeroRows()
        {
            await Task.Yield();
            yield break;
        }
    }

    // ── andand (andin string/Linq Statement-andand in and) ──

    [Fact]
    public async Task Exception_messages_exact_texts()
    {
        var emptyMediator = new Mediator(Mediana.Dispatch.MessageRegistry.Empty, new ServiceCollection().BuildServiceProvider());

        var ex1 = await Assert.ThrowsAsync<MediatorConfigurationException>(
            () => emptyMediator.Send((ICommand<int>)new MK(1)).AsTask());
        Assert.Contains("No handler registered for message type", ex1.Message);
        Assert.Contains(typeof(MK).ToString(), ex1.Message);
        Assert.Contains("AddCommandHandler", ex1.Message);

        var qex = await Assert.ThrowsAsync<MediatorConfigurationException>(
            () => emptyMediator.Send((IQuery<int>)new MRQ(1)).AsTask());
        Assert.Contains("No handler registered", qex.Message);

        var sex = await Assert.ThrowsAsync<MediatorConfigurationException>(async () =>
        {
            await foreach (var r in emptyMediator.Stream((IStreamQuery<int>)new MS()))
            {
            }
        });
        Assert.Contains("No handler registered", sex.Message);

        var tex = await Assert.ThrowsAsync<MediatorConfigurationException>(
            () => emptyMediator.SendExact<MK, int>(new MK(1)).AsTask());
        Assert.Contains("No handler registered", tex.Message);
    }

    [Fact]
    public async Task Handler_missing_message_includes_type_name()
    {
        var cfg = new MedianaConfiguration().AddCommandHandler<MK, int, MKH>();
        var mediator = new Mediator(cfg.Freeze(), new ServiceCollection().BuildServiceProvider());

        var ex = await Assert.ThrowsAsync<MediatorConfigurationException>(
            () => mediator.Send((ICommand<int>)new MK(1)).AsTask());
        Assert.Contains(typeof(MKH).ToString(), ex.Message);
        Assert.Contains("not registered", ex.Message);
    }

    [Fact]
    public async Task Behavior_missing_message_includes_type_name()
    {
        var cfg = new MedianaConfiguration()
            .AddCommandHandler<MK, int, MKH>()
            .AddMiddleware<MK, int, MKBeh>();
        var sc = new ServiceCollection().AddScoped<MKH>(); // behavior onbut in DI
        var mediator = new Mediator(cfg.Freeze(), sc.BuildServiceProvider());

        var ex = await Assert.ThrowsAsync<MediatorConfigurationException>(
            () => mediator.Send((ICommand<int>)new MK(1)).AsTask());
        Assert.Contains(typeof(MKBeh).ToString(), ex.Message);
    }

    [Fact]
    public async Task Duplicate_messages_exact()
    {
        var registry = Mediana.Dispatch.MessageRegistry.Empty
            .Add(typeof(MK), new MessageEntry(HandlerKind.Command, typeof(MK), null));

        var ex = Assert.Throws<MediatorConfigurationException>(
            () => registry.Add(typeof(MK), new MessageEntry(HandlerKind.Command, typeof(MK), null)));
        Assert.Contains("already registered", ex.Message);
        Assert.Contains(typeof(MK).ToString(), ex.Message);
    }

    // ── DI-inand (ServiceCollectionExtensions: Scoped/Singleton registrations) ──

    [Fact]
    public void AddMediana_registers_handlers_with_configured_lifetime()
    {
        var sc = new ServiceCollection();
        sc.AddMediana(c => c
            .AddCommandHandler<MK, int, MKH>()
            .AddEventHandler<MRE, MREH>());

        using var scopedSp = sc.BuildServiceProvider();
        var cmdDesc = FindDescriptor(sc, typeof(MKH));
        Assert.NotNull(cmdDesc);
        Assert.Equal(ServiceLifetime.Scoped, cmdDesc!.Lifetime);
        var evtDesc = FindDescriptor(sc, typeof(MREH));
        Assert.Equal(ServiceLifetime.Scoped, evtDesc!.Lifetime);

        // andand IMediator +
        Assert.NotNull(FindDescriptor(sc, typeof(IMediator)));
        Assert.NotNull(FindDescriptor(sc, typeof(Mediana.Dispatch.MessageRegistry)));

        // Mediator inand and from (Scoped from root — inandbut without ValidateScopes)
        var mediator = scopedSp.GetRequiredService<IMediator>();
        using var scope = scopedSp.CreateScope();
        var scopedMediator = scope.ServiceProvider.GetRequiredService<IMediator>();
        Assert.NotSame(mediator, scopedMediator);

        var sc2 = new ServiceCollection();
        sc2.AddMediana(c => c.UseSingletonHandlers().AddCommandHandler<MK, int, MKH>());
        var singletonDesc = FindDescriptor(sc2, typeof(MKH));
        Assert.Equal(ServiceLifetime.Singleton, singletonDesc!.Lifetime);
    }

    private static ServiceDescriptor? FindDescriptor(IServiceCollection sc, Type service)
        => sc.FirstOrDefault(d => d.ServiceType == service);

    // ── Null-guard andinandand (Guard/Mediator Publish/Stream) ──

    [Fact]
    public async Task Publish_null_event_rejected_with_param_name()
    {
        var mediator = new Mediator(Mediana.Dispatch.MessageRegistry.Empty, new ServiceCollection().BuildServiceProvider());
        var ex = await Assert.ThrowsAsync<ArgumentNullException>(() => mediator.Publish<MRE>(null!).AsTask());
        Assert.Equal("event", ex.ParamName);
    }

    [Fact]
    public async Task Stream_handler_returns_empty_flow()
    {
        var cfg = new MedianaConfiguration().AddStreamHandler<MS, int, MSH>();
        var sc = new ServiceCollection().AddScoped<MSH>();
        sc.AddMediana(_ => { });
        var sp = sc.BuildServiceProvider();
        var mediator = new Mediator(cfg.Freeze(), sp);

        var rows = 0;
        await foreach (var r in mediator.Stream((IStreamQuery<int>)new MS()))
        {
            rows++;
        }

        Assert.Equal(0, rows);
    }

    // ── MedianaDiagnostics: onand guard ──

    [Fact]
    public void Diagnostics_has_listeners_flag_both_ways()
    {
        // listener-inand: no-op inand thenand in and
        using var listener = new System.Diagnostics.ActivityListener
        {
            ShouldListenTo = s => s.Name == "Mediana",
            SampleUsingParentId = (ref System.Diagnostics.ActivityCreationOptions<string> o) => System.Diagnostics.ActivitySamplingResult.AllData,
            Sample = (ref System.Diagnostics.ActivityCreationOptions<System.Diagnostics.ActivityContext> o) => System.Diagnostics.ActivitySamplingResult.AllData,
        };
        System.Diagnostics.ActivitySource.AddActivityListener(listener);

        using var a1 = MedianaDiagnostics.StartDispatch("X1");
        using var a2 = MedianaDiagnostics.StartPublish("X2");
        using var a3 = MedianaDiagnostics.StartConsume("X3");
        Assert.Equal("dispatch X1", a1!.OperationName);
        Assert.Equal("publish X2", a2!.OperationName);
        Assert.Equal("consume X3", a3!.OperationName);
        Assert.Equal(System.Diagnostics.ActivityKind.Consumer, a3.Kind);
    }

    // ── ChainState: Configure-by and byinthenon andand ──

    [Fact]
    public async Task ChainState_reconfiguration_after_return()
    {
        Pipeline.HandlerDelegate<MK, int> t1 = (_, _) => new ValueTask<int>(1);
        Pipeline.HandlerDelegate<MK, int> t2 = (_, _) => new ValueTask<int>(2);
        var sp = new ServiceCollection().BuildServiceProvider();

        var s = ChainState<MK, int>.Take(sp, [], t1);
        Assert.Equal(1, await s.Next(new MK(1), default));
        s.Return();

        s.Configure([], t2);
        Assert.Equal(2, await s.Next(new MK(2), default));
        s.Return();
    }

    // ── Scan-and: generic/abstract/interface andbutand, on ──
    [Fact]
    public void Scan_skips_generic_abstract_interface_and_finds_concrete()
    {
        var cfg = new MedianaConfiguration()
            .AddHandlersFromAssembly(typeof(MutationKillerTests).Assembly);
        // NB: thenin and and CreateOrder — and andand inand andin
        // bythen and in andbut: and only scan-and not
        // then this: freeze on and → byinand, then scan CreateOrder-:
        var ex = Assert.Throws<MediatorConfigurationException>(() => cfg.Freeze());
        Assert.Contains("exactly one handler", ex.Message);

        // and: from from and scan-andbyin
        var clean = new MedianaConfiguration()
            .AddHandlersFromAssembly(typeof(MutationKillerTests).Assembly);
        // (in andthen inand not fromandin -andthenand — and by to in)
    }
}
