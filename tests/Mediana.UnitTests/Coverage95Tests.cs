using Mediana.Consuming;
using Mediana.Dispatch;
using Mediana.Messaging;
using Mediana.Outbox;
using Mediana.Reliability;
using Mediana.Transports;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Mediana.UnitTests;

/// <summary>Добор до 95%+ branch: точечное закрытие непокрытых веток всех пакетов ядра.</summary>
public class Coverage95Tests
{
    // ═══ Mediana: Serialization — нетипизированная перегрузка ═══

    [Fact]
    public void Serializer_deserialize_by_type_roundtrip_and_null()
    {
        var serializer = SystemTextJsonMessageSerializer.Instance;
        var payload = serializer.Serialize(new SerPayload(7, "x"));

        var back = (SerPayload)serializer.Deserialize(payload, typeof(SerPayload))!;
        Assert.Equal(7, back.A);
        Assert.Equal("x", back.B);

        var nullJson = serializer.Serialize((SerPayload?)null);
        Assert.Throws<SerializationException>(
            () => serializer.Deserialize(nullJson, typeof(SerPayload)));

        var inner = new FormatException("inner");
        var sex = new SerializationException("msg", inner);
        Assert.Same(inner, sex.InnerException);
        Assert.Equal("msg", sex.Message);
    }

    private sealed record SerPayload(int A, string B);

    // ═══ Mediana: StreamCallSite — middleware не в DI ═══

    private sealed record CS(int V) : IStreamQuery<int>;

    private sealed class CSH : Handlers.IStreamHandler<CS, int>
    {
        public IAsyncEnumerable<int> Handle(CS q, CancellationToken ct) => Empty();

        private static async IAsyncEnumerable<int> Empty()
        {
            await Task.Yield();
            yield break;
        }
    }

    private sealed class CSMw : Pipeline.IStreamMiddleware<CS, int>
    {
        public IAsyncEnumerable<int> Handle(CS q, Pipeline.StreamHandlerDelegate<CS, int> next, CancellationToken ct)
            => next(q, ct);
    }

    [Fact]
    public async Task Stream_middleware_not_in_DI_throws()
    {
        var cfg = new MedianaConfiguration()
            .AddStreamHandler<CS, int, CSH>()
            .AddStreamMiddleware<CS, int, CSMw>();
        var sc = new ServiceCollection().AddSingleton<CSH>(); // middleware намеренно не в DI
        var mediator = new Mediator(cfg.Freeze(), sc.BuildServiceProvider());

        await Assert.ThrowsAsync<MediatorConfigurationException>(async () =>
        {
            await foreach (var r in mediator.Stream((IStreamQuery<int>)new CS(1)))
            {
            }
        });
    }

    // ═══ Mediana: Compositor — middleware отсутствует в singleton-режиме ═══

    private sealed record CC(int V) : ICommand<int>;

    private sealed class CCH : Handlers.ICommandHandler<CC, int>
    {
        public ValueTask<int> Handle(CC c, CancellationToken ct) => new(c.V + 1);
    }

    private sealed class CCMw : Pipeline.IHandlerMiddleware<CC, int>
    {
        public ValueTask<int> Handle(CC r, Pipeline.HandlerDelegate<CC, int> next, CancellationToken ct) => next(r, ct);
    }

    [Fact]
    public async Task Compositor_missing_middleware_singleton_throws()
    {
        var cfg = new MedianaConfiguration().UseSingletonHandlers()
            .AddCommandHandler<CC, int, CCH>()
            .AddMiddleware<CC, int, CCMw>();
        var sc = new ServiceCollection().AddSingleton<CCH>();
        var mediator = new Mediator(cfg.Freeze(), sc.BuildServiceProvider());

        var ex = await Assert.ThrowsAsync<MediatorConfigurationException>(
            () => mediator.Send((ICommand<int>)new CC(1)).AsTask());
        Assert.Contains(typeof(CCMw).ToString(), ex.Message);
    }

    // ═══ Mediana: EventCallSite — singleton-режим, отсутствующий хендлер/middleware, холодный GetRoot, double-check ═══

    private sealed record CE : IEvent;

    private sealed class CEH : Handlers.IEventHandler<CE>
    {
        public ValueTask Handle(CE e, CancellationToken ct) => default;
    }

    private sealed class CEMw : Pipeline.IEventMiddleware<CE>
    {
        public ValueTask Handle(CE e, Pipeline.EventHandlerDelegate<CE> next, CancellationToken ct) => next(e, ct);
    }

    [Fact]
    public async Task Event_singleton_missing_handler_throws()
    {
        var cfg = new MedianaConfiguration().UseSingletonHandlers().AddEventHandler<CE, CEH>();
        var mediator = new Mediator(cfg.Freeze(), new ServiceCollection().BuildServiceProvider());

        var ex = await Assert.ThrowsAsync<MediatorConfigurationException>(
            () => mediator.Publish(new CE()).AsTask());
        Assert.Contains(typeof(CEH).ToString(), ex.Message);
    }

    [Fact]
    public async Task Event_singleton_missing_middleware_throws()
    {
        var cfg = new MedianaConfiguration().UseSingletonHandlers()
            .AddEventHandler<CE, CEH>()
            .AddEventMiddleware<CE, CEMw>();
        var sc = new ServiceCollection().AddSingleton<CEH>(); // middleware не в DI
        var mediator = new Mediator(cfg.Freeze(), sc.BuildServiceProvider());

        var ex = await Assert.ThrowsAsync<MediatorConfigurationException>(
            () => mediator.Publish(new CE()).AsTask());
        Assert.Contains(typeof(CEMw).ToString(), ex.Message);
    }

    [Fact]
    public async Task Event_getRoot_cold_then_build_twice()
    {
        var sc = new ServiceCollection().AddSingleton<CEH>().AddSingleton<CEMw>();
        var sp = sc.BuildServiceProvider();
        var cfg = new MedianaConfiguration().UseSingletonHandlers()
            .AddEventHandler<CE, CEH>()
            .AddEventMiddleware<CE, CEMw>();
        var registry = cfg.Freeze();
        var site = (EventCallSite<CE, CEH>)registry.TryGet(typeof(CE))!.EventCallSites[0];

        // холодный GetRoot: BuildSingletonRoot строит цепочку
        var cold = site.GetRoot(sp);
        // повторный BuildSingletonRoot: double-check внутри lock возвращает готовый корень
        var again = site.BuildSingletonRoot(sp);
        Assert.Same(cold, again);
        await cold(new CE(), default);
        // повторный вызов по построенному корню
        await site.GetRoot(sp)(new CE(), default);
    }

    // ═══ Mediana: Diagnostics — детерминированный обход всех веток в одном тесте ═══

    [Fact]
    public void Diagnostics_all_branches_deterministic()
    {
        // без слушателей: все Start* → null (false-ветки)
        Assert.Null(MedianaDiagnostics.StartDispatch("d1"));
        Assert.Null(MedianaDiagnostics.StartPublish("p1"));
        Assert.Null(MedianaDiagnostics.StartConsume("c1"));
        MedianaDiagnostics.Enrich(null, "k", "v"); // null-толерантность

        using var listener = new System.Diagnostics.ActivityListener
        {
            ShouldListenTo = s => s.Name == "Mediana",
            SampleUsingParentId = (ref System.Diagnostics.ActivityCreationOptions<string> o) => System.Diagnostics.ActivitySamplingResult.AllData,
            Sample = (ref System.Diagnostics.ActivityCreationOptions<System.Diagnostics.ActivityContext> o) => System.Diagnostics.ActivitySamplingResult.AllData,
        };
        System.Diagnostics.ActivitySource.AddActivityListener(listener);

        // со слушателем: все Start* → activity (true-ветки)
        using var d = MedianaDiagnostics.StartDispatch("d2");
        using var p = MedianaDiagnostics.StartPublish("p2");
        using var c = MedianaDiagnostics.StartConsume("c2");
        Assert.NotNull(d);
        Assert.NotNull(p);
        Assert.NotNull(c);

        // Enrich с реальной активностью (true-ветка SetTag)
        MedianaDiagnostics.Enrich(d, "messaging.message.id", "42");
        Assert.Equal("42", d!.GetTagItem("messaging.message.id"));
    }

    // ═══ Mediana: Mediator — command value-mismatch (false-ветка typed-каста) ═══

    [Fact]
    public async Task Send_command_value_response_mismatch_throws()
    {
        var cfg = new MedianaConfiguration().UseSingletonHandlers().AddCommandHandler<CC, int, CCH>();
        var sc = new ServiceCollection().AddSingleton<CCH>();
        var mediator = new Mediator(cfg.Freeze(), sc.BuildServiceProvider());

        var mismatch = System.Runtime.CompilerServices.Unsafe.As<ICommand<string>>(new CC(1));
        await Assert.ThrowsAsync<MediatorConfigurationException>(
            () => mediator.Send<string>(mismatch).AsTask());
    }

    // ═══ Mediana: MedianaConfiguration — default-arm через internal AddHandler(Event) ═══

    [Fact]
    public void Freeze_unknown_request_kind_throws_via_internal_add()
    {
        var cfg = new MedianaConfiguration();
        cfg.AddHandler(HandlerKind.Event, typeof(CE), typeof(CEH)); // Event на request-пути — недопустимо
        Assert.Throws<InvalidOperationException>(() => cfg.Freeze());
    }

    [Fact]
    public void Event_collect_filters_out_request_middlewares()
    {
        // command-middleware не должен попасть в event-цепочку (false-ветка фильтра)
        var cfg = new MedianaConfiguration()
            .AddEventHandler<CE, CEH>()
            .AddMiddleware<CC, int, CCMw>();
        var registry = cfg.Freeze();
        var entry = registry.TryGet(typeof(CE))!;
        Assert.Single(entry.EventCallSites);
        Assert.Equal(EventDispatchPolicy.Sequential, entry.Policy);
    }

    // ═══ Transport.Abstractions: RetryEngine — смешанные jitter-комбинации, invalid strategy, poison MediatR-cfg ═══

    [Fact]
    public void DelayFor_mixed_jitter_random_combinations()
    {
        var jitterNoRandom = new RetryPolicy { Strategy = BackoffStrategy.Fixed, BaseDelay = TimeSpan.FromSeconds(10), MaxDelay = TimeSpan.FromMinutes(1), Jitter = 1.0 };
        Assert.Equal(TimeSpan.FromSeconds(10), jitterNoRandom.DelayFor(1)); // random==null → без jitter

        var noJitterWithRandom = new RetryPolicy { Strategy = BackoffStrategy.Fixed, BaseDelay = TimeSpan.FromSeconds(10), MaxDelay = TimeSpan.FromMinutes(1), Jitter = 0 };
        Assert.Equal(TimeSpan.FromSeconds(10), noJitterWithRandom.DelayFor(1, new Random(1)));

        var invalid = (BackoffStrategy)999;
        var invalidPolicy = new RetryPolicy { Strategy = invalid, BaseDelay = TimeSpan.FromSeconds(10), MaxDelay = TimeSpan.FromMinutes(1), Jitter = 0 };
        Assert.Equal(TimeSpan.FromSeconds(10), invalidPolicy.DelayFor(1)); // default-arm

        Assert.True(PoisonDetector.IsPoison(new MediatorConfigurationException("x")));
        Assert.False(PoisonDetector.IsPoison(new Mediana.Messaging.SerializationException("x").InnerException!)); // null-safe
    }

    [Fact]
    public async Task RetryEngine_cancelled_token_exception_propagates_unfiltered()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        // filter-false: ct запрошен → исключение покидает движок как есть
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            RetryEngine.Execute(
                (_, _) => throw new InvalidOperationException("boom"),
                new RetryPolicy { Strategy = BackoffStrategy.Fixed, BaseDelay = TimeSpan.FromMilliseconds(1), MaxAttempts = 5, Jitter = 0 },
                cancellationToken: cts.Token).AsTask());
    }

    // ═══ Transport.Abstractions: ConsumerHostService (ExecuteAsync) + records + TransportException ═══

    private sealed class FakeHost : IConsumerHost
    {
        public int Starts;
        public int Stops;

        public Task Start()
        {
            Starts++;
            return Task.CompletedTask;
        }

        public Task Stop()
        {
            Stops++;
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync() => default;
    }

    [Fact]
    public async Task ConsumerHostService_starts_host_and_stops_on_cancellation()
    {
        var host = new FakeHost();
        var service = new ConsumerHostService(host);

        using var cts = new CancellationTokenSource(100);
        await service.StartAsync(cts.Token);
        await Task.Delay(200); // ExecuteAsync: Start → delay → OCE-catch → Stop

        Assert.Equal(1, host.Starts);
        Assert.Equal(1, host.Stops);
        await service.StopAsync(CancellationToken.None);
    }

    [Fact]
    public void Transport_records_and_exception_surface()
    {
        var caps = new TransportCapabilities { Name = "x", SupportsRequestReply = true, SupportsStreaming = false };
        Assert.Equal("x", caps.Name);
        Assert.True(caps.SupportsRequestReply);
        Assert.False(caps.SupportsStreaming);

        var endpoint = new ConsumerEndpoint { Name = "q", MaxConcurrency = 3, MessageTypes = ["t"] };
        Assert.Equal("q", endpoint.Name);
        Assert.Equal(3, endpoint.MaxConcurrency);

        var manifest = new TopologyManifest { Transport = "rabbit", Endpoints = [endpoint], PublishDestinations = ["d"] };
        Assert.Equal("rabbit", manifest.Transport);
        Assert.Single(manifest.Endpoints);
        Assert.Single(manifest.PublishDestinations);

        var options = PublishOptions.Default;
        Assert.False(options.ConfirmDelivery);

        var inner = new FormatException("i");
        var te = new TransportException("msg", inner);
        Assert.Same(inner, te.InnerException);
        Assert.Equal("msg", te.Message);
    }

    // ═══ Transport.Abstractions: Envelope — PartitionKey null-init ═══

    [Fact]
    public void Envelope_partition_key_null_init_leaves_headers()
    {
        var e = new Envelope
        {
            MessageId = GuidV7.NewGuid(),
            MessageType = new MessageTypeDescriptor { FullName = "X", TypeVersion = "1" },
            Timestamp = DateTimeOffset.UtcNow,
            PartitionKey = null,
        };
        Assert.Null(e.PartitionKey);
        Assert.Empty(e.Headers);
    }

    // ═══ Outbox: relay без опций (default-ветка ctor) + внешний catch LeaseBatch-сбой ═══

    private sealed class ThrowingOutboxStore : IOutboxStore
    {
        public ValueTask AddRange(IEnumerable<OutboxMessage> m, CancellationToken ct) => default;
        public ValueTask<IReadOnlyList<OutboxMessage>> LeaseBatch(int b, long l, CancellationToken ct) => throw new InvalidOperationException("store down");
        public ValueTask MarkDelivered(OutboxMessage m, CancellationToken ct) => default;
        public ValueTask MarkFailed(OutboxMessage m, string e, CancellationToken ct) => default;
        public ValueTask<int> CleanupOlderThan(TimeSpan a, CancellationToken ct) => new(0);
    }

    [Fact]
    public async Task Relay_without_options_and_store_failure_backs_off()
    {
        var relay = new OutboxRelay(new ThrowingOutboxStore(), _ => new ValueTask<ITransportPublisher>(new OutboxTestHelpers.NullPublisher()));
        // ctor без options: default-ветка; хранилище падает в LeaseBatch → внешний catch + backoff

        using var cts = new CancellationTokenSource(300);
        await relay.StartAsync(cts.Token);
        await Task.Delay(150); // несколько циклов с ошибками и backoff
        await relay.StopAsync(CancellationToken.None);
        relay.Dispose();
    }
}
