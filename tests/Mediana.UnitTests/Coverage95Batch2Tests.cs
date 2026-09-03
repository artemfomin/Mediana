using Mediana.Dispatch;
using Mediana.Consuming;
using Mediana.Inbox;
using Mediana.Messaging;
using Mediana.Outbox;
using Mediana.Reliability;
using Mediana.Transports;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Mediana.UnitTests;

/// <summary>#2 95+: query-, , null-, ctor-, scan-.</summary>
public class Coverage95Batch2Tests
{
    private sealed record BQ(int V) : IQuery<int>;
    private sealed record BR(int V) : IQuery<BR>;

    private sealed class BQH : Handlers.IQueryHandler<BQ, int>
    {
        public ValueTask<int> Handle(BQ q, CancellationToken ct) => new(q.V + 1);
    }

    // ── Query: DI (Resolve throw-QueryCallSite) ──

    [Fact]
    public async Task Query_handler_missing_in_DI_throws()
    {
        var cfg = new MedianaConfiguration().AddQueryHandler<BQ, int, BQH>();
        var mediator = new Mediator(cfg.Freeze(), new ServiceCollection().BuildServiceProvider());

        var ex = await Assert.ThrowsAsync<MediatorConfigurationException>(
            () => mediator.Send((IQuery<int>)new BQ(1)).AsTask());
        Assert.Contains(typeof(BQH).ToString(), ex.Message);
    }

    // ── Mediator: query-entry callsite → false-`is`-(Send + SendExact) ──

    [Fact]
    public async Task Query_entry_without_callsite_send_and_sendexact_throw()
    {
        var registry = Mediana.Dispatch.MessageRegistry.Empty.Add(
            typeof(BQ), new Mediana.Dispatch.MessageEntry(HandlerKind.Query, typeof(BQ), typeof(int)));
        var mediator = new Mediator(registry, new ServiceCollection().BuildServiceProvider());

        await Assert.ThrowsAsync<MediatorConfigurationException>(
            () => mediator.Send((IQuery<int>)new BQ(1)).AsTask());
        await Assert.ThrowsAsync<MediatorConfigurationException>(
            () => mediator.SendExact<BQ, int>(new BQ(1)).AsTask());
    }

    // ── Serialization: ctor (false-?? Default) ──

    [Fact]
    public void Serializer_ctor_with_explicit_options()
    {
        var options = new System.Text.Json.JsonSerializerOptions();
        var serializer = new SystemTextJsonMessageSerializer(options);
        Assert.Same(options, typeof(SystemTextJsonMessageSerializer)
            .GetField("_options", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
            .GetValue(serializer));
        Assert.Equal("application/json", new SystemTextJsonMessageSerializer().ContentType);
    }

    // ── Outbox: EnvelopeCodec.Decode body → throw ──

    [Fact]
    public void Outbox_envelope_codec_decode_null_throws()
    {
        Assert.Throws<Mediana.Messaging.SerializationException>(
            () => Mediana.Messaging.EnvelopeCodec.Decode("null"u8.ToArray()));
    }

    // ── Outbox: relay (true-LogError) ──

    private sealed class ThrowingLeaseStore : IOutboxStore
    {
        public ValueTask AddRange(IEnumerable<OutboxMessage> m, CancellationToken ct) => default;
        public ValueTask<IReadOnlyList<OutboxMessage>> LeaseBatch(int b, long l, CancellationToken ct) => throw new InvalidOperationException("down");
        public ValueTask MarkDelivered(OutboxMessage m, CancellationToken ct) => default;
        public ValueTask MarkFailed(OutboxMessage m, string e, int maxAttempts, CancellationToken ct) => default;
        public ValueTask<int> CleanupOlderThan(TimeSpan a, CancellationToken ct) => new(0);
    }

    [Fact]
    public async Task Relay_with_logger_exercises_log_branch()
    {
        var relay = new OutboxRelay(
            new ThrowingLeaseStore(),
            _ => new ValueTask<ITransportPublisher>(new OutboxTestHelpers.NullPublisher()),
            options: new OutboxRelayOptions { PollInterval = TimeSpan.FromMilliseconds(5), FailureBackoff = TimeSpan.FromMilliseconds(5) },
            logger: NullLogger<OutboxRelay>.Instance);

        using var cts = new CancellationTokenSource(250);
        await relay.StartAsync(cts.Token);
        await Task.Delay(120);
        await relay.StopAsync(CancellationToken.None);
        relay.Dispose();
    }

    // ── ConsumerPipeline: duplicate- failure-──

    private sealed class RecDelivery : ITransportDelivery
    {
        public int Acks;
        public int Nacks;

        public Envelope Envelope { get; } = Envelope.Create("App.B2", "1", []);

        public ValueTask Ack()
        {
            Acks++;
            return default;
        }

        public ValueTask Nack(bool requeue, TimeSpan? delay)
        {
            Nacks++;
            return default;
        }
    }

    [Fact]
    public async Task Consumer_pipeline_with_logger_duplicate_and_failure()
    {
        var pipeline = new ConsumerPipeline(new InMemoryInboxStore(), NullLogger.Instance);

        // duplicate-
        var env = Envelope.Create("App.B2", "1", []);
        var first = new RecDelivery();
        // envelope delivery
        var dup = new DupDelivery(env);
        await pipeline.Process(first, "h", (_, _) => default);
        await pipeline.Process(dup, "h", (_, _) => default);
        Assert.Equal(1, dup.Acks);

        // failure-
        var failing = new RecDelivery();
        await pipeline.Process(failing, "h2", (_, _) => throw new TimeoutException(),
            new RetryPolicy { Strategy = BackoffStrategy.Fixed, BaseDelay = TimeSpan.FromMilliseconds(1), MaxAttempts = 2, Jitter = 0 });
        Assert.Equal(1, failing.Nacks);
    }

    private sealed class DupDelivery(Envelope envelope) : ITransportDelivery
    {
        public int Acks;

        public Envelope Envelope { get; } = envelope;

        public ValueTask Ack()
        {
            Acks++;
            return default;
        }

        public ValueTask Nack(bool requeue, TimeSpan? delay) => default;
    }

    // ── Retry: poison-──

    [Fact]
    public void Poison_detector_full_matrix()
    {
        Assert.True(PoisonDetector.IsPoison(new Mediana.Messaging.SerializationException("s")));
        Assert.True(PoisonDetector.IsPoison(new FormatException()));
        Assert.False(PoisonDetector.IsPoison(new InvalidOperationException()));
        Assert.False(PoisonDetector.IsPoison(new ArgumentException()));
        Assert.True(PoisonDetector.IsPoison(new MediatorConfigurationException("c")));
        Assert.False(PoisonDetector.IsPoison(new TimeoutException()));
        Assert.False(PoisonDetector.IsPoison(new OperationCanceledException()));
    }

    // ── Scan: generic-interface abstract-generic ──

    [Fact]
    public void Scan_covers_generic_interface_and_abstract_generic_combinations()
    {
        // ScanTargets: IGenScanHandler (generic interface), AbstractGenericScanHandler<T>
        var cfg = new MedianaConfiguration()
            .AddHandlersFromAssembly(typeof(ScanTargets.ScanMsg).Assembly);
        // CreateOrder —
        Assert.Throws<MediatorConfigurationException>(() => cfg.Freeze());

        var clean = new MedianaConfiguration()
            .AddHandlersFromAssembly(typeof(object).Assembly);
        Assert.Null(clean.Freeze().TryGet(typeof(string)));
    }
}
