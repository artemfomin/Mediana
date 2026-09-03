using Mediana.Consuming;
using Mediana.Messaging;
using Mediana.Inbox;
using Mediana.Reliability;
using Mediana.Routing;
using Mediana.Transports;
using Xunit;

namespace Mediana.UnitTests;

public class EnvelopeTests
{
    [Fact]
    public void Create_fills_required_fields()
    {
        var envelope = Envelope.Create("App.OrderCreated", "1.0", [1, 2, 3], "orders-api");

        Assert.Equal(EnvelopeVersion.Current, envelope.Version);
        Assert.NotEqual(Guid.Empty, envelope.MessageId);
        Assert.Equal("App.OrderCreated", envelope.MessageType.FullName);
        Assert.Equal("1.0", envelope.MessageType.TypeVersion);
        Assert.Equal([1, 2, 3], envelope.Payload);
        Assert.Equal("orders-api", envelope.SourceEndpoint);
    }

    [Fact]
    public void PartitionKey_stored_in_headers()
    {
        var envelope = new Envelope
        {
            MessageId = GuidV7.NewGuid(),
            MessageType = new MessageTypeDescriptor { FullName = "X", TypeVersion = "1" },
            Timestamp = DateTimeOffset.UtcNow,
            PartitionKey = "order-42",
        };

        Assert.Equal("order-42", envelope.PartitionKey);
        Assert.Equal("order-42", envelope.Headers["mediana.partition-key"]);
    }

    [Fact]
    public void Serializer_roundtrip()
    {
        var serializer = SystemTextJsonMessageSerializer.Instance;
        var envelope = Envelope.Create("App.E", "1", serializer.Serialize(new Payload(7, "x")));

        var json = serializer.Serialize(envelope);
        var back = serializer.Deserialize<Envelope>(json);

        Assert.Equal(envelope.MessageId, back.MessageId);
        Assert.Equal(envelope.MessageType.FullName, back.MessageType.FullName);
        Assert.Equal(envelope.Payload, back.Payload);
    }

    private sealed record Payload(int A, string B);

    [Fact]
    public void Serializer_throws_on_null_deserialize()
    {
        var serializer = SystemTextJsonMessageSerializer.Instance;
        var json = serializer.Serialize((Payload?)null);

        Assert.Throws<SerializationException>(
            () => serializer.Deserialize<Payload?>(json));
    }
}

public class GuidV7Tests
{
    private static long DecodeTimestamp(ReadOnlySpan<byte> b)
    {
        // Guid-layout: data1 (LE), data2 (LE) → RFC-
        return ((long)b[3] << 40) | ((long)b[2] << 32) | ((long)b[1] << 24) | ((long)b[0] << 16) | ((long)b[5] << 8) | b[4];
    }

    [Fact]
    public void Version_bits_set_to_7()
    {
        Span<byte> b = stackalloc byte[16];
        for (var i = 0; i < 100; i++)
        {
            var guid = GuidV7.NewGuid();
            guid.TryWriteBytes(b);
            Assert.Equal(7, b[7] >> 4);
            // 10xx
            Assert.Equal(0b10, b[8] >> 6);
        }
    }

    [Fact]
    public void Roughly_monotonic_timestamps()
    {
        var before = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var first = GuidV7.NewGuid();
        var last = GuidV7.NewGuid();
        var after = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        Span<byte> fb = stackalloc byte[16];
        first.TryWriteBytes(fb);
        Span<byte> lb = stackalloc byte[16];
        last.TryWriteBytes(lb);

        var ts1 = DecodeTimestamp(fb);
        var ts2 = DecodeTimestamp(lb);

        Assert.InRange(ts1, before - 5, after + 5);
        Assert.InRange(ts2, before - 5, after + 5);
        Assert.True(ts1 <= ts2);
    }
}

public class RoutingTests
{
    private sealed record LocalMsg : IRequest;
    private sealed record RoutedMsg : IRequest;

    [Remote("orders", Transport = "rabbit")]
    private sealed record AttributedMsg : IRequest;

    [Fact]
    public void Default_is_local()
    {
        var registry = new RouteRegistry();
        var policy = registry.Resolve(typeof(LocalMsg));

        Assert.Equal(RouteTarget.Local, policy.Target);
        Assert.Equal(DeliveryMode.Direct, policy.Delivery);
    }

    [Fact]
    public void Fluent_registration_wins_over_attribute()
    {
        var registry = new RouteRegistry()
            .Set<AttributedMsg>(RoutePolicy.ToQueue("kafka", "k-orders"));
        var policy = registry.Resolve(typeof(AttributedMsg));

        Assert.Equal(RouteTarget.Remote, policy.Target);
        Assert.Equal("kafka", policy.Transport);
        Assert.Equal("k-orders", policy.Destination);
    }

    [Fact]
    public void Attribute_resolves_when_no_fluent()
    {
        var registry = new RouteRegistry();
        var policy = registry.Resolve(typeof(AttributedMsg));

        Assert.Equal(RouteTarget.Remote, policy.Target);
        Assert.Equal("rabbit", policy.Transport);
        Assert.Equal("orders", policy.Destination);
        Assert.Equal(TimeSpan.FromSeconds(30), policy.RequestTimeout);
    }

    [Fact]
    public void FanOut_pattern_policy()
    {
        var registry = new RouteRegistry().Set<RoutedMsg>(RoutePolicy.FanOut("rabbit", "order.{type}"));
        var policy = registry.Resolve(typeof(RoutedMsg));

        Assert.Equal("order.{type}", policy.TopicPattern);
        Assert.True(registry.Resolve(typeof(RoutedMsg)).Delivery == DeliveryMode.Direct);
    }
}

public class InMemoryInboxTests
{
    [Fact]
    public async Task First_delivery_wins_second_skipped()
    {
        var inbox = new InMemoryInboxStore();

        Assert.True(await inbox.TryBegin("m1", "h1"));
        Assert.False(await inbox.TryBegin("m1", "h1"));
        // See English documentation.
        Assert.True(await inbox.TryBegin("m1", "h2"));
        await inbox.Complete("m1", "h1");
    }

    [Fact]
    public async Task Concurrent_TryBegin_single_winner()
    {
        var inbox = new InMemoryInboxStore(capacity: 10);
        var results = new bool[16];

        await Task.Run(() =>
        {
            Parallel.For(0, 16, i => results[i] = inbox.TryBegin("same", "h").AsTask().Result);
        });

        Assert.Single(results, x => x);
    }

    [Fact]
    public async Task Eviction_keeps_capacity()
    {
        var inbox = new InMemoryInboxStore(capacity: 4);

        for (var i = 0; i < 10; i++)
        {
            Assert.True(await inbox.TryBegin("m" + i, "h"));
        }

        // See English documentation.
        Assert.True(await inbox.TryBegin("m0", "h"));
        // See English documentation.
        Assert.False(await inbox.TryBegin("m9", "h"));
    }
}

public class RetryEngineTests
{
    private static RetryPolicy FastPolicy(int attempts = 3) => new()
    {
        Strategy = BackoffStrategy.Fixed,
        BaseDelay = TimeSpan.FromMilliseconds(1),
        MaxAttempts = attempts,
        Jitter = 0,
    };

    [Fact]
    public async Task Succeeds_without_retry()
    {
        var calls = 0;
        var outcome = await RetryEngine.Execute((_, _) => { calls++; return default; }, FastPolicy());

        Assert.Equal(RetryOutcome.Succeeded, outcome);
        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task Retries_transient_failure_then_succeeds()
    {
        var calls = 0;
        var outcome = await RetryEngine.Execute(
            (_, _) =>
            {
                calls++;
                if (calls < 3)
                {
                    throw new TransientException();
                }

                return default;
            },
            FastPolicy());

        Assert.Equal(RetryOutcome.Succeeded, outcome);
        Assert.Equal(3, calls);
    }

    [Fact]
    public async Task Exhausts_after_max_attempts()
    {
        var calls = 0;
        await Assert.ThrowsAsync<TransientException>(() =>
            RetryEngine.Execute(
                (_, _) => { calls++; throw new TransientException(); },
                FastPolicy(4)).AsTask());

        Assert.Equal(4, calls);
    }

    [Fact]
    public async Task Non_retryable_fails_immediately()
    {
        var calls = 0;
        await Assert.ThrowsAsync<ArgumentException>(() =>
            RetryEngine.Execute(
                (_, _) => { calls++; throw new ArgumentException(); },
                FastPolicy(),
                isRetryable: ex => ex is TransientException).AsTask());

        Assert.Equal(1, calls);
    }

    [Fact]
    public void DelayFor_strategies()
    {
        var fixedPolicy = new RetryPolicy { Strategy = BackoffStrategy.Fixed, BaseDelay = TimeSpan.FromSeconds(1), Jitter = 0 };
        var incrPolicy = new RetryPolicy { Strategy = BackoffStrategy.Incremental, BaseDelay = TimeSpan.FromSeconds(1), Jitter = 0 };
        var expPolicy = new RetryPolicy { Strategy = BackoffStrategy.Exponential, BaseDelay = TimeSpan.FromSeconds(1), MaxDelay = TimeSpan.FromSeconds(10), Jitter = 0 };

        Assert.Equal(TimeSpan.FromSeconds(1), fixedPolicy.DelayFor(5));
        Assert.Equal(TimeSpan.FromSeconds(3), incrPolicy.DelayFor(3));
        Assert.Equal(TimeSpan.FromSeconds(4), expPolicy.DelayFor(3));
        Assert.Equal(TimeSpan.FromSeconds(10), expPolicy.DelayFor(9)); // cap
        Assert.Equal(TimeSpan.FromSeconds(1), expPolicy.DelayFor(0)); // attempt<1 → 1
        Assert.Equal(TimeSpan.FromSeconds(1), fixedPolicy.DelayFor(-5));
    }

    [Fact]
    public void DelayFor_jitter_reduces_delay()
    {
        var policy = new RetryPolicy { Strategy = BackoffStrategy.Fixed, BaseDelay = TimeSpan.FromSeconds(10), Jitter = 1.0 };
        var random = new Random(42);

        var d1 = policy.DelayFor(1, random);
        var d2 = policy.DelayFor(1, random);

        Assert.InRange(d1.TotalMilliseconds, 0, 10_000);
        Assert.InRange(d2.TotalMilliseconds, 0, 10_000);
    }

    [Fact]
    public void Poison_classification()
    {
        Assert.True(PoisonDetector.IsPoison(new SerializationException("x")));
        Assert.True(PoisonDetector.IsPoison(new FormatException()));
        Assert.True(PoisonDetector.IsPoison(new InvalidOperationException()));
        Assert.False(PoisonDetector.IsPoison(new TransientException()));
        Assert.False(PoisonDetector.IsPoison(new TimeoutException()));
    }

    private sealed class TransientException : Exception;
}

public class ConsumerPipelineTests
{
    private sealed record FakeDelivery(Envelope Envelope) : ITransportDelivery
    {
        public int Acks;
        public int Nacks;

        public ValueTask Ack()
        {
            Acks++;
            return default;
        }

        public ValueTask Nack(bool requeue, TimeSpan? redeliveryDelay)
        {
            Nacks++;
            return default;
        }
    }

    private static Envelope NewEnvelope()
        => Envelope.Create("App.M", "1", []);

    [Fact]
    public async Task Happy_path_acks()
    {
        var delivery = new FakeDelivery(NewEnvelope());
        var pipeline = new ConsumerPipeline(new InMemoryInboxStore());
        var calls = 0;

        await pipeline.Process(delivery, "handler", (e, _) => { calls++; return default; },
            new RetryPolicy { Strategy = BackoffStrategy.Fixed, BaseDelay = TimeSpan.FromMilliseconds(1), MaxAttempts = 2, Jitter = 0 });

        Assert.Equal(1, calls);
        Assert.Equal(1, delivery.Acks);
        Assert.Equal(0, delivery.Nacks);
    }

    [Fact]
    public async Task Duplicate_delivery_skips_handler_and_acks()
    {
        var inbox = new InMemoryInboxStore();
        var envelope = NewEnvelope();
        var first = new FakeDelivery(envelope);
        var second = new FakeDelivery(envelope);
        var pipeline = new ConsumerPipeline(inbox);
        var calls = 0;

        await pipeline.Process(first, "handler", (e, _) => { calls++; return default; });
        await pipeline.Process(second, "handler", (e, _) => { calls++; return default; });

        Assert.Equal(1, calls);
        Assert.Equal(1, second.Acks);
    }

    [Fact]
    public async Task Exhausted_retries_nacks_without_requeue()
    {
        var delivery = new FakeDelivery(NewEnvelope());
        var pipeline = new ConsumerPipeline(new InMemoryInboxStore());
        var calls = 0;

        await pipeline.Process(delivery, "handler", (e, _) => { calls++; throw new TimeoutException(); },
            new RetryPolicy { Strategy = BackoffStrategy.Fixed, BaseDelay = TimeSpan.FromMilliseconds(1), MaxAttempts = 3, Jitter = 0 });

        Assert.Equal(3, calls);
        Assert.Equal(0, delivery.Acks);
        Assert.Equal(1, delivery.Nacks);
    }

    [Fact]
    public async Task Poison_skips_retries_and_nacks()
    {
        var delivery = new FakeDelivery(NewEnvelope());
        var pipeline = new ConsumerPipeline(new InMemoryInboxStore());
        var calls = 0;

        await pipeline.Process(delivery, "handler", (e, _) => { calls++; throw new SerializationException("bad payload"); },
            new RetryPolicy { Strategy = BackoffStrategy.Fixed, BaseDelay = TimeSpan.FromMilliseconds(1), MaxAttempts = 5, Jitter = 0 });

        Assert.Equal(1, calls); // poison — DLQ
        Assert.Equal(1, delivery.Nacks);
    }
}
