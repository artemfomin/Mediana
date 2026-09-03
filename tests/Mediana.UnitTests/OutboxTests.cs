using Mediana.Messaging;
using Mediana.Outbox;
using Mediana.Transports;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Mediana.UnitTests;

public class OutboxTests
{
    private sealed class FakeOutboxStore : IOutboxStore
    {
        public List<OutboxMessage> Messages = [];

        public ValueTask AddRange(IEnumerable<OutboxMessage> messages, CancellationToken ct)
        {
            Messages.AddRange(messages);
            return default;
        }

        public ValueTask<IReadOnlyList<OutboxMessage>> LeaseBatch(int batchSize, long leaseUnixMs, CancellationToken ct)
        {
            var leased = Messages
                .Where(m => m.LeaseUntil == 0)
                .Take(batchSize)
                .Select(m => m with { LeaseUntil = leaseUnixMs, DeliveryAttempts = m.DeliveryAttempts + 1 })
                .ToList();
            foreach (var l in leased)
            {
                var index = Messages.FindIndex(m => m.Sequence == l.Sequence);
                if (index >= 0)
                {
                    Messages[index] = l;
                }
            }

            return new ValueTask<IReadOnlyList<OutboxMessage>>(leased);
        }

        public ValueTask MarkDelivered(OutboxMessage message, CancellationToken ct)
        {
            Delivered.Add(message.MessageId);
            return default;
        }

        public ValueTask MarkFailed(OutboxMessage message, string error, int maxAttempts, CancellationToken ct)
        {
            Failed.Add((message.MessageId, error));
            var index = Messages.FindIndex(m => m.Sequence == message.Sequence);
            if (index >= 0)
            {
                Messages[index] = Messages[index] with { LeaseUntil = 0, LastError = error };
            }

            return default;
        }

        public ValueTask<int> CleanupOlderThan(TimeSpan age, CancellationToken ct)
        {
            CleanupCalls++;
            return new ValueTask<int>(0);
        }

        public List<Guid> Delivered { get; } = [];

        public List<(Guid, string)> Failed { get; } = [];

        public int CleanupCalls;
    }

    private sealed class FakePublisher(bool fail = false) : ITransportPublisher
    {
        public List<Envelope> Published { get; } = [];

        public ValueTask Publish(Envelope envelope, PublishOptions options, CancellationToken ct)
        {
            if (fail)
            {
                throw new TransportException("broker unavailable");
            }

            Published.Add(envelope);
            return default;
        }
    }

    private static OutboxMessage MakeMessage(Guid id, long sequence = 0)
        => new()
        {
            Sequence = sequence,
            MessageId = id,
            Destination = "orders",
            EnvelopeBytes = Mediana.Messaging.EnvelopeCodec.Encode(Envelope.Create("App.M", "1", [])),
            CreatedAt = DateTimeOffset.UtcNow,
        };

    [Fact]
    public void Collector_adds_and_takes_pending()
    {
        var collector = new OutboxCollector();
        var envelope = Envelope.Create("App.OrderCreated", "1.0", [1]);
        collector.Add(envelope, "orders", "rabbit");

        Assert.Equal(1, collector.Count);
        var pending = collector.TakePending();
        Assert.Single(pending);
        Assert.Equal("orders", pending[0].Destination);
        Assert.Equal(0, collector.Count);
        Assert.Empty((IEnumerable<OutboxMessage>)[]);
    }

    [Fact]
    public async Task Relay_delivers_leased_batch_and_marks_delivered()
    {
        var store = new FakeOutboxStore();
        var id = Guid.NewGuid();
        await store.AddRange([MakeMessage(id, sequence: 1)], default);
        var publisher = new FakePublisher();

        var relay = new OutboxRelay(
            store,
            _ => new ValueTask<ITransportPublisher>(publisher),
            new OutboxRelayOptions { PollInterval = TimeSpan.FromMilliseconds(10) });

        using var cts = new CancellationTokenSource(300);
        await relay.StartAsync(cts.Token);

        await WaitUntil(() => store.Delivered.Contains(id));
        await relay.StopAsync(CancellationToken.None);

        Assert.Single(publisher.Published);
        Assert.Equal("orders", publisher.Published[0].MessageType.FullName is not null ? "orders" : "");
    }

    [Fact]
    public async Task Relay_marks_failed_on_publish_error()
    {
        var store = new FakeOutboxStore();
        var id = Guid.NewGuid();
        await store.AddRange([MakeMessage(id, sequence: 2)], default);
        var publisher = new FakePublisher(fail: true);

        var relay = new OutboxRelay(
            store,
            _ => new ValueTask<ITransportPublisher>(publisher),
            new OutboxRelayOptions { PollInterval = TimeSpan.FromMilliseconds(10) });

        using var cts = new CancellationTokenSource(300);
        await relay.StartAsync(cts.Token);

        await WaitUntil(() => store.Failed.Any(f => f.Item1 == id));
        await relay.StopAsync(CancellationToken.None);

        Assert.Contains(store.Failed, f => f.Item1 == id && f.Item2 == "broker unavailable");
    }

    [Fact]
    public async Task Relay_backs_off_and_continues_after_transport_recovery()
    {
        var store = new FakeOutboxStore();
        await store.AddRange([MakeMessage(Guid.NewGuid(), sequence: 3)], default);
        var publisher = new FakePublisher();

        var factoryCalls = 0;
        var relay = new OutboxRelay(
            store,
            ct =>
            {
                factoryCalls++;
                return new ValueTask<ITransportPublisher>(factoryCalls == 1 ? new FakePublisher(fail: true) : publisher);
            },
            new OutboxRelayOptions { PollInterval = TimeSpan.FromMilliseconds(10), FailureBackoff = TimeSpan.FromMilliseconds(20) });

        using var cts = new CancellationTokenSource(1500);
        await relay.StartAsync(cts.Token);
        await WaitUntil(() => store.Delivered.Count > 0);
        await relay.StopAsync(CancellationToken.None);

        Assert.True(store.Delivered.Count > 0, "relay must recover after transient transport failure");
    }

    [Fact]
    public async Task Relay_survives_repeated_store_failures()
    {
        var store = new FakeOutboxStore();
        var relay = new OutboxRelay(
            store,
            _ => throw new InvalidOperationException("store exploded"),
            new OutboxRelayOptions { PollInterval = TimeSpan.FromMilliseconds(5), FailureBackoff = TimeSpan.FromMilliseconds(10) });

        using var cts = new CancellationTokenSource(300);
        await relay.StartAsync(cts.Token);
        await Task.Delay(100);
        await relay.StopAsync(CancellationToken.None);
    }

    private static async Task WaitUntil(Func<bool> condition, int timeoutMs = 2000)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        while (!condition() && sw.ElapsedMilliseconds < timeoutMs)
        {
            await Task.Delay(10);
        }

        Assert.True(condition(), "Condition not met within timeout");
    }
}

public static class OutboxTestHelpers
{
    public sealed class FakeOutboxStoreProxy : IOutboxStore
    {
        public ValueTask AddRange(IEnumerable<OutboxMessage> m, CancellationToken ct) => default;
        public ValueTask<IReadOnlyList<OutboxMessage>> LeaseBatch(int b, long l, CancellationToken ct) => new([]);
        public ValueTask MarkDelivered(OutboxMessage m, CancellationToken ct) => default;
        public ValueTask MarkFailed(OutboxMessage m, string e, int maxAttempts, CancellationToken ct) => default;
        public ValueTask<int> CleanupOlderThan(TimeSpan a, CancellationToken ct) => new(0);
    }

    public sealed class NullPublisher : ITransportPublisher
    {
        public ValueTask Publish(Envelope e, PublishOptions o, CancellationToken ct) => default;
    }
}
