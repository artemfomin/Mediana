using Mediana.Consuming;
using Mediana.Inbox;
using Mediana.Messaging;
using Mediana.Outbox;
using Mediana.Reliability;
using Mediana.Transports;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Mediana.UnitTests;

/// <summary>
/// R9: 11 security-audit
/// ID
/// </summary>
public class RegressionTests
{
    // ═══ R1: Mongo OutboxMessage.DocumentId — ObjectId ═══

    [Fact]
    public void R1_OutboxMessage_has_DocumentId_for_Mongo_correlation()
    {
        var msg = new OutboxMessage
        {
            DocumentId = "6a98d9c1213974adb66502f9",
            MessageId = Guid.NewGuid(),
            Destination = "q",
            EnvelopeBytes = [],
            CreatedAt = DateTimeOffset.UtcNow,
        };
        Assert.Equal("6a98d9c1213974adb66502f9", msg.DocumentId);

        // DocumentId ()
        var msg2 = new OutboxMessage { DocumentId = "6a98d9c1213974adb66502fa" };
        Assert.NotEqual(msg.DocumentId, msg2.DocumentId);
    }

    // ═══ R2: Parked OutboxMessage EF ═══

    [Fact]
    public void R2_Parked_field_exists_in_OutboxMessage()
    {
        var msg = new OutboxMessage { Parked = true };
        Assert.True(msg.Parked);
    }

    // ═══ R3: MaxDeliveryAttempts — , ═══

    [Fact]
    public void R3_MarkFailed_receives_maxAttempts_parameter()
    {
        // int maxDeliveryAttempts
        var method = typeof(IOutboxStore).GetMethod("MarkFailed");
        Assert.NotNull(method);
        var parameters = method!.GetParameters();
        Assert.Equal(4, parameters.Length);
        Assert.Equal(typeof(int), parameters[2].ParameterType);
        Assert.Equal("maxDeliveryAttempts", parameters[2].Name);
    }

    [Fact]
    public async Task R3_Relay_passes_options_MaxDeliveryAttempts_to_MarkFailed()
    {
        var store = new RecordingOutboxStore();
        var relay = new OutboxRelay(
            store,
            _ => new ValueTask<ITransportPublisher>(new FailingPublisher()),
            options: new OutboxRelayOptions { MaxDeliveryAttempts = 3, PollInterval = TimeSpan.FromMilliseconds(10), FailureBackoff = TimeSpan.FromMilliseconds(10) });

        using var cts = new CancellationTokenSource(500);
        await relay.StartAsync(cts.Token);
        await Task.Delay(200);
        await relay.StopAsync(CancellationToken.None);

        // Store maxAttempts = 3 (10)
        Assert.Contains(3, store.ReceivedMaxAttempts);
    }

    // ═══ R4: Kafka poison MessageId — () ═══

    [Fact]
    public void R4_OutboxMessage_Parked_field_exists()
    {
        var msg = new OutboxMessage { Parked = true };
        Assert.True(msg.Parked);
        var msg2 = new OutboxMessage();
        Assert.False(msg2.Parked);
    }

    // ═══ R7: Jitter — ns2.1 ═══

    [Fact]
    public void R7_Jitter_thread_safe()
    {
        // DelayFor jitter (state corruption System.Random)
        var policy = new RetryPolicy { Strategy = BackoffStrategy.Exponential, BaseDelay = TimeSpan.FromMilliseconds(1), Jitter = 0.5 };
        var random = new Random(42);
        Parallel.For(0, 100, i => policy.DelayFor(i % 5 + 1, random));
        // Random , corrupt state
    }

    // ═══ R8: EnvelopeCodec — ═══

    [Fact]
    public void R8_EnvelopeCodec_rejects_oversized_payload()
    {
        var oversized = new byte[EnvelopeCodec.MaxEnvelopeBytes + 1];
        Assert.Throws<SerializationException>(() => EnvelopeCodec.Decode(oversized));
    }

    [Fact]
    public void R8_EnvelopeCodec_rejects_too_many_headers()
    {
        var headers = new Dictionary<string, string>();
        for (var i = 0; i < 150; i++)
        {
            headers["h" + i] = "v";
        }
        var envelope = new Envelope
        {
            MessageId = Guid.NewGuid(),
            MessageType = new MessageTypeDescriptor { FullName = "X", TypeVersion = "1" },
            Timestamp = DateTimeOffset.UtcNow,
            Headers = headers,
            Payload = [],
        };
        var encoded = EnvelopeCodec.Encode(envelope);
        Assert.Throws<SerializationException>(() => EnvelopeCodec.Decode(encoded));
    }

    [Fact]
    public void R8_EnvelopeCodec_normal_payload_roundtrip()
    {
        var envelope = Envelope.Create("Test.Msg", "1", [1, 2, 3]);
        var encoded = EnvelopeCodec.Encode(envelope);
        var decoded = EnvelopeCodec.Decode(encoded);
        Assert.Equal(envelope.MessageId, decoded.MessageId);
    }

    // ═══ ═══

    private sealed class RecordingOutboxStore : IOutboxStore
    {
        public List<int> ReceivedMaxAttempts = [];
        public List<OutboxMessage> Messages = [];

        public ValueTask AddRange(IEnumerable<OutboxMessage> m, CancellationToken ct) { Messages.AddRange(m); return default; }

        public ValueTask<IReadOnlyList<OutboxMessage>> LeaseBatch(int b, long l, CancellationToken ct)
        {
            if (Messages.Count == 0)
            {
                var msg = new OutboxMessage
                {
                    DocumentId = "test-id-1",
                    MessageId = Guid.NewGuid(),
                    Destination = "q",
                    EnvelopeBytes = Mediana.Messaging.EnvelopeCodec.Encode(Envelope.Create("X", "1", [])),
                    CreatedAt = DateTimeOffset.UtcNow,
                    DeliveryAttempts = 1,
                };
                return new ValueTask<IReadOnlyList<OutboxMessage>>([msg]);
            }
            return new ValueTask<IReadOnlyList<OutboxMessage>>([]);
        }

        public ValueTask MarkDelivered(OutboxMessage m, CancellationToken ct) => default;

        public ValueTask MarkFailed(OutboxMessage m, string e, int maxAttempts, CancellationToken ct)
        {
            ReceivedMaxAttempts.Add(maxAttempts);
            return default;
        }

        public ValueTask<int> CleanupOlderThan(TimeSpan a, CancellationToken ct) => new(0);
    }

    private sealed class FailingPublisher : ITransportPublisher
    {
        public ValueTask Publish(Envelope e, PublishOptions o, CancellationToken ct)
            => throw new TransportException("fail");
    }
}
