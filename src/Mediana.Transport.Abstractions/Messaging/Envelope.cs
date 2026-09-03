namespace Mediana.Messaging;

/// <summary>Envelope wire format version: evolution is additive-only (spec §15).</summary>
public static class EnvelopeVersion
{
    public const int Current = 1;
}

/// <summary>Message type descriptor in the envelope.</summary>
public sealed record MessageTypeDescriptor
{
    public string FullName { get; init; } = "";

    /// <summary>Message contract version (semver-like string).</summary>
    public string TypeVersion { get; init; } = "1.0";

    /// <summary>Contract hash: incompatibility detection on receive (poison).</summary>
    public string? ContractHash { get; init; }
}

/// <summary>
/// Transport-agnostic envelope (spec §7): UUIDv7 MessageId, and/and
/// W3C traceparent, inand, payload. andfromand — IMessageSerializer
/// </summary>
public sealed record Envelope
{
    public int Version { get; init; } = EnvelopeVersion.Current;

    /// <summary>UUIDv7: sortable, inbox deduplication.</summary>
    public Guid MessageId { get; init; }

    public Guid? CorrelationId { get; init; }

    public Guid? CausationId { get; init; }

    public MessageTypeDescriptor MessageType { get; init; } = new();

    public DateTimeOffset Timestamp { get; init; }

    /// <summary>Publication source (application endpoint).</summary>
    public string? SourceEndpoint { get; init; }

    /// <summary>W3C Trace Context (end-to-end tracing, D15).</summary>
    public string? TraceParent { get; init; }

    /// <summary>User and system headers (partition key, reply-to, etc.).</summary>
    public IReadOnlyDictionary<string, string> Headers { get; init; } = new Dictionary<string, string>();

    /// <summary>Serialized message body.</summary>
    public byte[] Payload { get; init; } = [];

    /// <summary> andandandinand (ordering per key: Kafka partition, RabbitMQ routing).</summary>
    public string? PartitionKey
    {
        get => Headers.TryGetValue("mediana.partition-key", out var v) ? v : null;
        init
        {
            if (value is not null)
            {
                var dict = new Dictionary<string, string>(Headers) { ["mediana.partition-key"] = value };
                Headers = dict;
            }
        }
    }

    public static Envelope Create(string messageTypeFullName, string typeVersion, byte[] payload, string? sourceEndpoint = null)
        => new()
        {
            MessageId = GuidV7.NewGuid(),
            MessageType = new MessageTypeDescriptor { FullName = messageTypeFullName, TypeVersion = typeVersion },
            Timestamp = DateTimeOffset.UtcNow,
            SourceEndpoint = sourceEndpoint,
            Payload = payload,
        };
}
