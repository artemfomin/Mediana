namespace Mediana.Messaging;

/// <summary>wire-: additive (§15 ).</summary>
public static class EnvelopeVersion
{
    public const int Current = 1;
}

/// <summary>.</summary>
public sealed record MessageTypeDescriptor
{
    public string FullName { get; init; } = "";

    /// <summary>(semver-).</summary>
    public string TypeVersion { get; init; } = "1.0";

    /// <summary>: (poison).</summary>
    public string? ContractHash { get; init; }
}

/// <summary>
/// (§7 ): UUIDv7 MessageId, /
/// W3C traceparent, , payload. IMessageSerializer
/// </summary>
public sealed record Envelope
{
    public int Version { get; init; } = EnvelopeVersion.Current;

    /// <summary>UUIDv7: sortable, inbox.</summary>
    public Guid MessageId { get; init; }

    public Guid? CorrelationId { get; init; }

    public Guid? CausationId { get; init; }

    public MessageTypeDescriptor MessageType { get; init; } = new();

    public DateTimeOffset Timestamp { get; init; }

    /// <summary>(endpoint ).</summary>
    public string? SourceEndpoint { get; init; }

    /// <summary>W3C Trace Context (, D15).</summary>
    public string? TraceParent { get; init; }

    /// <summary>(partition key, reply-to...).</summary>
    public IReadOnlyDictionary<string, string> Headers { get; init; } = new Dictionary<string, string>();

    /// <summary>.</summary>
    public byte[] Payload { get; init; } = [];

    /// <summary>(ordering per key: Kafka partition, RabbitMQ routing).</summary>
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
