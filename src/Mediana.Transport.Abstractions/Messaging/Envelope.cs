namespace Mediana.Messaging;

/// <summary>Версия wire-формата конверта: эволюция только additive (§15 спеки).</summary>
public static class EnvelopeVersion
{
    public const int Current = 1;
}

/// <summary>Дескриптор типа сообщения в конверте.</summary>
public sealed record MessageTypeDescriptor
{
    public string FullName { get; init; } = "";

    /// <summary>Версия контракта сообщения (semver-подобная строка).</summary>
    public string TypeVersion { get; init; } = "1.0";

    /// <summary>Хэш контракта: детекция несовместимости на приёме (poison).</summary>
    public string? ContractHash { get; init; }
}

/// <summary>
/// Транспортно-независимый конверт (§7 спеки): UUIDv7 MessageId, корреляция/каузация,
/// W3C traceparent, заголовки, payload. Сериализация — IMessageSerializer.
/// </summary>
public sealed record Envelope
{
    public int Version { get; init; } = EnvelopeVersion.Current;

    /// <summary>UUIDv7: sortable, дедупликация inbox.</summary>
    public Guid MessageId { get; init; }

    public Guid? CorrelationId { get; init; }

    public Guid? CausationId { get; init; }

    public MessageTypeDescriptor MessageType { get; init; } = new();

    public DateTimeOffset Timestamp { get; init; }

    /// <summary>Источник публикации (endpoint приложения).</summary>
    public string? SourceEndpoint { get; init; }

    /// <summary>W3C Trace Context (сквозные трейсы, D15).</summary>
    public string? TraceParent { get; init; }

    /// <summary>Пользовательские и системные заголовки (partition key, reply-to...).</summary>
    public IReadOnlyDictionary<string, string> Headers { get; init; } = new Dictionary<string, string>();

    /// <summary>Сериализованное тело сообщения.</summary>
    public byte[] Payload { get; init; } = [];

    /// <summary>Ключ партиционирования (ordering per key: Kafka partition, RabbitMQ routing).</summary>
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
