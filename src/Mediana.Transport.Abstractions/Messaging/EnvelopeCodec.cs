using System.Diagnostics.CodeAnalysis;
using System.Text.Json;

namespace Mediana.Messaging;

/// <summary>
/// Единый codec конверта с лимитами защиты от memory-DoS (T-03 fix):
/// MaxEnvelopeBytes (default 1 MB), MaxDepth=32, лимит заголовков.
/// Используется всеми транспортами вместо копий.
/// </summary>
public static class EnvelopeCodec
{
    /// <summary>Максимальный размер конверта (T-03): 1 MB по умолчанию.</summary>
    public static int MaxEnvelopeBytes { get; set; } = 1_048_576;

    private static readonly JsonSerializerOptions SafeOptions = new(JsonSerializerDefaults.Web)
    {
        MaxDepth = 32,
        AllowTrailingCommas = false,
        ReadCommentHandling = JsonCommentHandling.Disallow,
    };

    [RequiresDynamicCode("Reflection-based JSON; для AOT — source-gen.")]
    public static byte[] Encode(Envelope envelope)
        => JsonSerializer.SerializeToUtf8Bytes(envelope, SafeOptions);

    [RequiresDynamicCode("Reflection-based JSON; для AOT — source-gen.")]
    public static Envelope Decode(byte[] body)
    {
        if (body.Length > MaxEnvelopeBytes)
        {
            throw new SerializationException(
                $"Envelope exceeds {MaxEnvelopeBytes} bytes limit (got {body.Length}).");
        }

        var envelope = JsonSerializer.Deserialize<Envelope>(body, SafeOptions)
            ?? throw new SerializationException("Empty envelope body.");

        if (envelope.Headers.Count > 100)
        {
            throw new SerializationException(
                $"Envelope headers exceed 100 entries (got {envelope.Headers.Count}).");
        }

        return envelope;
    }
}
