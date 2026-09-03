using System.Diagnostics.CodeAnalysis;
using System.Text.Json;

namespace Mediana.Messaging;

/// <summary>: per message type (D9). DI.</summary>
public interface IMessageSerializer
{
    string ContentType { get; }

 [RequiresDynamicCode(" NativeAOT source-gen ")]
    byte[] Serialize<T>(T message);

 [RequiresDynamicCode(" NativeAOT source-gen ")]
    T Deserialize<T>(ReadOnlySpan<byte> payload);

 [RequiresDynamicCode(" NativeAOT source-gen ")]
    object Deserialize(ReadOnlySpan<byte> payload, Type messageType);
}

/// <summary>
/// System.Text.Json (default, D9): reflection-free JsonSerializable
/// AOT source-gen
/// </summary>
public sealed class SystemTextJsonMessageSerializer : IMessageSerializer
{
    public static readonly SystemTextJsonMessageSerializer Instance = new();

    private readonly JsonSerializerOptions _options;

    public SystemTextJsonMessageSerializer(JsonSerializerOptions? options = null)
    {
        _options = options ?? JsonOptions.Default;
    }

    public string ContentType => "application/json";

 [RequiresDynamicCode("Reflection-based JSON: NativeAOT source-gen JsonSerializerContext).")]
 [RequiresUnreferencedCode("Reflection-based JSON: trimming source-gen ")]
    public byte[] Serialize<T>(T message)
        => JsonSerializer.SerializeToUtf8Bytes(message, _options);

 [RequiresDynamicCode("Reflection-based JSON: NativeAOT source-gen JsonSerializerContext).")]
 [RequiresUnreferencedCode("Reflection-based JSON: trimming source-gen ")]
    public T Deserialize<T>(ReadOnlySpan<byte> payload)
        => JsonSerializer.Deserialize<T>(payload, _options)
           ?? throw new SerializationException("Deserialized null for " + typeof(T) + ".");

 [RequiresDynamicCode("Reflection-based JSON: NativeAOT source-gen JsonSerializerContext).")]
 [RequiresUnreferencedCode("Reflection-based JSON: trimming source-gen ")]
    public object Deserialize(ReadOnlySpan<byte> payload, Type messageType)
        => JsonSerializer.Deserialize(payload.ToArray(), messageType, _options)
           ?? throw new SerializationException("Deserialized null for " + messageType + ".");

    internal static class JsonOptions
    {
        public static readonly JsonSerializerOptions Default = new(JsonSerializerDefaults.Web);
    }
}

/// <summary>/poison-(§9.3).</summary>
public class SerializationException : Exception
{
    public SerializationException(string message)
        : base(message)
    {
    }

    public SerializationException(string message, Exception inner)
        : base(message, inner)
    {
    }
}
