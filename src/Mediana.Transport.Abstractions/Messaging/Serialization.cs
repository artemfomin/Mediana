using System.Diagnostics.CodeAnalysis;
using System.Text.Json;

namespace Mediana.Messaging;

/// <summary>Message serialization provider: selection per message type (D9). Registry via DI.</summary>
public interface IMessageSerializer
{
    string ContentType { get; }

    [RequiresDynamicCode("The implementation may use reflection: for NativeAOT register a source-gen serializer.")]
    byte[] Serialize<T>(T message);

    [RequiresDynamicCode("The implementation may use reflection: for NativeAOT register a source-gen serializer.")]
    T Deserialize<T>(ReadOnlySpan<byte> payload);

    [RequiresDynamicCode("The implementation may use reflection: for NativeAOT register a source-gen serializer.")]
    object Deserialize(ReadOnlySpan<byte> payload, Type messageType);
}

/// <summary>
/// System.Text.Json andfromthen (default, D9): reflection-free via JsonSerializable
/// byand; for AOT register a source-gen context in settings
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

    [RequiresDynamicCode("Reflection-based JSON: for NativeAOT use a source-gen serializer (JsonSerializerContext).")]
    [RequiresUnreferencedCode("Reflection-based JSON: for trimming use a source-gen serializer.")]
    public byte[] Serialize<T>(T message)
        => JsonSerializer.SerializeToUtf8Bytes(message, _options);

    [RequiresDynamicCode("Reflection-based JSON: for NativeAOT use a source-gen serializer (JsonSerializerContext).")]
    [RequiresUnreferencedCode("Reflection-based JSON: for trimming use a source-gen serializer.")]
    public T Deserialize<T>(ReadOnlySpan<byte> payload)
        => JsonSerializer.Deserialize<T>(payload, _options)
           ?? throw new SerializationException("Deserialized null for " + typeof(T) + ".");

    [RequiresDynamicCode("Reflection-based JSON: for NativeAOT use a source-gen serializer (JsonSerializerContext).")]
    [RequiresUnreferencedCode("Reflection-based JSON: for trimming use a source-gen serializer.")]
    public object Deserialize(ReadOnlySpan<byte> payload, Type messageType)
        => JsonSerializer.Deserialize(payload.ToArray(), messageType, _options)
           ?? throw new SerializationException("Deserialized null for " + messageType + ".");

    internal static class JsonOptions
    {
        public static readonly JsonSerializerOptions Default = new(JsonSerializerDefaults.Web);
    }
}

/// <summary>and andfromandand/andfromandand — poison-and (§9.3).</summary>
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
