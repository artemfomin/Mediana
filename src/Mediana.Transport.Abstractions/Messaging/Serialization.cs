using System.Diagnostics.CodeAnalysis;
using System.Text.Json;

namespace Mediana.Messaging;

/// <summary>Провайдер сериализации сообщений: выбор per message type (D9). Реестр — через DI.</summary>
public interface IMessageSerializer
{
    string ContentType { get; }

    [RequiresDynamicCode("Реализация может использовать рефлексию: для NativeAOT подключайте source-gen сериализатор.")]
    byte[] Serialize<T>(T message);

    [RequiresDynamicCode("Реализация может использовать рефлексию: для NativeAOT подключайте source-gen сериализатор.")]
    T Deserialize<T>(ReadOnlySpan<byte> payload);

    [RequiresDynamicCode("Реализация может использовать рефлексию: для NativeAOT подключайте source-gen сериализатор.")]
    object Deserialize(ReadOnlySpan<byte> payload, Type messageType);
}

/// <summary>
/// System.Text.Json сериализатор (default, D9): reflection-free через JsonSerializable
/// у потребителя; для AOT подключайте source-gen контекст в настройках.
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

    [RequiresDynamicCode("Reflection-based JSON: для NativeAOT используйте source-gen сериализатор (JsonSerializerContext).")]
    [RequiresUnreferencedCode("Reflection-based JSON: для trimming используйте source-gen сериализатор.")]
    public byte[] Serialize<T>(T message)
        => JsonSerializer.SerializeToUtf8Bytes(message, _options);

    [RequiresDynamicCode("Reflection-based JSON: для NativeAOT используйте source-gen сериализатор (JsonSerializerContext).")]
    [RequiresUnreferencedCode("Reflection-based JSON: для trimming используйте source-gen сериализатор.")]
    public T Deserialize<T>(ReadOnlySpan<byte> payload)
        => JsonSerializer.Deserialize<T>(payload, _options)
           ?? throw new SerializationException("Deserialized null for " + typeof(T) + ".");

    [RequiresDynamicCode("Reflection-based JSON: для NativeAOT используйте source-gen сериализатор (JsonSerializerContext).")]
    [RequiresUnreferencedCode("Reflection-based JSON: для trimming используйте source-gen сериализатор.")]
    public object Deserialize(ReadOnlySpan<byte> payload, Type messageType)
        => JsonSerializer.Deserialize(payload.ToArray(), messageType, _options)
           ?? throw new SerializationException("Deserialized null for " + messageType + ".");

    internal static class JsonOptions
    {
        public static readonly JsonSerializerOptions Default = new(JsonSerializerDefaults.Web);
    }
}

/// <summary>Ошибка сериализации/десериализации — poison-категория (§9.3).</summary>
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
