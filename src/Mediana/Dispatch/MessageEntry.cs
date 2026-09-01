namespace Mediana.Dispatch;

/// <summary>Запись реестра для одного типа сообщения (мутабельна только в период Freeze).</summary>
public sealed class MessageEntry
{
    public MessageEntry(HandlerKind kind, Type messageType, Type? responseType)
    {
        Kind = kind;
        MessageType = messageType;
        ResponseType = responseType;
    }

    public HandlerKind Kind { get; }

    public Type MessageType { get; }

    /// <summary>Тип ответа для command/query; тип строки для stream; null для события.</summary>
    public Type? ResponseType { get; }

    /// <summary>Call-site команды (Kind == Command).</summary>
    public object? CommandCallSite { get; internal set; }

    /// <summary>Call-site запроса (Kind == Query).</summary>
    public object? QueryCallSite { get; internal set; }

    /// <summary>Call-site стрим-запроса (Kind == Stream).</summary>
    public object? StreamCallSite { get; internal set; }

    /// <summary>Call-site'ы хендлеров события (Kind == Event).</summary>
    public IReadOnlyList<IEventCallSite> EventCallSites { get; internal set; } = [];

    public EventDispatchPolicy Policy { get; internal set; } = EventDispatchPolicy.Sequential;
}
