namespace Mediana.Dispatch;

/// <summary>(Freeze).</summary>
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

    /// <summary>command/query; stream; null .</summary>
    public Type? ResponseType { get; }

    /// <summary>Call-site (Kind == Command).</summary>
    public object? CommandCallSite { get; internal set; }

    /// <summary>Call-site (Kind == Query).</summary>
    public object? QueryCallSite { get; internal set; }

    /// <summary>Call-site (Kind == Stream).</summary>
    public object? StreamCallSite { get; internal set; }

    /// <summary>Call-site'(Kind == Event).</summary>
    public IReadOnlyList<IEventCallSite> EventCallSites { get; internal set; } = [];

    public EventDispatchPolicy Policy { get; internal set; } = EventDispatchPolicy.Sequential;
}
