namespace Mediana.Dispatch;

/// <summary>and for but and and (on only in and Freeze).</summary>
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

    /// <summary>and fromin for command/query; and and for stream; null for and.</summary>
    public Type? ResponseType { get; }

    /// <summary>Call-site (Kind == Command).</summary>
    public object? CommandCallSite { get; internal set; }

    /// <summary>Call-site (Kind == Query).</summary>
    public object? QueryCallSite { get; internal set; }

    /// <summary>Call-site and- (Kind == Stream).</summary>
    public object? StreamCallSite { get; internal set; }

    /// <summary>Call-site' handlers and (Kind == Event).</summary>
    public IReadOnlyList<IEventCallSite> EventCallSites { get; internal set; } = [];

    public EventDispatchPolicy Policy { get; internal set; } = EventDispatchPolicy.Sequential;
}
