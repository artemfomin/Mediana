using Mediana.Messaging;

namespace Mediana.Routing;

/// <summary>Where the message is routed (spec §6).</summary>
public enum RouteTarget
{
    /// <summary>Local dispatch only (default, no policy).</summary>
    Local,

    /// <summary>Remote only via transport.</summary>
    Remote,

    /// <summary>but in (for and — natural fan-out; for — warning notthen).</summary>
    LocalAndRemote,
}

/// <summary>Message routing policy.</summary>
public sealed record RoutePolicy
{
    public required RouteTarget Target { get; init; }

    /// <summary>Transport name ("rabbit", "kafka", "masstransit"...).</summary>
    public string? Transport { get; init; }

    /// <summary>Destination: queue/topic/exchange name.</summary>
    public string? Destination { get; init; }

    /// <summary>Topic pattern for events ("order.{type}").</summary>
    public string? TopicPattern { get; init; }

    /// <summary>Request/reply timeout for queries (default 30s).</summary>
    public TimeSpan RequestTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>Delivery policy: Direct (without the outbox package) or Outbox (requires Mediana.Outbox).</summary>
    public DeliveryMode Delivery { get; init; } = DeliveryMode.Direct;

    public static RoutePolicy LocalOnly() => new() { Target = RouteTarget.Local };

    public static RoutePolicy ToQueue(string transport, string queue) => new()
    {
        Target = RouteTarget.Remote,
        Transport = transport,
        Destination = queue,
    };

    public static RoutePolicy FanOut(string transport, string topicPattern) => new()
    {
        Target = RouteTarget.Remote,
        Transport = transport,
        TopicPattern = topicPattern,
    };
}

/// <summary>and toinand (D4: outbox — opt-in).</summary>
public enum DeliveryMode
{
    /// <summary>Direct publish to transport; retry/DLQ work, no atomicity with the database.</summary>
    Direct,

    /// <summary>Through transactional outbox (requires the Mediana.Outbox package).</summary>
    Outbox,
}

/// <summary>and remote-fromandand and (; andthenand andand — fluent-andand).</summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
public sealed class RemoteAttribute : Attribute
{
    public RemoteAttribute(string destination)
    {
        Destination = destination;
    }

    public string Destination { get; }

    public string? Transport { get; set; }

    public RouteTarget Target { get; set; } = RouteTarget.Remote;
}

/// <summary>
/// byandand fromandand: and and → byandand. andand: fluent > and > Local
/// </summary>
public sealed class RouteRegistry
{
    private readonly Dictionary<Type, RoutePolicy> _policies = [];

    public RouteRegistry Set<TMessage>(RoutePolicy policy) where TMessage : IRequest
    {
        _policies[typeof(TMessage)] = policy;
        return this;
    }

    /// <summary>in byandandand: fluent-andand → and Remote → LocalOnly.</summary>
    public RoutePolicy Resolve(Type messageType)
    {
        if (_policies.TryGetValue(messageType, out var policy))
        {
            return policy;
        }

        var remote = messageType.GetCustomAttributes(typeof(RemoteAttribute), inherit: false);
        if (remote.Length > 0 && remote[0] is RemoteAttribute attribute)
        {
            return new RoutePolicy
            {
                Target = attribute.Target,
                Transport = attribute.Transport,
                Destination = attribute.Destination,
            };
        }

        return RoutePolicy.LocalOnly();
    }
}
