using Mediana.Messaging;

namespace Mediana.Routing;

/// <summary>(§6 ).</summary>
public enum RouteTarget
{
    /// <summary>(default, ).</summary>
    Local,

    /// <summary>.</summary>
    Remote,

    /// <summary>(natural fan-out; warning ).</summary>
    LocalAndRemote,
}

/// <summary>.</summary>
public sealed record RoutePolicy
{
    public required RouteTarget Target { get; init; }

    /// <summary>("rabbit", "kafka", "masstransit"...).</summary>
    public string? Transport { get; init; }

    /// <summary>: //exchange.</summary>
    public string? Destination { get; init; }

    /// <summary>topic ("order.{type}").</summary>
    public string? TopicPattern { get; init; }

    /// <summary>request/reply (default 30s).</summary>
    public TimeSpan RequestTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>: Direct (outbox-) Outbox (Mediana.Outbox).</summary>
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

/// <summary>(D4: outbox — opt-in).</summary>
public enum DeliveryMode
{
    /// <summary>; retry/DLQ , .</summary>
    Direct,

    /// <summary>transactional outbox (Mediana.Outbox).</summary>
    Outbox,
}

/// <summary>remote-(; fluent-).</summary>
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
/// : → . : fluent > > Local
/// </summary>
public sealed class RouteRegistry
{
    private readonly Dictionary<Type, RoutePolicy> _policies = [];

    public RouteRegistry Set<TMessage>(RoutePolicy policy) where TMessage : IRequest
    {
        _policies[typeof(TMessage)] = policy;
        return this;
    }

    /// <summary>: fluent-→ Remote → LocalOnly.</summary>
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
