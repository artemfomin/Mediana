using Mediana.Messaging;

namespace Mediana.Routing;

/// <summary>Куда направляется сообщение (§6 спеки).</summary>
public enum RouteTarget
{
    /// <summary>Только локальная диспетчеризация (default, без политики).</summary>
    Local,

    /// <summary>Только удалённо через транспорт.</summary>
    Remote,

    /// <summary>Локально И в очередь (для событий — natural fan-out; для команд — warning генератора).</summary>
    LocalAndRemote,
}

/// <summary>Политика маршрутизации сообщения.</summary>
public sealed record RoutePolicy
{
    public required RouteTarget Target { get; init; }

    /// <summary>Имя транспорта ("rabbit", "kafka", "masstransit"...).</summary>
    public string? Transport { get; init; }

    /// <summary>Цель: имя очереди/топика/exchange.</summary>
    public string? Destination { get; init; }

    /// <summary>Паттерн topic для событий ("order.{type}").</summary>
    public string? TopicPattern { get; init; }

    /// <summary>Таймаут request/reply для запросов (default 30s).</summary>
    public TimeSpan RequestTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>Политика доставки: Direct (без outbox-пакета) или Outbox (требует Mediana.Outbox).</summary>
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

/// <summary>Режим доставки (D4: outbox — opt-in).</summary>
public enum DeliveryMode
{
    /// <summary>Прямая публикация в транспорт; retry/DLQ работают, атомарности с БД нет.</summary>
    Direct,

    /// <summary>Через transactional outbox (требует установленный пакет Mediana.Outbox).</summary>
    Outbox,
}

/// <summary>Атрибут remote-маршрутизации сообщения (сахар; источник истины — fluent-конфигурация).</summary>
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
/// Реестр политик маршрутизации: тип сообщения → политика. Приоритет: fluent > атрибут > Local.
/// </summary>
public sealed class RouteRegistry
{
    private readonly Dictionary<Type, RoutePolicy> _policies = [];

    public RouteRegistry Set<TMessage>(RoutePolicy policy) where TMessage : IRequest
    {
        _policies[typeof(TMessage)] = policy;
        return this;
    }

    /// <summary>Резолв политики: fluent-регистрация → атрибут Remote → LocalOnly.</summary>
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
