namespace Mediana.Messaging;

/// <summary>Shared root of the message hierarchy (D7): commands, queries, events, and stream queries.</summary>
public interface IRequest
{
}

/// <summary>Message with a typed response.</summary>
public interface IRequest<TResponse> : IRequest
{
}

/// <summary>Command without a return value (side-effect).</summary>
public interface ICommand : IRequest
{
}

/// <summary>Command with a typed response. Must have exactly one handler (validated at registration).</summary>
public interface ICommand<TResponse> : IRequest<TResponse>
{
}

/// <summary>Query with a typed response. Must have exactly one handler.</summary>
public interface IQuery<TResponse> : IRequest<TResponse>
{
}

/// <summary>Event: fan-out to any number of handlers.</summary>
public interface IEvent : IRequest
{
}

/// <summary>Stream query: the response is a stream of <typeparamref name="TRow"/> rows.</summary>
public interface IStreamQuery<TRow> : IRequest
{
}

/// <summary>Message with a partition key: per-key ordering on transports (Kafka partition key, RabbitMQ routing).</summary>
public interface IPartitioned
{
    string PartitionKey { get; }
}
