namespace Mediana.Messaging;

/// <summary>(D7): , , .</summary>
public interface IRequest
{
}

/// <summary>.</summary>
public interface IRequest<TResponse> : IRequest
{
}

/// <summary>(side-effect).</summary>
public interface ICommand : IRequest
{
}

/// <summary>. ().</summary>
public interface ICommand<TResponse> : IRequest<TResponse>
{
}

/// <summary>(query) . .</summary>
public interface IQuery<TResponse> : IRequest<TResponse>
{
}

/// <summary>: fan-out .</summary>
public interface IEvent : IRequest
{
}

/// <summary>: <typeparamref name="TRow"/>.</summary>
public interface IStreamQuery<TRow> : IRequest
{
}

/// <summary>: ordering per key (Kafka partition key, RabbitMQ routing).</summary>
public interface IPartitioned
{
    string PartitionKey { get; }
}
