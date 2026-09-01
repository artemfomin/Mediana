namespace Mediana.Messaging;

/// <summary>Общий корень иерархии сообщений (D7): команды, запросы, события и стрим-запросы.</summary>
public interface IRequest
{
}

/// <summary>Сообщение с типизированным ответом.</summary>
public interface IRequest<TResponse> : IRequest
{
}

/// <summary>Команда без ответа (side-effect).</summary>
public interface ICommand : IRequest
{
}

/// <summary>Команда с типизированным ответом. Ровно один хендлер (валидируется при регистрации).</summary>
public interface ICommand<TResponse> : IRequest<TResponse>
{
}

/// <summary>Запрос (query) с типизированным ответом. Ровно один хендлер.</summary>
public interface IQuery<TResponse> : IRequest<TResponse>
{
}

/// <summary>Событие: fan-out на произвольное число хендлеров.</summary>
public interface IEvent : IRequest
{
}

/// <summary>Стрим-запрос: ответ — поток строк <typeparamref name="TRow"/>.</summary>
public interface IStreamQuery<TRow> : IRequest
{
}

/// <summary>Сообщение с ключом партиционирования: ordering per key на транспортах (Kafka partition key, RabbitMQ routing).</summary>
public interface IPartitioned
{
    string PartitionKey { get; }
}
