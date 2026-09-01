using Mediana.Messaging;

namespace Mediana.Handlers;

/// <summary>Хендлер команды с ответом.</summary>
public interface ICommandHandler<in TCommand, TResponse> where TCommand : ICommand<TResponse>
{
    ValueTask<TResponse> Handle(TCommand command, CancellationToken cancellationToken);
}

/// <summary>Хендлер запроса.</summary>
public interface IQueryHandler<in TQuery, TResponse> where TQuery : IQuery<TResponse>
{
    ValueTask<TResponse> Handle(TQuery query, CancellationToken cancellationToken);
}

/// <summary>Хендлер события. Хендлеров одного события может быть сколько угодно.</summary>
public interface IEventHandler<in TEvent> where TEvent : IEvent
{
    ValueTask Handle(TEvent @event, CancellationToken cancellationToken);
}

/// <summary>Хендлер стрим-запроса.</summary>
public interface IStreamHandler<in TQuery, TRow> where TQuery : IStreamQuery<TRow>
{
    IAsyncEnumerable<TRow> Handle(TQuery query, CancellationToken cancellationToken);
}
