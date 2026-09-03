using Mediana.Messaging;

namespace Mediana.Handlers;

/// <summary>Command handler with a response.</summary>
public interface ICommandHandler<in TCommand, TResponse> where TCommand : ICommand<TResponse>
{
    ValueTask<TResponse> Handle(TCommand command, CancellationToken cancellationToken);
}

/// <summary>Query handler.</summary>
public interface IQueryHandler<in TQuery, TResponse> where TQuery : IQuery<TResponse>
{
    ValueTask<TResponse> Handle(TQuery query, CancellationToken cancellationToken);
}

/// <summary>Event handler. There can be any number of handlers for the same event.</summary>
public interface IEventHandler<in TEvent> where TEvent : IEvent
{
    ValueTask Handle(TEvent @event, CancellationToken cancellationToken);
}

/// <summary>Stream query handler.</summary>
public interface IStreamHandler<in TQuery, TRow> where TQuery : IStreamQuery<TRow>
{
    IAsyncEnumerable<TRow> Handle(TQuery query, CancellationToken cancellationToken);
}
