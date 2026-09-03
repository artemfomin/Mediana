using Mediana.Messaging;

namespace Mediana.Pipeline;

/// <summary>Delegate to the next pipeline stage for requests.</summary>
public delegate ValueTask<TResponse> HandlerDelegate<in TRequest, TResponse>(TRequest request, CancellationToken cancellationToken)
    where TRequest : IRequest<TResponse>;

/// <summary>Delegate to the next pipeline stage for events (invariant: TEvent is already in contravariant position).</summary>
public delegate ValueTask EventHandlerDelegate<TEvent>(TEvent @event, CancellationToken cancellationToken)
    where TEvent : IEvent;

/// <summary>Delegate to the next pipeline stage for streams.</summary>
public delegate IAsyncEnumerable<TRow> StreamHandlerDelegate<in TQuery, TRow>(TQuery query, CancellationToken cancellationToken)
    where TQuery : IStreamQuery<TRow>;

/// <summary>Command/query pipeline middleware: cross-cutting concerns around the handler (logging, validation, transactions).</summary>
public interface IHandlerMiddleware<TRequest, TResponse> where TRequest : IRequest<TResponse>
{
    ValueTask<TResponse> Handle(TRequest request, HandlerDelegate<TRequest, TResponse> next, CancellationToken cancellationToken);
}

/// <summary>Event pipeline middleware (events have no response — separate contract).</summary>
public interface IEventMiddleware<TEvent> where TEvent : IEvent
{
    ValueTask Handle(TEvent @event, EventHandlerDelegate<TEvent> next, CancellationToken cancellationToken);
}

/// <summary>Stream pipeline middleware: wrappers over the row stream.</summary>
public interface IStreamMiddleware<TQuery, TRow> where TQuery : IStreamQuery<TRow>
{
    IAsyncEnumerable<TRow> Handle(TQuery query, StreamHandlerDelegate<TQuery, TRow> next, CancellationToken cancellationToken);
}

/// <summary>Pre-processor: runs before the handler (sugar over middleware).</summary>
public interface IPreProcessor<in TRequest> where TRequest : IRequest
{
    ValueTask Process(TRequest request, CancellationToken cancellationToken);
}

/// <summary>Post-processor: runs after a successful handler (sugar over middleware).</summary>
public interface IPostProcessor<in TRequest, in TResponse> where TRequest : IRequest<TResponse>
{
    ValueTask Process(TRequest request, TResponse response, CancellationToken cancellationToken);
}
