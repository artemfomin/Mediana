using Mediana.Messaging;

namespace Mediana.Pipeline;

/// <summary>.</summary>
public delegate ValueTask<TResponse> HandlerDelegate<in TRequest, TResponse>(TRequest request, CancellationToken cancellationToken)
    where TRequest : IRequest<TResponse>;

/// <summary>(: TEvent contravariant-behaviour).</summary>
public delegate ValueTask EventHandlerDelegate<TEvent>(TEvent @event, CancellationToken cancellationToken)
    where TEvent : IEvent;
/// <summary>.</summary>
public delegate IAsyncEnumerable<TRow> StreamHandlerDelegate<in TQuery, TRow>(TQuery query, CancellationToken cancellationToken)
    where TQuery : IStreamQuery<TRow>;

/// <summary>Behaviour /: (, , ...).</summary>
public interface IHandlerMiddleware<TRequest, TResponse> where TRequest : IRequest<TResponse>
{
    ValueTask<TResponse> Handle(TRequest request, HandlerDelegate<TRequest, TResponse> next, CancellationToken cancellationToken);
}

/// <summary>Behaviour ().</summary>
public interface IEventMiddleware<TEvent> where TEvent : IEvent
{
    ValueTask Handle(TEvent @event, EventHandlerDelegate<TEvent> next, CancellationToken cancellationToken);
}

/// <summary>Behaviour : .</summary>
public interface IStreamMiddleware<TQuery, TRow> where TQuery : IStreamQuery<TRow>
{
    IAsyncEnumerable<TRow> Handle(TQuery query, StreamHandlerDelegate<TQuery, TRow> next, CancellationToken cancellationToken);
}

/// <summary>: (behaviour).</summary>
public interface IPreProcessor<in TRequest> where TRequest : IRequest
{
    ValueTask Process(TRequest request, CancellationToken cancellationToken);
}

/// <summary>: (behaviour).</summary>
public interface IPostProcessor<in TRequest, in TResponse> where TRequest : IRequest<TResponse>
{
    ValueTask Process(TRequest request, TResponse response, CancellationToken cancellationToken);
}
