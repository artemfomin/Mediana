using Mediana.Messaging;
namespace Mediana.Dispatch;

/// <summary>.</summary>
public enum HandlerKind
{
    Command,
    Query,
    Event,
    Stream,
}

/// <summary>(§4.3 ).</summary>
public enum EventDispatchPolicy
{
    /// <summary>; . .</summary>
    Sequential,

    /// <summary>; AggregateException.</summary>
    Parallel,
}

/// <summary>
/// Call-site object-(<see cref="IMediator.Send{TResponse}(Mediana.Messaging.ICommand{TResponse}, CancellationToken)"/>)
/// Advanced API:
/// </summary>
public interface IObjectCommandCallSite<TResponse>
{
    ValueTask<TResponse> Invoke(object message, IServiceProvider serviceProvider, CancellationToken cancellationToken);
}

/// <summary>Call-site (zero-boxing , SendExact).</summary>
public interface ITypedCommandCallSite<TRequest, TResponse> where TRequest : IRequest<TResponse>
{
    ValueTask<TResponse> InvokeTyped(TRequest message, IServiceProvider serviceProvider, CancellationToken cancellationToken);
}

/// <summary>Call-site object-.</summary>
public interface IObjectQueryCallSite<TResponse>
{
    ValueTask<TResponse> Invoke(object message, IServiceProvider serviceProvider, CancellationToken cancellationToken);
}

/// <summary>Call-site (SendExact).</summary>
public interface ITypedQueryCallSite<TRequest, TResponse> where TRequest : IRequest<TResponse>
{
    ValueTask<TResponse> InvokeTyped(TRequest message, IServiceProvider serviceProvider, CancellationToken cancellationToken);
}

/// <summary>Call-site .</summary>
public interface IEventCallSite
{
    ValueTask Invoke(object message, IServiceProvider serviceProvider, CancellationToken cancellationToken);
}

/// <summary>
/// Non-generic : canon-generic (~24-32/, )
/// generic InvokeAny generic Mediator — . Value-
/// object-value-(. Mediator)
/// </summary>
public interface IUntypedCallSite
{
    ValueTask<object?> InvokeAny(object message, IServiceProvider serviceProvider, CancellationToken cancellationToken);
}

/// <summary>Call-site .</summary>
public interface IStreamCallSite<TRow>
{
    IAsyncEnumerable<TRow> Invoke(object message, IServiceProvider serviceProvider, CancellationToken cancellationToken);
}
