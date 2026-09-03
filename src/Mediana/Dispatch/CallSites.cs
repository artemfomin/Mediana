using Mediana.Messaging;
namespace Mediana.Dispatch;

/// <summary>Message kind.</summary>
public enum HandlerKind
{
    Command,
    Query,
    Event,
    Stream,
}

/// <summary>Event dispatch policy (spec 4.3).</summary>
public enum EventDispatchPolicy
{
    /// <summary>Sequential; the first throw interrupts the chain. Default.</summary>
    Sequential,

    /// <summary>All handlers start simultaneously; errors are aggregated into AggregateException.</summary>
    Parallel,
}

/// <summary>
/// Call-site for object-dispatch (<see cref="IMediator.Send{TResponse}(Mediana.Messaging.ICommand{TResponse}, CancellationToken)"/>).
/// Advanced API: used by the generator and engine; not needed in application code.
/// </summary>
public interface IObjectCommandCallSite<TResponse>
{
    ValueTask<TResponse> Invoke(object message, IServiceProvider serviceProvider, CancellationToken cancellationToken);
}

/// <summary>Call-site with typed message (zero-boxing path, SendExact).</summary>
public interface ITypedCommandCallSite<TRequest, TResponse> where TRequest : IRequest<TResponse>
{
    ValueTask<TResponse> InvokeTyped(TRequest message, IServiceProvider serviceProvider, CancellationToken cancellationToken);
}

/// <summary>Call-site for object-dispatch.</summary>
public interface IObjectQueryCallSite<TResponse>
{
    ValueTask<TResponse> Invoke(object message, IServiceProvider serviceProvider, CancellationToken cancellationToken);
}

/// <summary>Call-site with typed message (SendExact).</summary>
public interface ITypedQueryCallSite<TRequest, TResponse> where TRequest : IRequest<TResponse>
{
    ValueTask<TResponse> InvokeTyped(TRequest message, IServiceProvider serviceProvider, CancellationToken cancellationToken);
}

/// <summary>Event call-site.</summary>
public interface IEventCallSite
{
    ValueTask Invoke(object message, IServiceProvider serviceProvider, CancellationToken cancellationToken);
}

/// <summary>
/// Non-generic hop: invoking from canon-generic context allocates (~24-32B/inin, frombut)
/// non-generic InvokeAny from a non-generic Mediator method is zero. Value responses are boxed —
/// so the object path with value responses does not use this hop (see Mediator).
/// </summary>
public interface IUntypedCallSite
{
    ValueTask<object?> InvokeAny(object message, IServiceProvider serviceProvider, CancellationToken cancellationToken);
}

/// <summary>Call-site stream query.</summary>
public interface IStreamCallSite<TRow>
{
    IAsyncEnumerable<TRow> Invoke(object message, IServiceProvider serviceProvider, CancellationToken cancellationToken);
}
