using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Mediana.Dispatch;
using Mediana.Handlers;
using Mediana.Internal;
using Mediana.Messaging;

namespace Mediana;

/// <summary>
/// in-process (§5 )
/// Lookup → call-site
/// Send
/// </summary>
public sealed class Mediator : IMediator
{
    private readonly MessageRegistry _registry;
    private readonly IServiceProvider _serviceProvider;

    public Mediator(MessageRegistry registry, IServiceProvider serviceProvider)
    {
        _registry = registry;
        _serviceProvider = serviceProvider;
    }

    /// <summary>.</summary>
    public MessageRegistry Registry => _registry;

    /// <summary>
    /// (copy-on-write runtime-, §5.2)
    /// See English documentation.
    /// </summary>
    public Mediator WithRegistry(MessageRegistry updated)
        => new(updated, _serviceProvider);

    public ValueTask<TResponse> Send<TResponse>(ICommand<TResponse> command, CancellationToken cancellationToken = default)
    {
        Guard.NotNull(command, nameof(command));
        var entry = _registry.TryGet(command.GetType()) ?? ThrowNoHandler(command.GetType());

        if (ValueTypeResponse<TResponse>.Value)
        {
            // value-: typed-
            if (entry.CommandCallSite is IObjectCommandCallSite<TResponse> typed)
            {
                return typed.Invoke(command, _serviceProvider, cancellationToken);
            }

            return ThrowResponseTypeMismatch<TResponse>(entry, typeof(TResponse));
        }

        // ref-: non-generic static (canon-generic invoke —
        // canon → non-generic static → interface = , . PublishSequential)
        if (entry.ResponseType != typeof(TResponse))
        {
            return ThrowResponseTypeMismatch<TResponse>(entry, typeof(TResponse));
        }

        if (entry.CommandCallSite is IUntypedCallSite any)
        {
            return CastBack<TResponse>(UntypedCommandHop(any, command, _serviceProvider, cancellationToken));
        }

        return ThrowResponseTypeMismatch<TResponse>(entry, typeof(TResponse));
    }

    private static ValueTask<object?> UntypedCommandHop(
        IUntypedCallSite callSite, object message, IServiceProvider serviceProvider, CancellationToken cancellationToken)
        => callSite.InvokeAny(message, serviceProvider, cancellationToken);

    public ValueTask<TResponse> Send<TResponse>(IQuery<TResponse> query, CancellationToken cancellationToken = default)
    {
        Guard.NotNull(query, nameof(query));
        var entry = _registry.TryGet(query.GetType()) ?? ThrowNoHandler(query.GetType());

        // Stryker disable once negate: fallback/perf-(. CallSiteBranchTests: fast/slow )
        // Stryker disable once negate: fallback/perf-(. CallSiteBranchTests: fast/slow )
        if (ValueTypeResponse<TResponse>.Value)
        {
            if (entry.QueryCallSite is IObjectQueryCallSite<TResponse> typed)
            {
                return typed.Invoke(query, _serviceProvider, cancellationToken);
            }

            return ThrowResponseTypeMismatch<TResponse>(entry, typeof(TResponse));
        }

        if (entry.ResponseType != typeof(TResponse))
        {
            return ThrowResponseTypeMismatch<TResponse>(entry, typeof(TResponse));
        }

        if (entry.QueryCallSite is IUntypedCallSite any)
        {
            return CastBack<TResponse>(UntypedQueryHop(any, query, _serviceProvider, cancellationToken));
        }

        return ThrowResponseTypeMismatch<TResponse>(entry, typeof(TResponse));
    }

    private static ValueTask<object?> UntypedQueryHop(
        IUntypedCallSite callSite, object message, IServiceProvider serviceProvider, CancellationToken cancellationToken)
        => callSite.InvokeAny(message, serviceProvider, cancellationToken);

    public ValueTask Publish<TEvent>(TEvent @event, CancellationToken cancellationToken = default)
        where TEvent : IEvent
    {
        Guard.NotNull(@event, nameof(@event));
        var entry = _registry.TryGet(@event.GetType());
        if (entry is null)
        {
            // no-op (MediatR)
            return default;
        }

        var callSites = System.Runtime.CompilerServices.Unsafe.As<IEventCallSite[]>(entry.EventCallSites);
        if (callSites.Length == 0)
        // Stryker disable once block: fallback/perf-(. CallSiteBranchTests: fast/slow )
        {
            return default;
        }

        return entry.Policy == EventDispatchPolicy.Parallel
            ? PublishParallel(callSites, @event, _serviceProvider, cancellationToken)
            : PublishSequential(callSites, @event, _serviceProvider, cancellationToken);
    }

    public IAsyncEnumerable<TRow> Stream<TRow>(IStreamQuery<TRow> query, CancellationToken cancellationToken = default)
    {
        Guard.NotNull(query, nameof(query));
        var entry = _registry.TryGet(query.GetType()) ?? ThrowNoHandler(query.GetType());
        if (entry.StreamCallSite is IStreamCallSite<TRow> callSite)
        {
            return callSite.Invoke(query, _serviceProvider, cancellationToken);
        }

        return ThrowStreamMismatch<TRow>(entry);
    }

    public ValueTask<TResponse> SendExact<TRequest, TResponse>(TRequest request, CancellationToken cancellationToken = default)
        where TRequest : IRequest<TResponse>
    {
        // Guard struct-:
        if (ReferenceTypeFlag<TRequest>.Value && request is null)
        {
            Guard.ThrowNull(nameof(request));
        }

        var entry = _registry.TryGet(typeof(TRequest)) ?? ThrowNoHandler(typeof(TRequest));

        if (entry.CommandCallSite is ITypedCommandCallSite<TRequest, TResponse> commandCallSite)
        {
            return commandCallSite.InvokeTyped(request, _serviceProvider, cancellationToken);
        }

        if (entry.QueryCallSite is ITypedQueryCallSite<TRequest, TResponse> queryCallSite)
        {
            return queryCallSite.InvokeTyped(request, _serviceProvider, cancellationToken);
        }

        return ThrowResponseTypeMismatch<TResponse>(entry, typeof(TResponse));
    }

    private static async ValueTask PublishSequential(
        IEventCallSite[] callSites,
        object @event,
        IServiceProvider serviceProvider,
        CancellationToken cancellationToken)
    {
        for (var i = 0; i < callSites.Length; i++)
        {
            await callSites[i].Invoke(@event, serviceProvider, cancellationToken).ConfigureAwait(false);
        }
    }

    private static async ValueTask PublishParallel(
        IEventCallSite[] callSites,
        object @event,
        IServiceProvider serviceProvider,
        CancellationToken cancellationToken)
    {
        var pending = new ValueTask[callSites.Length];
        List<Exception>? errors = null;
        for (var i = 0; i < callSites.Length; i++)
        {
            try
            {
                pending[i] = callSites[i].Invoke(@event, serviceProvider, cancellationToken);
            }
            catch (Exception ex)
            {
                // /behavior — (§4.3)
                (errors ??= []).Add(ex);
            }
        }

        // (Invoke )
        // See English documentation.
        for (var i = 0; i < pending.Length; i++)
        {
            try
            {
                await pending[i].ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                (errors ??= []).Add(ex);
            }
        }

        if (errors is not null)
        {
            throw new AggregateException(errors);
        }
    }

    [DoesNotReturn]
    private static MessageEntry ThrowNoHandler(Type messageType)
        => throw new MediatorConfigurationException(
            $"No handler registered for message type {messageType}. " +
            "Register it via AddMediana(cfg => cfg.AddCommandHandler<...>/AddQueryHandler<...>/...) " +
            "or apply the Mediana.Generators registrar.");

    [DoesNotReturn]
    private static ValueTask<TResponse> ThrowResponseTypeMismatch<TResponse>(MessageEntry entry, Type requested)
        => throw new MediatorConfigurationException(
            $"Message {entry.MessageType} is registered with response type {entry.ResponseType} " +
            $"but was sent expecting {requested}.");

    [DoesNotReturn]
    private static IAsyncEnumerable<TRow> ThrowStreamMismatch<TRow>(MessageEntry entry)
        => throw new MediatorConfigurationException(
            $"Message {entry.MessageType} is not a stream query for row type {typeof(TRow)}.");

    private static ValueTask<TResponse> CastBack<TResponse>(ValueTask<object?> boxed)
    {
        if (boxed.IsCompletedSuccessfully)
        {
            return new ValueTask<TResponse>((TResponse)boxed.Result!);
        }

        return AwaitCastBack<TResponse>(boxed);
    }

    private static async ValueTask<TResult> AwaitCastBack<TResult>(ValueTask<object?> pending)
        => (TResult)(await pending.ConfigureAwait(false))!;

    /// <summary>(guard-).</summary>
    private static class ReferenceTypeFlag<T>
    {
        public static readonly bool Value = !typeof(T).IsValueType;
    }

    /// <summary>value-type (: typed vs untyped hop).</summary>
    private static class ValueTypeResponse<T>
    {
        public static readonly bool Value = typeof(T).IsValueType;
    }
}
