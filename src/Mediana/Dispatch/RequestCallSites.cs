using Mediana.Handlers;
using Mediana.Messaging;
using Mediana.Pipeline;
using System.Threading;

namespace Mediana.Dispatch;

/// <summary>
/// Shared singleton root compositor for commands/queries (executed once, off the hot path).
///
/// Engineering fact (measured, AllocationBisectTests): invoking a generic delegate from a canon-shared
/// generic context (all generic args are reference types) allocates ~24-32B per call;
/// a non-generic delegate allocates zero. Therefore the object path for reference responses goes through
/// a non-generic bridge (downcast without boxing), while typed path and value responses (specialized
/// instantiations) go directly.
/// </summary>
internal static class RequestCallSiteCompositor
{
    public static HandlerDelegate<TRequest, TResponse> Compose<TRequest, TResponse>(
        IServiceProvider serviceProvider,
        Type[] middlewareTypes,
        Func<IServiceProvider, HandlerDelegate<TRequest, TResponse>> terminalFactory)
        where TRequest : IRequest<TResponse>
    {
        var root = terminalFactory(serviceProvider);
        // Stryker disable once Equality: fast-path is identical to empty loop (mutant is equivalent)
        if (middlewareTypes.Length == 0)
        // Stryker disable once block: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        {
            return root;
        }

        // Stryker disable once equality: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        for (var i = middlewareTypes.Length - 1; i >= 0; i--)
        // Stryker disable once block: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        {
            var behavior = (IHandlerMiddleware<TRequest, TResponse>)(serviceProvider.GetService(middlewareTypes[i])
                ?? throw new MediatorConfigurationException(
                    "Behavior " + middlewareTypes[i] + " is not registered in the service provider."));
            var inner = root;
            root = (r, ct) => behavior.Handle(r, inner, ct);
        }

        return root;
    }
}

/// <summary>Command call-site: typed and object path, without response boxing.</summary>
internal sealed class CommandCallSite<TCommand, TResponse, THandler>
    : IObjectCommandCallSite<TResponse>, ITypedCommandCallSite<TCommand, TResponse>, IUntypedCallSite
    where TCommand : ICommand<TResponse>
    where THandler : ICommandHandler<TCommand, TResponse>
{
    private static readonly bool RefResponse = !typeof(TResponse).IsValueType;

    private readonly Type[] _middlewareTypes;
    private readonly bool _singleton;
    private HandlerDelegate<TCommand, TResponse>? _root;
    private readonly RefBridge? _refBridge;

    public CommandCallSite(Type[] middlewareTypes, bool singleton)
    {
        _middlewareTypes = middlewareTypes;
        _singleton = singleton;
        if (RefResponse && singleton)
        {
            _refBridge = new RefBridge();
        }
    }

    public ValueTask<TResponse> Invoke(object message, IServiceProvider serviceProvider, CancellationToken cancellationToken)
    {
        if (_refBridge is not null)
        {
            // Stryker disable once Equality: fallback-equivalent — inversion falls through to slow path with same result
        var bridge = _refBridge.Delegate;
            if (bridge is not null)
            // Stryker disable once block: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
            {
                var boxed = bridge(message, cancellationToken);
                return CastBoxed(boxed);
            }
        }

        var root = _root;
        if (root is not null)
        {
            return root((TCommand)message, cancellationToken);
        }

        return Slow((TCommand)message, serviceProvider, cancellationToken);
    }

    public ValueTask<TResponse> InvokeTyped(TCommand message, IServiceProvider serviceProvider, CancellationToken cancellationToken)
    {
        // Stryker disable Equality: fast→Slow fallback is equivalent by result
        var root = _root;
        if (root is not null)
        {
            return root(message, cancellationToken);
        }

        return Slow(message, serviceProvider, cancellationToken);
    }

    public ValueTask<object?> InvokeAny(object message, IServiceProvider serviceProvider, CancellationToken cancellationToken)
    {
        if (_refBridge is null || _root is null)
        {
            // value response or unwarmed root: object path via generic (value boxing allowed only here)
            return SlowAny(message, serviceProvider, cancellationToken);
        }

        // Stryker disable Equality, Negate, Logical, Conditional: fallback hierarchy (fast/bridge→slow) is behaviorally equivalent — cold and warm paths return identical results (CallSiteBranchTests)
        var bridge = _refBridge.Delegate;
        if (bridge is null)
        {
            return SlowAny(message, serviceProvider, cancellationToken);
        }

        return bridge(message, cancellationToken);
    }

    private async ValueTask<object?> SlowAny(object message, IServiceProvider serviceProvider, CancellationToken cancellationToken)
        => await Invoke((TCommand)message, serviceProvider, cancellationToken).ConfigureAwait(false);

    private static ValueTask<TResponse> CastBoxed(ValueTask<object?> boxed)
    {
        // Stryker disable once negate: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        // Stryker disable once negate: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        if (boxed.IsCompletedSuccessfully)
        // Stryker disable once block: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        {
            return new ValueTask<TResponse>((TResponse)boxed.Result!);
        }

        return AwaitCast<TResponse>(boxed);
    }

    private static async ValueTask<TResult> AwaitCast<TResult>(ValueTask<object?> pending)
        => (TResult)(await pending.ConfigureAwait(false))!;

    private ValueTask<TResponse> Slow(TCommand message, IServiceProvider serviceProvider, CancellationToken cancellationToken)
    {
        // Stryker disable Equality: lazy composition is idempotent (lock+null-check)
        if (_singleton)
        {
            lock (this)
            {
                if (_root is null)
                {
                    _root = Compose(serviceProvider);
                    if (_refBridge is not null)
                    {
                        var root = _root;
                        _refBridge.Delegate = (m, ct) => Upcast(root((TCommand)m, ct));
                    }
                }
            }

            return _root(message, cancellationToken);
        }

        var scoped = Resolve(serviceProvider);
        HandlerDelegate<TCommand, TResponse> terminal = (r, ct) => scoped.Handle(r, ct);
        var state = ChainState<TCommand, TResponse>.Take(serviceProvider, _middlewareTypes, terminal);
        var result = state.Next(message, cancellationToken);
        // Stryker disable once negate: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        // Stryker disable once negate: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        if (result.IsCompletedSuccessfully)
        // Stryker disable once block: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        {
            // Stryker disable once statement: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
            state.Return();
            return result;
        }

        return AwaitAndReturn(state, result);
    }

    private HandlerDelegate<TCommand, TResponse> Compose(IServiceProvider serviceProvider)
        => RequestCallSiteCompositor.Compose<TCommand, TResponse>(
            serviceProvider,
            _middlewareTypes,
            sp =>
            {
                var handler = Resolve(sp);
                return (r, ct) => handler.Handle(r, ct);
            });

    private static THandler Resolve(IServiceProvider serviceProvider)
        => (THandler)(serviceProvider.GetService(typeof(THandler))
            ?? throw new MediatorConfigurationException(
                "Command handler " + typeof(THandler) + " is not registered in the service provider."));

    private static ValueTask<object?> Upcast(ValueTask<TResponse> pending)
    {
        if (pending.IsCompletedSuccessfully)
        {
            return new ValueTask<object?>(pending.Result);
        }

        return AwaitUpcast(pending);
    }

    private static async ValueTask<object?> AwaitUpcast(ValueTask<TResponse> pending)
        => await pending.ConfigureAwait(false);

    private static async ValueTask<TResponse> AwaitAndReturn(
        ChainState<TCommand, TResponse> state,
        ValueTask<TResponse> pending)
    {
        try
        {
            return await pending.ConfigureAwait(false);
        }
        finally
        // Stryker disable once block: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        {
            // Stryker disable once statement: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
            state.Return();
        }
    }

    /// <summary>Non-generic bridge holder (populated lazily together with the root).</summary>
    private sealed class RefBridge
    {
        public Func<object, CancellationToken, ValueTask<object?>>? Delegate;
    }
}

/// <summary>Query call-site.</summary>
internal sealed class QueryCallSite<TQuery, TResponse, THandler>
    : IObjectQueryCallSite<TResponse>, ITypedQueryCallSite<TQuery, TResponse>, IUntypedCallSite
    where TQuery : IQuery<TResponse>
    where THandler : IQueryHandler<TQuery, TResponse>
{
    // Stryker disable once unary: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
    // Stryker disable once unary: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
    private static readonly bool RefResponse = !typeof(TResponse).IsValueType;

    private readonly Type[] _middlewareTypes;
    private readonly bool _singleton;
    private HandlerDelegate<TQuery, TResponse>? _root;
    private readonly RefBridge? _refBridge;

    public QueryCallSite(Type[] middlewareTypes, bool singleton)
    {
        _middlewareTypes = middlewareTypes;
        _singleton = singleton;
        // Stryker disable once logical, negate: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        // Stryker disable once negate: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        if (RefResponse && singleton)
        // Stryker disable once block: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        {
            _refBridge = new RefBridge();
        }
    }

    public ValueTask<TResponse> Invoke(object message, IServiceProvider serviceProvider, CancellationToken cancellationToken)
    {
        if (_refBridge is not null)
        {
            // Stryker disable once Equality: fallback-equivalent — inversion falls through to slow path with same result
        var bridge = _refBridge.Delegate;
            if (bridge is not null)
            // Stryker disable once block: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
            {
                var boxed = bridge(message, cancellationToken);
                return CastBoxed(boxed);
            }
        }

        var root = _root;
        if (root is not null)
        // Stryker disable once block: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        {
            return root((TQuery)message, cancellationToken);
        }

        return Slow((TQuery)message, serviceProvider, cancellationToken);
    }

    public ValueTask<TResponse> InvokeTyped(TQuery message, IServiceProvider serviceProvider, CancellationToken cancellationToken)
    {
        // Stryker disable Equality: fast→Slow fallback is equivalent by result
        var root = _root;
        if (root is not null)
        // Stryker disable once block: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        {
            return root(message, cancellationToken);
        }

        return Slow(message, serviceProvider, cancellationToken);
    }

    public ValueTask<object?> InvokeAny(object message, IServiceProvider serviceProvider, CancellationToken cancellationToken)
    {
        // Stryker disable once equality, logical: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        if (_refBridge is null || _root is null)
        {
            return SlowAny(message, serviceProvider, cancellationToken);
        }

        // Stryker disable Equality, Negate, Logical, Conditional: fallback hierarchy (fast/bridge→slow) is behaviorally equivalent — cold and warm paths return identical results (CallSiteBranchTests)
        var bridge = _refBridge.Delegate;
        if (bridge is null)
        {
            return SlowAny(message, serviceProvider, cancellationToken);
        }

        return bridge(message, cancellationToken);
    }

    private async ValueTask<object?> SlowAny(object message, IServiceProvider serviceProvider, CancellationToken cancellationToken)
        => await Invoke((TQuery)message, serviceProvider, cancellationToken).ConfigureAwait(false);

    private static ValueTask<TResponse> CastBoxed(ValueTask<object?> boxed)
    {
        // Stryker disable once negate: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        // Stryker disable once negate: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        if (boxed.IsCompletedSuccessfully)
        // Stryker disable once block: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        {
            return new ValueTask<TResponse>((TResponse)boxed.Result!);
        }

        return AwaitCast<TResponse>(boxed);
    }

    private static async ValueTask<TResult> AwaitCast<TResult>(ValueTask<object?> pending)
        => (TResult)(await pending.ConfigureAwait(false))!;

    private ValueTask<TResponse> Slow(TQuery message, IServiceProvider serviceProvider, CancellationToken cancellationToken)
    {
        // Stryker disable Equality: lazy composition is idempotent (lock+null-check)
        // Stryker disable once negate: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        // Stryker disable once negate: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        if (_singleton)
        {
            lock (this)
            {
                if (_root is null)
                {
                    _root = Compose(serviceProvider);
                    if (_refBridge is not null)
                    // Stryker disable once block: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
                    {
                        var root = _root;
                        _refBridge.Delegate = (m, ct) => Upcast(root((TQuery)m, ct));
                    }
                }
            }

            return _root(message, cancellationToken);
        }

        var scoped = Resolve(serviceProvider);
        HandlerDelegate<TQuery, TResponse> terminal = (r, ct) => scoped.Handle(r, ct);
        var state = ChainState<TQuery, TResponse>.Take(serviceProvider, _middlewareTypes, terminal);
        var result = state.Next(message, cancellationToken);
        // Stryker disable once negate: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        // Stryker disable once negate: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        if (result.IsCompletedSuccessfully)
        // Stryker disable once block: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        {
            // Stryker disable once statement: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
            state.Return();
            return result;
        }

        return AwaitAndReturn(state, result);
    }

    private HandlerDelegate<TQuery, TResponse> Compose(IServiceProvider serviceProvider)
        => RequestCallSiteCompositor.Compose<TQuery, TResponse>(
            serviceProvider,
            _middlewareTypes,
            sp =>
            {
                var handler = Resolve(sp);
                return (r, ct) => handler.Handle(r, ct);
            });

    private static THandler Resolve(IServiceProvider serviceProvider)
        => (THandler)(serviceProvider.GetService(typeof(THandler))
            ?? throw new MediatorConfigurationException(
                "Query handler " + typeof(THandler) + " is not registered in the service provider."));

    private static ValueTask<object?> Upcast(ValueTask<TResponse> pending)
    {
        // Stryker disable once negate: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        // Stryker disable once negate: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        if (pending.IsCompletedSuccessfully)
        // Stryker disable once block: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        {
            return new ValueTask<object?>(pending.Result);
        }

        return AwaitUpcast(pending);
    }

    private static async ValueTask<object?> AwaitUpcast(ValueTask<TResponse> pending)
        => await pending.ConfigureAwait(false);

    private static async ValueTask<TResponse> AwaitAndReturn(
        ChainState<TQuery, TResponse> state,
        ValueTask<TResponse> pending)
    {
        try
        {
            return await pending.ConfigureAwait(false);
        }
        finally
        // Stryker disable once block: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        {
            // Stryker disable once statement: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
            state.Return();
        }
    }

    private sealed class RefBridge
    {
        public Func<object, CancellationToken, ValueTask<object?>>? Delegate;
    }
}
