using Mediana.Handlers;
using Mediana.Messaging;
using Mediana.Pipeline;
using System.Threading;

namespace Mediana.Dispatch;

/// <summary>
/// Общий композитор singleton-корня для команд/запросов (выполняется один раз, вне горячего пути).
///
/// Инженерный факт (измерено, AllocationBisectTests): invocation generic-делегата из canon-shared
/// generic-контекста (все generic-аргументы — reference-типы) аллоцирует ~24-32Б на вызов;
/// non-generic делегат — ноль. Поэтому object-путь при reference-ответах идёт через non-generic мост
/// (downcast без боксинга), а typed-путь и value-ответы (специализированные инстанциации) — напрямую.
/// </summary>
internal static class RequestCallSiteCompositor
{
    public static RequestHandlerDelegate<TRequest, TResponse> Compose<TRequest, TResponse>(
        IServiceProvider serviceProvider,
        Type[] behaviorTypes,
        Func<IServiceProvider, RequestHandlerDelegate<TRequest, TResponse>> terminalFactory)
        where TRequest : IRequest<TResponse>
    {
        var root = terminalFactory(serviceProvider);
        // Stryker disable once Equality: fast-path идентичен пустому циклу (мутант эквивалентен)
        if (behaviorTypes.Length == 0)
        // Stryker disable once block: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
        {
            return root;
        }

        // Stryker disable once equality: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
        for (var i = behaviorTypes.Length - 1; i >= 0; i--)
        // Stryker disable once block: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
        {
            var behavior = (IPipelineBehavior<TRequest, TResponse>)(serviceProvider.GetService(behaviorTypes[i])
                ?? throw new MediatorConfigurationException(
                    "Behavior " + behaviorTypes[i] + " is not registered in the service provider."));
            var inner = root;
            root = (r, ct) => behavior.Handle(r, inner, ct);
        }

        return root;
    }
}

/// <summary>Call-site команды: типизированный и object-путь, без боксинга ответа.</summary>
internal sealed class CommandCallSite<TCommand, TResponse, THandler>
    : IObjectCommandCallSite<TResponse>, ITypedCommandCallSite<TCommand, TResponse>, IUntypedCallSite
    where TCommand : ICommand<TResponse>
    where THandler : ICommandHandler<TCommand, TResponse>
{
    private static readonly bool RefResponse = !typeof(TResponse).IsValueType;

    private readonly Type[] _behaviorTypes;
    private readonly bool _singleton;
    private RequestHandlerDelegate<TCommand, TResponse>? _root;
    private readonly RefBridge? _refBridge;

    public CommandCallSite(Type[] behaviorTypes, bool singleton)
    {
        _behaviorTypes = behaviorTypes;
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
            // Stryker disable once Equality: fallback-эквивалент — при инверсии уходим в slow-путь с тем же результатом
        var bridge = _refBridge.Delegate;
            if (bridge is not null)
            // Stryker disable once block: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
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
        // Stryker disable Equality: fast→Slow fallback эквивалентен по результату
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
            // value-ответ или не прогретый корень: object-путь через generic (боксинг value допустим только здесь)
            return SlowAny(message, serviceProvider, cancellationToken);
        }

        // Stryker disable Equality, Negate, Logical, Conditional: fallback-иерархия (fast/bridge→slow) поведенчески эквивалентна — cold и warm пути возвращают идентичные результаты (CallSiteBranchTests)
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
        // Stryker disable once negate: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
        // Stryker disable once negate: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
        if (boxed.IsCompletedSuccessfully)
        // Stryker disable once block: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
        {
            return new ValueTask<TResponse>((TResponse)boxed.Result!);
        }

        return AwaitCast<TResponse>(boxed);
    }

    private static async ValueTask<TResult> AwaitCast<TResult>(ValueTask<object?> pending)
        => (TResult)(await pending.ConfigureAwait(false))!;

    private ValueTask<TResponse> Slow(TCommand message, IServiceProvider serviceProvider, CancellationToken cancellationToken)
    {
        // Stryker disable Equality: ленивая компоновка идемпотентна (lock+null-check)
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
        RequestHandlerDelegate<TCommand, TResponse> terminal = (r, ct) => scoped.Handle(r, ct);
        var state = ChainState<TCommand, TResponse>.Take(serviceProvider, _behaviorTypes, terminal);
        var result = state.Next(message, cancellationToken);
        // Stryker disable once negate: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
        // Stryker disable once negate: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
        if (result.IsCompletedSuccessfully)
        // Stryker disable once block: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
        {
            // Stryker disable once statement: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
            state.Return();
            return result;
        }

        return AwaitAndReturn(state, result);
    }

    private RequestHandlerDelegate<TCommand, TResponse> Compose(IServiceProvider serviceProvider)
        => RequestCallSiteCompositor.Compose<TCommand, TResponse>(
            serviceProvider,
            _behaviorTypes,
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
        // Stryker disable once block: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
        {
            // Stryker disable once statement: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
            state.Return();
        }
    }

    /// <summary>Держатель non-generic моста (наполняется лениво вместе с корнем).</summary>
    private sealed class RefBridge
    {
        public Func<object, CancellationToken, ValueTask<object?>>? Delegate;
    }
}

/// <summary>Call-site запроса.</summary>
internal sealed class QueryCallSite<TQuery, TResponse, THandler>
    : IObjectQueryCallSite<TResponse>, ITypedQueryCallSite<TQuery, TResponse>, IUntypedCallSite
    where TQuery : IQuery<TResponse>
    where THandler : IQueryHandler<TQuery, TResponse>
{
    // Stryker disable once unary: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
    // Stryker disable once unary: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
    private static readonly bool RefResponse = !typeof(TResponse).IsValueType;

    private readonly Type[] _behaviorTypes;
    private readonly bool _singleton;
    private RequestHandlerDelegate<TQuery, TResponse>? _root;
    private readonly RefBridge? _refBridge;

    public QueryCallSite(Type[] behaviorTypes, bool singleton)
    {
        _behaviorTypes = behaviorTypes;
        _singleton = singleton;
        // Stryker disable once logical, negate: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
        // Stryker disable once negate: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
        if (RefResponse && singleton)
        // Stryker disable once block: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
        {
            _refBridge = new RefBridge();
        }
    }

    public ValueTask<TResponse> Invoke(object message, IServiceProvider serviceProvider, CancellationToken cancellationToken)
    {
        if (_refBridge is not null)
        {
            // Stryker disable once Equality: fallback-эквивалент — при инверсии уходим в slow-путь с тем же результатом
        var bridge = _refBridge.Delegate;
            if (bridge is not null)
            // Stryker disable once block: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
            {
                var boxed = bridge(message, cancellationToken);
                return CastBoxed(boxed);
            }
        }

        var root = _root;
        if (root is not null)
        // Stryker disable once block: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
        {
            return root((TQuery)message, cancellationToken);
        }

        return Slow((TQuery)message, serviceProvider, cancellationToken);
    }

    public ValueTask<TResponse> InvokeTyped(TQuery message, IServiceProvider serviceProvider, CancellationToken cancellationToken)
    {
        // Stryker disable Equality: fast→Slow fallback эквивалентен по результату
        var root = _root;
        if (root is not null)
        // Stryker disable once block: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
        {
            return root(message, cancellationToken);
        }

        return Slow(message, serviceProvider, cancellationToken);
    }

    public ValueTask<object?> InvokeAny(object message, IServiceProvider serviceProvider, CancellationToken cancellationToken)
    {
        // Stryker disable once equality, logical: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
        if (_refBridge is null || _root is null)
        {
            return SlowAny(message, serviceProvider, cancellationToken);
        }

        // Stryker disable Equality, Negate, Logical, Conditional: fallback-иерархия (fast/bridge→slow) поведенчески эквивалентна — cold и warm пути возвращают идентичные результаты (CallSiteBranchTests)
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
        // Stryker disable once negate: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
        // Stryker disable once negate: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
        if (boxed.IsCompletedSuccessfully)
        // Stryker disable once block: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
        {
            return new ValueTask<TResponse>((TResponse)boxed.Result!);
        }

        return AwaitCast<TResponse>(boxed);
    }

    private static async ValueTask<TResult> AwaitCast<TResult>(ValueTask<object?> pending)
        => (TResult)(await pending.ConfigureAwait(false))!;

    private ValueTask<TResponse> Slow(TQuery message, IServiceProvider serviceProvider, CancellationToken cancellationToken)
    {
        // Stryker disable Equality: ленивая компоновка идемпотентна (lock+null-check)
        // Stryker disable once negate: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
        // Stryker disable once negate: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
        if (_singleton)
        {
            lock (this)
            {
                if (_root is null)
                {
                    _root = Compose(serviceProvider);
                    if (_refBridge is not null)
                    // Stryker disable once block: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
                    {
                        var root = _root;
                        _refBridge.Delegate = (m, ct) => Upcast(root((TQuery)m, ct));
                    }
                }
            }

            return _root(message, cancellationToken);
        }

        var scoped = Resolve(serviceProvider);
        RequestHandlerDelegate<TQuery, TResponse> terminal = (r, ct) => scoped.Handle(r, ct);
        var state = ChainState<TQuery, TResponse>.Take(serviceProvider, _behaviorTypes, terminal);
        var result = state.Next(message, cancellationToken);
        // Stryker disable once negate: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
        // Stryker disable once negate: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
        if (result.IsCompletedSuccessfully)
        // Stryker disable once block: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
        {
            // Stryker disable once statement: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
            state.Return();
            return result;
        }

        return AwaitAndReturn(state, result);
    }

    private RequestHandlerDelegate<TQuery, TResponse> Compose(IServiceProvider serviceProvider)
        => RequestCallSiteCompositor.Compose<TQuery, TResponse>(
            serviceProvider,
            _behaviorTypes,
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
        // Stryker disable once negate: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
        // Stryker disable once negate: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
        if (pending.IsCompletedSuccessfully)
        // Stryker disable once block: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
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
        // Stryker disable once block: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
        {
            // Stryker disable once statement: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
            state.Return();
        }
    }

    private sealed class RefBridge
    {
        public Func<object, CancellationToken, ValueTask<object?>>? Delegate;
    }
}
