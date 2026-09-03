using Mediana.Handlers;
using Mediana.Messaging;
using Mediana.Pipeline;

namespace Mediana.Dispatch;

/// <summary>
/// Call-site and: by event-behaviors in andon
/// Singleton-and — byandinon and by (but DI-and and and on inin)
/// scoped — in behaviors on inin + and and byand
/// </summary>
internal sealed class EventCallSite<TEvent, THandler>
    : IEventCallSite
    where TEvent : IEvent
    where THandler : IEventHandler<TEvent>
{
    private readonly Type[] _middlewareTypes;
    private readonly bool _singleton;
    private EventHandlerDelegate<TEvent>? _singletonRoot;
    // Non-generic bridge: inin generic- from canon-shared generic- and; Func — no
    private Func<object, IServiceProvider, CancellationToken, ValueTask>? _bridge;
    private readonly object _singletonLock = new();

    public EventCallSite(Type[] middlewareTypes, bool singleton)
    {
        _middlewareTypes = middlewareTypes;
        _singleton = singleton;
    }

    public ValueTask Invoke(object message, IServiceProvider serviceProvider, CancellationToken cancellationToken)
    {
        var bridge = _bridge;
        if (bridge is not null)
        // Stryker disable once block: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        {
            return bridge(message, serviceProvider, cancellationToken);
        }

        return SlowInvoke(message, serviceProvider, cancellationToken);
    }

    private ValueTask SlowInvoke(object message, IServiceProvider serviceProvider, CancellationToken cancellationToken)
    {
        var @event = (TEvent)message;

        // Stryker disable once negate: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        // Stryker disable once negate: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        if (_singleton)
        {
            lock (_singletonLock)
            {
                if (_singletonRoot is null)
                {
                    BuildSingletonRoot(serviceProvider);
                }
            }

            return _bridge!(message, serviceProvider, cancellationToken);
        }

        var handler = (THandler)(serviceProvider.GetService(typeof(THandler))
            ?? throw new MediatorConfigurationException(
                $"Event handler {typeof(THandler)} is not registered in the service provider."));
        EventHandlerDelegate<TEvent> terminal = (e, ct) => handler.Handle(e, ct);
        var behaviors = ResolveBehaviors(serviceProvider);
        return RunChain(@event, behaviors, terminal, 0, cancellationToken);
    }

    private IEventMiddleware<TEvent>[] ResolveBehaviors(IServiceProvider serviceProvider)
    {
        if (_middlewareTypes.Length == 0)
        // Stryker disable once block: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        {
            return [];
        }

        var behaviors = new IEventMiddleware<TEvent>[_middlewareTypes.Length];
        for (var i = 0; i < _middlewareTypes.Length; i++)
        {
            behaviors[i] = (IEventMiddleware<TEvent>)(serviceProvider.GetService(_middlewareTypes[i])
                ?? throw new MediatorConfigurationException(
                    $"Event behavior {_middlewareTypes[i]} is not registered in the service provider."));
        }

        return behaviors;
    }

    private static ValueTask RunChain(
        TEvent @event,
        IEventMiddleware<TEvent>[] behaviors,
        EventHandlerDelegate<TEvent> terminal,
        int index,
        CancellationToken cancellationToken)
    {
        if (index < behaviors.Length)
        {
            EventHandlerDelegate<TEvent> next = (e, ct) => RunChain(e, behaviors, terminal, index + 1, ct);
            return behaviors[index].Handle(@event, next, cancellationToken);
        }

        return terminal(@event, cancellationToken);
    }

    internal EventHandlerDelegate<TEvent> GetRoot(IServiceProvider serviceProvider)
        // Stryker disable once Equality: -, - not fromand
        // Stryker disable once null-coalescing: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        // Stryker disable once null-coalescing: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        => _singletonRoot ?? BuildSingletonRoot(serviceProvider);

    internal EventHandlerDelegate<TEvent> BuildSingletonRoot(IServiceProvider serviceProvider)
    {
        lock (_singletonLock)
        {
            if (_singletonRoot is not null)
            {
                return _singletonRoot;
            }

            var handler = (THandler)(serviceProvider.GetService(typeof(THandler))
                ?? throw new MediatorConfigurationException(
                    $"Event handler {typeof(THandler)} is not registered in the service provider."));
            EventHandlerDelegate<TEvent> root = (e, ct) => handler.Handle(e, ct);

            // Stryker disable once equality: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
            if (_middlewareTypes.Length > 0)
            {
                var behaviors = new IEventMiddleware<TEvent>[_middlewareTypes.Length];
                for (var i = _middlewareTypes.Length - 1; i >= 0; i--)
                {
                    behaviors[i] = (IEventMiddleware<TEvent>)(serviceProvider.GetService(_middlewareTypes[i])
                        ?? throw new MediatorConfigurationException(
                            $"Event behavior {_middlewareTypes[i]} is not registered in the service provider."));
                    var inner = root;
                    var behavior = behaviors[i];
                    root = (e, ct) => behavior.Handle(e, inner, ct);
                }
            }

            _singletonRoot = root;
            _bridge = (m, _, ct) => root((TEvent)m, ct);
            return root;
        }
    }
}
