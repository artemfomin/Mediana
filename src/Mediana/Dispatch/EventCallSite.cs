using Mediana.Handlers;
using Mediana.Messaging;
using Mediana.Pipeline;

namespace Mediana.Dispatch;

/// <summary>
/// Call-site хендлера события: цепочка event-behaviors вокруг терминала.
/// Singleton-режим — композированная один раз цепочка (ноль DI-обращений и аллокаций на вызов);
/// scoped — резолв behaviors на вызов + статическая рекурсия цепочки.
/// </summary>
internal sealed class EventCallSite<TEvent, THandler>
    : IEventCallSite
    where TEvent : IEvent
    where THandler : IEventHandler<TEvent>
{
    private readonly Type[] _behaviorTypes;
    private readonly bool _singleton;
    private EventHandlerDelegate<TEvent>? _singletonRoot;
    // Non-generic bridge: вызов generic-делегата из canon-shared generic-контекста аллоцирует; Func — нет.
    private Func<object, IServiceProvider, CancellationToken, ValueTask>? _bridge;
    private readonly object _singletonLock = new();

    public EventCallSite(Type[] behaviorTypes, bool singleton)
    {
        _behaviorTypes = behaviorTypes;
        _singleton = singleton;
    }

    public ValueTask Invoke(object message, IServiceProvider serviceProvider, CancellationToken cancellationToken)
    {
        var bridge = _bridge;
        if (bridge is not null)
        {
            return bridge(message, serviceProvider, cancellationToken);
        }

        return SlowInvoke(message, serviceProvider, cancellationToken);
    }

    private ValueTask SlowInvoke(object message, IServiceProvider serviceProvider, CancellationToken cancellationToken)
    {
        var @event = (TEvent)message;

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

    private IEventPipelineBehavior<TEvent>[] ResolveBehaviors(IServiceProvider serviceProvider)
    {
        if (_behaviorTypes.Length == 0)
        {
            return [];
        }

        var behaviors = new IEventPipelineBehavior<TEvent>[_behaviorTypes.Length];
        for (var i = 0; i < _behaviorTypes.Length; i++)
        {
            behaviors[i] = (IEventPipelineBehavior<TEvent>)(serviceProvider.GetService(_behaviorTypes[i])
                ?? throw new MediatorConfigurationException(
                    $"Event behavior {_behaviorTypes[i]} is not registered in the service provider."));
        }

        return behaviors;
    }

    private static ValueTask RunChain(
        TEvent @event,
        IEventPipelineBehavior<TEvent>[] behaviors,
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
        => _singletonRoot ?? BuildSingletonRoot(serviceProvider);

    private EventHandlerDelegate<TEvent> BuildSingletonRoot(IServiceProvider serviceProvider)
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

            if (_behaviorTypes.Length > 0)
            {
                var behaviors = new IEventPipelineBehavior<TEvent>[_behaviorTypes.Length];
                for (var i = _behaviorTypes.Length - 1; i >= 0; i--)
                {
                    behaviors[i] = (IEventPipelineBehavior<TEvent>)(serviceProvider.GetService(_behaviorTypes[i])
                        ?? throw new MediatorConfigurationException(
                            $"Event behavior {_behaviorTypes[i]} is not registered in the service provider."));
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
