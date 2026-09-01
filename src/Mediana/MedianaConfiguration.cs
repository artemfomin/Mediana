using System.Diagnostics.CodeAnalysis;
using Mediana.Dispatch;
using Mediana.Handlers;
using Mediana.Internal;
using Mediana.Messaging;
using Mediana.Pipeline;

namespace Mediana;

/// <summary>Lifetime-политика хендлеров (§5.4): singleton — ноль DI-обращений на вызов.</summary>
public enum HandlerLifetime
{
    /// <summary>Хендлер резолвится из текущего scope на каждый вызов (корректно для scoped-зависимостей).</summary>
    Scoped,

    /// <summary>Хендлеры регистрируются как синглтоны; цепочка композируется один раз — ноль DI-обращений и аллокаций.</summary>
    Singleton,
}

/// <summary>
/// Конфигурация графа сообщений: хендлеры, behaviors (порядок = порядок регистрации), политики событий.
/// Freeze валидирует граф (дубликаты command/query/stream хендлеров — ошибка) и строит <see cref="MessageRegistry"/>.
/// Типизированные регистрации создают call sites через закрытые generic-фабрики без рефлексии (AOT-совместимо);
/// рефлексия — только в <see cref="AddHandlersFromAssembly"/> (runtime escape hatch, не для NativeAOT).
/// </summary>
public sealed class MedianaConfiguration
{
    private readonly List<RequestRegistration> _requests = [];
    private readonly List<EventRegistration> _events = [];
    private readonly List<(Type BehaviorType, Type OpenInterface)> _behaviors = [];
    private readonly Dictionary<Type, EventDispatchPolicy> _eventPolicies = [];
    private HandlerLifetime _lifetime = HandlerLifetime.Scoped;

    /// <summary>Фабрика call-site команды/запроса/стрим-запроса: (behaviorTypes, singleton) → call-site.</summary>
    private delegate object RequestCallSiteFactory(Type[] behaviorTypes, bool singleton);

    /// <summary>Фабрика call-site события.</summary>
    private delegate IEventCallSite EventCallSiteFactory(Type[] behaviorTypes, bool singleton);

    private readonly record struct RequestRegistration(
        HandlerKind Kind, Type MessageType, Type ResponseType, Type HandlerType, RequestCallSiteFactory Factory);

    private readonly record struct EventRegistration(Type EventType, Type HandlerType, EventCallSiteFactory Factory);

    /// <summary>Хендлеры без scoped-зависимостей: синглтоны, цепочки композируются один раз (D16).</summary>
    public MedianaConfiguration UseSingletonHandlers()
    {
        _lifetime = HandlerLifetime.Singleton;
        return this;
    }

    public MedianaConfiguration AddCommandHandler<TCommand, TResponse, THandler>()
        where TCommand : ICommand<TResponse>
        where THandler : ICommandHandler<TCommand, TResponse>
    {
        _requests.Add(new RequestRegistration(
            HandlerKind.Command, typeof(TCommand), typeof(TResponse), typeof(THandler),
            (behaviors, singleton) => new CommandCallSite<TCommand, TResponse, THandler>(behaviors, singleton)));
        return this;
    }

    public MedianaConfiguration AddQueryHandler<TQuery, TResponse, THandler>()
        where TQuery : IQuery<TResponse>
        where THandler : IQueryHandler<TQuery, TResponse>
    {
        _requests.Add(new RequestRegistration(
            HandlerKind.Query, typeof(TQuery), typeof(TResponse), typeof(THandler),
            (behaviors, singleton) => new QueryCallSite<TQuery, TResponse, THandler>(behaviors, singleton)));
        return this;
    }

    public MedianaConfiguration AddStreamHandler<TQuery, TRow, THandler>()
        where TQuery : IStreamQuery<TRow>
        where THandler : IStreamHandler<TQuery, TRow>
    {
        _requests.Add(new RequestRegistration(
            HandlerKind.Stream, typeof(TQuery), typeof(TRow), typeof(THandler),
            (behaviors, _) => new StreamCallSite<TQuery, TRow, THandler>(behaviors, singleton: false)));
        return this;
    }

    public MedianaConfiguration AddEventHandler<TEvent, THandler>()
        where TEvent : IEvent
        where THandler : IEventHandler<TEvent>
    {
        _events.Add(new EventRegistration(
            typeof(TEvent), typeof(THandler),
            (behaviors, singleton) => new EventCallSite<TEvent, THandler>(behaviors, singleton)));
        return this;
    }

    /// <summary>Глобальный behaviour команд/запросов (порядок применения = порядок регистрации).</summary>
    public MedianaConfiguration AddBehavior<TRequest, TResponse, TBehavior>()
        where TRequest : IRequest<TResponse>
        where TBehavior : IPipelineBehavior<TRequest, TResponse>
    {
        _behaviors.Add((typeof(TBehavior), typeof(IPipelineBehavior<,>)));
        return this;
    }

    /// <summary>Глобальный event-behaviour (ко всем совместимым событиям).</summary>
    public MedianaConfiguration AddEventBehavior<TEvent, TBehavior>()
        where TEvent : IEvent
        where TBehavior : IEventPipelineBehavior<TEvent>
    {
        _behaviors.Add((typeof(TBehavior), typeof(IEventPipelineBehavior<>)));
        return this;
    }

    /// <summary>Глобальный stream-behaviour (ко всем совместимым стрим-запросам).</summary>
    public MedianaConfiguration AddStreamBehavior<TQuery, TRow, TBehavior>()
        where TQuery : IStreamQuery<TRow>
        where TBehavior : IStreamPipelineBehavior<TQuery, TRow>
    {
        _behaviors.Add((typeof(TBehavior), typeof(IStreamPipelineBehavior<,>)));
        return this;
    }

    /// <summary>Политика диспетчеризации события (по умолчанию Sequential).</summary>
    public MedianaConfiguration SetEventPolicy<TEvent>(EventDispatchPolicy policy) where TEvent : IEvent
    {
        _eventPolicies[typeof(TEvent)] = policy;
        return this;
    }

    /// <summary>
    /// Runtime-сканирование сборки (opt-in escape hatch, §5.2). Использует рефлексию —
    /// несовместимо с NativeAOT/trimming; для AOT используйте генератор Mediana.Generators.
    /// </summary>
    [RequiresUnreferencedCode("Assembly scanning traverses all types; use the source generator for AOT.")]
    [RequiresDynamicCode("Creates closed generic call sites at runtime; use the source generator for AOT.")]
    public MedianaConfiguration AddHandlersFromAssembly(System.Reflection.Assembly assembly)
    {
        Guard.NotNull(assembly, nameof(assembly));

        foreach (var type in assembly.GetTypes())
        {
            if (type is not { IsAbstract: false, IsInterface: false, IsGenericTypeDefinition: false })
            {
                continue;
            }

            foreach (var iface in type.GetInterfaces())
            {
                if (!iface.IsGenericType)
                {
                    continue;
                }

                var def = iface.GetGenericTypeDefinition();
                var args = iface.GetGenericArguments();
                if (def == typeof(ICommandHandler<,>))
                {
                    AddScanned(typeof(CommandCallSite<,,>), args, type, HandlerKind.Command);
                }
                else if (def == typeof(IQueryHandler<,>))
                {
                    AddScanned(typeof(QueryCallSite<,,>), args, type, HandlerKind.Query);
                }
                else if (def == typeof(IStreamHandler<,>))
                {
                    AddScanned(typeof(StreamCallSite<,,>), args, type, HandlerKind.Stream);
                }
                else if (def == typeof(IEventHandler<>))
                {
                    var callSiteType = typeof(EventCallSite<,>).MakeGenericType(args[0], type);
                    var eventType = args[0];
                    _events.Add(new EventRegistration(
                        eventType, type,
                        (behaviors, singleton) => (IEventCallSite)Activator.CreateInstance(
                            callSiteType, new object[] { behaviors, singleton })!));
                }
                else
                {
                    continue;
                }

                break;
            }
        }

        return this;
    }

    [RequiresDynamicCode("Creates closed generic call sites at runtime.")]
    private void AddScanned(Type openCallSite, Type[] args, Type handlerType, HandlerKind kind)
    {
        var messageType = args[0];
        var responseType = args[1];
        var callSiteType = openCallSite.MakeGenericType(messageType, responseType, handlerType);
        _requests.Add(new RequestRegistration(
            kind, messageType, responseType, handlerType,
            (behaviors, singleton) => Activator.CreateInstance(callSiteType, new object[] { behaviors, singleton })!));
    }

    internal MessageRegistry Freeze()
    {
        var entries = new Dictionary<Type, MessageEntry>();
        var singleton = _lifetime == HandlerLifetime.Singleton;
        var eventCallSites = new Dictionary<Type, List<IEventCallSite>>();

        foreach (var (kind, messageType, responseType, _, factory) in _requests)
        {
            var entry = EnsureEntry(entries, messageType, kind, responseType);
            var behaviorTypes = CollectBehaviorTypes(messageType, responseType, kind);
            var callSite = factory(behaviorTypes, singleton);

            switch (kind)
            {
                case HandlerKind.Command:
                    if (entry.CommandCallSite is not null)
                    {
                        throw new MediatorConfigurationException(
                            $"Duplicate command handler for {messageType}: a command must have exactly one handler.");
                    }

                    entry.CommandCallSite = callSite;
                    break;
                case HandlerKind.Query:
                    if (entry.QueryCallSite is not null)
                    {
                        throw new MediatorConfigurationException(
                            $"Duplicate query handler for {messageType}: a query must have exactly one handler.");
                    }

                    entry.QueryCallSite = callSite;
                    break;
                case HandlerKind.Stream:
                    if (entry.StreamCallSite is not null)
                    {
                        throw new MediatorConfigurationException(
                            $"Duplicate stream handler for {messageType}: a stream query must have exactly one handler.");
                    }

                    entry.StreamCallSite = callSite;
                    break;
                default:
                    throw new InvalidOperationException($"Unknown handler kind {kind}.");
            }
        }

        foreach (var (eventType, handlerType, factory) in _events)
        {
            if (!eventCallSites.TryGetValue(eventType, out var list))
            {
                list = [];
                eventCallSites[eventType] = list;
            }

            list.Add(factory(CollectEventBehaviorTypes(eventType), singleton));
        }

        foreach (var (messageType, list) in eventCallSites)
        {
            EnsureEntry(entries, messageType, HandlerKind.Event, null).EventCallSites = list.ToArray();
        }

        foreach (var (messageType, policy) in _eventPolicies)
        {
            if (!entries.TryGetValue(messageType, out var entry) || entry.Kind != HandlerKind.Event)
            {
                throw new MediatorConfigurationException(
                    $"Event policy set for {messageType}, but no event handlers are registered for it.");
            }

            entry.Policy = policy;
        }

        var pairs = new List<KeyValuePair<RuntimeTypeHandle, MessageEntry>>(entries.Count);
        foreach (var (messageType, entry) in entries)
        {
            pairs.Add(new KeyValuePair<RuntimeTypeHandle, MessageEntry>(messageType.TypeHandle, entry));
        }

        return MessageRegistry.Build(pairs);
    }

    private static MessageEntry EnsureEntry(
        Dictionary<Type, MessageEntry> entries, Type messageType, HandlerKind kind, Type? responseType)
    {
        if (!entries.TryGetValue(messageType, out var entry))
        {
            entry = new MessageEntry(kind, messageType, responseType);
            entries[messageType] = entry;
        }

        return entry;
    }

    /// <summary>Проверка без динамического кода: поведение реализует закрытый интерфейс с точным совпадением аргументов.</summary>
    private static bool ImplementsClosedInterface(Type behaviorType, Type openInterface, Type arg0, Type? arg1)
    {
        foreach (var iface in behaviorType.GetInterfaces())
        {
            if (!iface.IsGenericType || iface.GetGenericTypeDefinition() != openInterface)
            {
                continue;
            }

            var args = iface.GetGenericArguments();
            if (args.Length == 1)
            {
                return args[0] == arg0;
            }

            if (args[0] == arg0 && args[1] == arg1)
            {
                return true;
            }
        }

        return false;
    }

    private Type[] CollectBehaviorTypes(Type requestType, Type responseType, HandlerKind kind)
    {
        var openInterface = kind == HandlerKind.Stream ? typeof(IStreamPipelineBehavior<,>) : typeof(IPipelineBehavior<,>);
        return [.. _behaviors
            .Where(b => b.OpenInterface == openInterface
                && ImplementsClosedInterface(b.BehaviorType, openInterface, requestType, responseType))
            .Select(b => b.BehaviorType)];
    }

    private Type[] CollectEventBehaviorTypes(Type eventType)
    {
        return [.. _behaviors
            .Where(b => b.OpenInterface == typeof(IEventPipelineBehavior<>)
                && ImplementsClosedInterface(b.BehaviorType, typeof(IEventPipelineBehavior<>), eventType, null))
            .Select(b => b.BehaviorType)];
    }

    internal bool IsSingleton => _lifetime == HandlerLifetime.Singleton;

    internal IEnumerable<Type> HandlerTypes =>
        _requests.Select(r => r.HandlerType).Concat(_events.Select(e => e.HandlerType));
}
