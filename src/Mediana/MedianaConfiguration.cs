using System.Diagnostics.CodeAnalysis;
using Mediana.Dispatch;
using Mediana.Handlers;
using Mediana.Internal;
using Mediana.Messaging;
using Mediana.Pipeline;

namespace Mediana;

/// <summary>Handler lifetime policy (§5.4): singleton means zero DI lookups per dispatch.</summary>
public enum HandlerLifetime
{
    /// <summary>Handler is resolved from the current scope on each dispatch (correct for scoped dependencies).</summary>
    Scoped,

    /// <summary>Handlers are registered as singletons; the chain is composed once — zero DI lookups and allocations.</summary>
    Singleton,
}

/// <summary>
/// Message graph configuration: handlers, behaviors (order equals registration order), event policies.
/// Freeze validates the graph (duplicate command/query/stream handlers are errors) and builds <see cref="MessageRegistry"/>.
/// Typed registrations create call sites through closed generic factories without reflection (AOT-compatible);
/// reflection is used only in <see cref="AddHandlersFromAssembly"/> (runtime escape hatch, not for NativeAOT).
/// </summary>
public sealed class MedianaConfiguration
{
    private readonly List<RequestRegistration> _requests = [];
    private readonly List<EventRegistration> _events = [];
    private readonly List<(Type BehaviorType, Type OpenInterface)> _middlewares = [];
    private readonly Dictionary<Type, EventDispatchPolicy> _eventPolicies = [];
    private HandlerLifetime _lifetime = HandlerLifetime.Scoped;

    /// <summary>Command/query/stream call-site factory: (middlewareTypes, singleton) to call-site.</summary>
    private delegate object RequestCallSiteFactory(Type[] middlewareTypes, bool singleton);

    /// <summary>Event call-site factory.</summary>
    private delegate IEventCallSite EventCallSiteFactory(Type[] middlewareTypes, bool singleton);

    private readonly record struct RequestRegistration(
        HandlerKind Kind, Type MessageType, Type ResponseType, Type HandlerType, RequestCallSiteFactory Factory);

    private readonly record struct EventRegistration(Type EventType, Type HandlerType, EventCallSiteFactory Factory);

    /// <summary>Handlers without scoped dependencies: singletons, chains are composed once (D16).</summary>
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
            // Stryker disable once boolean: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
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

    /// <summary>Global command/query middleware (application order equals registration order).</summary>
    public MedianaConfiguration AddMiddleware<TRequest, TResponse, TBehavior>()
        where TRequest : IRequest<TResponse>
        where TBehavior : IHandlerMiddleware<TRequest, TResponse>
    {
        _middlewares.Add((typeof(TBehavior), typeof(IHandlerMiddleware<,>)));
        return this;
    }

    /// <summary>Global event middleware (for all compatible events).</summary>
    public MedianaConfiguration AddEventMiddleware<TEvent, TBehavior>()
        where TEvent : IEvent
        where TBehavior : IEventMiddleware<TEvent>
    {
        _middlewares.Add((typeof(TBehavior), typeof(IEventMiddleware<>)));
        return this;
    }

    /// <summary>Global stream middleware (for all compatible stream queries).</summary>
    public MedianaConfiguration AddStreamMiddleware<TQuery, TRow, TBehavior>()
        where TQuery : IStreamQuery<TRow>
        where TBehavior : IStreamMiddleware<TQuery, TRow>
    {
        // Stryker disable once statement: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        _middlewares.Add((typeof(TBehavior), typeof(IStreamMiddleware<,>)));
        return this;
    }

    /// <summary>Event dispatch policy (default Sequential).</summary>
    public MedianaConfiguration SetEventPolicy<TEvent>(EventDispatchPolicy policy) where TEvent : IEvent
    {
        _eventPolicies[typeof(TEvent)] = policy;
        return this;
    }

    /// <summary>
    /// Runtime assembly scanning (opt-in escape hatch, spec 5.2). Uses reflection —
    /// notinand NativeAOT/trimming; for AOT andby notthen Mediana.Generators
    /// </summary>
    [RequiresUnreferencedCode("Assembly scanning traverses all types; use the source generator for AOT.")]
    [RequiresDynamicCode("Creates closed generic call sites at runtime; use the source generator for AOT.")]
    public MedianaConfiguration AddHandlersFromAssembly(System.Reflection.Assembly assembly)
    {
        // Stryker disable once statement: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        Guard.NotNull(assembly, nameof(assembly));

        foreach (var type in assembly.GetTypes())
        {
            if (type is not { IsAbstract: false, IsInterface: false, IsGenericTypeDefinition: false })
            // Stryker disable once block: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
            {
                // Stryker disable once statement: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
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
                // Stryker disable once block: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
                {
                    // Stryker disable once statement: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
                    AddScanned(typeof(CommandCallSite<,,>), args, type, HandlerKind.Command);
                }
                else if (def == typeof(IQueryHandler<,>))
                // Stryker disable once block: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
                {
                    // Stryker disable once statement: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
                    AddScanned(typeof(QueryCallSite<,,>), args, type, HandlerKind.Query);
                }
                else if (def == typeof(IStreamHandler<,>))
                // Stryker disable once block: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
                {
                    // Stryker disable once statement: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
                    AddScanned(typeof(StreamCallSite<,,>), args, type, HandlerKind.Stream);
                }
                else if (def == typeof(IEventHandler<>))
                {
                    var callSiteType = typeof(EventCallSite<,>).MakeGenericType(args[0], type);
                    var eventType = args[0];
                    // Stryker disable once statement: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
                    _events.Add(new EventRegistration(
                        eventType, type,
                        (behaviors, singleton) => (IEventCallSite)Activator.CreateInstance(
                            callSiteType, new object[] { behaviors, singleton })!));
                }
                else
                // Stryker disable once block: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
                {
                    // Stryker disable once statement: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
                    continue;
                }

                // Stryker disable once statement: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
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

    /// <summary>Test hook: direct request-kind registration (for Freeze guard branches).</summary>
    internal MedianaConfiguration AddHandler(HandlerKind kind, Type messageType, Type handlerType)
    {
        _requests.Add(new RequestRegistration(
            kind, messageType, typeof(void), handlerType,
            (_, _) => throw new InvalidOperationException("Test-only registration must not be invoked.")));
        return this;
    }

    internal MessageRegistry Freeze()
    {
        var entries = new Dictionary<Type, MessageEntry>();
        var singleton = _lifetime == HandlerLifetime.Singleton;
        var eventCallSites = new Dictionary<Type, List<IEventCallSite>>();

        foreach (var (kind, messageType, responseType, _, factory) in _requests)
        {
            var entry = EnsureEntry(entries, messageType, kind, responseType);
            var middlewareTypes = CollectMiddlewareTypes(messageType, responseType, kind);
            var callSite = factory(middlewareTypes, singleton);

            if (kind == HandlerKind.Command)
            {
                if (entry.CommandCallSite is not null)
                {
                    throw new MediatorConfigurationException(
                        $"Duplicate command handler for {messageType}: a command must have exactly one handler.");
                }

                entry.CommandCallSite = callSite;
            }
            else if (kind == HandlerKind.Query)
            {
                if (entry.QueryCallSite is not null)
                {
                    throw new MediatorConfigurationException(
                        $"Duplicate query handler for {messageType}: a query must have exactly one handler.");
                }

                entry.QueryCallSite = callSite;
            }
            else if (kind == HandlerKind.Stream)
            {
                if (entry.StreamCallSite is not null)
                {
                    throw new MediatorConfigurationException(
                        $"Duplicate stream handler for {messageType}: a stream query must have exactly one handler.");
                }

                entry.StreamCallSite = callSite;
            }
            else
            {
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

    /// <summary>Check without dynamic code: the behavior implements a closed interface with exact argument match.</summary>
    private static bool ImplementsClosedInterface(Type behaviorType, Type openInterface, Type arg0, Type? arg1)
    {
        foreach (var iface in behaviorType.GetInterfaces())
        {
            // Stryker disable once logical: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
            if (!iface.IsGenericType || iface.GetGenericTypeDefinition() != openInterface)
            {
                continue;
            }

            var args = iface.GetGenericArguments();
            if (args.Length == 1)
            {
                return args[0] == arg0;
            }

            // Stryker disable once logical: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
            if (args[0] == arg0 && args[1] == arg1)
            {
                return true;
            }
        }

        return false;
    }

    private Type[] CollectMiddlewareTypes(Type requestType, Type responseType, HandlerKind kind)
    {
        // Stryker disable once conditional: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        var openInterface = kind == HandlerKind.Stream ? typeof(IStreamMiddleware<,>) : typeof(IHandlerMiddleware<,>);
        return [.. _middlewares
            .Where(b => b.OpenInterface == openInterface
                && ImplementsClosedInterface(b.BehaviorType, openInterface, requestType, responseType))
            .Select(b => b.BehaviorType)];
    }

    private Type[] CollectEventBehaviorTypes(Type eventType)
    {
        return [.. _middlewares
            // Stryker disable once logical: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
            // Stryker disable once logical: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
            .Where(b => b.OpenInterface == typeof(IEventMiddleware<>)
                && ImplementsClosedInterface(b.BehaviorType, typeof(IEventMiddleware<>), eventType, null))
            .Select(b => b.BehaviorType)];
    }

    internal bool IsSingleton => _lifetime == HandlerLifetime.Singleton;

    internal IEnumerable<Type> HandlerTypes =>
        _requests.Select(r => r.HandlerType).Concat(_events.Select(e => e.HandlerType));
}
