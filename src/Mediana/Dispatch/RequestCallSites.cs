using Mediana.Handlers;
using Mediana.Messaging;
using Mediana.Pipeline;

namespace Mediana.Dispatch;

/// <summary>
/// Общая логика call-site'ов команд/запросов.
/// Singleton-режим: цепочка behaviors + терминал композируется один раз (лениво, из DI-синглтонов) —
/// на вызове ноль обращений к DI и ноль аллокаций (D16, §5.4).
/// Scoped-режим: behaviors резолвятся в pooled <see cref="ChainState{TRequest,TResponse}"/> —
/// синхронный путь без аллокаций, асинхронный — один pooled-state.
/// </summary>
internal abstract class RequestCallSiteCore<TRequest, TResponse, THandler>
    where TRequest : IRequest<TResponse>
{
    private readonly Type[] _behaviorTypes;
    private readonly bool _singleton;

    private RequestHandlerDelegate<TRequest, TResponse>? _singletonRoot;
    private readonly object _singletonLock = new();

    protected RequestCallSiteCore(Type[] behaviorTypes, bool singleton)
    {
        _behaviorTypes = behaviorTypes;
        _singleton = singleton;
    }

    protected abstract THandler ResolveHandler(IServiceProvider serviceProvider);

    protected abstract ValueTask<TResponse> InvokeHandler(THandler handler, TRequest request, CancellationToken cancellationToken);

    internal ValueTask<TResponse> InvokeCore(TRequest request, IServiceProvider serviceProvider, CancellationToken cancellationToken)
    {
        if (_singleton)
        {
            var root = _singletonRoot;
            if (root is null)
            {
                root = BuildSingletonRoot(serviceProvider);
            }

            return root(request, cancellationToken);
        }

        var handler = ResolveHandler(serviceProvider);
        RequestHandlerDelegate<TRequest, TResponse> terminal = (r, ct) => InvokeHandler(handler, r, ct);
        var state = ChainState<TRequest, TResponse>.Take(serviceProvider, _behaviorTypes, terminal);
        var result = state.Next(request, cancellationToken);
        if (result.IsCompletedSuccessfully)
        {
            state.Return();
            return result;
        }

        return AwaitAndReturn(state, result);
    }

    private static async ValueTask<TResponse> AwaitAndReturn(
        ChainState<TRequest, TResponse> state,
        ValueTask<TResponse> pending)
    {
        try
        {
            return await pending.ConfigureAwait(false);
        }
        finally
        {
            state.Return();
        }
    }

    private RequestHandlerDelegate<TRequest, TResponse> BuildSingletonRoot(IServiceProvider serviceProvider)
    {
        lock (_singletonLock)
        {
            if (_singletonRoot is not null)
            {
                return _singletonRoot;
            }

            var handler = ResolveHandler(serviceProvider);
            RequestHandlerDelegate<TRequest, TResponse> root = (r, ct) => InvokeHandler(handler, r, ct);

            if (_behaviorTypes.Length > 0)
            {
                var behaviors = new IPipelineBehavior<TRequest, TResponse>[_behaviorTypes.Length];
                for (var i = _behaviorTypes.Length - 1; i >= 0; i--)
                {
                    behaviors[i] = (IPipelineBehavior<TRequest, TResponse>)(serviceProvider.GetService(_behaviorTypes[i])
                        ?? throw new MediatorConfigurationException(
                            $"Behavior {_behaviorTypes[i]} is not registered in the service provider."));
                    var inner = root;
                    var behavior = behaviors[i];
                    root = (r, ct) => behavior.Handle(r, inner, ct);
                }
            }

            _singletonRoot = root;
            return root;
        }
    }
}

/// <summary>Call-site команды: типизированный и object-путь, без боксинга ответа.</summary>
internal sealed class CommandCallSite<TCommand, TResponse, THandler>
    : IObjectCommandCallSite<TResponse>, ITypedCommandCallSite<TCommand, TResponse>
    where TCommand : ICommand<TResponse>
    where THandler : ICommandHandler<TCommand, TResponse>
{
    private readonly RequestCallSiteCore<TCommand, TResponse, THandler> _core;

    public CommandCallSite(Type[] behaviorTypes, bool singleton)
    {
        _core = new Impl(behaviorTypes, singleton);
    }

    public ValueTask<TResponse> Invoke(object message, IServiceProvider serviceProvider, CancellationToken cancellationToken)
        => _core.InvokeCore((TCommand)message, serviceProvider, cancellationToken);

    public ValueTask<TResponse> InvokeTyped(TCommand message, IServiceProvider serviceProvider, CancellationToken cancellationToken)
        => _core.InvokeCore(message, serviceProvider, cancellationToken);

    private sealed class Impl(Type[] behaviorTypes, bool singleton)
        : RequestCallSiteCore<TCommand, TResponse, THandler>(behaviorTypes, singleton)
    {
        protected override THandler ResolveHandler(IServiceProvider serviceProvider)
            => (THandler)(serviceProvider.GetService(typeof(THandler))
                ?? throw new MediatorConfigurationException(
                    $"Command handler {typeof(THandler)} is not registered in the service provider."));

        protected override ValueTask<TResponse> InvokeHandler(
            THandler handler, TCommand request, CancellationToken cancellationToken)
            => handler.Handle(request, cancellationToken);
    }
}

/// <summary>Call-site запроса.</summary>
internal sealed class QueryCallSite<TQuery, TResponse, THandler>
    : IObjectQueryCallSite<TResponse>, ITypedQueryCallSite<TQuery, TResponse>
    where TQuery : IQuery<TResponse>
    where THandler : IQueryHandler<TQuery, TResponse>
{
    private readonly RequestCallSiteCore<TQuery, TResponse, THandler> _core;

    public QueryCallSite(Type[] behaviorTypes, bool singleton)
    {
        _core = new Impl(behaviorTypes, singleton);
    }

    public ValueTask<TResponse> Invoke(object message, IServiceProvider serviceProvider, CancellationToken cancellationToken)
        => _core.InvokeCore((TQuery)message, serviceProvider, cancellationToken);

    public ValueTask<TResponse> InvokeTyped(TQuery message, IServiceProvider serviceProvider, CancellationToken cancellationToken)
        => _core.InvokeCore(message, serviceProvider, cancellationToken);

    private sealed class Impl(Type[] behaviorTypes, bool singleton)
        : RequestCallSiteCore<TQuery, TResponse, THandler>(behaviorTypes, singleton)
    {
        protected override THandler ResolveHandler(IServiceProvider serviceProvider)
            => (THandler)(serviceProvider.GetService(typeof(THandler))
                ?? throw new MediatorConfigurationException(
                    $"Query handler {typeof(THandler)} is not registered in the service provider."));

        protected override ValueTask<TResponse> InvokeHandler(
            THandler handler, TQuery request, CancellationToken cancellationToken)
            => handler.Handle(request, cancellationToken);
    }
}
