using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Mediana.Dispatch;
using Mediana.Handlers;
using Mediana.Internal;
using Mediana.Messaging;

namespace Mediana;

/// <summary>
/// Диспетчер in-process сообщений (§5 спеки).
/// Lookup по точному типу в иммутабельном реестре → типизированный call-site без боксинга ответа.
/// Локальный Send исключение хендлера прокидывает как есть.
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

    /// <summary>Текущая версия реестра (после runtime-добавлений — Copy-on-write).</summary>
    public MessageRegistry Registry => _runtimeAdded ?? _registry;

    private MessageRegistry? _runtimeAdded;

    /// <summary>Runtime-регистрация нового сообщения (opt-in, §5.2): расширяет реестр copy-on-write.</summary>
    public void RegisterRuntime(MessageRegistry updated)
    {
        _runtimeAdded = updated;
    }

    public ValueTask<TResponse> Send<TResponse>(ICommand<TResponse> command, CancellationToken cancellationToken = default)
    {
        Guard.NotNull(command, nameof(command));
        var entry = Registry.TryGet(command.GetType()) ?? ThrowNoHandler(command.GetType());
        if (entry.CommandCallSite is IObjectCommandCallSite<TResponse> callSite)
        {
            return callSite.Invoke(command, _serviceProvider, cancellationToken);
        }

        return ThrowResponseTypeMismatch<TResponse>(entry, typeof(TResponse));
    }

    public ValueTask<TResponse> Send<TResponse>(IQuery<TResponse> query, CancellationToken cancellationToken = default)
    {
        Guard.NotNull(query, nameof(query));
        var entry = Registry.TryGet(query.GetType()) ?? ThrowNoHandler(query.GetType());
        if (entry.QueryCallSite is IObjectQueryCallSite<TResponse> callSite)
        {
            return callSite.Invoke(query, _serviceProvider, cancellationToken);
        }

        return ThrowResponseTypeMismatch<TResponse>(entry, typeof(TResponse));
    }

    public ValueTask Publish<TEvent>(TEvent @event, CancellationToken cancellationToken = default)
        where TEvent : IEvent
    {
        Guard.NotNull(@event, nameof(@event));
        var entry = Registry.TryGet(@event.GetType());
        if (entry is null)
        {
            // Событие без подписчиков — no-op (семантика MediatR).
            return default;
        }

        var callSites = entry.EventCallSites;
        if (callSites.Count == 0)
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
        var entry = Registry.TryGet(query.GetType()) ?? ThrowNoHandler(query.GetType());
        if (entry.StreamCallSite is IStreamCallSite<TRow> callSite)
        {
            return callSite.Invoke(query, _serviceProvider, cancellationToken);
        }

        return ThrowStreamMismatch<TRow>(entry);
    }

    public ValueTask<TResponse> SendExact<TRequest, TResponse>(TRequest request, CancellationToken cancellationToken = default)
        where TRequest : IRequest<TResponse>
    {
        Guard.NotNull(request, nameof(request));
        var entry = Registry.TryGet(typeof(TRequest)) ?? ThrowNoHandler(typeof(TRequest));

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
        IReadOnlyList<IEventCallSite> callSites,
        object @event,
        IServiceProvider serviceProvider,
        CancellationToken cancellationToken)
    {
        foreach (var callSite in callSites)
        {
            await callSite.Invoke(@event, serviceProvider, cancellationToken).ConfigureAwait(false);
        }
    }

    private static async ValueTask PublishParallel(
        IReadOnlyList<IEventCallSite> callSites,
        object @event,
        IServiceProvider serviceProvider,
        CancellationToken cancellationToken)
    {
        var pending = new ValueTask[callSites.Count];
        for (var i = 0; i < callSites.Count; i++)
        {
            pending[i] = callSites[i].Invoke(@event, serviceProvider, cancellationToken);
        }

        // Хендлеры уже запущены (Invoke выполняется синхронно до первой реальной паузы);
        // ожидаем все, агрегируем ошибки (§4.3).
        List<Exception>? errors = null;
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
}
