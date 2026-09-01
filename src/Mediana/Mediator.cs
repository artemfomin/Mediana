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

    /// <summary>Иммутабельная версия реестра этого медиатора.</summary>
    public MessageRegistry Registry => _registry;

    /// <summary>
    /// Возвращает новый медиатор с расширенным реестром (copy-on-write runtime-регистрация, §5.2);
    /// этот экземпляр остаётся на прежней версии.
    /// </summary>
    public Mediator WithRegistry(MessageRegistry updated)
        => new(updated, _serviceProvider);

    public ValueTask<TResponse> Send<TResponse>(ICommand<TResponse> command, CancellationToken cancellationToken = default)
    {
        Guard.NotNull(command, nameof(command));
        var entry = _registry.TryGet(command.GetType()) ?? ThrowNoHandler(command.GetType());

        if (ValueTypeResponse<TResponse>.Value)
        {
            // value-ответ: специализированная инстанциация — прямой typed-путь без аллокаций
            if (entry.CommandCallSite is IObjectCommandCallSite<TResponse> typed)
            {
                return typed.Invoke(command, _serviceProvider, cancellationToken);
            }

            return ThrowResponseTypeMismatch<TResponse>(entry, typeof(TResponse));
        }

        // ref-ответ: non-generic static хоп (canon-generic контекст аллоцирует на любом invoke — измерено;
        // цепочка canon → non-generic static → interface = ноль, см. PublishSequential)
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

        if (ValueTypeResponse<TResponse>.Value)
        {
            if (entry.QueryCallSite is IObjectQueryCallSite<TResponse> typed)
            {
                return typed.Invoke(query, _serviceProvider, cancellationToken);
            }

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
            // Событие без подписчиков — no-op (семантика MediatR).
            return default;
        }

        var callSites = System.Runtime.CompilerServices.Unsafe.As<IEventCallSite[]>(entry.EventCallSites);
        if (callSites.Length == 0)
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
        // Guard без боксинга struct-сообщений: кэшированный признак ссылочности типа.
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
                // Синхронный бросок хендлера/behavior — тоже агрегируется (§4.3).
                (errors ??= []).Add(ex);
            }
        }

        // Хендлеры уже запущены (Invoke выполняется синхронно до первой реальной паузы);
        // ожидаем все, ошибки агрегируем.
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

    /// <summary>Кэшированный признак ссылочного типа (guard-проверки без боксинга).</summary>
    private static class ReferenceTypeFlag<T>
    {
        public static readonly bool Value = !typeof(T).IsValueType;
    }

    /// <summary>Кэшированный признак value-type ответа (выбор пути: typed vs untyped hop).</summary>
    private static class ValueTypeResponse<T>
    {
        public static readonly bool Value = typeof(T).IsValueType;
    }
}
