using Mediana.Messaging;
namespace Mediana.Dispatch;

/// <summary>Вид сообщения.</summary>
public enum HandlerKind
{
    Command,
    Query,
    Event,
    Stream,
}

/// <summary>Политика диспетчеризации события (§4.3 спеки).</summary>
public enum EventDispatchPolicy
{
    /// <summary>Последовательно; первый бросок прерывает цепочку. По умолчанию.</summary>
    Sequential,

    /// <summary>Все хендлеры стартуют одновременно; ошибки агрегируются в AggregateException.</summary>
    Parallel,
}

/// <summary>
/// Call-site команды для object-вызова (<see cref="IMediator.Send{TResponse}(Mediana.Messaging.ICommand{TResponse}, CancellationToken)"/>).
/// Advanced API: используется генератором и движком; в прикладном коде не нужна.
/// </summary>
public interface IObjectCommandCallSite<TResponse>
{
    ValueTask<TResponse> Invoke(object message, IServiceProvider serviceProvider, CancellationToken cancellationToken);
}

/// <summary>Call-site команды с типизированным сообщением (zero-boxing путь, SendExact).</summary>
public interface ITypedCommandCallSite<TRequest, TResponse> where TRequest : IRequest<TResponse>
{
    ValueTask<TResponse> InvokeTyped(TRequest message, IServiceProvider serviceProvider, CancellationToken cancellationToken);
}

/// <summary>Call-site запроса для object-вызова.</summary>
public interface IObjectQueryCallSite<TResponse>
{
    ValueTask<TResponse> Invoke(object message, IServiceProvider serviceProvider, CancellationToken cancellationToken);
}

/// <summary>Call-site запроса с типизированным сообщением (SendExact).</summary>
public interface ITypedQueryCallSite<TRequest, TResponse> where TRequest : IRequest<TResponse>
{
    ValueTask<TResponse> InvokeTyped(TRequest message, IServiceProvider serviceProvider, CancellationToken cancellationToken);
}

/// <summary>Call-site события.</summary>
public interface IEventCallSite
{
    ValueTask Invoke(object message, IServiceProvider serviceProvider, CancellationToken cancellationToken);
}

/// <summary>
/// Non-generic хоп: вызов из canon-generic контекста аллоцирует (~24-32Б/вызов, измерено);
/// не-generic InvokeAny из не-generic метода Mediator — ноль. Value-ответы боксируются —
/// потому object-путь с value-ответами не использует этот хоп (см. Mediator).
/// </summary>
public interface IUntypedCallSite
{
    ValueTask<object?> InvokeAny(object message, IServiceProvider serviceProvider, CancellationToken cancellationToken);
}

/// <summary>Call-site стрим-запроса.</summary>
public interface IStreamCallSite<TRow>
{
    IAsyncEnumerable<TRow> Invoke(object message, IServiceProvider serviceProvider, CancellationToken cancellationToken);
}
