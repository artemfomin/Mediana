using Mediana.Messaging;

namespace Mediana.Pipeline;

/// <summary>Делегат следующего звена пайплайна запроса.</summary>
public delegate ValueTask<TResponse> RequestHandlerDelegate<in TRequest, TResponse>(TRequest request, CancellationToken cancellationToken)
    where TRequest : IRequest<TResponse>;

/// <summary>Делегат следующего звена пайплайна события (инвариантен: TEvent уже в contravariant-позиции behaviour).</summary>
public delegate ValueTask EventHandlerDelegate<TEvent>(TEvent @event, CancellationToken cancellationToken)
    where TEvent : IEvent;
/// <summary>Делегат следующего звена стрим-пайплайна.</summary>
public delegate IAsyncEnumerable<TRow> StreamHandlerDelegate<in TQuery, TRow>(TQuery query, CancellationToken cancellationToken)
    where TQuery : IStreamQuery<TRow>;

/// <summary>Behaviour пайплайна команд/запросов: кросс-каттинг вокруг хендлера (логирование, валидация, транзакции...).</summary>
public interface IPipelineBehavior<TRequest, TResponse> where TRequest : IRequest<TResponse>
{
    ValueTask<TResponse> Handle(TRequest request, RequestHandlerDelegate<TRequest, TResponse> next, CancellationToken cancellationToken);
}

/// <summary>Behaviour пайплайна событий (событие не имеет ответа — отдельный контракт).</summary>
public interface IEventPipelineBehavior<TEvent> where TEvent : IEvent
{
    ValueTask Handle(TEvent @event, EventHandlerDelegate<TEvent> next, CancellationToken cancellationToken);
}

/// <summary>Behaviour стрим-пайплайна: обёртки над потоком строк.</summary>
public interface IStreamPipelineBehavior<TQuery, TRow> where TQuery : IStreamQuery<TRow>
{
    IAsyncEnumerable<TRow> Handle(TQuery query, StreamHandlerDelegate<TQuery, TRow> next, CancellationToken cancellationToken);
}

/// <summary>Пре-процессор: выполняется до хендлера (сахар над behaviour).</summary>
public interface IPreProcessor<in TRequest> where TRequest : IRequest
{
    ValueTask Process(TRequest request, CancellationToken cancellationToken);
}

/// <summary>Пост-процессор: выполняется после успешного хендлера (сахар над behaviour).</summary>
public interface IPostProcessor<in TRequest, in TResponse> where TRequest : IRequest<TResponse>
{
    ValueTask Process(TRequest request, TResponse response, CancellationToken cancellationToken);
}
