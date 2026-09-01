using Mediana.Messaging;

namespace Mediana;

/// <summary>
/// Точка входа медиатора.
/// Локальный <see cref="Send{TResponse}"/> пропускает исключение хендлера как есть;
/// аллокации на пути — ноль (бюджеты спеки §12).
/// </summary>
public interface IMediator
{
    /// <summary>Отправить команду (единственный хендлер).</summary>
    ValueTask<TResponse> Send<TResponse>(ICommand<TResponse> command, CancellationToken cancellationToken = default);

    /// <summary>Выполнить запрос (единственный хендлер).</summary>
    ValueTask<TResponse> Send<TResponse>(IQuery<TResponse> query, CancellationToken cancellationToken = default);

    /// <summary>Опубликовать событие всем локальным хендлерам (политика Sequential/Parallel per event type).</summary>
    ValueTask Publish<TEvent>(TEvent @event, CancellationToken cancellationToken = default) where TEvent : IEvent;

    /// <summary>Стрим-запрос: поток строк от хендлера.</summary>
    IAsyncEnumerable<TRow> Stream<TRow>(IStreamQuery<TRow> query, CancellationToken cancellationToken = default);

    /// <summary>Zero-boxing escape hatch для struct-сообщений на горячих путях (команды и запросы).</summary>
    ValueTask<TResponse> SendExact<TRequest, TResponse>(TRequest request, CancellationToken cancellationToken = default)
        where TRequest : IRequest<TResponse>;
}
