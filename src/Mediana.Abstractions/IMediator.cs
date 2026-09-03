using Mediana.Messaging;

namespace Mediana;

/// <summary>
/// Mediator entry point.
/// Local <see cref="Send{TResponse}"/> propagates handler exceptions as-is;
/// allocations on the dispatch path are zero (spec §12 budgets).
/// </summary>
public interface IMediator
{
    /// <summary>Send a command (exactly one handler).</summary>
    ValueTask<TResponse> Send<TResponse>(ICommand<TResponse> command, CancellationToken cancellationToken = default);

    /// <summary>Execute a query (exactly one handler).</summary>
    ValueTask<TResponse> Send<TResponse>(IQuery<TResponse> query, CancellationToken cancellationToken = default);

    /// <summary>Publish an event to all local handlers (Sequential/Parallel policy per event type).</summary>
    ValueTask Publish<TEvent>(TEvent @event, CancellationToken cancellationToken = default) where TEvent : IEvent;

    /// <summary>Stream query: yields rows from the handler.</summary>
    IAsyncEnumerable<TRow> Stream<TRow>(IStreamQuery<TRow> query, CancellationToken cancellationToken = default);

    /// <summary>Zero-boxing escape hatch for struct messages on hot paths (commands and queries).</summary>
    ValueTask<TResponse> SendExact<TRequest, TResponse>(TRequest request, CancellationToken cancellationToken = default)
        where TRequest : IRequest<TResponse>;
}
