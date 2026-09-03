using Mediana.Messaging;

namespace Mediana;

/// <summary>
/// See English documentation.
/// <see cref="Send{TResponse}"/>
/// (§12)
/// </summary>
public interface IMediator
{
    /// <summary>().</summary>
    ValueTask<TResponse> Send<TResponse>(ICommand<TResponse> command, CancellationToken cancellationToken = default);

    /// <summary>().</summary>
    ValueTask<TResponse> Send<TResponse>(IQuery<TResponse> query, CancellationToken cancellationToken = default);

    /// <summary>(Sequential/Parallel per event type).</summary>
    ValueTask Publish<TEvent>(TEvent @event, CancellationToken cancellationToken = default) where TEvent : IEvent;

    /// <summary>: .</summary>
    IAsyncEnumerable<TRow> Stream<TRow>(IStreamQuery<TRow> query, CancellationToken cancellationToken = default);

    /// <summary>Zero-boxing escape hatch struct-().</summary>
    ValueTask<TResponse> SendExact<TRequest, TResponse>(TRequest request, CancellationToken cancellationToken = default)
        where TRequest : IRequest<TResponse>;
}
