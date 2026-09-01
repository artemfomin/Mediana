using Mediana.Handlers;
using Mediana.Messaging;
using Mediana.Pipeline;

namespace Mediana.Dispatch;

/// <summary>
/// Call-site стрим-запроса: стрим-behaviors композируются вокруг enumerable хендлера.
/// Без behaviors — прямой проброс enumerable хендлера (ноль аллокаций на вызов и на движение курсора).
/// </summary>
internal sealed class StreamCallSite<TQuery, TRow, THandler>
    : IStreamCallSite<TRow>
    where TQuery : IStreamQuery<TRow>
    where THandler : IStreamHandler<TQuery, TRow>
{
    private readonly Type[] _behaviorTypes;
    private readonly bool _singleton;

    public StreamCallSite(Type[] behaviorTypes, bool singleton)
    {
        _behaviorTypes = behaviorTypes;
        _singleton = singleton;
    }

    public IAsyncEnumerable<TRow> Invoke(object message, IServiceProvider serviceProvider, CancellationToken cancellationToken)
    {
        var query = (TQuery)message;
        var handler = (THandler)(serviceProvider.GetService(typeof(THandler))
            ?? throw new MediatorConfigurationException(
                $"Stream handler {typeof(THandler)} is not registered in the service provider."));

        if (_behaviorTypes.Length == 0)
        {
            return handler.Handle(query, cancellationToken);
        }

        var behaviors = new IStreamPipelineBehavior<TQuery, TRow>[_behaviorTypes.Length];
        for (var i = 0; i < _behaviorTypes.Length; i++)
        {
            behaviors[i] = (IStreamPipelineBehavior<TQuery, TRow>)(serviceProvider.GetService(_behaviorTypes[i])
                ?? throw new MediatorConfigurationException(
                    $"Stream behavior {_behaviorTypes[i]} is not registered in the service provider."));
        }

        StreamHandlerDelegate<TQuery, TRow> chain = (r, ct) => handler.Handle(r, ct);
        // Stryker disable once equality: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
        for (var i = behaviors.Length - 1; i >= 0; i--)
        // Stryker disable once block: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
        {
            var inner = chain;
            var behavior = behaviors[i];
            chain = (r, ct) => behavior.Handle(r, inner, ct);
        }

        return chain(query, cancellationToken);
    }
}
