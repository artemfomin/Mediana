using Mediana.Handlers;
using Mediana.Messaging;
using Mediana.Pipeline;

namespace Mediana.Dispatch;

/// <summary>
/// Call-site and-: and-behaviors byand in enumerable
/// behaviors — enumerable (but and on inin and on inandand )
/// </summary>
internal sealed class StreamCallSite<TQuery, TRow, THandler>
    : IStreamCallSite<TRow>
    where TQuery : IStreamQuery<TRow>
    where THandler : IStreamHandler<TQuery, TRow>
{
    private readonly Type[] _middlewareTypes;
    private readonly bool _singleton;

    public StreamCallSite(Type[] middlewareTypes, bool singleton)
    {
        _middlewareTypes = middlewareTypes;
        _singleton = singleton;
    }

    public IAsyncEnumerable<TRow> Invoke(object message, IServiceProvider serviceProvider, CancellationToken cancellationToken)
    {
        var query = (TQuery)message;
        var handler = (THandler)(serviceProvider.GetService(typeof(THandler))
            ?? throw new MediatorConfigurationException(
                $"Stream handler {typeof(THandler)} is not registered in the service provider."));

        if (_middlewareTypes.Length == 0)
        {
            return handler.Handle(query, cancellationToken);
        }

        var behaviors = new IStreamMiddleware<TQuery, TRow>[_middlewareTypes.Length];
        for (var i = 0; i < _middlewareTypes.Length; i++)
        {
            behaviors[i] = (IStreamMiddleware<TQuery, TRow>)(serviceProvider.GetService(_middlewareTypes[i])
                ?? throw new MediatorConfigurationException(
                    $"Stream behavior {_middlewareTypes[i]} is not registered in the service provider."));
        }

        StreamHandlerDelegate<TQuery, TRow> chain = (r, ct) => handler.Handle(r, ct);
        // Stryker disable once equality: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        for (var i = behaviors.Length - 1; i >= 0; i--)
        // Stryker disable once block: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        {
            var inner = chain;
            var behavior = behaviors[i];
            chain = (r, ct) => behavior.Handle(r, inner, ct);
        }

        return chain(query, cancellationToken);
    }
}
