using Mediana.Handlers;
using Mediana.Messaging;
using Mediana.Pipeline;

namespace Mediana.Dispatch;

/// <summary>
/// State pool behavior chain (Treiber stack, lock-free).
/// Synchronous chain completion returns to pool immediately, zero allocations;
/// true asynchrony uses one pooled state per dispatch plus one async-box (toandin )
/// thenand byandin bytoin inin next (index-based); byinthen inin and
/// andon — andand (and from byinand )
/// </summary>
internal sealed class ChainState<TRequest, TResponse> where TRequest : IRequest<TResponse>
{
    internal IHandlerMiddleware<TRequest, TResponse>[] Behaviors = [];
    internal HandlerDelegate<TRequest, TResponse> Terminal = null!;
    internal HandlerDelegate<TRequest, TResponse> NextDelegate = null!;
    internal int Index;

    [ThreadStatic]
    private static ChainState<TRequest, TResponse>? _pooled;

    public ChainState()
    {
        // next and on thenand; fromand and
        NextDelegate = Next;
    }

    public void Configure(IHandlerMiddleware<TRequest, TResponse>[] behaviors, HandlerDelegate<TRequest, TResponse> terminal)
    {
        Behaviors = behaviors;
        Terminal = terminal;
        Index = 0;
    }

    public ValueTask<TResponse> Next(TRequest request, CancellationToken cancellationToken)
    {
        var behaviors = Behaviors;
        var index = Index;
        if (index < behaviors.Length)
        {
            Index = index + 1;
            return behaviors[index].Handle(request, NextDelegate, cancellationToken);
        }

        if (index == behaviors.Length)
        {
            // andon inin byinthenbut (behaviors in next not )
            // andand byandand, then byinthen not in and
            Index = index + 1;
            return Terminal(request, cancellationToken);
        }

        throw new InvalidOperationException(
            "The pipeline 'next' delegate was invoked after the chain completed. " +
            "Behaviors must not invoke 'next' concurrently or after completion.");
    }

    public void Return()
    // Stryker disable once block: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
    {
        Behaviors = [];
        Terminal = null!;
        _pooled = this;
    }

    /// <summary>Take state from thread-static pool or create; resolves behaviors by type.</summary>
    public static ChainState<TRequest, TResponse> Take(
        IServiceProvider serviceProvider,
        Type[] middlewareTypes,
        HandlerDelegate<TRequest, TResponse> terminal)
    {
        var state = _pooled;
        if (state is not null)
        // Stryker disable once block: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        {
            _pooled = null;
        }
        else
        {
            state = new ChainState<TRequest, TResponse>();
        }

        IHandlerMiddleware<TRequest, TResponse>[] behaviors;
        if (middlewareTypes.Length == 0)
        {
            behaviors = [];
        }
        else
        {
            behaviors = new IHandlerMiddleware<TRequest, TResponse>[middlewareTypes.Length];
            for (var i = 0; i < middlewareTypes.Length; i++)
            {
                behaviors[i] = (IHandlerMiddleware<TRequest, TResponse>)(serviceProvider.GetService(middlewareTypes[i])
                    ?? throw new MediatorConfigurationException(
                        $"Behavior {middlewareTypes[i]} is not registered in the service provider."));
            }
        }

        state.Configure(behaviors, terminal);
        return state;
    }
}
