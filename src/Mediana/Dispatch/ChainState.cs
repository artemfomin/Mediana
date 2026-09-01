using Mediana.Handlers;
using Mediana.Messaging;
using Mediana.Pipeline;

namespace Mediana.Dispatch;

/// <summary>
/// Пул состояний цепочки behaviors (Treiber stack, lock-free).
/// Синхронное завершение цепочки — возврат в пул сразу, ноль аллокаций;
/// истинная асинхронность — одно состояние на вызов из пула + один async-box (документированный бюджет).
/// Состояние поддерживает последовательные вызовы next (index-based); повторный вызов за пределами
/// терминала — исключение (защита от повреждения пула).
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
        // Делегат next создаётся один раз на состояние; пул амортизирует аллокацию.
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
            // терминал может вызываться повторно (behaviors могут звать next несколько раз);
            // фиксируем позицию, чтобы повторный проход не вышел за границы
            Index = index + 1;
            return Terminal(request, cancellationToken);
        }

        throw new InvalidOperationException(
            "The pipeline 'next' delegate was invoked after the chain completed. " +
            "Behaviors must not invoke 'next' concurrently or after completion.");
    }

    public void Return()
    // Stryker disable once block: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
    {
        Behaviors = [];
        Terminal = null!;
        _pooled = this;
    }

    /// <summary>Взять состояние из thread-static пула или создать; резолвит behaviors по типам.</summary>
    public static ChainState<TRequest, TResponse> Take(
        IServiceProvider serviceProvider,
        Type[] middlewareTypes,
        HandlerDelegate<TRequest, TResponse> terminal)
    {
        var state = _pooled;
        if (state is not null)
        // Stryker disable once block: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
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
