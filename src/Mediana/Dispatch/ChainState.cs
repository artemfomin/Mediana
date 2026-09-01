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
    internal IPipelineBehavior<TRequest, TResponse>[] Behaviors = [];
    internal RequestHandlerDelegate<TRequest, TResponse> Terminal = null!;
    internal RequestHandlerDelegate<TRequest, TResponse> NextDelegate = null!;
    internal int Index;

    [ThreadStatic]
    private static ChainState<TRequest, TResponse>? _pooled;

    public ChainState()
    {
        // Делегат next создаётся один раз на состояние; пул амортизирует аллокацию.
        NextDelegate = Next;
    }

    public void Configure(IPipelineBehavior<TRequest, TResponse>[] behaviors, RequestHandlerDelegate<TRequest, TResponse> terminal)
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
        Type[] behaviorTypes,
        RequestHandlerDelegate<TRequest, TResponse> terminal)
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

        IPipelineBehavior<TRequest, TResponse>[] behaviors;
        if (behaviorTypes.Length == 0)
        {
            behaviors = [];
        }
        else
        {
            behaviors = new IPipelineBehavior<TRequest, TResponse>[behaviorTypes.Length];
            for (var i = 0; i < behaviorTypes.Length; i++)
            {
                behaviors[i] = (IPipelineBehavior<TRequest, TResponse>)(serviceProvider.GetService(behaviorTypes[i])
                    ?? throw new MediatorConfigurationException(
                        $"Behavior {behaviorTypes[i]} is not registered in the service provider."));
            }
        }

        state.Configure(behaviors, terminal);
        return state;
    }
}
