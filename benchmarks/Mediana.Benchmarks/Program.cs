
using Mediana;
using Mediana.Messaging;
using Mediana.Pipeline;
using Microsoft.Extensions.DependencyInjection;

namespace Mediana.Benchmarks;

public sealed record Echo(int Value) : ICommand<int>;
public sealed class EchoHandler : Handlers.ICommandHandler<Echo, int>
{
    public ValueTask<int> Handle(Echo c, CancellationToken ct) => new(c.Value * 2);
}
public sealed class PassBehavior : IPipelineBehavior<Echo, int>
{
    public ValueTask<int> Handle(Echo r, RequestHandlerDelegate<Echo, int> next, CancellationToken ct) => next(r, ct);
}

public static class Program
{
    public static void Main()
    {
        var sc = new ServiceCollection()
            .AddSingleton<EchoHandler>()
            .AddSingleton<PassBehavior>()
            .AddMediana(c => c.UseSingletonHandlers()
                .AddCommandHandler<Echo, int, EchoHandler>()
                .AddBehavior<Echo, int, PassBehavior>());
        var sp = sc.BuildServiceProvider();
        var mediator = sp.GetRequiredService<IMediator>();
        var command = (ICommand<int>)new Echo(21);

        for (var i = 0; i < 1000; i++)
        {
            _ = mediator.Send(command);
        }

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 10_000; i++)
        {
            _ = mediator.Send(command);
        }

        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Console.WriteLine($"Send allocated {allocated} bytes total, {allocated / 10_000.0:F2} per call");
    }
}
