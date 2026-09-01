using Mediana;
using Mediana.Handlers;
using Mediana.Messaging;
using Mediana.Pipeline;
using Microsoft.Extensions.DependencyInjection;

namespace Mediana.AotTests;

public sealed record AotCommand(int Value) : ICommand<string>;
public sealed record AotEvent(int Value) : IEvent;
public sealed record AotQuery(int Value) : IQuery<int>;

public sealed class AotCommandHandler : ICommandHandler<AotCommand, string>
{
    public ValueTask<string> Handle(AotCommand command, CancellationToken ct)
        => new("handled:" + command.Value);
}

public sealed class AotQueryHandler : IQueryHandler<AotQuery, int>
{
    public ValueTask<int> Handle(AotQuery query, CancellationToken ct)
        => new(query.Value + 1);
}

public sealed class AotEventHandler : IEventHandler<AotEvent>
{
    public static int Count;

    public ValueTask Handle(AotEvent @event, CancellationToken ct)
    {
        Count++;
        return default;
    }
}

public sealed class AotBehavior : IPipelineBehavior<AotCommand, string>
{
    public ValueTask<string> Handle(AotCommand request, RequestHandlerDelegate<AotCommand, string> next, CancellationToken ct)
        => next(request, ct);
}

public static class Program
{
    public static async Task<int> Main()
    {
        var sc = new ServiceCollection()
            .AddSingleton<AotCommandHandler>()
            .AddSingleton<AotQueryHandler>()
            .AddSingleton<AotEventHandler>()
            .AddSingleton<AotBehavior>()
            .AddMediana(c => c
                .UseSingletonHandlers()
                .AddCommandHandler<AotCommand, string, AotCommandHandler>()
                .AddQueryHandler<AotQuery, int, AotQueryHandler>()
                .AddEventHandler<AotEvent, AotEventHandler>()
                .AddBehavior<AotCommand, string, AotBehavior>());
        var sp = sc.BuildServiceProvider();
        var mediator = sp.GetRequiredService<IMediator>();

        var command = (ICommand<string>)new AotCommand(42);
        var result = await mediator.Send(command);
        if (result != "handled:42")
        {
            Console.Error.WriteLine("AOT SMOKE FAIL: command " + result);
            return 1;
        }

        var query = (IQuery<int>)new AotQuery(41);
        if (await mediator.Send(query) != 42)
        {
            Console.Error.WriteLine("AOT SMOKE FAIL: query");
            return 1;
        }

        await mediator.Publish(new AotEvent(1));
        if (AotEventHandler.Count != 1)
        {
            Console.Error.WriteLine("AOT SMOKE FAIL: event");
            return 1;
        }

        Console.WriteLine("Mediana AOT smoke: OK (command+query+event+behavior)");
        return 0;
    }
}
