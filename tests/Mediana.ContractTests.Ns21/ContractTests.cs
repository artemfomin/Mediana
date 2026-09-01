using Microsoft.Extensions.DependencyInjection;
using System.Reflection;
using Mediana;
using Mediana.Dispatch;
using Mediana.Messaging;
using Xunit;

namespace Mediana.ContractTests.Ns21;

/// <summary>
/// Контракт идентичности публичной API-поверхности ns2.1-ассета ядра относительно net10.0-ассета (D2).
/// Тестовая сборка ссылается на ns2.1-ассеты; эта же проверка выполняется скриптом verify.ps1
/// для обоих ассетов через reflection-сравнение.
/// </summary>
public class ApiSurfaceTests
{
    private static HashSet<string> PublicApi(Assembly assembly)
    {
        var api = new HashSet<string>();
        foreach (var type in assembly.GetExportedTypes())
        {
            api.Add(TypeName(type));
            foreach (var method in type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly))
            {
                if (method.IsSpecialName)
                {
                    continue;
                }

                api.Add(TypeName(type) + "::" + method.Name + "/" + method.GetGenericArguments().Length);
            }

            foreach (var prop in type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly))
            {
                api.Add(TypeName(type) + "#" + prop.Name);
            }
        }

        return api;
    }

    private static string TypeName(Type type)
        => type.FullName ?? type.Name;

    [Fact]
    public void Abstractions_surface_contains_core_contracts()
    {
        var api = PublicApi(typeof(IMediator).Assembly);
        Assert.Contains("Mediana.IMediator", api);
        Assert.Contains("Mediana.IMediator::SendExact/2", api);
        Assert.Contains("Mediana.Messaging.IRequest", api);
        Assert.Contains("Mediana.Messaging.ICommand`1", api);
        Assert.Contains("Mediana.Messaging.IQuery`1", api);
        Assert.Contains("Mediana.Messaging.IEvent", api);
        Assert.Contains("Mediana.Messaging.IStreamQuery`1", api);
        Assert.Contains("Mediana.Messaging.IPartitioned", api);
        Assert.Contains("Mediana.Handlers.ICommandHandler`2", api);
        Assert.Contains("Mediana.Handlers.IQueryHandler`2", api);
        Assert.Contains("Mediana.Handlers.IEventHandler`1", api);
        Assert.Contains("Mediana.Handlers.IStreamHandler`2", api);
        Assert.Contains("Mediana.Pipeline.IPipelineBehavior`2", api);
        Assert.Contains("Mediana.Pipeline.IEventPipelineBehavior`1", api);
        Assert.Contains("Mediana.Pipeline.IStreamPipelineBehavior`2", api);
        Assert.Contains("Mediana.Pipeline.IPreProcessor`1", api);
        Assert.Contains("Mediana.Pipeline.IPostProcessor`2", api);
        Assert.Contains("Mediana.MediatorConfigurationException", api);
        Assert.Contains("Mediana.RemoteExecutionException", api);
        Assert.Contains("Mediana.RemoteTimeoutException", api);
    }

    [Fact]
    public void Core_dispatch_surface_stable()
    {
        var api = PublicApi(typeof(MessageRegistry).Assembly);
        Assert.Contains("Mediana.Mediator", api);
        Assert.Contains("Mediana.MedianaConfiguration", api);
        Assert.Contains("Mediana.MedianaConfiguration::AddCommandHandler/3", api);
        Assert.Contains("Mediana.MedianaConfiguration::AddQueryHandler/3", api);
        Assert.Contains("Mediana.MedianaConfiguration::AddEventHandler/2", api);
        Assert.Contains("Mediana.MedianaConfiguration::AddStreamHandler/3", api);
        Assert.Contains("Mediana.MedianaConfiguration::AddBehavior/3", api);
        Assert.Contains("Mediana.MedianaConfiguration::UseSingletonHandlers/0", api);
        Assert.Contains("Mediana.HandlerLifetime", api);
        Assert.Contains("Mediana.Dispatch.MessageRegistry", api);
        Assert.Contains("Mediana.Dispatch.EventDispatchPolicy", api);
        Assert.Contains("Mediana.MedianaDiagnostics", api);
    }

    [Fact]
    public async Task Behavioral_parity_ns21_asset_dispatches_all_kinds()
    {
        // Поведенческий паритет: тот же сценарий, что AotTests, против ns2.1-ассета
        var sc = new Microsoft.Extensions.DependencyInjection.ServiceCollection();
        sc.AddMediana(c => c
            .UseSingletonHandlers()
            .AddCommandHandler<NsCommand, string, NsHandler>()
            .AddEventHandler<NsEvent, NsEventHandler>()
            .AddStreamHandler<NsStream, int, NsStreamHandler>());
        sc.AddSingleton<NsHandler>();
        sc.AddSingleton<NsEventHandler>();
        sc.AddSingleton<NsStreamHandler>();
        var sp = sc.BuildServiceProvider();
        var mediator = sp.GetRequiredService<IMediator>();

        Assert.Equal("ns:ok", await mediator.Send((ICommand<string>)new NsCommand()));
        await mediator.Publish(new NsEvent());
        Assert.Equal(1, NsEventHandler.Count);

        var rows = 0;
        await foreach (var row in mediator.Stream((IStreamQuery<int>)new NsStream()))
        {
            rows++;
        }

        Assert.Equal(2, rows);
    }
}

public sealed record NsCommand() : ICommand<string>;
public sealed record NsEvent() : IEvent;
public sealed record NsStream() : IStreamQuery<int>;

public sealed class NsHandler : Handlers.ICommandHandler<NsCommand, string>
{
    public ValueTask<string> Handle(NsCommand command, CancellationToken ct) => new("ns:ok");
}

public sealed class NsEventHandler : Handlers.IEventHandler<NsEvent>
{
    public static int Count;

    public ValueTask Handle(NsEvent @event, CancellationToken ct)
    {
        Count++;
        return default;
    }
}

public sealed class NsStreamHandler : Handlers.IStreamHandler<NsStream, int>
{
    public IAsyncEnumerable<int> Handle(NsStream query, CancellationToken ct) => Rows();

    private static async IAsyncEnumerable<int> Rows()
    {
        yield return 1;
        await Task.Yield();
        yield return 2;
    }
}
