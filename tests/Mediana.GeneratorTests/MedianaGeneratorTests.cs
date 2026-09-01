using System.Collections.Immutable;
using System.Linq;
using Mediana.Generators;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Xunit;

namespace Mediana.GeneratorTests;

public class MedianaGeneratorTests
{
    private static readonly string AbstractionsSource = """
        namespace Mediana.Messaging
        {
            public interface IRequest {}
            public interface IRequest<TResponse> : IRequest {}
            public interface ICommand : IRequest {}
            public interface ICommand<TResponse> : IRequest<TResponse> {}
            public interface IQuery<TResponse> : IRequest<TResponse> {}
            public interface IEvent : IRequest {}
            public interface IStreamQuery<TRow> : IRequest {}
        }
        namespace Mediana.Handlers
        {
            public interface ICommandHandler<in TCommand, TResponse> { System.Threading.Tasks.ValueTask<TResponse> Handle(TCommand c, System.Threading.CancellationToken ct); }
            public interface IQueryHandler<in TQuery, TResponse> { System.Threading.Tasks.ValueTask<TResponse> Handle(TQuery q, System.Threading.CancellationToken ct); }
            public interface IEventHandler<in TEvent> { System.Threading.Tasks.ValueTask Handle(TEvent e, System.Threading.CancellationToken ct); }
            public interface IStreamHandler<in TQuery, TRow> { System.Collections.Generic.IAsyncEnumerable<TRow> Handle(TQuery q, System.Threading.CancellationToken ct); }
        }
        """;

    private static (ImmutableArray<Diagnostic> Diagnostics, string GeneratedSource) RunGenerator(string userSource)
    {
        var compilation = CSharpCompilation.Create(
            "TestAssembly",
            new[] { CSharpSyntaxTree.ParseText(userSource), CSharpSyntaxTree.ParseText(AbstractionsSource) },
            new[]
            {
                MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
            },
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        var generator = new MedianaGenerator();
        var driver = CSharpGeneratorDriver.Create(generator).RunGenerators(compilation);
        GeneratorDriverRunResult result = driver.GetRunResult();
        var diagnostics = result.Diagnostics;
        var source = result.GeneratedTrees.Length > 0
            ? result.GeneratedTrees[0].GetText().ToString()
            : string.Empty;
        return (diagnostics, source);
    }

    private const string HandlersSource = """
        namespace TestApp
        {
            public sealed record CreateOrder(int Id) : Mediana.Messaging.ICommand<OrderId>;
            public readonly record struct OrderId(int Value);
            public sealed record GetOrder(int Id) : Mediana.Messaging.IQuery<OrderDto>;
            public sealed record OrderDto(int Id);
            public sealed record OrderCreated(int Id) : Mediana.Messaging.IEvent;
            public sealed record SearchOrders(string F) : Mediana.Messaging.IStreamQuery<OrderDto>;

            public sealed class CreateOrderHandler : Mediana.Handlers.ICommandHandler<CreateOrder, OrderId>
            {
                public System.Threading.Tasks.ValueTask<OrderId> Handle(CreateOrder c, System.Threading.CancellationToken ct) => new(new OrderId(c.Id));
            }

            public sealed class GetOrderHandler : Mediana.Handlers.IQueryHandler<GetOrder, OrderDto>
            {
                public System.Threading.Tasks.ValueTask<OrderDto> Handle(GetOrder q, System.Threading.CancellationToken ct) => new(new OrderDto(q.Id));
            }

            public sealed class AuditHandler : Mediana.Handlers.IEventHandler<OrderCreated>
            {
                public System.Threading.Tasks.ValueTask Handle(OrderCreated e, System.Threading.CancellationToken ct) => default;
            }

            public sealed class MetricsHandler : Mediana.Handlers.IEventHandler<OrderCreated>
            {
                public System.Threading.Tasks.ValueTask Handle(OrderCreated e, System.Threading.CancellationToken ct) => default;
            }

            public sealed class SearchHandler : Mediana.Handlers.IStreamHandler<SearchOrders, OrderDto>
            {
                public System.Collections.Generic.IAsyncEnumerable<OrderDto> Handle(SearchOrders q, System.Threading.CancellationToken ct) => throw new System.NotImplementedException();
            }
        }
        """;

    [Fact]
    public void Generates_registrations_for_all_handler_kinds()
    {
        var (diagnostics, source) = RunGenerator(HandlersSource);

        Assert.Empty(diagnostics.Where(d => d.Severity == DiagnosticSeverity.Error));
        Assert.Contains("AddCommandHandler<TestApp.CreateOrder, TestApp.OrderId, TestApp.CreateOrderHandler>", source);
        Assert.Contains("AddQueryHandler<TestApp.GetOrder, TestApp.OrderDto, TestApp.GetOrderHandler>", source);
        Assert.Contains("AddEventHandler<TestApp.OrderCreated, TestApp.AuditHandler>", source);
        Assert.Contains("AddEventHandler<TestApp.OrderCreated, TestApp.MetricsHandler>", source);
        Assert.Contains("AddStreamHandler<TestApp.SearchOrders, TestApp.OrderDto, TestApp.SearchHandler>", source);
    }

    [Fact]
    public void Ignores_non_handler_classes()
    {
        var (diagnostics, source) = RunGenerator("""
            namespace TestApp
            {
                public sealed class PlainService {}
                public abstract class AbstractHandler : Mediana.Handlers.ICommandHandler<CreateOrder, OrderId>
                {
                    public System.Threading.Tasks.ValueTask<OrderId> Handle(CreateOrder c, System.Threading.CancellationToken ct) => default;
                }
                public sealed record CreateOrder(int Id) : Mediana.Messaging.ICommand<OrderId>;
                public readonly record struct OrderId(int Value);
            }
            """);

        Assert.Empty(diagnostics.Where(d => d.Severity == DiagnosticSeverity.Error));
        Assert.DoesNotContain("AddCommandHandler", source);
    }

    [Fact]
    public void Duplicate_command_handler_produces_MED001()
    {
        var (diagnostics, _) = RunGenerator("""
            namespace TestApp
            {
                public sealed record Ping(string V) : Mediana.Messaging.ICommand<int>;
                public sealed class H1 : Mediana.Handlers.ICommandHandler<Ping, int>
                {
                    public System.Threading.Tasks.ValueTask<int> Handle(Ping c, System.Threading.CancellationToken ct) => default;
                }
                public sealed class H2 : Mediana.Handlers.ICommandHandler<Ping, int>
                {
                    public System.Threading.Tasks.ValueTask<int> Handle(Ping c, System.Threading.CancellationToken ct) => default;
                }
            }
            """);

        var med001 = diagnostics.FirstOrDefault(d => d.Id == "MED001");
        Assert.NotNull(med001);
        Assert.Equal(DiagnosticSeverity.Error, med001.Severity);
        Assert.Contains("Ping", med001.GetMessage());
    }

    [Fact]
    public void Multiple_event_handlers_are_allowed()
    {
        var (diagnostics, source) = RunGenerator("""
            namespace TestApp
            {
                public sealed record Evt : Mediana.Messaging.IEvent;
                public sealed class A : Mediana.Handlers.IEventHandler<Evt>
                {
                    public System.Threading.Tasks.ValueTask Handle(Evt e, System.Threading.CancellationToken ct) => default;
                }
                public sealed class B : Mediana.Handlers.IEventHandler<Evt>
                {
                    public System.Threading.Tasks.ValueTask Handle(Evt e, System.Threading.CancellationToken ct) => default;
                }
            }
            """);

        Assert.Empty(diagnostics.Where(d => d.Severity == DiagnosticSeverity.Error));
        Assert.Contains("AddEventHandler<TestApp.Evt, TestApp.A>", source);
        Assert.Contains("AddEventHandler<TestApp.Evt, TestApp.B>", source);
    }
}
