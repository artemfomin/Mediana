using System.Collections.Immutable;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace Mediana.Generators;

/// <summary>
/// Incremental source generator: находит закрытые реализации ICommandHandler/IQueryHandler/
/// IEventHandler/IStreamHandler и генерирует MedianaRegistrar.AddGeneratedHandlers() —
/// регистрацию без рефлексии (AOT-совместимо, D6). Дубликаты command/query/stream хендлеров —
/// диагностика MED001 на компиляции.
/// </summary>
[Generator(LanguageNames.CSharp)]
public sealed class MedianaGenerator : IIncrementalGenerator
{
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var handlers = context.SyntaxProvider
            .CreateSyntaxProvider(
                static (node, _) => node is ClassDeclarationSyntax { BaseList.Types.Count: > 0 },
                static (ctx, _) => ExtractHandler(ctx))
            .Where(static h => h is not null)
            .Select(static (h, _) => h!.Value)
            .WithTrackingName(TrackingNames.Extract);

        var collected = handlers.Collect().WithTrackingName(TrackingNames.Collect);

        context.RegisterSourceOutput(collected, static (spc, handlers) =>
        {
            Emit(spc, handlers);
        });
    }

    internal static class TrackingNames
    {
        public const string Extract = "Mediana_Extract";
        public const string Collect = "Mediana_Collect";
    }

    internal readonly record struct HandlerEntry(
        HandlerKind Kind,
        string MessageTypeFqn,
        string ResponseTypeFqn,
        string HandlerTypeFqn,
        Location Location);

    internal enum HandlerKind
    {
        Command,
        Query,
        Event,
        Stream,
    }

    private static HandlerEntry? ExtractHandler(GeneratorSyntaxContext context)
    {
        if (context.Node is not ClassDeclarationSyntax classDecl)
        {
            return null;
        }

        var symbol = context.SemanticModel.GetDeclaredSymbol(classDecl);
        if (symbol is null || symbol.TypeKind != TypeKind.Class || symbol.IsGenericType || symbol.IsAbstract)
        {
            return null;
        }

        foreach (var iface in symbol.AllInterfaces)
        {
            if (!iface.IsGenericType)
            {
                continue;
            }

            var def = iface.OriginalDefinition;
            var args = iface.TypeArguments;
            if (args.Length != 2 || args[0] is not INamedTypeSymbol message || args[1] is not INamedTypeSymbol response)
            {
                continue;
            }

            var fqn = def.ToDisplayString();
            if (fqn == "Mediana.Handlers.ICommandHandler<TCommand, TResponse>")
            {
                return new HandlerEntry(HandlerKind.Command, Fqn(message), Fqn(response), Fqn(symbol), classDecl.GetLocation());
            }

            if (fqn == "Mediana.Handlers.IQueryHandler<TQuery, TResponse>")
            {
                return new HandlerEntry(HandlerKind.Query, Fqn(message), Fqn(response), Fqn(symbol), classDecl.GetLocation());
            }

            if (fqn == "Mediana.Handlers.IStreamHandler<TQuery, TRow>")
            {
                return new HandlerEntry(HandlerKind.Stream, Fqn(message), Fqn(response), Fqn(symbol), classDecl.GetLocation());
            }

            if (fqn == "Mediana.Handlers.IEventHandler<TEvent>")
            {
                continue; // arity 2 check выше пропускает события; отдельная ветка ниже
            }
        }

        // события: arity 1
        foreach (var iface in symbol.AllInterfaces)
        {
            if (!iface.IsGenericType || iface.TypeArguments.Length != 1)
            {
                continue;
            }

            if (iface.OriginalDefinition.ToDisplayString() == "Mediana.Handlers.IEventHandler<TEvent>"
                && iface.TypeArguments[0] is INamedTypeSymbol message)
            {
                return new HandlerEntry(HandlerKind.Event, Fqn(message), "void", Fqn(symbol), classDecl.GetLocation());
            }
        }

        return null;
    }

    private static string Fqn(INamedTypeSymbol symbol)
    {
        if (symbol.ContainingType is not null)
        {
            return symbol.ContainingType.ToDisplayString() + "." + symbol.Name;
        }

        var ns = symbol.ContainingNamespace is { IsGlobalNamespace: false } n
            ? n.ToDisplayString() + "."
            : string.Empty;
        return ns + symbol.Name;
    }

    private static void Emit(SourceProductionContext context, ImmutableArray<HandlerEntry> handlers)
    {
        // MED001: дубликаты command/query/stream
        var reported = new HashSet<(HandlerKind, string)>();
        foreach (var handler in handlers)
        {
            if (handler.Kind == HandlerKind.Event)
            {
                continue;
            }

            var key = (handler.Kind, handler.MessageTypeFqn);
            if (!reported.Add(key))
            {
                context.ReportDiagnostic(Diagnostic.Create(
                    new DiagnosticDescriptor(
                        MedianaDiagnostics.DuplicateHandlerId,
                        MedianaDiagnostics.DuplicateHandlerTitle,
                        MedianaDiagnostics.DuplicateHandlerMessage,
                        MedianaDiagnostics.Category,
                        DiagnosticSeverity.Error,
                        isEnabledByDefault: true),
                    handler.Location,
                    handler.MessageTypeFqn,
                    KindName(handler.Kind),
                    handler.HandlerTypeFqn));
            }
        }

        var sb = new StringBuilder(2048);
        sb.AppendLine("// <auto-generated>Mediana.Generators</auto-generated>");
        sb.AppendLine("#nullable enable");
        sb.AppendLine();
        sb.AppendLine("namespace Mediana.Generated");
        sb.AppendLine("{");
        sb.AppendLine("    /// <summary>Сгенерированный регистратор хендлеров (без рефлексии, AOT-совместимо).</summary>");
        sb.AppendLine("    public static class MedianaRegistrar");
        sb.AppendLine("    {");
        sb.AppendLine("        /// <summary>Добавить все найденные в сборке хендлеры.</summary>");
        sb.AppendLine("        public static Mediana.MedianaConfiguration AddGeneratedHandlers(this Mediana.MedianaConfiguration configuration)");
        sb.AppendLine("        {");

        foreach (var handler in handlers)
        {
            switch (handler.Kind)
            {
                case HandlerKind.Command:
                    sb.AppendLine($"            configuration.AddCommandHandler<{handler.MessageTypeFqn}, {handler.ResponseTypeFqn}, {handler.HandlerTypeFqn}>();");
                    break;
                case HandlerKind.Query:
                    sb.AppendLine($"            configuration.AddQueryHandler<{handler.MessageTypeFqn}, {handler.ResponseTypeFqn}, {handler.HandlerTypeFqn}>();");
                    break;
                case HandlerKind.Event:
                    sb.AppendLine($"            configuration.AddEventHandler<{handler.MessageTypeFqn}, {handler.HandlerTypeFqn}>();");
                    break;
                case HandlerKind.Stream:
                    sb.AppendLine($"            configuration.AddStreamHandler<{handler.MessageTypeFqn}, {handler.ResponseTypeFqn}, {handler.HandlerTypeFqn}>();");
                    break;
            }
        }

        sb.AppendLine("            return configuration;");
        sb.AppendLine("        }");
        sb.AppendLine("    }");
        sb.AppendLine("}");

        context.AddSource("MedianaRegistrar.g.cs", SourceText.From(sb.ToString(), Encoding.UTF8));
    }

    private static string KindName(HandlerKind kind)
    {
        return kind switch
        {
            HandlerKind.Command => "command",
            HandlerKind.Query => "query",
            HandlerKind.Stream => "stream",
            _ => "event",
        };
    }
}
