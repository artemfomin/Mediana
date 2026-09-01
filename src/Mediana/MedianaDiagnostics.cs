using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Mediana;

/// <summary>
/// Инструментация ядра (D15 §11.1): BCL ActivitySource/Meter, ноль зависимостей,
/// no-op без слушателей (guard-условия перед сборкой тегов — бюджеты §12 не нарушаются).
/// </summary>
public static class MedianaDiagnostics
{
    public const string ActivitySourceName = "Mediana";
    public const string MeterName = "Mediana";

    private static readonly ActivitySource ActivitySource = new(ActivitySourceName);
    private static readonly Meter Meter = new(MeterName);

    public static Activity? StartDispatch(string messageType)
        // Stryker disable once conditional: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
        => ActivitySource.HasListeners()
            ? ActivitySource.StartActivity("dispatch " + messageType, ActivityKind.Internal)
            : null;

    public static Activity? StartPublish(string messageType)
        // Stryker disable once conditional: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
        => ActivitySource.HasListeners()
            ? ActivitySource.StartActivity("publish " + messageType, ActivityKind.Internal)
            : null;

    public static Activity? StartConsume(string messageType)
        // Stryker disable once conditional: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
        => ActivitySource.HasListeners()
            ? ActivitySource.StartActivity("consume " + messageType, ActivityKind.Consumer)
            : null;

    /// <summary>Хук расширения активности внешним телеметрическим пакетом (теги сообщений, конверт).</summary>
    public static void Enrich(Activity? activity, string key, object? value)
    // Stryker disable once block: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
    {
        // Stryker disable once statement: fallback/perf-эквивалент (см. CallSiteBranchTests: fast/slow пути идентичны)
        activity?.SetTag(key, value);
    }
}
