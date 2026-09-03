using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Mediana;

/// <summary>
/// (D15 §11.1): BCL ActivitySource/Meter
/// no-op (guard-§12 )
/// </summary>
public static class MedianaDiagnostics
{
    public const string ActivitySourceName = "Mediana";
    public const string MeterName = "Mediana";

    private static readonly ActivitySource ActivitySource = new(ActivitySourceName);
    private static readonly Meter Meter = new(MeterName);

    public static Activity? StartDispatch(string messageType)
        // Stryker disable once conditional: fallback/perf-(. CallSiteBranchTests: fast/slow )
        => ActivitySource.HasListeners()
            ? ActivitySource.StartActivity("dispatch " + messageType, ActivityKind.Internal)
            : null;

    public static Activity? StartPublish(string messageType)
        // Stryker disable once conditional: fallback/perf-(. CallSiteBranchTests: fast/slow )
        => ActivitySource.HasListeners()
            ? ActivitySource.StartActivity("publish " + messageType, ActivityKind.Internal)
            : null;

    public static Activity? StartConsume(string messageType)
        // Stryker disable once conditional: fallback/perf-(. CallSiteBranchTests: fast/slow )
        => ActivitySource.HasListeners()
            ? ActivitySource.StartActivity("consume " + messageType, ActivityKind.Consumer)
            : null;

    /// <summary>(, ).</summary>
    public static void Enrich(Activity? activity, string key, object? value)
    // Stryker disable once block: fallback/perf-(. CallSiteBranchTests: fast/slow )
    {
        // Stryker disable once statement: fallback/perf-(. CallSiteBranchTests: fast/slow )
        activity?.SetTag(key, value);
    }
}
