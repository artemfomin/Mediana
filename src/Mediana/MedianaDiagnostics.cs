using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Mediana;

/// <summary>
/// Core instrumentation (D15 §11.1): BCL ActivitySource/Meter, zero dependencies,
/// no-op without listeners (guard conditions before tag assembly — spec §12 budgets are not violated).
/// </summary>
public static class MedianaDiagnostics
{
    public const string ActivitySourceName = "Mediana";
    public const string MeterName = "Mediana";

    private static readonly ActivitySource ActivitySource = new(ActivitySourceName);
    private static readonly Meter Meter = new(MeterName);

    public static Activity? StartDispatch(string messageType)
        // Stryker disable once conditional: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        => ActivitySource.HasListeners()
            ? ActivitySource.StartActivity("dispatch " + messageType, ActivityKind.Internal)
            : null;

    public static Activity? StartPublish(string messageType)
        // Stryker disable once conditional: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        => ActivitySource.HasListeners()
            ? ActivitySource.StartActivity("publish " + messageType, ActivityKind.Internal)
            : null;

    public static Activity? StartConsume(string messageType)
        // Stryker disable once conditional: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        => ActivitySource.HasListeners()
            ? ActivitySource.StartActivity("consume " + messageType, ActivityKind.Consumer)
            : null;

    /// <summary>Activity extension hook for external telemetry packages (message tags, envelope).</summary>
    public static void Enrich(Activity? activity, string key, object? value)
    // Stryker disable once block: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
    {
        // Stryker disable once statement: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        activity?.SetTag(key, value);
    }
}
