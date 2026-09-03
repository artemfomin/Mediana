using System.Threading.Channels;
using Mediana;
using Mediana.Messaging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using OpenTelemetry;
using OpenTelemetry.Exporter;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace Mediana.Telemetry;

/// <summary>
/// OTLP-(D15): OTel SDK traces + metrics + logs
/// (§11.4): bounded lock-free drain
/// drop (DropNewest default), ; shutdown — flush
/// </summary>
public sealed class MedianaOpenTelemetryOptions
{
    /// <summary>OTLP endpoint (env OTEL_EXPORTER_OTLP_ENDPOINT , ).</summary>
    public string? Endpoint { get; set; }

    /// <summary>OTLP (gRPC ; env OTEL_EXPORTER_OTLP_PROTOCOL).</summary>
    public OtlpExportProtocol Protocol { get; set; } = OtlpExportProtocol.Grpc;

    public bool EnableTraces { get; set; } = true;

    public bool EnableMetrics { get; set; } = true;

    public bool EnableLogs { get; set; } = true;

    /// <summary>Sampling traces (100%).</summary>
    public Sampler? Sampler { get; set; }

    /// <summary>Delta-.</summary>
    public bool DeltaTemporality { get; set; }

    /// <summary>bounded-(= drop, ).</summary>
    public int LogChannelCapacity { get; set; } = 10_000;

    /// <summary>: true — (default), false — .</summary>
    public bool DropNewest { get; set; } = true;

    /// <summary>flush shutdown.</summary>
    public TimeSpan ShutdownFlushTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>(env OTEL_SERVICE_NAME ).</summary>
    public string ServiceName { get; set; } = "mediana-app";

    /// <summary>Mediana SDK-().</summary>
    public bool AddToExistingSdk { get; set; }
}

/// <summary>(§11.4: , ).</summary>
public static class TelemetryDropCounters
{
    private static long _droppedLogs;
    private static long _droppedExports;

    public static void LogDropped() => Interlocked.Increment(ref _droppedLogs);

    public static void ExportDropped() => Interlocked.Increment(ref _droppedExports);

    public static long DroppedLogs => Interlocked.Read(ref _droppedLogs);

    public static long DroppedExports => Interlocked.Read(ref _droppedExports);
}

/// <summary>
/// : bounded Channel + drain OTLP logger
/// (hot path )
/// </summary>
public sealed class AsyncLogBridge : IDisposable
{
    private readonly Channel<LogEntryInternal> _channel;

    public readonly struct LogEntry(string category, LogLevel level, EventId id, string message, Exception? exception)
    {
        public string Category { get; } = category;
        public LogLevel Level { get; } = level;
        public EventId Id { get; } = id;
        public string Message { get; } = message;
        public Exception? Exception { get; } = exception;
    }

    private readonly struct LogEntryInternal(string category, LogLevel level, EventId id, string message, Exception? exception, IReadOnlyList<KeyValuePair<string, object?>>? state)
    {
        public string Category { get; } = category;
        public LogLevel Level { get; } = level;
        public EventId Id { get; } = id;
        public string Message { get; } = message;
        public Exception? Exception { get; } = exception;
        public IReadOnlyList<KeyValuePair<string, object?>>? State { get; } = state;
    }

    private readonly CancellationTokenSource _cts = new();
    private readonly Task _drainTask;

    public AsyncLogBridge(MedianaOpenTelemetryOptions options, Action<LogEntry> forward)
    {
        _channel = Channel.CreateBounded<LogEntryInternal>(new BoundedChannelOptions(options.LogChannelCapacity)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = options.DropNewest ? BoundedChannelFullMode.DropWrite : BoundedChannelFullMode.DropOldest,
        });
        _drainTask = Task.Run(() => DrainLoop(forward, _cts.Token));
    }

    /// <summary>: try-write ; .</summary>
    public void Write(string category, LogLevel level, EventId eventId, string message, Exception? exception = null)
    {
        if (!_channel.Writer.TryWrite(new LogEntryInternal(category, level, eventId, message, exception, null)))
        {
            TelemetryDropCounters.LogDropped();
        }
    }

    private async Task DrainLoop(Action<LogEntry> forward, CancellationToken cancellationToken)
    {
        await foreach (var entry in _channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
        {
            forward(new LogEntry(entry.Category, entry.Level, entry.Id, entry.Message, entry.Exception));
        }
    }

    /// <summary>flush: drain (§11.4.5).</summary>
    public async Task FlushAsync(TimeSpan timeout)
    {
        _channel.Writer.TryComplete();
        var delay = Task.Delay(timeout);
        var done = await Task.WhenAny(_drainTask, delay).ConfigureAwait(false);
        if (done == delay)
        {
            TelemetryDropCounters.ExportDropped();
        }
    }

    public void Dispose()
    {
        _cts.Cancel();
        _cts.Dispose();
    }
}

/// <summary>ILogger AsyncLogBridge (no-op ).</summary>
public sealed class BridgeLogger(string category, AsyncLogBridge bridge) : ILogger
{
    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Information;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        if (!IsEnabled(logLevel))
        {
            return;
        }

        bridge.Write(category, logLevel, eventId, formatter(state, exception), exception);
    }
}

internal static class OtlpEndpointValidator
{
    public static Uri Validate(string? endpoint)
    {
        if (string.IsNullOrWhiteSpace(endpoint))
            throw new InvalidOperationException("OTLP endpoint is required but not set.");
        if (!Uri.TryCreate(endpoint, UriKind.Absolute, out var uri))
            throw new InvalidOperationException($"Invalid OTLP endpoint URI: '{endpoint}'");
        if (uri.Scheme is not ("http" or "https" or "grpc"))
            throw new InvalidOperationException($"OTLP endpoint scheme must be http/https/grpc, got '{uri.Scheme}'");
        if (uri.Scheme == "http" && !uri.IsLoopback)
            throw new InvalidOperationException($"OTLP endpoint uses insecure http on non-loopback '{uri}'. Set AllowInsecureOtlp = true to override.");
        return uri;
    }
}

public static class MedianaTelemetryExtensions
{
    /// <summary>
    /// OTLP-Mediana: ActivitySource("Mediana") + Meter("Mediana") +
    /// env OTEL_* ()
    /// </summary>
    public static IServiceCollection AddMedianaOpenTelemetry(
        this IServiceCollection services,
        Action<MedianaOpenTelemetryOptions>? configure = null)
    {
        var options = new MedianaOpenTelemetryOptions();
        configure?.Invoke(options);

        var serviceName = Environment.GetEnvironmentVariable("OTEL_SERVICE_NAME") ?? options.ServiceName;
        var endpoint = Environment.GetEnvironmentVariable("OTEL_EXPORTER_OTLP_ENDPOINT") ?? options.Endpoint;
        var protocol = Environment.GetEnvironmentVariable("OTEL_EXPORTER_OTLP_PROTOCOL") is { } p
            ? (p.Equals("http/protobuf", StringComparison.OrdinalIgnoreCase) ? OtlpExportProtocol.HttpProtobuf : OtlpExportProtocol.Grpc)
            : options.Protocol;

        var resourceBuilder = ResourceBuilder.CreateDefault()
            .AddService(serviceName: serviceName, serviceVersion: typeof(MedianaTelemetryExtensions).Assembly.GetName().Version?.ToString());

        if (!options.AddToExistingSdk)
        {
            var telemetryBuilder = services.AddOpenTelemetry();
            telemetryBuilder.ConfigureResource(r => r.AddService(serviceName, serviceVersion: null));

            if (options.EnableTraces)
            {
                telemetryBuilder.WithTracing(t =>
                {
                    t.SetResourceBuilder(resourceBuilder);
                    if (options.Sampler is { } sampler)
                    {
                        t.SetSampler(sampler);
                    }

                    if (endpoint is not null)
                    {
                        t.AddOtlpExporter(o =>
                        {
                            o.Endpoint = OtlpEndpointValidator.Validate(endpoint);
                            o.Protocol = protocol;
                        });
                    }
                });
            }

            if (options.EnableMetrics)
            {
                telemetryBuilder.WithMetrics(m =>
                {
                    m.SetResourceBuilder(resourceBuilder);
                    m.AddMeter("Mediana");
                    if (options.DeltaTemporality)
                    {
                        m.SetExemplarFilter(ExemplarFilterType.TraceBased);
                    }

                    if (endpoint is not null)
                    {
                        m.AddOtlpExporter(o =>
                        {
                            o.Endpoint = OtlpEndpointValidator.Validate(endpoint);
                            o.Protocol = protocol;
                        });
                    }
                });
            }

            if (options.EnableLogs && endpoint is not null)
            {
                // C-1/C-2 fix: OTLP logs MEL (Microsoft.Extensions.Logging OpenTelemetry)
                // ILoggerFactory — sink
                services.AddLogging(b => b.AddOpenTelemetry(o =>
                {
                    o.SetResourceBuilder(resourceBuilder);
                    o.AddOtlpExporter(e =>
                    {
                        e.Endpoint = OtlpEndpointValidator.Validate(endpoint);
                        e.Protocol = protocol;
                    });
                }));
            }
        }
        else
        {
            // H-6 fix: composition-Mediana SDK
            services.ConfigureOpenTelemetryTracerProvider((_, b) => b.AddSource(Mediana.MedianaDiagnostics.ActivitySourceName));
            services.ConfigureOpenTelemetryMeterProvider((_, b) => b.AddMeter(Mediana.MedianaDiagnostics.MeterName));
            if (options.EnableLogs && endpoint is not null)
            {
                services.AddLogging(b => b.AddOpenTelemetry(o =>
                {
                    o.SetResourceBuilder(ResourceBuilder.CreateDefault().AddService(serviceName));
                    o.AddOtlpExporter(e =>
                    {
                        e.Endpoint = OtlpEndpointValidator.Validate(endpoint);
                        e.Protocol = protocol;
                    });
                }));
            }

            services.AddSingleton(_ => options);
        }

        return services;
    }
}
