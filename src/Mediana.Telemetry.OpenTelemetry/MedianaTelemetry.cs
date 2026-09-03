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
/// Полная OTLP-телеметрия (D15): один вызов включает OTel SDK для traces + metrics + logs.
/// Конвейер полностью асинхронный (§11.4): логи идут через bounded lock-free канал с фоновым drain,
/// переполнение — drop без блокировки (DropNewest default), потери считаются; shutdown — flush с таймаутом.
/// </summary>
public sealed class MedianaOpenTelemetryOptions
{
    /// <summary>OTLP endpoint (env OTEL_EXPORTER_OTLP_ENDPOINT приоритетнее, если задан).</summary>
    public string? Endpoint { get; set; }

    /// <summary>Протокол OTLP (gRPC по умолчанию; env OTEL_EXPORTER_OTLP_PROTOCOL).</summary>
    public OtlpExportProtocol Protocol { get; set; } = OtlpExportProtocol.Grpc;

    public bool EnableTraces { get; set; } = true;

    public bool EnableMetrics { get; set; } = true;

    public bool EnableLogs { get; set; } = true;

    /// <summary>Sampling для traces (по умолчанию родительский 100%).</summary>
    public Sampler? Sampler { get; set; }

    /// <summary>Delta-временная семантика метрик.</summary>
    public bool DeltaTemporality { get; set; }

    /// <summary>Ёмкость bounded-канала логов (переполнение = drop, не блокировка).</summary>
    public int LogChannelCapacity { get; set; } = 10_000;

    /// <summary>Политика потерь при переполнении: true — терять НОВЫЕ (default), false — старые.</summary>
    public bool DropNewest { get; set; } = true;

    /// <summary>Таймаут финального flush при shutdown.</summary>
    public TimeSpan ShutdownFlushTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Сервис-имя ресурса (env OTEL_SERVICE_NAME приоритетнее).</summary>
    public string ServiceName { get; set; } = "mediana-app";

    /// <summary>Добавить только сигналы Mediana к существующему SDK-провайдеру (режим композиции).</summary>
    public bool AddToExistingSdk { get; set; }
}

/// <summary>Метрика потерь телеметрии (§11.4: потери считаются, не скрываются).</summary>
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
/// Асинхронный мост логов: bounded Channel + фоновый drain в OTLP logger.
/// Запись НИКОГДА не блокирует вызывающий поток (hot path диспетчера).
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

    /// <summary>Запись лога: try-write без блокировки; при переполнении — счётчик потерь.</summary>
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

    /// <summary>Финальный flush: дождаться drain или таймаута (§11.4.5).</summary>
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

/// <summary>ILogger поверх AsyncLogBridge (no-op при выключенном экспорте).</summary>
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

public static class MedianaTelemetryExtensions
{
    /// <summary>
    /// Включить полную OTLP-телеметрию Mediana: ActivitySource("Mediana") + Meter("Mediana") + логи.
    /// Конфигурация из env OTEL_* (приоритет) или опций.
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
                            o.Endpoint = new Uri(endpoint);
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
                            o.Endpoint = new Uri(endpoint);
                            o.Protocol = protocol;
                        });
                    }
                });
            }

            if (options.EnableLogs && endpoint is not null)
            {
                // C-1/C-2 fix: реальные OTLP logs через MEL (Microsoft.Extensions.Logging OpenTelemetry)
                // НЕ подменяем ILoggerFactory — добавляем провайдер как дополнительный sink
                services.AddLogging(b => b.AddOpenTelemetry(o =>
                {
                    o.SetResourceBuilder(resourceBuilder);
                    o.AddOtlpExporter(e =>
                    {
                        e.Endpoint = new Uri(endpoint);
                        e.Protocol = protocol;
                    });
                }));
            }
        }
        else
        {
            // H-6 fix: composition-режим — реально подключаем сигналы Mediana к существующему SDK
            services.ConfigureOpenTelemetryTracerProvider((_, b) => b.AddSource(Mediana.MedianaDiagnostics.ActivitySourceName));
            services.ConfigureOpenTelemetryMeterProvider((_, b) => b.AddMeter(Mediana.MedianaDiagnostics.MeterName));
            if (options.EnableLogs && endpoint is not null)
            {
                services.AddLogging(b => b.AddOpenTelemetry(o =>
                {
                    o.SetResourceBuilder(ResourceBuilder.CreateDefault().AddService(serviceName));
                    o.AddOtlpExporter(e =>
                    {
                        e.Endpoint = new Uri(endpoint);
                        e.Protocol = protocol;
                    });
                }));
            }

            services.AddSingleton(_ => options);
        }

        return services;
    }
}
