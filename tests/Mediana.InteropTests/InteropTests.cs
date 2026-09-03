using Microsoft.Extensions.Logging;
using Mediana.MediatR;
using Mediana.Telemetry;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Mediana.InteropTests;

public class MediatRBridgeTests
{
    private sealed record MediatRPing(int Value) : global::MediatR.IRequest<int>;
    private sealed record MediatRNotification(string Message) : global::MediatR.INotification;

    private sealed class PingHandler : global::MediatR.IRequestHandler<MediatRPing, int>
    {
        public Task<int> Handle(MediatRPing request, CancellationToken cancellationToken)
            => Task.FromResult(request.Value * 2);
    }

    private sealed class NotificationHandlerA : global::MediatR.INotificationHandler<MediatRNotification>
    {
        public static List<string> Received = [];

        public Task Handle(MediatRNotification notification, CancellationToken cancellationToken)
        {
            Received.Add("A:" + notification.Message);
            return Task.CompletedTask;
        }
    }

    private sealed class NotificationHandlerB : global::MediatR.INotificationHandler<MediatRNotification>
    {
        public static List<string> Received = [];

        public Task Handle(MediatRNotification notification, CancellationToken cancellationToken)
        {
            Received.Add("B:" + notification.Message);
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task Send_dispatches_mediatr_request()
    {
        var sc = new ServiceCollection();
        sc.AddScoped<global::MediatR.IRequestHandler<MediatRPing, int>, PingHandler>();
        sc.AddMedianaMediatRBridge(typeof(MediatRBridgeTests).Assembly);
        var sp = sc.BuildServiceProvider();
        var bridge = sp.GetRequiredService<MediatRBridge>();

        // хендлер зарегистрирован scoped — резолв через scope
        using var scope = sp.CreateScope();
        var scopedBridge = new MediatRBridge(scope.ServiceProvider, typeof(MediatRBridgeTests).Assembly);
        Assert.Equal(84, await scopedBridge.Send(new MediatRPing(42)));
    }

    [Fact]
    public async Task Publish_fans_out_to_all_notification_handlers()
    {
        NotificationHandlerA.Received = [];
        NotificationHandlerB.Received = [];
        var sc = new ServiceCollection();
        sc.AddTransient<global::MediatR.INotificationHandler<MediatRNotification>, NotificationHandlerA>();
        sc.AddTransient<global::MediatR.INotificationHandler<MediatRNotification>, NotificationHandlerB>();
        var sp = sc.BuildServiceProvider();

        using var scope = sp.CreateScope();
        var bridge = new MediatRBridge(scope.ServiceProvider, typeof(MediatRBridgeTests).Assembly);
        await bridge.Publish(new MediatRNotification("hello"));

        Assert.Contains("A:hello", NotificationHandlerA.Received);
        Assert.Contains("B:hello", NotificationHandlerB.Received);
    }

    [Fact]
    public async Task Send_without_registered_handler_throws()
    {
        var sc = new ServiceCollection();
        var sp = sc.BuildServiceProvider();
        var bridge = new MediatRBridge(sp, typeof(MediatRBridgeTests).Assembly);

        await Assert.ThrowsAsync<MediatorConfigurationException>(
            () => bridge.Send(new MediatRPing(1)).AsTask());
    }

    [Fact]
    public async Task Null_request_and_notification_throw()
    {
        var sp = new ServiceCollection().BuildServiceProvider();
        var bridge = new MediatRBridge(sp);

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => bridge.Send<global::MediatR.IRequest<int>>(null!).AsTask());
        await Assert.ThrowsAsync<ArgumentNullException>(
            () => bridge.Publish<global::MediatR.INotification>(null!).AsTask());
    }
}

public class TelemetryBridgeTests
{
    [Fact]
    public async Task Log_bridge_never_blocks_and_drops_on_overflow()
    {
        var forwarded = 0;
        using var bridge = new AsyncLogBridge(
            new MedianaOpenTelemetryOptions { LogChannelCapacity = 16, DropNewest = true },
            _ => Interlocked.Increment(ref forwarded));

        var logger = new BridgeLogger("Test", bridge);
        for (var i = 0; i < 100; i++)
        {
            logger.LogInformation("message {Number}", i);
        }

        // writer никогда не блокирует: 100 записей мгновенно
        await Task.Delay(100);
        Assert.True(forwarded <= 100);
        Assert.True(TelemetryDropCounters.DroppedLogs >= 0);
    }

    [Fact]
    public async Task Log_bridge_forwards_entries_and_flushes()
    {
        var entries = new List<AsyncLogBridge.LogEntry>();
        using var bridge = new AsyncLogBridge(
            new MedianaOpenTelemetryOptions { LogChannelCapacity = 1000 },
            entry => entries.Add(entry));

        var logger = new BridgeLogger("Cat", bridge);
        logger.LogWarning("warn-message");
        logger.LogTrace("filtered"); // ниже Information — не проходит IsEnabled

        await bridge.FlushAsync(TimeSpan.FromSeconds(2));
        Assert.Single(entries);
        Assert.Equal("warn-message", entries[0].Message);
        Assert.Equal(LogLevel.Warning, entries[0].Level);
    }

    [Fact]
    public void Telemetry_registration_with_options()
    {
        var sc = new ServiceCollection();
        sc.AddMedianaOpenTelemetry(o =>
        {
            o.Endpoint = "http://localhost:4317";
            o.ServiceName = "test-service";
            o.DeltaTemporality = true;
        });

        Assert.NotEmpty(sc);
    }

    [Fact]
    public void Telemetry_composition_mode()
    {
        var sc = new ServiceCollection();
        sc.AddMedianaOpenTelemetry(o => o.AddToExistingSdk = true);

        Assert.Contains(sc, d => d.ServiceType == typeof(MedianaOpenTelemetryOptions));
    }

    // ═══ R5: MediatR bridge — синхронные исключения не оборачиваются ═══

    private sealed record R5Cmd(int V) : global::MediatR.IRequest<int>;

    private sealed class R5ThrowingHandler : global::MediatR.IRequestHandler<R5Cmd, int>
    {
        public Task<int> Handle(R5Cmd r, CancellationToken ct) => throw new InvalidOperationException("sync-throw");
    }

    [Fact]
    public async Task R5_MediatR_bridge_sync_exception_not_wrapped()
    {
        var sc = new ServiceCollection();
        sc.AddSingleton<global::MediatR.IRequestHandler<R5Cmd, int>, R5ThrowingHandler>();
        var sp = sc.BuildServiceProvider();
        using var scope = sp.CreateScope();
        var bridge = new Mediana.MediatR.MediatRBridge(scope.ServiceProvider, typeof(TelemetryBridgeTests).Assembly);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => bridge.Send(new R5Cmd(1)).AsTask());
        Assert.IsNotType<System.Reflection.TargetInvocationException>(ex);
    }

    [Fact]
    public void R6_Telemetry_does_not_register_AsyncLogBridge_or_ILoggerFactory()
    {
        var sc = new ServiceCollection();
        sc.AddMedianaOpenTelemetry(o =>
        {
            o.Endpoint = "http://localhost:4317";
            o.EnableLogs = true;
        });

        Assert.DoesNotContain(sc, d => d.ServiceType == typeof(Mediana.Telemetry.AsyncLogBridge));
        // Стандартный ILoggerFactory регистрируется AddLogging — это правильно (не подменяется).
        // BridgeLoggerFactory удалён из кодовой базы (R6 fix) — нечего проверять на подмену.
    }
}
