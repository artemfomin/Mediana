# Security Audit — Telemetry, MedianaDiagnostics, MediatR Bridge

Scope: `src/Mediana.Telemetry.OpenTelemetry`, `src/Mediana/MedianaDiagnostics.cs`, `src/Mediana.MediatR`. Verified behaviour with `tests/Mediana.InteropTests`.
Method: read-only source inspection. Every finding cites `file:line`.

---

## Findings

### [Critical] AsyncLogBridge silently discards every log — README "full OTLP logs" is not implemented

- Location: `src/Mediana.Telemetry.OpenTelemetry/MedianaTelemetry.cs:236-241` (specifically :238).
- Description: When `EnableLogs` is true and an endpoint is configured, the bridge is instantiated with `new AsyncLogBridge(options, entry => { })` — the forward action is an empty lambda. `DrainLoop` reads channel entries and passes them to that lambda (:125), so every log line is silently dropped. There is no wiring to `OpenTelemetryLoggerProvider` / OTLP log exporter anywhere in the file (grep confirms `.AddOtlpExporter` only on trace and metric builders; `WithLogging` is never called).
- Impact: Applications enabling Mediana OTLP telemetry lose all ILogger output routed through the bridge, including their own application logs (see next finding — ILoggerFactory is replaced). Security-relevant events (auth failures, tenant switches, admin actions) vanish. README / D15 §11 claim of "OTLP logs" is unverifiable in code.
- Recommendation: Either remove the log branch entirely and document that logs are out of scope, or construct an `OpenTelemetryLoggerProvider` with the OTLP log exporter and pass a forward lambda that calls `provider.CreateLogger(category).Log(...)`. Add a unit test that asserts at least one record is exported.

---

### [Critical] AddMedianaOpenTelemetry replaces the host ILoggerFactory — every application log is silenced

- Location: `src/Mediana.Telemetry.OpenTelemetry/MedianaTelemetry.cs:240`.
- Description: `services.AddSingleton<ILoggerFactory>(sp => new BridgeLoggerFactory(...))` is appended after the host has already registered logging (via `AddLogging`, Serilog, Application Insights, etc.). MS.DI resolves the last registration, so `GetRequiredService<ILoggerFactory>()` — and every `ILogger<T>` created via `Microsoft.Extensions.Logging.Logger<T>` — will use `BridgeLoggerFactory`. `BridgeLoggerFactory.AddProvider` at :258-260 is a no-op, so any pre-registered Serilog / AppInsights / EventLog / Debug providers are discarded. Combined with C-1, every application log line is dropped.
- Impact: Total loss of application logging including security audit trails. Regulatory / compliance impact (GDPR Art. 30, SOC 2 CC7.2). Detection and forensic analysis blinded.
- Recommendation: Do NOT register `ILoggerFactory`. Register an `ILoggerProvider` via `services.AddLogging(b => b.AddProvider(new BridgeLoggerProvider(bridge)))` so Mediana adds an additional sink alongside existing providers. Use `TryAddEnumerable` where appropriate.

---

### [High] BridgeLogger ignores MEL filter configuration; hardcoded floor at Information

- Location: `src/Mediana.Telemetry.OpenTelemetry/MedianaTelemetry.cs:153`.
- Description: `IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Information;` is hardcoded. Standard `LoggerFilterOptions` / `appsettings.json` `Logging:LogLevel` configuration is bypassed. Operators cannot raise (to Warning for cost) or lower (to Debug / Trace for incident response) the level.
- Impact: No way to enable Debug / Trace during an incident; Debug / Trace security events silently swallowed; inflexible — may push operators to inject their own factory (impossible per C-2).
- Recommendation: Use `LoggerFilterOptions` / `IExternalScopeProvider` pattern like `OpenTelemetryLoggerProvider` does, or accept a `Func<string, LogLevel, bool>` in `MedianaOpenTelemetryOptions`.

---

### [High] BridgeLogger.BeginScope returns null — correlation IDs are lost

- Location: `src/Mediana.Telemetry.OpenTelemetry/MedianaTelemetry.cs:151`.
- Description: `BeginScope<TState>(TState state) where TState : notnull => null;`. Scopes carry TraceId, SpanId, RequestId, TenantId, user identity. Returning null drops the scope stack entirely.
- Impact: Log records that reach the bridge carry no request / trace correlation. Attribution during incident response impossible; cross-service tracing broken.
- Recommendation: Implement `IExternalScopeProvider` support: `AsyncLocal<Stack<object>>` and forward the scope stack into `LogEntryInternal.State`. Return a struct IDisposable that pops the stack.

---

### [High] Unvalidated OTEL_EXPORTER_OTLP_ENDPOINT — cleartext exfiltration and startup DoS

- Location: `src/Mediana.Telemetry.OpenTelemetry/MedianaTelemetry.cs:180`, consumed at :207 and :229 via `new Uri(endpoint)`.
- Description: The endpoint from the environment variable is passed unmodified to `new Uri(...)`. No scheme allow-list (accepts http, https, net.tcp, file, ldap, etc.), no host validation, no `IsWellFormedUriString` pre-check. `new Uri` throws `UriFormatException` on malformed input, propagating out of `AddMedianaOpenTelemetry` and crashing the DI container build.
- Impact: (1) Any actor able to influence the process environment (container CMD injection, compromised sidecar, K8s ConfigMap tampering, CI variable leak) can redirect all traces and metrics — and any future enrichment tags — to their own collector. (2) `http://attacker.example:4317` is accepted and used cleartext with no warning. (3) A malformed value is a one-shot startup crash / boot-loop DoS.
- Recommendation: `Uri.TryCreate(endpoint, UriKind.Absolute, out var uri)` + `uri.Scheme is "http" or "https"` + explicit warning when `Scheme == "http"`. Consider an opt-in `AllowInsecureOtlp = true` to accept plain http.

---

### [High] Composition mode (AddToExistingSdk = true) is a silent no-op

- Location: `src/Mediana.Telemetry.OpenTelemetry/MedianaTelemetry.cs:243-247`.
- Description: With `AddToExistingSdk = true` the else-branch runs `services.AddSingleton(_ => options);` and returns. No `ActivitySource("Mediana")` and no `Meter("Mediana")` is added to any existing `TracerProviderBuilder` / `MeterProviderBuilder`. The user's assumption is that Mediana signals will be picked up by their own OTel SDK; instead nothing happens.
- Impact: Users of the composition pattern get zero traces and metrics from Mediana, with no runtime warning. Silent observability failure.
- Recommendation: Register configurators that call `.AddSource("Mediana")` and `.AddMeter("Mediana")` on the existing builder, e.g. `services.ConfigureOpenTelemetryTracerProvider((_, b) => b.AddSource(MedianaDiagnostics.ActivitySourceName));`. Integration-test that the source is registered.

---

### [High] `assembly.GetTypes()` without ReflectionTypeLoadException handling — startup DoS

- Location: `src/Mediana.MediatR/MediatRAdapter.cs:39`.
- Description: `foreach (var type in assembly.GetTypes())` throws `ReflectionTypeLoadException` on any partially-loadable assembly (missing optional dependency, TFM mismatch, plugin scenarios). Uncaught; propagates from `MediatRBridge`'s constructor, aborting DI container build.
- Impact: A single bad type in a scanned assembly crashes startup even if the target handler is fine. Common when apps enable plugin folders or reference libraries with optional deps.
- Recommendation: Wrap in `try { types = assembly.GetTypes(); } catch (ReflectionTypeLoadException ex) { types = ex.Types.Where(t => t is not null)!; }`. Log a warning listing `ex.LoaderExceptions`.

---

### [High] MethodInfo.Invoke wraps handler exceptions in TargetInvocationException — contradicts XML doc

- Location: `src/Mediana.MediatR/MediatRAdapter.cs:93-95`.
- Description: The class XML doc at :12 claims exceptions pass through as-is. But `handleMethod.Invoke(...)` wraps synchronous handler exceptions (thrown before the Task is returned) in `TargetInvocationException`. Only exceptions faulting the returned Task reach `await` unchanged. Caller `catch (MyDomainException)` misses synchronously-thrown exceptions.
- Impact: (1) Broken exception semantics — caller error handling bypassed and exception type is rewritten; (2) TargetInvocationException.Message may leak reflection-internal details into logs that filter by exception message; (3) breaks middleware that inspects exception type for retry / DLQ classification.
- Recommendation: Use `MethodInfo.CreateDelegate` to a typed delegate cached per (requestType, TResponse); or catch `TargetInvocationException` and `ExceptionDispatchInfo.Capture(tie.InnerException!).Throw()`.

---

### [Medium] DrainLoop has no try/catch — a throwing forward lambda kills the drain thread permanently

- Location: `src/Mediana.Telemetry.OpenTelemetry/MedianaTelemetry.cs:121-127`.
- Description: `await foreach` invokes `forward(...)` synchronously without wrapping. If the forward action throws (a test seam or a future OTLP hook), the exception exits `DrainLoop`, `_drainTask` transitions to Faulted, and no further entries are drained. Subsequent `Write` calls succeed (channel still open) but back up until full, then increment `DroppedLogs`. Silent DoS on the log sink.
- Impact: One transient error in the forward path disables the entire log pipeline for the process lifetime.
- Recommendation: `try { forward(...); } catch (Exception ex) { TelemetryDropCounters.LogDropped(); /* best-effort console */ }`. Consider restart-on-fault or an `OnDrainError` callback.

---

### [Medium] AsyncLogBridge.Dispose does not flush; ShutdownFlushTimeout option is dead

- Location: `src/Mediana.Telemetry.OpenTelemetry/MedianaTelemetry.cs:141-145`; option defined at :47; `FlushAsync` at :130-139.
- Description: `Dispose()` cancels the CTS and disposes it, but never calls `FlushAsync`. Buffered entries in the channel are abandoned. `ShutdownFlushTimeout` is not referenced anywhere except its own property. The bridge is registered as `AddSingleton` at :239, so the only Dispose is via container disposal at process shutdown — exactly when a flush is required.
- Impact: Log entries produced in the last few milliseconds before shutdown (which often contain the actual crash / shutdown-cause information) are lost.
- Recommendation: Implement `IAsyncDisposable`; `DisposeAsync` awaits `FlushAsync(options.ShutdownFlushTimeout)` before cancelling. Or register a hosted service that ties `ApplicationStopping` to flush.

---

### [Medium] `services.AddOpenTelemetry()` is called unconditionally — duplicate exporters when host also uses OTel

- Location: `src/Mediana.Telemetry.OpenTelemetry/MedianaTelemetry.cs:190-234`.
- Description: `AddOpenTelemetry()` is idempotent for provider registration, but each `WithTracing` / `WithMetrics` action is stacked. If the host already called `AddOpenTelemetry().WithTracing(t => t.AddOtlpExporter(...))`, this method appends a second OTLP exporter and a second `SetResourceBuilder` call (the second silently overwrites host-configured resource attributes like `deployment.environment`, `service.instance.id`).
- Impact: Every span exported twice — doubled backend cost; host's resource attributes silently replaced, breaking multi-tenant / deployment attribution.
- Recommendation: Document that non-composition mode is exclusive; detect prior `TracerProvider` registration and refuse to double-register (throw), or move exporter setup behind an `!alreadyConfigured` check. Fix composition mode (H-6) so users have a real alternative.

---

### [Medium] Publish silently skips remaining handlers when one throws

- Location: `src/Mediana.MediatR/MediatRAdapter.cs:99-108`.
- Description: `foreach (var handler in handlers) await handler.Handle(...);` — on the first thrown exception iteration terminates. Handlers 2..N never run and there is no `AggregateException`. Diverges from Mediana's own `PublishParallel` semantics (Mediator.cs:176-215 per 00-context.md) and violates at-least-one-delivery expectations for observers that record audit events.
- Impact: Security-relevant handlers (audit logger, tamper-detector) registered after a fragile handler are silently skipped. No log or metric emitted.
- Recommendation: Catch each handler exception, collect into a list, throw `AggregateException` after the loop.

---

### [Medium] No reflection cache in Send — per-request `GetMethod` and boxing allocations

- Location: `src/Mediana.MediatR/MediatRAdapter.cs:82-93`.
- Description: Every call performs `MakeGenericType`, `GetService`, `handlerType.GetMethod("Handle")`, and `new object[] { request, cancellationToken }`. `GetMethod` is expensive and unnecessary — the handler type is known at scan time.
- Impact: Elevated CPU / GC on the hot dispatch path. Not a direct security issue, but a low-rate resource-exhaustion lever when combined with an unbounded request rate.
- Recommendation: Cache `ConcurrentDictionary<(Type, Type), Func<object, object, CancellationToken, Task>>` where the delegate is built once per (requestType, TResponse) via `MethodInfo.CreateDelegate` or `Expression.Compile`. Also removes `MethodInfo.Invoke` (fixes H-8).

---

### [Medium] No OTEL_EXPORTER_OTLP_HEADERS / OTLP protocol variants — insecure by omission

- Location: `src/Mediana.Telemetry.OpenTelemetry/MedianaTelemetry.cs:179-232`.
- Description: The OTel spec defines `OTEL_EXPORTER_OTLP_HEADERS` for bearer tokens / API keys used to authenticate to the collector. The code reads only `OTEL_SERVICE_NAME`, `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_PROTOCOL` and never sets `OtlpExporterOptions.Headers`. Additionally, values of `OTEL_EXPORTER_OTLP_PROTOCOL` other than `http/protobuf` silently downgrade to gRPC (:181-183) — the spec value `http/json` is not honoured and unknown values are not rejected.
- Impact: Operators securing their collector via bearer tokens will silently ship telemetry unauthenticated (rejected = data loss; accepted by a misconfigured collector = data leak). Silent protocol downgrade masks configuration errors.
- Recommendation: Wire `OtlpExporterOptions.Headers = Environment.GetEnvironmentVariable("OTEL_EXPORTER_OTLP_HEADERS")` (or per-signal overrides). Reject unknown protocol values with a startup exception rather than defaulting to gRPC.

---

### [Medium] `handlerType.GetMethod("Handle")` may throw AmbiguousMatchException

- Location: `src/Mediana.MediatR/MediatRAdapter.cs:91`.
- Description: If a handler implements another interface exposing a differently-typed `Handle` method (uncommon but legal), the ambiguity throws `AmbiguousMatchException`, uncaught. Propagates as a bare exception with no context.
- Impact: Startup or first-request crash with a cryptic reflection message.
- Recommendation: Bind by full signature: `handlerType.GetMethod("Handle", new[] { requestType, typeof(CancellationToken) })`.

---

### [Low] `_cts.Dispose()` called immediately after `_cts.Cancel()` — potential ObjectDisposedException race

- Location: `src/Mediana.Telemetry.OpenTelemetry/MedianaTelemetry.cs:143-144`.
- Description: The drain task holds the cancellation token and is signalled via internal registrations. Disposing the source immediately after cancellation may race with the callback firing on the drain thread. Not deterministic; likely benign because `ReadAllAsync` unregisters promptly.
- Recommendation: Await `_drainTask` before disposing the CTS; wrap in `try/catch (OperationCanceledException)`.

---

### [Low] Non-composition mode does not register MedianaOpenTelemetryOptions in DI

- Location: `src/Mediana.Telemetry.OpenTelemetry/MedianaTelemetry.cs:176-247`.
- Description: The options instance is built locally at :176 and (in the non-composition branch) never registered. Only the composition branch registers it (:246). Consumers cannot read effective options (endpoint, capacity, drop policy) from DI.
- Recommendation: Always `services.AddSingleton(options)`; use `IOptions<T>` for consistency.

---

### [Low] ResourceBuilder.AddService invoked twice with conflicting service versions

- Location: `src/Mediana.Telemetry.OpenTelemetry/MedianaTelemetry.cs:185-186` and :191.
- Description: The outer `resourceBuilder` uses `serviceVersion: Assembly.GetName().Version?.ToString()` (assembly version of the telemetry package). The inner `telemetryBuilder.ConfigureResource(r => r.AddService(serviceName, serviceVersion: null))` passes null. Last-write behaviour is undefined and depends on OTel SDK merge rules.
- Impact: `service.version` on emitted spans may be the telemetry package's version rather than the application's, or missing.
- Recommendation: Choose one; accept `options.ServiceVersion` and use it in both places.

---

### [Low] No TLS enforcement / warning for http:// OTLP endpoint

- Location: `src/Mediana.Telemetry.OpenTelemetry/MedianaTelemetry.cs:207,229`.
- Description: `new Uri("http://...")` is accepted silently. gRPC exporter defaults to plaintext `http://localhost:4317`. In production this is trivially sniffable.
- Recommendation: Warn (once) when scheme is `http` and host is not loopback. Provide `AllowInsecureOtlp` opt-in flag.

---

### [Info] MedianaDiagnostics API is defined but never called from src/ — the tracing pipeline is unwired

- Location: `src/Mediana/MedianaDiagnostics.cs:18-42`.
- Description: Grep across `src/` confirms `StartDispatch`, `StartPublish`, `StartConsume`, `Enrich` are called only from test files (`tests/Mediana.UnitTests/{Coverage95Tests,BranchCoverageTests,ClosingBranchTests,MutationKillerTests}.cs`). The Mediator and CallSite code paths do not invoke them. Adding `AddMedianaOpenTelemetry` yields zero real spans regardless of endpoint / exporter configuration.
- Impact: The "full OTLP telemetry" positioning is unverifiable at runtime.
- Recommendation: Wire `StartDispatch/StartPublish/StartConsume` at the appropriate points in `Mediator.Send/Publish/Stream` and inside the transport consumers. Add an integration test with an in-memory exporter that asserts non-zero span emission for a Send.

---

### [Info] Meter "Mediana" has zero instruments; drop counters are not exported as metrics

- Location: `src/Mediana/MedianaDiagnostics.cs:16`; `src/Mediana.Telemetry.OpenTelemetry/MedianaTelemetry.cs:57-69,219`.
- Description: A `Meter` is constructed but no `CreateCounter` / `CreateHistogram` / etc. is ever invoked in `src/`. `AddMeter("Mediana")` at :219 registers a source with no instruments. `TelemetryDropCounters` uses `Interlocked` on static longs — values are readable via static properties but never exported through OTel metrics.
- Impact: Operators cannot alert on `mediana_telemetry_logs_dropped` — the failure modes the code explicitly counts (§11.4) are hidden.
- Recommendation: Replace `Interlocked.Increment` with `Meter.CreateCounter<long>("mediana.telemetry.logs.dropped")`. Wire real dispatch / handler metrics on the Mediator hot path.

---

### [Info] Activity name assembly uses only bounded message-type strings

- Location: `src/Mediana/MedianaDiagnostics.cs:21,27,33`.
- Description: `"dispatch " + messageType` concatenation. `messageType` is a CLR type name from the generator / callsite registry; cardinality is bounded by declared message types. No headers or payload values enter the activity name. `Enrich` at :41 accepts arbitrary key/value — cardinality risk is delegated to callers (not currently wired, see I-20).
- Verdict: Safe as written. Document a callers-guide warning against passing user-controlled values to `Enrich`.

---

### [Info] Trimming / AOT annotations partially applied on MediatR bridge

- Location: `src/Mediana.MediatR/MediatRAdapter.cs:20-27, 36, 76-78, 123`.
- Description: `[RequiresUnreferencedCode]` / `[RequiresDynamicCode]` are on `Scan`, `Send`, and `AddMedianaMediatRBridge`. The `MediatRBridge(IServiceProvider, params Assembly[])` constructor lacks both attributes even though it directly calls `Scan`. `Publish` uses only closed-generic `GetServices<INotificationHandler<TNotification>>`, so `RequiresDynamicCode` is arguably not needed there.
- Recommendation: Add `[RequiresUnreferencedCode]` to the public constructor for consistency and to surface the warning to callers who bypass the DI helper.

---

## Summary Table

| # | Severity | Title |
|---|---|---|
| C-1 | Critical | AsyncLogBridge silently discards every log — README claim unimplemented (MedianaTelemetry.cs:238) |
| C-2 | Critical | AddMedianaOpenTelemetry replaces host ILoggerFactory — every app log silenced (MedianaTelemetry.cs:240) |
| H-3 | High | BridgeLogger.IsEnabled hardcoded >= Information, ignores MEL filters (MedianaTelemetry.cs:153) |
| H-4 | High | BridgeLogger.BeginScope returns null — correlation lost (MedianaTelemetry.cs:151) |
| H-5 | High | Unvalidated OTEL_EXPORTER_OTLP_ENDPOINT — exfiltration and startup DoS (MedianaTelemetry.cs:180,207) |
| H-6 | High | Composition mode is a silent no-op — no signals registered (MedianaTelemetry.cs:243-247) |
| H-7 | High | MediatR scan GetTypes() uncaught ReflectionTypeLoadException (MediatRAdapter.cs:39) |
| H-8 | High | MethodInfo.Invoke wraps handler exceptions in TargetInvocationException (MediatRAdapter.cs:93) |
| M-9 | Medium | DrainLoop lacks try/catch — one throw kills the drain thread (MedianaTelemetry.cs:121-127) |
| M-10 | Medium | Dispose does not flush; ShutdownFlushTimeout option is dead (MedianaTelemetry.cs:141-145) |
| M-11 | Medium | services.AddOpenTelemetry() non-idempotent -> duplicate exporters (MedianaTelemetry.cs:190-234) |
| M-12 | Medium | Publish silently skips remaining handlers on first throw (MediatRAdapter.cs:99-108) |
| M-13 | Medium | No reflection cache in Send — allocations per request (MediatRAdapter.cs:82-93) |
| M-14 | Medium | OTLP headers dropped; unknown protocol silently downgraded to gRPC (MedianaTelemetry.cs:179-183) |
| M-15 | Medium | GetMethod("Handle") may throw AmbiguousMatchException (MediatRAdapter.cs:91) |
| L-16 | Low | _cts.Dispose() race with drain-thread callback (MedianaTelemetry.cs:143-144) |
| L-17 | Low | Non-composition mode does not register options in DI (MedianaTelemetry.cs:176-247) |
| L-18 | Low | Conflicting AddService versions (MedianaTelemetry.cs:186 vs :191) |
| L-19 | Low | No TLS enforcement / warning for http:// OTLP endpoint (MedianaTelemetry.cs:207,229) |
| I-20 | Info | MedianaDiagnostics defined but never called from src — tracing unwired (MedianaDiagnostics.cs:18-42) |
| I-21 | Info | Meter has zero instruments; drop counters not exported (MedianaDiagnostics.cs:16; MedianaTelemetry.cs:57-69) |
| I-22 | Info | Activity names use bounded type strings — safe (MedianaDiagnostics.cs:21,27,33) |
| I-23 | Info | MediatR bridge — trimming annotation missing on constructor (MediatRAdapter.cs:20-27) |

---

## Checked & OK

- PII in activity tags: no SetTag / AddTag / RecordException / AddException call anywhere in src/ except `MedianaDiagnostics.Enrich` (which is never invoked from src/). No payload data is currently emitted to any Activity or Meter. Verified by grep across src/.
- Cardinality bombs on metrics: no `Meter.CreateCounter/Histogram` instruments exist; no tag values recorded. Zero cardinality surface. (Consequence: also zero useful metrics — see I-21.)
- Environment / process info in resource attributes: `ResourceBuilder.CreateDefault().AddService(name, version)` at MedianaTelemetry.cs:185-186 — no AddDetector / AddProcessDetector / AddHostDetector; no Environment.MachineName / UserName / OSVersion / ProcessName reads in the telemetry package (grep). Default resource attrs from OTel SDK are limited to `service.*` and `telemetry.sdk.*`. No info-disclosure via this package.
- `dynamic` / `Type.GetType` / `Assembly.Load`: none in `src/Mediana.Telemetry.OpenTelemetry`, `src/Mediana/MedianaDiagnostics.cs`, `src/Mediana.MediatR`.
- Env var reads: only `OTEL_SERVICE_NAME`, `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_PROTOCOL` (MedianaTelemetry.cs:179-183). No secret env leaks; no arbitrary env-var reflection.
- Bounded channel drop policy: `BoundedChannelFullMode.DropWrite` / `DropOldest` (:107) — non-blocking, correct semantics. Drop counter incremented atomically (:117).
- Thread-safety of TelemetryDropCounters: uses `Interlocked.Increment` / `Read` — safe.
- MediatR bridge null-guarding: `Guard(item)` at :110-116 correctly throws `ArgumentNullException` for both Send and Publish; verified by `Null_request_and_notification_throw` (InteropTests.cs:87-96).
- Handler-not-found: throws `MediatorConfigurationException` (:86-89), not a silent no-op. Verified by `Send_without_registered_handler_throws` (InteropTests.cs:76-84).
- Publish DI resolution: closed-generic `GetServices<INotificationHandler<TNotification>>` (:103) — no reflection, standard MS.DI resolution.
- Scan interface filters: correctly restricted to non-abstract, non-interface, non-open-generic types (MediatRAdapter.cs:41).
