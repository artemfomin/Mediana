# Mediana — specification v1

in –3 North star: **level ** — performance where is ## 1. Mediana — library mediator for .NET 10 API (not MediatR), messages (RabbitMQ, Kafka, MassTransit and and where transactional outbox — opt-in option NuGet-package.

### Non-goals (v1)

- / not MassTransit (in MassTransit, in Mediana).
- NET Framework 4.x (netstandard2.0) — north star.
- «→ in queue» — exceptions and latency. only policy RPC-TCP — only queues.

---

## 2. decision log)

| # | | |
|---|---------|-------------|
| D1 | API + package `Mediana.MediatR` (MediatR-handlers) | without MediatR; without |
| D2 | **versions — **: net10.0 and netstandard2.1 for where allows library, D13), API-namespace and how **and NuGet-** (ID) | commands net10.0-all FrozenDictionary — ~47% lookup Dictionary, System.Threading.Lock, GetAlternateLookup, R2R), ns2.1-where API and = ID on TFM on build in «40+%»: this benchmark lookup'on end-to-end Send source-gen lookup net10-on fallback-async-and startup (R2R) |
| D13 | /packages per TFM: RabbitMQ — net10.0: RabbitMQ.Client 7.x, ns2.1: 6.x (API — package); Kafka — Confluent.Kafka unified API (in MassTransit 8.x — supports ns2.1-Dapper/Mongo — ns2.1 **EF Core-provider — net10.0-only** (EF Core 6+ not ns2.1; ns2.1-outbox Dapper/Mongo-| «» without API; exception (EF) |
| D14 | **third-party metric north star.** Abstractions, Mediana, Transport.Abstractions, Generators, Outbox) — only dependencies Microsoft and only where this MEDI-for DI, Roslyn for STJ how by only in where SDK and is package (DB-MessagePack/protobuf) | and in retry-and backoff (not Polly), /IVTS (not ObjectPooling), UUIDv7 on ns2.1 (on net10.0 — `Guid.CreateVersion7`), relay. metric CI-|
| D15 | **OTLP-telemetry.** in BCL (`ActivitySource("Mediana")`, `Meter("Mediana")`, `ILogger`), and on no-op Activity/Meter API). OTLP-package `Mediana.Telemetry.OpenTelemetry` (net10.0 + ns2.1): OTel SDK for traces + metrics + logs) OTLP-OTel messaging semantic conventions; setting — `OTEL_EXPORTER_OTLP_*` env + fluent-options. **async**: inline — only in memory, I/O logs and not bounded-queues, drop-on-overflow flush on shutdown — §11.4) | telemetry from without D14 and D16; Tempo/Jaeger/OTel Collector) |
| D16 | **not only Send.** in-process MediatR on Publish (sequential/parallel), Stream and benchmark-all MediatR 12.x in CI | MediatR in-process on per-call behaviors from DI, on each and Task-Mediana these by what |
| D3 | integration policy / queue / + transport-MassTransit — and how transport, and how | + |
| D4 | in v1: retry, DLQ, poison detection, inbox — in **outbox — opt-in NuGet-packages** | without on transactional-guarantees — by |
| D5 | parity MediatR + `IAsyncEnumerable`), without | on ns2.1+, MassTransit |
| D6 | source-gen static fast-path + runtime-copy-on-write) | + escape hatch for |
| D7 | messages: shared `IRequest`; `ICommand`/`IQuery`/`IEvent`/`IStreamQuery` — | for generic-by on |

without on | # | | |
|---|---------|-------------|
| D8 | DI — only `Microsoft.Extensions.DependencyInjection` | keyed services and on ns2.1 |
| D9 | serialization by System.Text.Json source-gen; MessagePack and protobuf per message type | Zero-reflection + by |
| D10 | `MessageId` — UUIDv7 (net10.0: `Guid.CreateVersion7()`; ns2.1: implementation, D14) | Sortable → friendly for outbox/inbox |
| D11 | in v1 — only RabbitMQ (chunked reply frames) and MassTransit; Kafka — documented limitation) | Kafka not for streaming reply; fetch-loop pattern |
| D12 | OpenTelemetry-first Send — `RemoteExecutionException` | MassTransit-Fault-events |

---

## 2.1. | # | | |
|---|---------|-------------|
| D17 | behaviors in **Middleware** (IPipelineBehavior → IHandlerMiddleware, IEventPipelineBehavior → IEventMiddleware, IStreamPipelineBehavior → IStreamMiddleware, RequestHandlerDelegate<,> → HandlerDelegate<,>, methods Add*Behavior → Add*Middleware | MediatR CS0104-on Middleware — model (ASP.NET Core/MassTransit), Namespace Mediana.Pipeline |

## 3. and packages

```
Mediana.sln
├── src/
│ ├── Mediana.Abstractions/ # net10.0 + ns2.1. messages/handlers,
│ │ # envelope, │ ├── Mediana/ # net10.0 + ns2.1. In-process dispatcher, pipelines,
│ │ # runtime-DI-integration, │ ├── Mediana.Generators/ # netstandard2.0 (│ │ # Incremental source generator + │ ├── Mediana.Transport.Abstractions/ # net10.0 + ns2.1. SPI transports: ITransport,
│ │ # publisher/consumer, capabilities,
│ │ # IInboxStore + in-memory implementation.
│ ├── Mediana.RabbitMQ/ # net10.0 (client 7.x) + ns2.1 (client 6.x, │ ├── Mediana.Kafka/ # net10.0 + ns2.1 (Confluent.Kafka, in │ ├── Mediana.MassTransit/ # net10.0 + ns2.1 (MassTransit 8.x).
│ ├── Mediana.Outbox/ # net10.0 + ns2.1. transactional outbox + relay
│ │ # (opt-in); DB-inbox/outbox — in │ ├── Mediana.Outbox.EFCore/ # net10.0-only (EF Core 6+ not ns2.1; D13).
│ ├── Mediana.Outbox.Dapper/ # net10.0 + ns2.1. Dapper/SQL provider (opt-in).
│ ├── Mediana.Outbox.MongoDB/ # net10.0 + ns2.1. MongoDB provider (opt-in).
│ ├── Mediana.Telemetry.OpenTelemetry/ # net10.0 + ns2.1. OTLP-OTel SDK for
│ │ # traces/metrics/logs Mediana, │ └── Mediana.MediatR/ # net10.0 + ns2.1. MediatR 12.x ├── tests/
│ ├── Mediana.UnitTests/ # registry, pipelines, envelope, retry-│ ├── Mediana.IntegrationTests/ # Testcontainers: RabbitMQ, Kafka, SQL, Mongo
│ ├── Mediana.InteropTests/ # Mediana ⇄ MassTransit, MassTransit-envelope
│ ├── Mediana.AotTests/ # NativeAOT publish + trimming smoke
│ └── Mediana.ContractTests.Ns21/ # tests API-│ # + tests ns2.1-├── benchmarks/
│ └── Mediana.Benchmarks/ # BenchmarkDotNet: dispatch, serialization, e2e
└── docs/
```

multi-target (D2/D13): each package csproj'in API-on types/`#if NET10_0` not types. always in D14 — `Abstractions` not on what; `Mediana` — only `Abstractions` + MEDI-`Transport.Abstractions` — without in-memory inbox, SPI); `Generators` — only Roslyn; outbox-relay — only packages on `Mediana.Transport.Abstractions` and own SDK; outbox-on `Transport.Abstractions` and own DB SDK. SDK in where SDK and is package. package `Mediana.Outbox` (and DB-**not ** for without transactional-without in transport retry/DLQ, but without ## 4. API

### 4.1 messages

```csharp
public interface IRequest { }
public interface IRequest<TResponse> : IRequest { }

public interface ICommand : IRequest { }
public interface ICommand<TResponse> : IRequest<TResponse> { }
public interface IQuery<TResponse> : IRequest<TResponse> { }
public interface IEvent : IRequest { }
public interface IStreamQuery<TRow> : IRequest { }
```

compile-time only: not on and performance. ### 4.2 handlers

```csharp
public interface ICommandHandler<in TCommand, TResponse> where TCommand : ICommand<TResponse>
{
 ValueTask<TResponse> Handle(TCommand command, CancellationToken ct);
}

public interface IQueryHandler<in TQuery, TResponse> where TQuery : IQuery<TResponse>
{
 ValueTask<TResponse> Handle(TQuery query, CancellationToken ct);
}

public interface IEventHandler<in TEvent> where TEvent : IEvent
{
 ValueTask Handle(TEvent @event, CancellationToken ct);
}

public interface IStreamHandler<in TQuery, TRow> where TQuery : IStreamQuery<TRow>
{
 IAsyncEnumerable<TRow> Handle(TQuery query, CancellationToken ct);
}
```

`ICommand`/`IQuery` — handler on type messages in `IEvent` — how many message remote-stable-serializable).

### 4.3 ```csharp
public interface IMediator
{
 ValueTask<TResponse> Send<TResponse>(ICommand<TResponse> command, CancellationToken ct = default);
 ValueTask<TResponse> Send<TResponse>(IQuery<TResponse> query, CancellationToken ct = default);
 ValueTask Publish<TEvent>(TEvent @event, CancellationToken ct = default) where TEvent : IEvent;
 IAsyncEnumerable<TRow> Stream<TRow>(IStreamQuery<TRow> query, CancellationToken ct = default);

 // Zero-boxing escape hatch for struct-messages on // (is and for IQuery<TResponse>)
 ValueTask<TResponse> SendExact<TCommand, TResponse>(TCommand command, CancellationToken ct = default)
 where TCommand : ICommand<TResponse>;
}
```

`Send` (local) — exception how is (MediatR-`Publish` — dispatch policy per event type: `Sequential` (by first or `Parallel` (all `AggregateException` by `Stream` — by `ct` source.
- without ### 4.4 pipeline

```csharp
public interface IHandlerMiddleware<TRequest, TResponse> where TRequest : IRequest<TResponse>
{
 ValueTask<TResponse> Handle(TRequest request, HandlerDelegate<TResponse> next, CancellationToken ct);
}

public delegate ValueTask<TResponse> HandlerDelegate<in TRequest, TResponse>(TRequest request, CancellationToken ct);

// pipeline events (IEvent not public interface IEventMiddleware<in TEvent> where TEvent : IEvent
{
 ValueTask Handle(TEvent @event, EventHandlerDelegate next, CancellationToken ct);
}
public delegate ValueTask EventHandlerDelegate<in TEvent>(TEvent @event, CancellationToken ct) where TEvent : IEvent;

// how behavior (public interface IPreProcessor<in TRequest> where TRequest : IRequest { ValueTask Process(TRequest r, CancellationToken ct); }
public interface IPostProcessor<in TRequest, in TResponse> where TRequest : IRequest<TResponse> { ValueTask Process(TRequest r, TResponse response, CancellationToken ct); }
```

behaviors (by → per-message behaviors → pre-processors → handler → post-processors. for events — from `IEventMiddleware` (→ per-event → handlers). on pipeline in static scoped-not on Behaviors and and from queues (unified behaviors for `IStreamQuery` — `IStreamMiddleware<TQuery, TRow>` (`IAsyncEnumerable`, not on ## 5. dispatch (D6)

### 5.1 static fast-path (source generator)

on by in build:

- `MedianaRegistrar` — partial-methods handlers in DI (without switch-dispatcher by messages: `RuntimeTypeHandle`-switch → O(1) without JIT;
- pipelines per (message × handler): behaviors in on scoped-not on from JSON-for `ITransport.BuildTopology`;
- STJ source-gen for and payload-### 5.2 Runtime-opt-in escape hatch)

`cfg.AddRuntimeHandlers(assemblies)` — principle freeze-on-first-dispatch, copy-on-write: registry and `Volatile.Write`; never not AOT-runtime-not Reflection.Emit; dispatch generic-types NativeAOT on `DynamicallyAccessedMemberTypes`-on requirement Roslyn-only on JIT-### 5.3 model (on Send)

| | | allocations |
|---|---|---|
| | switch by | 0 |
| pipeline | static //| 0 |
| Async | `ValueTask`; on state machine not | 0 |
| | `IValueTaskSource` on `ManualResetValueTaskSourceCore` (and in ns2.1) | 0 in steady state (|
| message | class-record by struct — `SendExact` without | 0 |

net10.0 — `FrozenDictionary` (~47% lookup Dictionary); ns2.1 — immutable bucket-by `RuntimeTypeHandle` (on N `BackgroundService`, backpressure `System.Threading.Channels`, concurrency, graceful drain on shutdown (stop consume → in-flight → ack/nack).

### 5.4 MediatR and D16)

| MediatR in-process | Mediana |
|------------------------------|--------------------|
| Behaviors from DI **on each Send** (N service-lookup'on | pipeline in static on on DI (for singleton-or lookup scoped-|
| pipeline (on each | per (message × handler); and on |
| `Task<TResponse>` — allocation Task on each | `ValueTask<T>` + sync fast-path without state machine; pooled `IValueTaskSource` (0 in steady state) |
| `Publish` handlers and on | Pre-stitched invoker'per event type; sequential) |
| `CreateStream` on each | `IAsyncEnumerable` without stream-behaviors — behaviors — without |
| on | Source-gen startup-on not in |
| lookup'and on each Send/TypedHandler | Switch-by source-gen), fallback-only for runtime-|

**Lifetime-policy handlers** (by `Scoped` — handler from scope on each for DbContext; service-lookup on Opt-in `cfg.UseSingletonHandlers()` — handlers without scoped-and service-lookup'on checks, what singleton-not scoped-## 6. source fluent-configuration; for without message ```csharp
services.AddMediana(cfg => {
 cfg.Route<CreateOrder>().ToQueue("orders"); // command → queue
 cfg.Route<OrderCreated>().FanOut(Topic.Pattern("order.{type}")); // event → fan-out
 cfg.Route<GetOrder>().Remote(timeout: TimeSpan.FromSeconds(5)); // query → request/reply
 cfg.Route<ReserveStock>().LocalAndRemote("stock"); // + in queue
});
```

```csharp
[Remote("orders", Transport = "rabbit")]
public sealed record CreateOrder(Guid OrderId, ...) : ICommand<OrderId>;
```

by **Command** → queue, load-balancing), handler-type on **Event** → exchange/topic fan-out: each queue/delivery at-least-once **Query** → request/reply: correlation id, policy per route, `RemoteTimeoutException` by `LocalAndRemote` — and in queue (for event — natural fan-out; for command — only for warning-per route: `Direct` (without outbox-package — by or `Outbox` (package Mediana.Outbox; without configuration on ## 7. envelope and wire-```
Envelope {
 EnvelopeVersion: int, // only additive
 MessageId: UUIDv7, // sortable, deduplication inbox
 CorrelationId: UUID?, // CausationId: UUID?, // messageId messages-MessageType { FullName, TypeVersion, ContractHash },
 Timestamp: DateTimeOffset,
 SourceEndpoint: string,
 TraceParent: string?, // W3C, Headers: bag<string,string>, // user + partition key, reply-to...)
 Payload: bytes
}
```

- per message type (fluent: `cfg.UseMessagePack<CreateOrder>()`); envelope always payload-bytes, STJ Utf8 source-gen for JSON-`ContractHash` — on → poison without retry.
- only additive-specific settings, per provider.
- PartitionKey (optional, from `IPartitioned { string PartitionKey { get; } }` on → Kafka partition key / RabbitMQ routing-by ordering per key.

---

## 8. SPI and ```csharp
public interface ITransport
{
 string Name { get; }
 TransportCapabilities Capabilities { get; }
 ValueTask BuildTopology(TopologyManifest manifest, CancellationToken ct); // idempotent declare
 ValueTask<ITransportPublisher> CreatePublisher(CancellationToken ct);
 IConsumerHostFactory CreateConsumers(IReadOnlyList<ConsumerEndpoint> endpoints);
}

public interface ITransportPublisher
{
 ValueTask Publish(Envelope envelope, PublishOptions options, CancellationToken ct);
 // PublishOptions: confirmDelivery (for outbox-relay), partitionKey, headers-merge
}
```

### 8.1 RabbitMQ (`Mediana.RabbitMQ`)

- Exchange: direct (command/query) or topic (event, pattern from queues + bindings from Dead-letter: DLX on queue → `<queue>.dlq`; poison and retry-Retry: nack requeue=false → DLX-cycle c TTL-`<queue>.retry.<delay>`), from retry-Request/reply: **direct reply-to** (without on streaming — chunked frames + completion/error frame by reply-to.
- reliability publisher confirms (opt-in per route; on outbox-on and declare).

### 8.2 Kafka (`Mediana.Kafka`)

- from command → + consumer group (event → on group per subscriber).
- Retry-pattern retry-`topic.retry.5s`, `topic.retry.30s` → `topic.dlq`; non-blocking retries.
- Ordering: partition key from PartitionKey messages (or MessageId); per-key ordering.
- Request/reply and streaming — not D11); configuration Query/StreamQuery on kafka-→ on ### 8.3 MassTransit (`Mediana.MassTransit`) — **transport**: Mediana-MassTransit `IBus`/`IRequestClient` — saga-and MassTransit; Mediana MassTransit-transport (RabbitMQ/Azure Service Bus/...).
2. **in Mediana**: MassTransit-`cfg.AddMedianaDispatch()` on receive endpoint) MassTransit-messages in local Mediana-pipeline — behaviors, retry, idempotency **MassTransit-envelope **: Mediana envelope in MassTransit (messageType envelope) — MassTransit-messages without Mediana; Fault-events in MassTransit Fault-outbox/retry Mediana on MassTransit-by MassTransit (outbox/ retry), not ## 9. reliability ### 9.1 Inbox (in always for remote-deduplication `(MessageId, HandlerIdentity)`; interface `IInboxStore` in outbox-**and** in-memory (for dev/test; not «» to unique constraint → «» → skip ### 9.2 Retry-Per message type: `Fixed / Incremental / Exponential (+jitter)`, MaxAttempts; in-process (transient-without and transport-level (redelivery by by Exponential 50ms→5s, 5 in-process, retry/backoff/jitter — implementation (D14), not Polly.

### 9.3 DLQ and poison detection

- retry → dead-letter envelope fingerprint type+stack-hash) in Poison (deserialization, ContractHash mismatch, non-retryable) → DLQ without retry, metric `mediana_poison_total`.

### 9.4 Transactional Outbox — **opt-in NuGet** (D4)

- `Mediana.Outbox` — transactions (EF Core `SaveChangesInterceptor` / Dapper-transaction / Mongo session provider-packages), in relay: `FOR UPDATE SKIP LOCKED` (SQL) / lease (Mongo), publisher confirms, backoff on policy cleanup by at-least-once delivery + inbox on = effectively-once **without package**: `Direct`-retry/DLQ configuration `Outbox`-without package → NuGet-package.

---

## 10. local: `IAsyncEnumerable` from `IMediator.Stream`, behaviors `IStreamMiddleware` (without on RabbitMQ chunked reply-frames + completion/error frame (D11); MassTransit — where Kafka — Backpressure: consumer-prefetch; `ct`) → cancel-frame ## 11. and D15 — OTLP-telemetry)

### 11.1 on all BCL API (`ActivitySource`/`Meter`/`ILogger`): without `Activity` API — no-op without guarantee north star); metrics reusable **ActivitySource "Mediana") — **

| Span | where | OTel messaging semconv) |
|------|-----|--------------------------------------------|
| `dispatch {MessageType}` | local Send/Stream | `messaging.message.id`, `messaging.system`="inproc" |
| `publish {MessageType}` | direct or outbox) | `messaging.destination.name`, `messaging.system`, partition key |
| `consume {MessageType}` | and | + `messaging.destination.name` queues/|
| `request.send {MessageType}` | Send, | correlation, destination, timeout |
| `request.handle {MessageType}` | Send, | traceparent |
| `outbox.relay` | relay | batch size, taken/sent/skipped |
| `inbox.dedup` | | hit/miss |

tracing: `traceparent` (W3C TraceContext) in local→queue→handler trace; `CorrelationId`/`CausationId` in span'**metrics (Meter "Mediana"):** dispatch duration histogram (by command/query/event/stream), in-flight count, publish/consume duration, consumer lag, retry attempts counter (by DLQ rate, `mediana_poison_total`, outbox lag/age/batch size, request/reply duration + timeout counter, stream rows counter.

**logs:** `ILogger` `message.type`, `message.id`, `correlation.id`, `causation.id`, `transport`, `endpoint`; ambient log-scope from ### 11.2 package Mediana.Telemetry.OpenTelemetry (OTLP-```csharp
builder.Services.AddMedianaOpenTelemetry(otel => {
 otel.WithOtlpExporter() // gRPC/HTTP, env OTEL_EXPORTER_OTLP_ENDPOINT/*
 .WithTraces(t => t.SetSampler(new ParentBasedTraceIdRatio(0.1)))
 .WithMetrics(m => m.AddDeltaTemporality())
 .WithLogs(); // bridge ILogger → OTLP logs
});
```

- OTel SDK only Mediana (not `AddToExisting(sdk)` for already OTel.
- already OTel messaging semantic conventions — and without OTLP exporter: env-configuration `OTEL_EXPORTER_OTLP_ENDPOINT`, `..._PROTOCOL`, `..._HEADERS`), `OTEL_SERVICE_NAME` + `service.namespace`/`service.version` by from dependency OpenTelemetry SDK — only in D14 not ### 11.4 async principle: **inline on only in memory; I/O — **. not queues **Guard-to builds **: span'`ActivitySource.HasListeners()`/`IsEnabled(sampling)`; metrics — reusable / sampled-out → and **Span'/metrics**: only in memory OTel (`BatchExportProcessor`): bounded-queue, delivery by and by inline-not **logs (ILogger-bridge)**: bounded-channel (`System.Threading.Channels`, lock-free) + drain → OTLP batch-queues — **drop without **: policy by `DropNewest` (`DropOldest`/`Block` — `Block` how pattern for `mediana_telemetry_dropped_total` (by **Drop-policy **: bounded queue OTLP-on not `mediana_telemetry_export_dropped_total`), backoff **Graceful shutdown**: flush by not on on ****: (test dispatch OTLP-endpoint'not by from test bounded-queue drain not producer'dropped in) shutdown-flush — all to events ### 11.3 local `Send` — exception how is (MediatR-span status=ERROR + `exception.*` events.
- `Send` — `RemoteExecutionException { RemoteErrorType, Message, Details, Envelope }`.
- events — Fault-event (in MassTransit Fault-+ retry-DLQ-events fingerprint in ## 12. performance: and CI-BenchmarkDotNet, `MemoryDiagnoser`, CI-on PR):

1. In-process `Send` (pipeline 2 behaviors, sync-completion handler): **0 ** net10.0 and ns2.1-in singleton-handlers — also 0 DI on In-process `Send` (async handler): 0 in steady state (IVTS), latency not MediatR; target ≥2× throughput on async-In-process `Publish` sequential (1–8 handlers): **0 ** on In-process `Publish` parallel (1–8 handlers): ≤ 1 allocations on handler in steady state (pooled waiter'In-process `Stream`: 0 on on stream-behaviors; ≤ 1 allocations on on deserialization + ≤ 1.2× payload.
7. Outbox-envelope + ≤ 1 KB baseline on message.
8. CI-benchmark-main and PR; > 5% on → red build; CI-D14): Abstractions, Mediana, Transport.Abstractions, Generators, Outbox) — **not-Microsoft **; dependencies Microsoft-package approve in PR (in CI-packages on benchmark-D16 — all MediatR 12.x in CI): Send sync/async, Publish sequential/parallel (1–8 handlers), Stream, serialization (STJ/MessagePack), envelope, e2e Testcontainers-RabbitMQ (throughput concurrent Send (scoped- and singleton-contention on ## 13. **Unit** (≥90% registry (copy-on-write stress-tests), pipelines, envelope, retry-poison detection. TDD: RED→GREEN for and **Integration** (Testcontainers): RabbitMQ/Kafka — request/reply, retry/DLQ, inbox outbox Postgres/SQL Server/Mongo — relay, SKIP LOCKED **telemetry**: test in-process OTLP-test HTTP/gRPC endpoint) — local→queue→handler trace, span'metrics no-op without test latency on OTLP-endpoint'drop-policy on shutdown-flush.
- ****: Mediana⇄MassTransit MassTransit-envelope MediatR-handlers.
- **AOT/trimming**: NativeAOT publish smoke + `TreatWarningsAsErrors` on trimming-**ContractTests.Ns21**: (tests ns2.1-reflection-free test API-/reflection on tests Parallel/Sequential virtual time for retry.

---

## 14. DX: and Incremental source generator (netstandard2.0, how analyzer + generator in `Mediana.Generators`):

- commands; handler without messages; remote-messages without serializable-transport in Query/StreamQuery on kafka-`LocalAndRemote` for command — warning.
- DI-registrar, switch-dispatcher, pipelines, JSON-STJ-for TFM (branch by `#if NET10_0`).
- naming/formatting `EmitCompilerGeneratedFiles` for ## 15. and SemVer 2.0; packages transports and outbox in x.
- Wire-`EnvelopeVersion`, only additive; `Mediana.MediatR` supports MediatR 12.x (`IRequestHandler<,>`, `INotificationHandler<>`, `IHandlerMiddleware<,>`) `cfg.AddMediatRHandlers(assemblies)` — handlers in Mediana-handlers, in versions in csproj how RabbitMQ.Client 7.x, Confluent.Kafka 2.x, MassTransit 8.x+) — on by ## 16. and | | |
|------|----------|
| incremental generator (| in test on incremental behavior |
| Zero-alloc on exception path | only on happy path; exception-path — |
| copy-on-write | Stress-tests + model review; immutable snapshot |
| ns2.1-on Unity/Mono) | documentation; benchmark-on CI-best effort) |
| MassTransit envelope-| tests MassTransit |
| | in D13 — already |
| API RabbitMQ.Client 6.x/7.x — on ns2.1-| Mediana.RabbitMQ: //retry shared, per-TFM only tests |

versions SQL-outbox/inbox (EF); policy cleanup relay; `required`-in ns2.1-in public API).

---

## 17. v1 (in **M1 **: Abstractions + dispatcher (source-gen + runtime), pipelines, DI, benchmark-each milestone **** (net10.0 and ns2.1) test API-**M2 and envelope**: envelope, STJ source-gen, SPI.
3. **M3 SPI + RabbitMQ**: publisher/consumer, retry/DLQ, request/reply, streaming, in-memory inbox.
4. **M4 Kafka**: retry-ordering.
5. **M5 MassTransit**: transport, envelope-tests.
6. **M6 reliability**: poison detection, DB-backed inbox, opt-in Outbox + EF/Dapper/Mongo relay.
7. **M7 MediatR-OTLP-package documentation, **. (M1–M6, not on M7.)
