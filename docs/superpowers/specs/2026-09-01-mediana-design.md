# Mediana — - v1

: 2026-09-01
: ( 1–3 )
North star: ** ** — , .

---

## 1. 

Mediana — .NET 10 API ( MediatR), (RabbitMQ, Kafka, MassTransit .) , transactional outbox — opt-in NuGet-.

### Non-goals (v1)

- / - — ; MassTransit ( MassTransit, Mediana).
- .NET Framework 4.x (netstandard2.0) — : north star.
- « → » — : . .
- RPC- TCP — .

---

## 2. (decision log)

| # | | |
|---|---------|-------------|
| D1 | API + `Mediana.MediatR` ( MediatR-) | MediatR; |
| D2 | ** — **: net10.0 netstandard2.1 ( , . D13), API-, namespace ; - ** NuGet-** ( ID) | ; net10.0- (FrozenDictionary — ~47% lookup Dictionary, System.Threading.Lock, GetAlternateLookup, R2R), ns2.1- — , API . = : ID TFM . «40+%»: - lookup'; end-to-end Send , .. source-gen lookup ; net10- fallback-, async- startup (R2R) |
| D13 | / - per TFM: RabbitMQ — net10.0: RabbitMQ.Client 7.x, ns2.1: 6.x ( API — ); Kafka — Confluent.Kafka API ( ); MassTransit 8.x — ns2.1-; Dapper/Mongo — ns2.1 ; **EF Core- — net10.0-only** (EF Core 6+ ns2.1; ns2.1- outbox Dapper/Mongo-) | « » API; (EF) |
| D14 | ** — north star.** (Abstractions, Mediana, Transport.Abstractions, Generators, - Outbox) — ; Microsoft , (MEDI- DI, Roslyn , STJ ). , SDK ( , DB-, - MessagePack/protobuf) | : . : retry- backoff ( Polly), /IVTS ( ObjectPooling), UUIDv7 ns2.1 ( net10.0 — `Guid.CreateVersion7`), relay. CI- (§12.6) |
| D15 | ** OTLP-.** — , BCL (`ActivitySource("Mediana")`, `Meter("Mediana")`, `ILogger`), (no-op Activity/Meter API). OTLP- — `Mediana.Telemetry.OpenTelemetry` (net10.0 + ns2.1): OTel SDK (traces + metrics + logs) OTLP-; OTel messaging semantic conventions; — `OTEL_EXPORTER_OTLP_*` env + fluent-. ** **: inline — , I/O , (bounded-, drop-on-overflow , flush shutdown — §11.4) | D14 D16; (Tempo/Jaeger/OTel Collector) |
| D16 | ** , Send.** in-process MediatR ( - §5.4); §12 Publish (sequential/parallel), Stream ; - MediatR 12.x CI | MediatR in-process per-call behaviors DI, Task-; Mediana , |
| D3 | : ( / / ) + -; MassTransit — , | + |
| D4 | v1: retry, DLQ, poison detection, inbox — ; **outbox — opt-in NuGet-** | ; transactional- — ( ) |
| D5 | : parity MediatR + (`IAsyncEnumerable`), | ns2.1+, — MassTransit |
| D6 | : — source-gen fast-path + runtime- (copy-on-write) | + escape hatch |
| D7 | : `IRequest`; `ICommand`/`IQuery`/`IEvent`/`IStreamQuery` — | generic- ( ) |

, ( ):

| # | | |
|---|---------|-------------|
| D8 | DI — `Microsoft.Extensions.DependencyInjection` | ; keyed services ns2.1 |
| D9 | — System.Text.Json source-gen; MessagePack protobuf , per message type | Zero-reflection + - |
| D10 | `MessageId` — UUIDv7 (net10.0: `Guid.CreateVersion7()`; ns2.1: , D14) | Sortable → -friendly outbox/inbox |
| D11 | v1 — RabbitMQ (chunked reply frames) MassTransit; Kafka — (documented limitation) | Kafka streaming reply; fetch-loop - |
| D12 | OpenTelemetry-first ; Send — `RemoteExecutionException` | ; MassTransit- Fault- |

---

## 2.1. (- )

| # | | |
|---|---------|-------------|
| D17 | behaviors **Middleware** ( ): IPipelineBehavior → IHandlerMiddleware, IEventPipelineBehavior → IEventMiddleware, IStreamPipelineBehavior → IStreamMiddleware, RequestHandlerDelegate<,> → HandlerDelegate<,>, - Add*Behavior → Add*Middleware | MediatR CS0104- ; Middleware — (ASP.NET Core/MassTransit), . Namespace Mediana.Pipeline |

## 3. 

```
Mediana.sln
├── src/
│ ├── Mediana.Abstractions/ # net10.0 + ns2.1. /,
│ │ # envelope, . .
│ ├── Mediana/ # net10.0 + ns2.1. In-process , ,
│ │ # runtime-, DI-, -.
│ ├── Mediana.Generators/ # netstandard2.0 ( ).
│ │ # Incremental source generator + .
│ ├── Mediana.Transport.Abstractions/ # net10.0 + ns2.1. SPI : ITransport,
│ │ # publisher/consumer, , capabilities,
│ │ # IInboxStore + in-memory .
│ ├── Mediana.RabbitMQ/ # net10.0 ( 7.x) + ns2.1 ( 6.x, ).
│ ├── Mediana.Kafka/ # net10.0 + ns2.1 (Confluent.Kafka, — ).
│ ├── Mediana.MassTransit/ # net10.0 + ns2.1 (MassTransit 8.x).
│ ├── Mediana.Outbox/ # net10.0 + ns2.1. transactional outbox + relay
│ │ # (opt-in); DB- inbox/outbox — .
│ ├── Mediana.Outbox.EFCore/ # net10.0-only (EF Core 6+ ns2.1; D13).
│ ├── Mediana.Outbox.Dapper/ # net10.0 + ns2.1. Dapper/SQL (opt-in).
│ ├── Mediana.Outbox.MongoDB/ # net10.0 + ns2.1. MongoDB (opt-in).
│ ├── Mediana.Telemetry.OpenTelemetry/ # net10.0 + ns2.1. OTLP-: OTel SDK 
│ │ # traces/metrics/logs Mediana, .
│ └── Mediana.MediatR/ # net10.0 + ns2.1. MediatR 12.x .
├── tests/
│ ├── Mediana.UnitTests/ # , , , , retry-
│ ├── Mediana.IntegrationTests/ # Testcontainers: RabbitMQ, Kafka, SQL, Mongo
│ ├── Mediana.InteropTests/ # Mediana ⇄ MassTransit, MassTransit-envelope
│ ├── Mediana.AotTests/ # NativeAOT publish + trimming smoke
│ └── Mediana.ContractTests.Ns21/ # API-
│ # + ns2.1-
├── benchmarks/
│ └── Mediana.Benchmarks/ # BenchmarkDotNet: dispatch, serialization, e2e
└── docs/
```

 multi-target (D2/D13): csproj' ; API- ( /); — `#if NET10_0` , . - — .

 (D14 — ): `Abstractions` ; `Mediana` — `Abstractions` + MEDI-; `Transport.Abstractions` — (in-memory inbox, SPI); `Generators` — Roslyn; outbox-relay — ; `Mediana.Transport.Abstractions` SDK; outbox- — `Transport.Abstractions` DB SDK. SDK , SDK . `Mediana.Outbox` ( DB-) ** ** transactional-: retry/DLQ, -.

---

## 4. API

### 4.1 

```csharp
public interface IRequest { }
public interface IRequest<TResponse> : IRequest { }

public interface ICommand : IRequest { }
public interface ICommand<TResponse> : IRequest<TResponse> { }
public interface IQuery<TResponse> : IRequest<TResponse> { }
public interface IEvent : IRequest { }
public interface IStreamQuery<TRow> : IRequest { }
```

 — compile-time only: . (§6).

### 4.2 

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

 ( ): `ICommand`/`IQuery` — ; `IEvent` — ; remote- stable- (serializable).

### 4.3 

```csharp
public interface IMediator
{
 ValueTask<TResponse> Send<TResponse>(ICommand<TResponse> command, CancellationToken ct = default);
 ValueTask<TResponse> Send<TResponse>(IQuery<TResponse> query, CancellationToken ct = default);
 ValueTask Publish<TEvent>(TEvent @event, CancellationToken ct = default) where TEvent : IEvent;
 IAsyncEnumerable<TRow> Stream<TRow>(IStreamQuery<TRow> query, CancellationToken ct = default);

 // Zero-boxing escape hatch struct- 
 // ( IQuery<TResponse>)
 ValueTask<TResponse> SendExact<TCommand, TResponse>(TCommand command, CancellationToken ct = default)
 where TCommand : ICommand<TResponse>;
}
```

:

- `Send` () — ( MediatR-).
- `Publish` — ; per event type: `Sequential` ( ; ) `Parallel` (: , `AggregateException` ).
- `Stream` — ; `ct` .
- .

### 4.4 

```csharp
public interface IHandlerMiddleware<TRequest, TResponse> where TRequest : IRequest<TResponse>
{
 ValueTask<TResponse> Handle(TRequest request, HandlerDelegate<TResponse> next, CancellationToken ct);
}

public delegate ValueTask<TResponse> HandlerDelegate<in TRequest, TResponse>(TRequest request, CancellationToken ct);

// (IEvent — )
public interface IEventMiddleware<in TEvent> where TEvent : IEvent
{
 ValueTask Handle(TEvent @event, EventHandlerDelegate next, CancellationToken ct);
}
public delegate ValueTask EventHandlerDelegate<in TEvent>(TEvent @event, CancellationToken ct) where TEvent : IEvent;

// , behavior ( )
public interface IPreProcessor<in TRequest> where TRequest : IRequest { ValueTask Process(TRequest r, CancellationToken ct); }
public interface IPostProcessor<in TRequest, in TResponse> where TRequest : IRequest<TResponse> { ValueTask Process(TRequest r, TResponse response, CancellationToken ct); }
```

: behaviors ( ) → per-message behaviors → pre-processors → handler → post-processors. — `IEventMiddleware` (: → per-event → ). ; scoped- , (§5.1). Behaviors , ( -).

: behaviors `IStreamQuery` — `IStreamMiddleware<TQuery, TRow>` ( `IAsyncEnumerable`, ).

---

## 5. ( D6)

### 5.1 fast-path (source generator)

 :

- `MedianaRegistrar` — partial- DI ( );
- switch- : `RuntimeTypeHandle`-switch → . O(1) , JIT;
- per ( × ): behaviors - ( scoped-), ;
- (§6) — JSON- `ITransport.BuildTopology`;
- STJ source-gen payload-.

### 5.2 Runtime- (opt-in escape hatch)

`cfg.AddRuntimeHandlers(assemblies)` — . freeze-on-first-dispatch, copy-on-write: ; `Volatile.Write`; . AOT- : runtime- Reflection.Emit; generic- NativeAOT `DynamicallyAccessedMemberTypes`- - ( ); Roslyn- — , JIT-.

### 5.3 (: 0 Send)

| | | |
|---|---|---|
| | switch , | 0 |
| | ; // | 0 |
| Async | `ValueTask`; state machine | 0 |
| | `IValueTaskSource` `ManualResetValueTaskSourceCore` ( ns2.1) | 0 steady state () |
| | class-record ; struct — `SendExact` | 0 |

: net10.0 — `FrozenDictionary` (~47% lookup Dictionary); ns2.1 — immutable bucket- `RuntimeTypeHandle` ( N ).

 : `BackgroundService`, backpressure `System.Threading.Channels`, concurrency, graceful drain shutdown (stop consume → in-flight → ack/nack).

### 5.4 MediatR - (D16)

| MediatR in-process | - Mediana |
|------------------------------|--------------------|
| Behaviors DI ** Send** (N service-lookup' ) | ; — DI ( singleton-, . ) lookup (scoped-) |
| ( ) | per ( × ); |
| `Task<TResponse>` — Task | `ValueTask<T>` + fast-path state machine; — pooled `IValueTaskSource` (0 steady state) |
| `Publish` handlers | Pre-stitched invoker' per event type; — , 0 (sequential) |
| `CreateStream` | `IAsyncEnumerable` ; stream-behaviors — , behaviors — |
| | Source-gen ; startup- — , |
| lookup' Send/TypedHandler | Switch- (source-gen), fallback- runtime- |

**Lifetime- ** (- ): `Scoped` — scope ( DbContext; — service-lookup ). Opt-in `cfg.UseSingletonHandlers()` — scoped- : service-lookup' . , singleton- scoped- (-).

---

## 6. 

 — fluent-; — ; .

```csharp
services.AddMediana(cfg => {
 cfg.Route<CreateOrder>().ToQueue("orders"); // command → 
 cfg.Route<OrderCreated>().FanOut(Topic.Pattern("order.{type}")); // event → fan-out
 cfg.Route<GetOrder>().Remote(timeout: TimeSpan.FromSeconds(5)); // query → request/reply
 cfg.Route<ReserveStock>().LocalAndRemote("stock"); // : + 
});
```

```csharp
[Remote("orders", Transport = "rabbit")]
public sealed record CreateOrder(Guid OrderId, ...) : ICommand<OrderId>;
```

 :

- **Command** → , (load-balancing), - .
- **Event** → exchange/topic fan-out: — /; at-least-once .
- **Query** → request/reply: correlation id, - per route, `RemoteTimeoutException` .
- `LocalAndRemote` — ( event — natural fan-out; command — : , -; warning-).

 per route: `Direct` ( outbox- — ) `Outbox` ( Mediana.Outbox; ).

---

## 7. wire-

```
Envelope {
 EnvelopeVersion: int, // additive
 MessageId: UUIDv7, // sortable, inbox
 CorrelationId: UUID?, // 
 CausationId: UUID?, // messageId -
 MessageType { FullName, TypeVersion, ContractHash },
 Timestamp: DateTimeOffset,
 SourceEndpoint: string,
 TraceParent: string?, // W3C, 
 Headers: bag<string,string>, // user + (partition key, reply-to...)
 Payload: bytes
}
```

- per message type (fluent: `cfg.UseMessagePack<CreateOrder>()`); - ( payload-bytes, STJ Utf8 source-gen JSON- ).
- `ContractHash` — → poison retry.
- : additive-; — -specific , per provider.
- PartitionKey (optional, `IPartitioned { string PartitionKey { get; } }` ) → Kafka partition key / RabbitMQ routing- : ordering per key.

---

## 8. SPI 

```csharp
public interface ITransport
{
 string Name { get; }
 TransportCapabilities Capabilities { get; }
 ValueTask BuildTopology(TopologyManifest manifest, CancellationToken ct); // declare
 ValueTask<ITransportPublisher> CreatePublisher(CancellationToken ct);
 IConsumerHostFactory CreateConsumers(IReadOnlyList<ConsumerEndpoint> endpoints);
}

public interface ITransportPublisher
{
 ValueTask Publish(Envelope envelope, PublishOptions options, CancellationToken ct);
 // PublishOptions: confirmDelivery ( outbox-relay), partitionKey, headers-merge
}
```

### 8.1 RabbitMQ (`Mediana.RabbitMQ`)

- Exchange: direct (command/query) topic (event, ); + bindings .
- Dead-letter: DLX → `<queue>.dlq`; poison retry- .
- Retry: nack requeue=false → DLX-cycle c TTL- (`<queue>.retry.<delay>`), retry-.
- Request/reply: **direct reply-to** ( ); ; streaming — chunked frames + completion/error frame reply-to.
- : publisher confirms (opt-in per route; outbox-).
- ( declare).

### 8.2 Kafka (`Mediana.Kafka`)

- ; command → + consumer group (), event → - (group per subscriber).
- Retry- retry-: `topic.retry.5s`, `topic.retry.30s` → `topic.dlq`; non-blocking retries.
- Ordering: partition key PartitionKey ( MessageId); per-key ordering.
- Request/reply streaming — (D11); Query/StreamQuery kafka- → .

### 8.3 MassTransit (`Mediana.MassTransit`) — 

1. ****: Mediana- MassTransit `IBus`/`IRequestClient` — saga-, MassTransit; Mediana MassTransit- (RabbitMQ/Azure Service Bus/...).
2. ** Mediana**: MassTransit- (`cfg.AddMedianaDispatch()` receive endpoint) MassTransit- Mediana- — behaviors, retry, .
3. **MassTransit-envelope **: Mediana MassTransit (messageType envelope) — MassTransit- Mediana; Fault- MassTransit Fault-.

 : outbox/retry Mediana MassTransit- MassTransit ( outbox/ retry), ; .

---

## 9. 

### 9.1 Inbox ( , remote-)

- `(MessageId, HandlerIdentity)`; — `IInboxStore` outbox- **** in-memory ( dev/; : ).
- «» (unique constraint ), → «» ; → skip .

### 9.2 Retry-

- Per message type: `Fixed / Incremental / Exponential (+jitter)`, MaxAttempts; — in-process (transient-, ) transport-level (redelivery §8). : Exponential 50ms→5s, 5 in-process, . retry/backoff/jitter — (D14), Polly.

### 9.3 DLQ poison detection

- retry → dead-letter ; , fingerprint (+stack-hash) .
- Poison (, ContractHash mismatch, non-retryable) → DLQ , retry, - `mediana_poison_total`.

### 9.4 Transactional Outbox — **opt-in NuGet** (D4)

- `Mediana.Outbox` — : - (EF Core `SaveChangesInterceptor` / Dapper- / Mongo session -), .
- relay: - `FOR UPDATE SKIP LOCKED` (SQL) / lease (Mongo), publisher confirms, backoff , cleanup .
- : at-least-once + inbox = effectively-once .
- ** **: `Direct`- (retry/DLQ , - ). `Outbox`- → NuGet-.

---

## 10. 

- : `IAsyncEnumerable` `IMediator.Stream`, behaviors `IStreamMiddleware` ( ).
- : RabbitMQ chunked reply-frames + completion/error frame (D11); MassTransit — , . Kafka — .
- Backpressure: consumer-prefetch; (`ct`) → cancel-frame .

---

## 11. (D15 — OTLP-)

### 11.1 ( , )

 BCL API (`ActivitySource`/`Meter`/`ILogger`): `Activity` API — no-op ( north star); reusable -.

** (ActivitySource "Mediana") — :**

| Span | | (OTel messaging semconv) |
|------|-----|--------------------------------------------|
| `dispatch {MessageType}` | Send/Stream | `messaging.message.id`, `messaging.system`="inproc" |
| `publish {MessageType}` | (direct outbox) | `messaging.destination.name`, `messaging.system`, partition key |
| `consume {MessageType}` | | + `messaging.destination.name` / |
| `request.send {MessageType}` | Send, | correlation, destination, timeout |
| `request.handle {MessageType}` | Send, | traceparent |
| `outbox.relay` | relay | batch size, taken/sent/skipped |
| `inbox.dedup` | | hit/miss |

 : `traceparent` (W3C TraceContext) — →→ trace; `CorrelationId`/`CausationId` span'.

** (Meter "Mediana"):** dispatch duration histogram ( command/query/event/stream), in-flight count, publish/consume duration, consumer lag, retry attempts counter ( ), DLQ rate, `mediana_poison_total`, outbox lag/age/batch size, request/reply duration + timeout counter, stream rows counter.

**:** `ILogger` `message.type`, `message.id`, `correlation.id`, `causation.id`, `transport`, `endpoint`; ambient log-scope .

### 11.2 Mediana.Telemetry.OpenTelemetry ( OTLP-)

```csharp
builder.Services.AddMedianaOpenTelemetry(otel => {
 otel.WithOtlpExporter() // gRPC/HTTP, env OTEL_EXPORTER_OTLP_ENDPOINT/*
 .WithTraces(t => t.SetSampler(new ParentBasedTraceIdRatio(0.1)))
 .WithMetrics(m => m.AddDeltaTemporality())
 .WithLogs(); // bridge ILogger → OTLP logs
});
```

- OTel SDK Mediana ( — ); `AddToExisting(sdk)` OTel.
- OTel messaging semantic conventions — .
- OTLP exporter: env- (`OTEL_EXPORTER_OTLP_ENDPOINT`, `..._PROTOCOL`, `..._HEADERS`), — `OTEL_SERVICE_NAME` + `service.namespace`/`service.version` .
- OpenTelemetry SDK — (D14 ).

### 11.4 ( , )

: **inline — ; I/O — **. , , .

1. **Guard- **: span' — `ActivitySource.HasListeners()`/`IsEnabled(sampling)`; — reusable -. / sampled-out → ( §12 ).
2. **Span'/**: OTel (`BatchExportProcessor`): bounded-, , inline- .
3. ** (ILogger-bridge)**: bounded- (`System.Threading.Channels`, lock-free) + drain → OTLP batch-. — **drop **: `DropNewest` ( `DropOldest`/`Block` — `Block` - ); `mediana_telemetry_dropped_total` ( ).
4. **Drop- **: bounded queue OTLP- ; (`mediana_telemetry_export_dropped_total`), backoff .
5. **Graceful shutdown**: flush ( 5 , ) — ; — .
6. ****: () — OTLP-endpoint' ; () — bounded- drain producer', dropped ; () shutdown-flush — .

### 11.3 

- `Send` — ( MediatR-); span status=ERROR + `exception.*` .
- `Send` — `RemoteExecutionException { RemoteErrorType, Message, Details, Envelope }`.
- — Fault- ( .. MassTransit Fault-) + retry-; DLQ- fingerprint .

---

## 12. : CI-

 (BenchmarkDotNet, `MemoryDiagnoser`, CI- PR):

1. In-process `Send` ( 2 behaviors, sync-completion handler): **0 ** , (net10.0 ns2.1-); singleton- — 0 DI .
2. In-process `Send` (async handler): 0 steady state ( IVTS), MediatR; ≥2× throughput async-.
3. In-process `Publish` sequential (1–8 ): **0 ** .
4. In-process `Publish` parallel (1–8 ): ≤ 1 steady state ( — pooled waiter'; ).
5. In-process `Stream`: 0 stream-behaviors; ≤ 1 .
6. + : ≤ 1.2× payload.
7. Outbox-: + ≤ 1 KB baseline .
8. CI-: benchmark- main PR; > 5% → red build; — .
9. CI- (D14): (Abstractions, Mediana, Transport.Abstractions, Generators, Outbox) — ** -Microsoft **; Microsoft- approve PR ( CI-); .

- (D16 — MediatR 12.x CI): Send sync/async, Publish sequential/parallel (1–8 ), Stream, (STJ/MessagePack), , e2e Testcontainers-RabbitMQ (throughput ). : Send ( , scoped- singleton-) — contention .

---

## 13. 

- **Unit** (≥90% ): , ( copy-on-write — stress-), , , retry-, poison detection. TDD: RED→GREEN .
- **Integration** (Testcontainers): RabbitMQ/Kafka — , request/reply, retry/DLQ, inbox ; outbox Postgres/SQL Server/Mongo — , relay, SKIP LOCKED .
- ****: in-process OTLP- (test HTTP/gRPC endpoint) — →→ trace, span' §11.1, , no-op ( ); §11.4 — OTLP-endpoint', drop- , shutdown-flush.
- ****: Mediana⇄MassTransit ; MassTransit-envelope ; MediatR-.
- **AOT/trimming**: NativeAOT publish smoke + `TreatWarningsAsErrors` trimming-; .
- **ContractTests.Ns21**: () ns2.1- ( reflection-free ); () API- ( / reflection ).
- : Parallel/Sequential virtual time retry.

---

## 14. DX: 

Incremental source generator (netstandard2.0, analyzer + generator `Mediana.Generators`):

- -: ; ; remote- serializable-; ; Query/StreamQuery kafka-; `LocalAndRemote` command — warning.
- : DI-registrar, switch-, , JSON- , STJ-. TFM ( — `#if NET10_0`).
- naming/formatting (`EmitCompilerGeneratedFiles` ).

---

## 15. 

- SemVer 2.0; outbox 1.x.
- Wire- : `EnvelopeVersion`, additive; .
- `Mediana.MediatR` MediatR 12.x (`IRequestHandler<,>`, `INotificationHandler<>`, `IHandlerMiddleware<,>`) `cfg.AddMediatRHandlers(assemblies)` — Mediana-, .
- csproj (RabbitMQ.Client 7.x, Confluent.Kafka 2.x, MassTransit 8.x+) — .

---

## 16. 

| | |
|------|----------|
| incremental generator (, ) | - ; incremental behavior |
| Zero-alloc (exception path ) | happy path; exception-path — |
| copy-on-write | Stress- + review; immutable snapshot |
| ns2.1- - (Unity/Mono) | ; benchmark- CI- (best effort) |
| MassTransit envelope-: | - MassTransit ; |
| | (D13 — ) |
| API RabbitMQ.Client 6.x/7.x — ns2.1- | Mediana.RabbitMQ: //retry , per-TFM ; |

 : ; SQL- outbox/inbox ( EF); cleanup relay; `required`- ns2.1- ( public API).

---

## 17. v1 ( )

1. **M1 **: Abstractions + (source-gen + runtime), , DI, -, §12. milestone ** ** (net10.0 ns2.1) , API-.
2. **M2 **: -, , STJ source-gen, SPI.
3. **M3 SPI + RabbitMQ**: publisher/consumer, , retry/DLQ, request/reply, streaming, in-memory inbox.
4. **M4 Kafka**: , retry-, ordering.
5. **M5 MassTransit**: , , envelope-, -.
6. **M6 **: poison detection, DB-backed inbox, opt-in Outbox + EF/Dapper/Mongo , relay.
7. **M7 MediatR-, OTLP- , , **. ( §11.1 M1–M6, M7.)
