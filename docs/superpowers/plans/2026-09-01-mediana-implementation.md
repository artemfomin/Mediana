# Mediana Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Mediana : + + (RabbitMQ/Kafka/MassTransit) + opt-in outbox + OTLP-, (branch ≥95%, mutation score ≥90%).

**Architecture:** (source-gen switch + runtime- copy-on-write), zero-alloc hot path (ValueTask + pooled IVTS, ), - net10.0 + netstandard2.1 API-, -based SPI, (retry/DLQ/inbox ; transactional outbox — opt-in ).

**Tech Stack:** .NET 10 SDK (10.0.302), C#, MEDI, Roslyn (source generators), STJ source-gen, RabbitMQ.Client 7.x/6.x, Confluent.Kafka, MassTransit 8.x, EF Core, Dapper, MongoDB.Driver, OpenTelemetry, xUnit, coverlet, Stryker.NET, BenchmarkDotNet, Testcontainers.

**Spec:** `docs/superpowers/specs/2026-09-01-mediana-design.md` (16 ADR). ; .

## Global Constraints (from spec, verbatim)

- TFM: — `net10.0;netstandard2.1`, `Mediana.Outbox.EFCore` → net10.0-only; `Mediana.Generators` → netstandard2.0 (D2/D13).
- (Abstractions, Mediana, Transport.Abstractions, Generators, Outbox-relay) — -Microsoft (D14, CI- §12.9). SDK — .
- : in-process Send/Publish-sequential/Stream — 0 ; Publish-parallel ≤1 ; async Send — 0 steady state (D16, §12).
- : no-op ; I/O ; (D15, §11.4).
- : branch coverage ≥95%, mutation score ≥90% (Stryker) ; TDD .
- Outbox — opt-in; (D4).
- API + MediatR 12.x (D1); IRequest (D7).
- UUIDv7: net10.0 `Guid.CreateVersion7()`, ns2.1 — (D10/D14).
- Retry/backoff — , Polly (D14).
- milestone TFM-; API-.

## Execution order

Phase 0 () → M1 → M2 / → M3 SPI+RabbitMQ → M4 Kafka → M5 MassTransit → M6 /outbox → M7 //. Load-testing doc ( , ) — M7.

---

### Task 0.1: Solution skeleton

**Files:** `Mediana.sln`, `Directory.Build.props`, `Directory.Packages.props`, `docs/QUESTIONS.md`, `.gitignore` (exists).

**Steps:**
- [ ] Directory.Build.props: `<TargetFrameworks>net10.0;netstandard2.1</TargetFrameworks>` ( ), LangVersion latest, Nullable enable, ImplicitUsings, AnalyzersLevel, `<TreatWarningsAsErrors>true`, CI `MEDIANA_CI`.
- [ ] Directory.Packages.props: (CPM).
- [ ] `docs/QUESTIONS.md` — ( ).
- [ ] csproj' 14 + + benchmarks; `dotnet build` .
- [ ] Commit `chore: solution skeleton`.

### Task 0.2: Test foundation

**Files:** `tests/Mediana.UnitTests/*`, `coverlet.runsettings`, `stryker-config.json`, `global.json`.

**Steps:**
- [ ] xUnit + coverlet.collector; runsettings branch- 95% UnitTests.
- [ ] tool manifest Stryker.NET (`dotnet tool restore`).
- [ ] `global.json` — pin SDK 10.0.302.
- [ ] Commit.

### Task 1.1: Abstractions — 

**Files:** Create `src/Mediana.Abstractions/**`:
`Messaging/IRequest.cs`, `ICommand.cs`, `IQuery.cs`, `IEvent.cs`, `IStreamQuery.cs`, `IPartitioned.cs`;
`Handlers/ICommandHandler.cs`, `IQueryHandler.cs`, `IEventHandler.cs`, `IStreamHandler.cs`;
`Pipeline/IPipelineBehavior.cs`, `IEventPipelineBehavior.cs`, `IStreamPipelineBehavior.cs`, `RequestHandlerDelegate.cs`, `EventHandlerDelegate.cs`, `StreamHandlerDelegate.cs`, `IPreProcessor.cs`, `IPostProcessor.cs`;
`IMediator.cs`; `MediatorConfigurationException.cs`; `RemoteExecutionException.cs` ( ).

**Interfaces (Produces — ):**

```csharp
public interface IRequest { }
public interface IRequest<TResponse> : IRequest { }
public interface ICommand : IRequest { }
public interface ICommand<TResponse> : IRequest<TResponse> { }
public interface IQuery<TResponse> : IRequest<TResponse> { }
public interface IEvent : IRequest { }
public interface IStreamQuery<TRow> : IRequest { }

public interface ICommandHandler<in TCommand, TResponse> where TCommand : ICommand<TResponse>
{ ValueTask<TResponse> Handle(TCommand command, CancellationToken ct); }
public interface IQueryHandler<in TQuery, TResponse> where TQuery : IQuery<TResponse>
{ ValueTask<TResponse> Handle(TQuery query, CancellationToken ct); }
public interface IEventHandler<in TEvent> where TEvent : IEvent
{ ValueTask Handle(TEvent @event, CancellationToken ct); }
public interface IStreamHandler<in TQuery, TRow> where TQuery : IStreamQuery<TRow>
{ IAsyncEnumerable<TRow> Handle(TQuery query, CancellationToken ct); }

public delegate ValueTask<TResponse> HandlerDelegate<in TRequest, TResponse>(TRequest request, CancellationToken ct) where TRequest : IRequest<TResponse>;
public delegate ValueTask EventHandlerDelegate<in TEvent>(TEvent @event, CancellationToken ct) where TEvent : IEvent;
public delegate IAsyncEnumerable<TRow> StreamHandlerDelegate<in TQuery, TRow>(TQuery query, CancellationToken ct) where TQuery : IStreamQuery<TRow>;

public interface IHandlerMiddleware<TRequest, TResponse> where TRequest : IRequest<TResponse>
{ ValueTask<TResponse> Handle(TRequest request, HandlerDelegate<TRequest, TResponse> next, CancellationToken ct); }
public interface IEventMiddleware<in TEvent> where TEvent : IEvent
{ ValueTask Handle(TEvent @event, EventHandlerDelegate<TEvent> next, CancellationToken ct); }
public interface IStreamMiddleware<in TQuery, TRow> where TQuery : IStreamQuery<TRow>
{ IAsyncEnumerable<TRow> Handle(TQuery query, StreamHandlerDelegate<TQuery, TRow> next, CancellationToken ct); }

public interface IMediator
{
 ValueTask<TResponse> Send<TResponse>(ICommand<TResponse> command, CancellationToken ct = default);
 ValueTask<TResponse> Send<TResponse>(IQuery<TResponse> query, CancellationToken ct = default);
 ValueTask Publish<TEvent>(TEvent @event, CancellationToken ct = default) where TEvent : IEvent;
 IAsyncEnumerable<TRow> Stream<TRow>(IStreamQuery<TRow> query, CancellationToken ct = default);
 ValueTask<TResponse> SendExact<TCommand, TResponse>(TCommand command, CancellationToken ct = default) where TCommand : ICommand<TResponse>;
 ValueTask<TResponse> SendExact<TQuery, TResponse>(TQuery query, CancellationToken ct = default) where TQuery : IQuery<TResponse>;
}
```

- [ ] TDD: ( , variance), .
- [ ] Branch coverage ≥95% ( ), commit.

### Task 1.2: + copy-on-write

**Files:** `src/Mediana/Dispatch/MessageRegistry.cs`, `RegistryBuilder.cs`, `HandlerKind.cs`, `HandlerEntry.cs`, `EventDispatchPolicy.cs`.

**Interfaces (Produces):**

```csharp
internal enum HandlerKind { Command, Query, Event, Stream }
internal sealed class HandlerEntry { public Type HandlerType; public Func<IServiceProvider, object> Factory; public Delegate Invoker; /* compiled */ }
public enum EventDispatchPolicy { Sequential, Parallel }
internal sealed class MessageRegistry {
 public bool TryGet(Type requestType, out MessageEntry entry);
 public MessageRegistry Add(Type requestType, HandlerEntry handler); // copy-on-write
}
```

 : `RuntimeTypeHandle`; — hash-handle, ; rebuild Add → , `Volatile.Write` . net10.0 — FrozenDictionary. Zero-allocation TryGet.

- [ ] TDD: add/resolve/ (add ), stress- copy-on-write ( add + read, , ).
- [ ] Commit.

### Task 1.3: + Mediator

**Files:** `src/Mediana/Dispatch/PipelineCompiler.cs`, `Mediator.cs`, `Dispatch/PrePostProcessorBehavior.cs`, `ValueTasks/PooledValueTaskSource.cs`, `ValueTasks/ValueTaskPool.cs`.

** :**
- `PipelineCompiler` registration-time : command/query — resolve behaviors (`IHandlerMiddleware<T,R>` ), wrapper pre/post, handler. — , ( captured-free static lambdas + `IServiceProvider` singleton/scoped).
- `Mediator.Send`: lookup → compiled `Invoker(message, serviceProvider, ct)`. — `new ValueTask<T>(result)` (struct, ); — ValueTask ( /await sync-: ValueTask ).
- Publish sequential: compiled invokers, await ; Parallel: `ParallelAsyncBarrier` pooled IVTS — ValueTaskPool.
- `ValueTaskPool`: `ManualResetValueTaskSourceCore<bool>` (ns2.1-).
- [ ] TDD: Send command/query (sync/async handler), Publish sequential/parallel (1–8 handlers, : / AggregateException parallel), Stream ( behaviors, behaviors), SendExact struct- ( — typeof/boxing detection), cancellation propagation.
- [ ] (GC.GetAllocatedBytesForCurrentThread): Send sync = 0; Publish seq = 0; Stream cursor = 0 behaviors. — , [Trait("Category","Allocation")], CI .
- [ ] Commit.

### Task 1.4: DI-

**Files:** `src/Mediana/ServiceCollectionExtensions.cs`, `MedianaConfiguration.cs`, `HandlerLifetime.cs`.

`services.AddMediana(cfg)` — (`cfg.RegisterCommandHandler<TC,TR,H>()`, `RegisterFromAssembly` -produced `MedianaRegistrar`), behaviors , `cfg.UseSingletonHandlers()`, `cfg.SetEventPolicy<TEvent>(policy)`. IMediator → singleton Mediator ( immutable ).
- [ ] TDD: , dup- /queries (MediatorConfigurationException), lifetime singleton/scoped, event policy.
- [ ] Commit.

### Task 1.5: Source generator

**Files:** `src/Mediana.Generators/MedianaGenerator.cs` (incremental), `MedianaDiagnostics.cs`, emitter.

 `MedianaRegistrar` (partial class, `AddHandlers(IServiceCollection)`) `ICommandHandler<>`/etc. ; switch- ( O(1)) — registration + diagnostics: MED001 ( ), MED002 (remote route — M2), MED003 (singleton handler scoped — IServiceProvider- : , ).
- [ ] TDD: `Microsoft.CodeAnalysis.CSharp.SourceGenerators.Testing`- harness ( snippets + System.Reflection.Metadata emitted syntax trees); incremental- (-).
- [ ] Commit.

### Task 1.6: M1 + CI

**Files:** `benchmarks/Mediana.Benchmarks/*` (Send/Publish/Stream vs MediatR 12.x, allocation dumps), `.github/workflows/ci.yml` (build, test, coverage gate, stryker nightly?, benchmark diff — gate- approve).
- [ ] , `benchmarks/RESULTS.md` (baseline).
- [ ] Commit, merge M1.

### Task 2.1: + 

**Files:** `src/Mediana.Transport.Abstractions/Messaging/Envelope.cs`, `MessageTypeDescriptor.cs`, `EnvelopeWriter/Reader` (STJ source-gen context), `Serialization/IMessageSerializer.cs`, `SystemTextJsonSerializer.cs`, `EnvelopeVersion.cs`.

Envelope §7 ; UUIDv7: `GuidV7.cs` (net10.0 → `Guid.CreateVersion7()`, ns2.1 → , RFC 9562).
- [ ] TDD: roundtrip ( /traceparent), UUIDv7 /-, versioning (additive).
- [ ] Commit.

### Task 2.2: 

**Files:** `src/Mediana.Transport.Abstractions/Routing/RouteRegistry.cs`, `RoutePolicy.cs` (Local/Queue/Topic/LocalAndRemote/RemoteQuery), `RemoteAttribute.cs`, fluent- MedianaConfiguration.

- [ ] TDD: per , < fluent, Local; (Query → 30s; command LocalAndRemote → diagnostic warning).
- [ ] Commit.

### Task 3.1: SPI + + in-memory inbox

**Files:** `src/Mediana.Transport.Abstractions/Transport/ITransport.cs`, `ITransportPublisher.cs`, `IConsumerHost.cs`, `ConsumerEndpoint.cs`, `TopologyManifest.cs`, `TransportCapabilities.cs`, `Inbox/IInboxStore.cs`, `InMemoryInboxStore.cs`, `Hosting/ConsumerHostBuilder.cs` (Channels + semaphore + graceful drain).

- [ ] TDD: inbox dedup (parallel TryBegin), drain- (virtual-time ), backpressure (bounded channel, ).
- [ ] Commit.

### Task 3.2: RabbitMQ- (net10.0 — 7.x; ns2.1 — 6.x )

**Files:** `src/Mediana.RabbitMQ/**`: `RabbitMqTransport.cs`, `TopologyProvisioner.cs`, `RabbitMqPublisher.cs` (confirms), `RabbitMqConsumer.cs` (prefetch, ack/nack, DLX-cycle retry `<q>.retry.<delay>`), `RequestReplyClient.cs` (direct reply-to, ), `StreamFrameReader/Writer.cs` (chunked frames), `Adapter/IAmqpClient.cs` + `Client7/Client6` (#if TFM).

- [ ] Unit-: , retry-delay , framing. Integration (Testcontainers-RabbitMQ, Docker ; — QUESTIONS.md): publish/consume, request/reply, retry/DLX, streaming.
- [ ] Commit.

### Task 4.1: Kafka-

**Files:** `src/Mediana.Kafka/**`: , partition key, consumer groups, retry-topics (`topic.retry.<delay>` → `topic.dlq`), guard Query/StreamQuery (NotSupportedException ).
- [ ] Unit + integration 3.2.
- [ ] Commit.

### Task 5.1: MassTransit — , , envelope-

**Files:** `src/Mediana.MassTransit/**`: `MassTransitTransport.cs` (publish IBus, request IRequestClient), `MedianaDispatchBridge.cs` ( → ), `MassTransitEnvelopeMapper.cs` ( + Fault-).
- [ ] - MassTransit in-memory harness (MassTransit.TestFramework) — .
- [ ] Commit.

### Task 6.1: Retry- + DLQ + poison ( , D14)

**Files:** `src/Mediana.Transport.Abstractions/Reliability/RetryPolicy.cs`, `RetryEngine.cs`, `Backoff.cs` (fixed/incremental/exponential+jitter), `PoisonDetector.cs`, `DeadLetterPolicy.cs`.
- [ ] TDD: backoff ( seeded jitter), → DLQ, poison → DLQ, non-retryable.
- [ ] Commit.

### Task 6.2: Opt-in Outbox + 

**Files:** `src/Mediana.Outbox/**` (`OutboxDispatcher.cs`, `OutboxRelay.cs` — , lease, backoff, cleanup-, `IOutboxStore.cs`), `Mediana.Outbox.Dapper/**` (SQL: + FOR UPDATE SKIP LOCKED; Postgres/SqlServer-), `Mediana.Outbox.MongoDB/**` (lease), `Mediana.Outbox.EFCore/**` (net10.0-only, SaveChangesInterceptor).
- [ ] TDD: relay- store (, relay), , cleanup. Integration — Testcontainers/ , QUESTIONS.md.
- [ ] Commit.

### Task 7.1: MediatR-

**Files:** `src/Mediana.MediatR/**`: `MediatRAdapterRegistration.cs`, `IRequestHandler<,>`→`ICommandHandler`/`IQueryHandler`, `INotificationHandler<>`→`IEventHandler`, `IPipelineBehavior` bridge.
- [ ] TDD: , behaviors-, .
- [ ] Commit.

### Task 7.2: Mediana.Telemetry.OpenTelemetry

**Files:** `src/Mediana.Telemetry.OpenTelemetry/**`: `AddMedianaOpenTelemetry`, OTLP exporter wiring (traces/metrics/logs), -, bounded- log-bridge (`AsyncLogBridge.cs`, DropNewest/DropOldest, ), shutdown-flush.
- [ ] TDD: wiring-, drop-, flush-, endpoint (in-memory channel-based fake exporter).
- [ ] Commit.

### Task 7.3: , load-testing consideration, 

**Files:** `README.md`, `docs/getting-started.md`, `docs/load-testing-options.md` (: NBomber, k6+OTLP, BenchmarkDotNet e2e, Testcontainers- — , ), `docs/QUESTIONS.md` .
- [ ] Commit + : build/test/coverage/mutation — .

---

## Self-Review

- **Spec coverage**: M1 (§4,§5) → Tasks 1.1–1.5; M2 (§6,§7) → 2.1–2.2; M3 (§8.1, §8 SPI) → 3.1–3.2 (+streaming §10 3.2); M4 (§8.2) → 4.1; M5 (§8.3) → 5.1; M6 (§9) → 6.1–6.2 (inbox 3.1, poison 6.1); M7 (§11 OTLP-, , ) → 7.1–7.3. - — 1.3/3.x/6.x (ActivitySource/Meter constants + guard-). CI- → 0.2/1.6. Load-testing — → 7.3. : .
- **Placeholder scan**: - — ; .
- **Type consistency**: Tasks 1.1–1.3 (RequestHandlerDelegate/HandlerEntry/MessageRegistry); .
