# Mediana Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Реализовать библиотеку Mediana полностью по спеке: медиатор + роутинг + транспорты (RabbitMQ/Kafka/MassTransit) + opt-in outbox + полная асинхронная OTLP-телеметрия, с тестами (branch ≥95%, mutation score ≥90%).

**Architecture:** Гибридная диспетчеризация (source-gen switch + runtime-реестр copy-on-write), zero-alloc hot path (ValueTask + pooled IVTS, сшитые пайплайны), мульти-таргет net10.0 + netstandard2.1 с идентичной API-поверхностью, конверт-based транспортный SPI, надёжность слоями (retry/DLQ/inbox в транспортном ядре; transactional outbox — opt-in пакеты).

**Tech Stack:** .NET 10 SDK (10.0.302), C#, MEDI, Roslyn (source generators), STJ source-gen, RabbitMQ.Client 7.x/6.x, Confluent.Kafka, MassTransit 8.x, EF Core, Dapper, MongoDB.Driver, OpenTelemetry, xUnit, coverlet, Stryker.NET, BenchmarkDotNet, Testcontainers.

**Spec:** `docs/superpowers/specs/2026-09-01-mediana-design.md` (16 ADR). План аргументируется от спеки; исполнители читают оба документа.

## Global Constraints (from spec, verbatim)

- TFM: все пакеты — `net10.0;netstandard2.1`, единственное исключение `Mediana.Outbox.EFCore` → net10.0-only; `Mediana.Generators` → netstandard2.0 (D2/D13).
- Ядро (Abstractions, Mediana, Transport.Abstractions, Generators, Outbox-relay) — ноль не-Microsoft внешних зависимостей (D14, CI-гейт §12.9). Сторонние SDK — только в спутниковых пакетах.
- Бюджеты: in-process Send/Publish-sequential/Stream — 0 байт аллокаций; Publish-parallel ≤1 малой аллокации на хендлер; async Send — 0 в steady state (D16, §12).
- Телеметрия: no-op без слушателей; весь I/O фоновый; логи не блокируют (D15, §11.4).
- Тесты: branch coverage ≥95%, mutation score ≥90% (Stryker) на ядра пакетов; TDD для ядра диспетча.
- Outbox — opt-in; ядро без него полностью функционально (D4).
- Свой API + адаптер MediatR 12.x (D1); иерархия IRequest (D7).
- UUIDv7: net10.0 `Guid.CreateVersion7()`, ns2.1 — своя реализация (D10/D14).
- Retry/backoff — собственный движок, не Polly (D14).
- Каждый milestone закрывает оба TFM-ассета; контрактный тест идентичности API-поверхности.

## Execution order

Phase 0 (каркас) → M1 ядро → M2 роутинг/конверт → M3 SPI+RabbitMQ → M4 Kafka → M5 MassTransit → M6 надёжность/outbox → M7 адаптер/телеметрия/доки. Load-testing doc (рассмотрение вариантов, без реализации) — в M7.

---

### Task 0.1: Solution skeleton

**Files:** `Mediana.sln`, `Directory.Build.props`, `Directory.Packages.props`, `docs/QUESTIONS.md`, `.gitignore` (exists).

**Steps:**
- [ ] Directory.Build.props: `<TargetFrameworks>net10.0;netstandard2.1</TargetFrameworks>` (переопределяется в исключениях), LangVersion latest, Nullable enable, ImplicitUsings, AnalyzersLevel, `<TreatWarningsAsErrors>true`, CI определяет `MEDIANA_CI`.
- [ ] Directory.Packages.props: центральный менеджмент версий пакетов (CPM).
- [ ] `docs/QUESTIONS.md` — файл вопросов пользователю (заполняется по ходу).
- [ ] Пустые csproj'ы всех 14 пакетов + тестовые проекты + benchmarks; `dotnet build` зелёный.
- [ ] Commit `chore: solution skeleton`.

### Task 0.2: Test foundation

**Files:** `tests/Mediana.UnitTests/*`, `coverlet.runsettings`, `stryker-config.json`, `global.json`.

**Steps:**
- [ ] xUnit + coverlet.collector; runsettings с branch-порогом 95% для UnitTests.
- [ ] Локальный tool manifest с Stryker.NET (`dotnet tool restore`).
- [ ] `global.json` — pin SDK 10.0.302.
- [ ] Commit.

### Task 1.1: Abstractions — контракты сообщений и хендлеров

**Files:** Create `src/Mediana.Abstractions/**`:
`Messaging/IRequest.cs`, `ICommand.cs`, `IQuery.cs`, `IEvent.cs`, `IStreamQuery.cs`, `IPartitioned.cs`;
`Handlers/ICommandHandler.cs`, `IQueryHandler.cs`, `IEventHandler.cs`, `IStreamHandler.cs`;
`Pipeline/IPipelineBehavior.cs`, `IEventPipelineBehavior.cs`, `IStreamPipelineBehavior.cs`, `RequestHandlerDelegate.cs`, `EventHandlerDelegate.cs`, `StreamHandlerDelegate.cs`, `IPreProcessor.cs`, `IPostProcessor.cs`;
`IMediator.cs`; `MediatorConfigurationException.cs`; `RemoteExecutionException.cs` (ядровая часть).

**Interfaces (Produces — точные сигнатуры):**

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

public delegate ValueTask<TResponse> RequestHandlerDelegate<in TRequest, TResponse>(TRequest request, CancellationToken ct) where TRequest : IRequest<TResponse>;
public delegate ValueTask EventHandlerDelegate<in TEvent>(TEvent @event, CancellationToken ct) where TEvent : IEvent;
public delegate IAsyncEnumerable<TRow> StreamHandlerDelegate<in TQuery, TRow>(TQuery query, CancellationToken ct) where TQuery : IStreamQuery<TRow>;

public interface IPipelineBehavior<TRequest, TResponse> where TRequest : IRequest<TResponse>
{ ValueTask<TResponse> Handle(TRequest request, RequestHandlerDelegate<TRequest, TResponse> next, CancellationToken ct); }
public interface IEventPipelineBehavior<in TEvent> where TEvent : IEvent
{ ValueTask Handle(TEvent @event, EventHandlerDelegate<TEvent> next, CancellationToken ct); }
public interface IStreamPipelineBehavior<in TQuery, TRow> where TQuery : IStreamQuery<TRow>
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

- [ ] TDD: тесты на иерархию (компилируемость контрактов, variance), исключения.
- [ ] Branch coverage ≥95% (тривиально для маркеров), commit.

### Task 1.2: Реестр сообщений + copy-on-write

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

Ключевой алгоритм: иммутабельный массив бакетов по `RuntimeTypeHandle`; поиск — вычисление бакета по hash-handle, линейный пробег; rebuild при Add → новый массив, `Volatile.Write` публикация. На net10.0 при фризе — FrozenDictionary. Zero-allocation TryGet.

- [ ] TDD: тесты add/resolve/конкурентность (add во время чтения), stress-тест copy-on-write (многопоточный add + read, без исключений, консистентные снапшоты).
- [ ] Commit.

### Task 1.3: Компиляция пайплайнов + Mediator

**Files:** `src/Mediana/Dispatch/PipelineCompiler.cs`, `Mediator.cs`, `Dispatch/PrePostProcessorBehavior.cs`, `ValueTasks/PooledValueTaskSource.cs`, `ValueTasks/ValueTaskPool.cs`.

**Ключевые решения:**
- `PipelineCompiler` строит на registration-time цепочку: для command/query — resolve behaviors (`IPipelineBehavior<T,R>` в порядке регистрации), wrapper для pre/post, терминальный вызов handler. Цепочка — статические делегаты, замыканий нет (всё через captured-free static lambdas + передаваемый `IServiceProvider` только в точке резолва singleton/scoped).
- `Mediator.Send`: lookup в реестре → вызов compiled `Invoker(message, serviceProvider, ct)`. Синхронное завершение — `new ValueTask<T>(result)` (struct, не аллоцирует); истинная асинхронность — хендлер возвращает ValueTask напрямую (никакого ожидания/await в диспетче для sync-пути: диспетч возвращает ValueTask хендлера как есть).
- Publish sequential: цикл по compiled invokers, await каждый; Parallel: `ParallelAsyncBarrier` на pooled IVTS — ValueTaskPool.
- `ValueTaskPool`: кольцевой пул `ManualResetValueTaskSourceCore<bool>` (ns2.1-совместимо).
- [ ] TDD: Send command/query (sync/async handler), Publish sequential/parallel (1–8 handlers, ошибки: первый бросок прерывает / AggregateException в parallel), Stream (без behaviors, с behaviors), SendExact для struct-команд (проверка отсутствия боксинга — тест через typeof/boxing detection), cancellation propagation.
- [ ] Аллокационные тесты (GC.GetAllocatedBytesForCurrentThread): Send sync = 0; Publish seq = 0; Stream cursor = 0 без behaviors. Эти тесты — маркерные, помечены [Trait("Category","Allocation")], в CI обязательны.
- [ ] Commit.

### Task 1.4: DI-интеграция

**Files:** `src/Mediana/ServiceCollectionExtensions.cs`, `MedianaConfiguration.cs`, `HandlerLifetime.cs`.

`services.AddMediana(cfg)` — ручная регистрация хендлеров (`cfg.RegisterCommandHandler<TC,TR,H>()`, `RegisterFromAssembly` через генератор-produced `MedianaRegistrar`), behaviors порядок, `cfg.UseSingletonHandlers()`, `cfg.SetEventPolicy<TEvent>(policy)`. IMediator → singleton Mediator (реестр immutable после билда).
- [ ] TDD: конфигурация, dup-детекция команд/queries (MediatorConfigurationException), lifetime singleton/scoped, event policy.
- [ ] Commit.

### Task 1.5: Source generator

**Files:** `src/Mediana.Generators/MedianaGenerator.cs` (incremental), `MedianaDiagnostics.cs`, emitter.

Генерирует `MedianaRegistrar` (partial class, метод `AddHandlers(IServiceCollection)`) по `ICommandHandler<>`/etc. реализациям в компилируемой сборке; switch-диспетчер не нужен в рантайме (реестр уже O(1)) — генератор даёт registration без рефлексии + diagnostics: MED001 (два хендлера команды), MED002 (remote route без контракта — после M2), MED003 (singleton handler со scoped зависимостью — статический анализ конструктора на IServiceProvider-параметры ограничен: реализуем проверку типа через констрейнты интерфейсов, где возможно).
- [ ] TDD: тесты генератора через `Microsoft.CodeAnalysis.CSharp.SourceGenerators.Testing`-подобный harness (используем snippets + System.Reflection.Metadata проверку emitted syntax trees); incremental-стабильность (кэш-тест).
- [ ] Commit.

### Task 1.6: M1 бенчмарки + CI

**Files:** `benchmarks/Mediana.Benchmarks/*` (Send/Publish/Stream vs MediatR 12.x, allocation dumps), `.github/workflows/ci.yml` (build, test, coverage gate, stryker nightly?, benchmark diff — gate-заготовка с ручным approve).
- [ ] Бенчмарки прогнаны, числа зафиксированы в `benchmarks/RESULTS.md` (baseline).
- [ ] Commit, merge M1.

### Task 2.1: Конверт + сериализация

**Files:** `src/Mediana.Transport.Abstractions/Messaging/Envelope.cs`, `MessageTypeDescriptor.cs`, `EnvelopeWriter/Reader` (STJ source-gen context), `Serialization/IMessageSerializer.cs`, `SystemTextJsonSerializer.cs`, `EnvelopeVersion.cs`.

Envelope поля по §7 спеки; UUIDv7: `GuidV7.cs` (net10.0 → `Guid.CreateVersion7()`, ns2.1 → собственная реализация, тесты на соответствие формату RFC 9562).
- [ ] TDD: roundtrip конверта (с заголовками/traceparent), UUIDv7 монотонность/версия-биты, versioning (additive).
- [ ] Commit.

### Task 2.2: Роутинг

**Files:** `src/Mediana.Transport.Abstractions/Routing/RouteRegistry.cs`, `RoutePolicy.cs` (Local/Queue/Topic/LocalAndRemote/RemoteQuery), `RemoteAttribute.cs`, fluent-конфиг в MedianaConfiguration.

- [ ] TDD: резолв политики per тип, приоритет атрибут < fluent, дефолт Local; валидации (Query без таймаута → дефолт 30s; command на LocalAndRemote → diagnostic warning).
- [ ] Commit.

### Task 3.1: Транспортный SPI + хост консьюмеров + in-memory inbox

**Files:** `src/Mediana.Transport.Abstractions/Transport/ITransport.cs`, `ITransportPublisher.cs`, `IConsumerHost.cs`, `ConsumerEndpoint.cs`, `TopologyManifest.cs`, `TransportCapabilities.cs`, `Inbox/IInboxStore.cs`, `InMemoryInboxStore.cs`, `Hosting/ConsumerHostBuilder.cs` (Channels + semaphore + graceful drain).

- [ ] TDD: inbox dedup гонки (parallel TryBegin), drain-логика (virtual-time тесты), backpressure (bounded channel, нет блокировки).
- [ ] Commit.

### Task 3.2: RabbitMQ-провайдер (net10.0 — клиент 7.x; ns2.1 — клиент 6.x через адаптер)

**Files:** `src/Mediana.RabbitMQ/**`: `RabbitMqTransport.cs`, `TopologyProvisioner.cs`, `RabbitMqPublisher.cs` (confirms), `RabbitMqConsumer.cs` (prefetch, ack/nack, DLX-cycle retry `<q>.retry.<delay>`), `RequestReplyClient.cs` (direct reply-to, таймауты), `StreamFrameReader/Writer.cs` (chunked frames), `Adapter/IAmqpClient.cs` + `Client7/Client6` (#if по TFM).

- [ ] Unit-тесты: топология из манифеста, retry-delay расчёт, framing. Integration (Testcontainers-RabbitMQ, если Docker есть; иначе — помечено и задокументировано в QUESTIONS.md): publish/consume, request/reply, retry/DLX, streaming.
- [ ] Commit.

### Task 4.1: Kafka-провайдер

**Files:** `src/Mediana.Kafka/**`: топики, partition key, consumer groups, retry-topics (`topic.retry.<delay>` → `topic.dlq`), guard на Query/StreamQuery (NotSupportedException на конфигурации).
- [ ] Unit + integration как в 3.2.
- [ ] Commit.

### Task 5.1: MassTransit — транспорт, мост, envelope-режим

**Files:** `src/Mediana.MassTransit/**`: `MassTransitTransport.cs` (publish через IBus, request через IRequestClient), `MedianaDispatchBridge.cs` (консюмеры → пайплайн), `MassTransitEnvelopeMapper.cs` (совместимый формат + Fault-маппинг).
- [ ] Интероп-тесты с реальным MassTransit in-memory harness (MassTransit.TestFramework) — без контейнера.
- [ ] Commit.

### Task 6.1: Retry-движок + DLQ + poison (собственная реализация, D14)

**Files:** `src/Mediana.Transport.Abstractions/Reliability/RetryPolicy.cs`, `RetryEngine.cs`, `Backoff.cs` (fixed/incremental/exponential+jitter), `PoisonDetector.cs`, `DeadLetterPolicy.cs`.
- [ ] TDD: стратегии backoff (детерминированные с seeded jitter), исчерпание → DLQ, poison → сразу DLQ, non-retryable.
- [ ] Commit.

### Task 6.2: Opt-in Outbox + провайдеры

**Files:** `src/Mediana.Outbox/**` (`OutboxDispatcher.cs`, `OutboxRelay.cs` — батчи, lease, backoff, cleanup-политика, `IOutboxStore.cs`), `Mediana.Outbox.Dapper/**` (SQL: таблицы + FOR UPDATE SKIP LOCKED; Postgres/SqlServer-диалекты), `Mediana.Outbox.MongoDB/**` (lease), `Mediana.Outbox.EFCore/**` (net10.0-only, SaveChangesInterceptor).
- [ ] TDD: relay-логика с фейковым store (батчи, конкурентные relay), идемпотентность, cleanup. Integration с БД — Testcontainers/или локальные контейнеры, статус в QUESTIONS.md.
- [ ] Commit.

### Task 7.1: MediatR-адаптер

**Files:** `src/Mediana.MediatR/**`: `MediatRAdapterRegistration.cs`, обёртки `IRequestHandler<,>`→`ICommandHandler`/`IQueryHandler`, `INotificationHandler<>`→`IEventHandler`, `IPipelineBehavior` bridge.
- [ ] TDD: адаптация всех видов хендлеров, behaviors-мост, ошибки типов.
- [ ] Commit.

### Task 7.2: Mediana.Telemetry.OpenTelemetry

**Files:** `src/Mediana.Telemetry.OpenTelemetry/**`: `AddMedianaOpenTelemetry`, OTLP exporter wiring (traces/metrics/logs), семантические конвенции-константы, bounded-канальный log-bridge (`AsyncLogBridge.cs`, DropNewest/DropOldest, счётчики), shutdown-flush.
- [ ] TDD: wiring-тесты, drop-политика, flush-таймаут, латентность при заблокированном endpoint (in-memory channel-based fake exporter).
- [ ] Commit.

### Task 7.3: Документация, load-testing consideration, релиз

**Files:** `README.md`, `docs/getting-started.md`, `docs/load-testing-options.md` (варианты: NBomber, k6+OTLP, BenchmarkDotNet e2e, Testcontainers-стенд с метриками — сравнение, без реализации), `docs/QUESTIONS.md` финализация.
- [ ] Commit + финальная проверка: build/test/coverage/mutation — все гейты зелёные.

---

## Self-Review

- **Spec coverage**: M1 (§4,§5) → Tasks 1.1–1.5; M2 (§6,§7) → 2.1–2.2; M3 (§8.1, §8 SPI) → 3.1–3.2 (+streaming §10 в 3.2); M4 (§8.2) → 4.1; M5 (§8.3) → 5.1; M6 (§9) → 6.1–6.2 (inbox в 3.1, poison в 6.1); M7 (§11 OTLP-пакет, адаптер, доки) → 7.1–7.3. Телеметрия-инструментация ядра — инкрементально в 1.3/3.x/6.x (ActivitySource/Meter constants + guard-паттерн). CI-гейты → 0.2/1.6. Load-testing — рассмотрение → 7.3. Гэпы: нет.
- **Placeholder scan**: код-блоки — контракты уровня сигнатур и алгоритмических решений; каждый шаг исполняем.
- **Type consistency**: сигнатуры Tasks 1.1–1.3 согласованы (RequestHandlerDelegate/HandlerEntry/MessageRegistry); дальние задачи потребляют только эти контракты.
