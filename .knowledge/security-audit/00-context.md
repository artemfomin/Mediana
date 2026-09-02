# Mediana — Security Audit Context (2026-09-02)

Shared context for all auditors. Read this first; do not re-derive.

Repo: `F:\Projects\Mediana`, HEAD `5be92c5`, branch `main`, clean tree, 149 tracked files. .NET OSS mediator library, MIT, v1.0.0, author artemfomin. Comments/docs mostly Russian.

## Solution layout (src/)

| Project | TFM | Deps / purpose |
|---|---|---|
| Mediana.Abstractions | net10.0;netstandard2.1 | contracts, zero deps |
| Mediana | net10.0;netstandard2.1 | dispatcher + DI; MS.Ext.DI.Abstractions, Logging.Abstractions, DiagnosticSource |
| Mediana.Generators | netstandard2.0 | Roslyn incremental generator, packaged to analyzers/dotnet/cs |
| Mediana.Transport.Abstractions | net10.0;ns2.1 | envelope, routing, inbox, retry, consumer pipeline, STJ serializer |
| Mediana.Kafka | net10.0;ns2.1 | Confluent.Kafka 2.15.0 |
| Mediana.RabbitMQ | net10.0;ns2.1 | RabbitMQ.Client 7.2.2 |
| Mediana.MassTransit | net10.0;ns2.1 | MassTransit 9.2.1 / 8.5.10 (ns2.1) |
| Mediana.MediatR | net10.0;ns2.1 | MediatR 14.2.0 bridge |
| Mediana.Outbox | net10.0;ns2.1 | outbox core + BackgroundService relay |
| Mediana.Outbox.EFCore | net10.0 | EF Core 10.0.11 |
| Mediana.Outbox.Dapper | net10.0;ns2.1 | Dapper 2.1.79, user supplies Func<DbConnection> |
| Mediana.Outbox.MongoDB | net10.0;ns2.1 | MongoDB.Driver 3.11.1 |
| Mediana.Telemetry.OpenTelemetry | net10.0;ns2.1 | OpenTelemetry 1.18.0 + OTLP |

Tests: tests/Mediana.UnitTests (+ .Ns21 same sources vs ns2.1 assets), ContractTests.Ns21, GeneratorTests, InteropTests, AotTests (NativeAOT exe), IntegrationTests (**csproj only, zero tests**, CI `continue-on-error`).

## Key entry points
- `IMediator` — src/Mediana.Abstractions/IMediator.cs:10-27 (Send/Publish/Stream/SendExact). No ISender/IPublisher split.
- Middleware: src/Mediana.Abstractions/Pipeline/Pipeline.cs (IHandlerMiddleware/IEventMiddleware/IStreamMiddleware).
- DI: src/Mediana/ServiceCollectionExtensions.cs:15-46 `AddMediana`; IMediator scoped; MessageRegistry singleton frozen.
- Config: src/Mediana/MedianaConfiguration.cs — `AddHandlersFromAssembly` :134 (reflection escape hatch, Activator.CreateInstance :177-209), `Freeze` :220.
- Dispatch internals src/Mediana/Dispatch/: MessageRegistry, MessageEntry (public, `internal set`), RequestCallSites.cs (`lock(this)` :147, :340), EventCallSite, StreamCallSite, ChainState.cs (`[ThreadStatic]` single-slot pool :21-22, throws on double `next` :55-57).
- src/Mediana/Mediator.cs:116 `Unsafe.As<IEventCallSite[]>(entry.EventCallSites)` — unchecked reinterpret of public IReadOnlyList property. :109-114 Publish unregistered event = silent no-op. :176-215 PublishParallel → AggregateException.
- InternalsVisibleTo (unsigned) src/Mediana/Properties/AssemblyInfo.cs:3-6.

## Generator (src/Mediana.Generators/MedianaGenerator.cs)
- Discovery by display-string comparison against `"Mediana.Handlers.ICommandHandler<TCommand, TResponse>"` etc. (:86,:91,:96,:115).
- `Fqn` :125-136 — no `global::`, no generic-arity handling; emitted straight into C#.
- Emit :138-205 → `Mediana.Generated.MedianaRegistrar.AddGeneratedHandlers()`; MED001 duplicate handler error.

## Reflection / dynamic code in runtime packages
- MedianaConfiguration.cs:134-209 (GetTypes/MakeGenericType/Activator) — [RequiresUnreferencedCode]/[RequiresDynamicCode].
- Mediana.MediatR/MediatRAdapter.cs:37-73 scan; :82-95 `MakeGenericType` + `GetMethod("Handle")` + `MethodInfo.Invoke` on request path.
- RouteRegistry.cs:102 GetCustomAttributes. No Type.GetType, Assembly.Load, Expression.Compile, Emit, BinaryFormatter, Newtonsoft anywhere.

## I/O surfaces
- Serialization: only System.Text.Json reflection-based. `IMessageSerializer`/`SystemTextJsonMessageSerializer` src/Mediana.Transport.Abstractions/Messaging/Serialization.cs:7-59 (Web defaults).
- **4 copies of EnvelopeCodec** (no options, default limits): Kafka/KafkaTransport.cs:205-214, RabbitMQ/RabbitMqTransport.cs:249-258, MassTransit/MassTransitTransport.cs:140-150, Outbox/OutboxRelay.cs:80-90.
- Envelope src/Mediana.Transport.Abstractions/Messaging/Envelope.cs:25-74; `MessageTypeDescriptor{FullName,TypeVersion,ContractHash}` :10-19 — ContractHash never computed/verified. Type name on the wire in headers `mediana.message-type` (Kafka :90, Rabbit :221); nothing resolves it to Type.
- Eager deserialization in delivery constructors: KafkaTransport.cs:182, RabbitMqConsumer.cs:99 — before ConsumerPipeline try/catch.
- RabbitMQ: transport :24-28 takes IConnectionFactory; CreateConnection :66-69 ignores CT. Topology :77-149. Publisher :186-245 — destination = options.DestinationOverride else **header `mediana.destination`** (:200-211). Consumer RabbitMqConsumer.cs: Stop :72 `WaitAsync(int)` = ms timeout not permits; :78 un-awaited CloseAsync; Nack retry :113-136. RequestClient :153-208 direct reply-to, autoAck true, correlation by MessageId only.
- Kafka KafkaTransport.cs: AdminClientConfig :35 and ConsumerConfig :120-133 carry **only BootstrapServers** (SASL/SSL dropped). PollLoop :135-150 catches only OCE. Nack :190-201 commits offset, no DLQ (comment only). Publish destination :80-82 from header. Guards :220-236.
- MassTransit MassTransitTransport.cs: publish :36-45; bridge consumer decodes bus body :157-163; `ToMassTransitFault` :111-131 leaks exception type/message + **Environment.MachineName**.
- Dapper src/Mediana.Outbox.Dapper/DapperOutboxStore.cs: `GetCreateTableSql(string table)` :30-63 interpolates table into DDL; LeaseBatch :85-115 `IN ({ids})` string-join :104; rest parameterized; dialect locking :91-93.
- EFCore EfOutboxStore.cs: FromSqlRaw :129-131 with {0}/{1} (parameterized), Postgres-only; interceptor :53-103.
- MongoDB MongoOutboxStore.cs: typed builders; LeaseBatch :57-84 returns ≤1 doc; Sequence never set (:113-123) → MarkDelivered/Failed match Sequence==0.
- Relay OutboxRelay.cs:118-189: Deliver swallows all exceptions → `MarkFailed(ex.Message)` persisted :185-188; MaxDeliveryAttempts :104 unused.
- Inbox InboxStore.cs:22-63 in-memory, key `messageId|handlerIdentity` :62-63; marked consumed **before** handler runs (ConsumerPipeline.cs:26-60).
- Retry.cs: defaults :28-35; jitter only if Random passed :59, never passed → jitter off. PoisonDetector :119-126 treats InvalidOperationException as poison.
- GuidV7.cs ns2.1 fallback uses System.Random (:12,:30,:61,:66).
- Telemetry MedianaTelemetry.cs: env OTEL_* :179-183 override options; :238 AsyncLogBridge forward action is empty lambda; :240 replaces app ILoggerFactory; BridgeLogger.IsEnabled hardcoded ≥Information :153, BeginScope null :151.
- Logging: 3 call sites (OutboxRelay:158, ConsumerPipeline:40,:56) — no payload logging.
- No secrets/connection strings in source. Only env reads: OTEL_*.

## Build / CI / supply chain
- Directory.Build.props: TreatWarningsAsErrors, AnalysisLevel latest, Deterministic, EnableAotAnalyzer (net10). Disabled for tests/benchmarks.
- Directory.Build.targets: NuGet metadata, SourceLink, snupkg, ContinuousIntegrationBuild when CI.
- Directory.Packages.props: CPM on; versions listed above; xunit 2.9.3, Test.Sdk 18.9.0, coverlet 10.0.1, BenchmarkDotNet 0.15.8, Testcontainers 4.14.0, Npgsql 10.0.0. **No NuGetAudit props, no nuget.config, no lock files.**
- global.json SDK 10.0.302 latestFeature. dotnet-tools.json stryker 4.16.0.
- ci.yml: jobs build-test (windows), vs-mediatr, mutation (push), dependency-audit (ubuntu; `2>/dev/null ... || true` — stderr suppressed, failing list passes), aot (ubuntu). **No `permissions:`**. Actions checkout@v4/setup-dotnet@v4/upload-artifact@v4 tag-pinned. Trigger pull_request (not _target).
- release.yml: tags v*; verify → aot → pack (windows; `"${{ github.ref_name }}"` interpolated into pwsh :66,:73) → publish-nuget (environment nuget, `secrets.NUGET_API_KEY`, empty key ⇒ warn + exit 0 :131-134, no checkout) → github-release (`softprops/action-gh-release@v2`, third-party, tag-pinned). **No `permissions:`**; docs/release.md:31 tells maintainer to set workflow perms read/write.
- dependabot.yml: nuget + github-actions weekly, grouped.
- scripts: check-coverage.ps1, coverage-gaps.py, make_icon.py. `scripts/verify.ps1` referenced (docs/QUESTIONS.md:12, ContractTests.cs:13) but missing.

## Repo hygiene
- 17 tracked-but-ignored files: 12 `artifacts/*.nupkg` (Generators nupkg and all snupkg NOT tracked — inconsistent), 5 `tests/**/TestResults/coverage*` (embed absolute local paths).
- `g.pack.binlog` (380 KB) in root — untracked, ignored, working tree only.
- `src/Mediana/Dispa` — stray tracked 8 KB file, orphaned pre-rename copy of RequestCallSites.cs.
- StrykerOutput/ untracked.

## Docs
- SECURITY.md (Russian): report via "GitHub profile of owner" DM — no email, no PVR link, no PGP. 72h/7d SLA. Scope all Mediana.* packages.
- README claims to verify: jitter by default, OTLP logs pipeline, dependency audit enforced in CI, Kafka DLQ.
- docs/superpowers/specs/2026-09-01-mediana-design.md — authoritative design (ADR D1–D17).

## Test commands
```
dotnet build -c Release
dotnet test tests/Mediana.UnitTests
dotnet test tests/Mediana.UnitTests.Ns21
dotnet test tests/Mediana.GeneratorTests
dotnet test tests/Mediana.ContractTests.Ns21
dotnet test tests/Mediana.InteropTests
```
