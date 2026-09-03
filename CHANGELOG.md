# Changelog

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) · Versioning: [SemVer 2.0](https://semver.org/)

## [Unreleased]

## [1.0.0] — 2026-09-03

Initial public release.

### Added

- **Message hierarchy**: `IRequest` ← `ICommand<T>` / `IQuery<T>` / `IEvent` / `IStreamQuery<T>`; middlewares for commands (`IHandlerMiddleware`), events (`IEventMiddleware`), and streams (`IStreamMiddleware`); `IMediator` on `ValueTask`; zero-alloc singleton mode (zero DI lookups per dispatch) and scoped mode with state pooling; `SendExact` for struct messages without boxing; sequential/parallel event dispatch policies.
- **Source generator** (`Mediana.Generators`): reflection-free registration (NativeAOT/trimming compatible), MED001 diagnostic on duplicate command/query/stream handlers.
- **Transport SPI** (`Mediana.Transport.Abstractions`): envelope (UUIDv7 MessageId, W3C traceparent, partition key), routing policies (fluent + `Remote` attribute), `IMessageSerializer` (STJ default), in-memory inbox deduplication, retry engine (fixed/incremental/exponential + jitter, own implementation — not Polly), poison detection → DLQ, consumer pipeline.
- **Transports**: `Mediana.RabbitMQ` (DLX-cycle retries, direct reply-to, publisher confirms, graceful drain), `Mediana.Kafka` (retry topics, partition ordering; no RPC/streaming by design — D11), `Mediana.MassTransit` (transport + bidirectional bridge + Fault format compatibility).
- **Transactional outbox (opt-in)**: `Mediana.Outbox` (relay with lease/backoff/cleanup) + `.EFCore` (net10.0-only), `.Dapper` (Postgres SKIP LOCKED / SQL Server READPAST), `.MongoDB` (lease-based).
- **Telemetry**: `Mediana.Telemetry.OpenTelemetry` — OTLP export (traces + metrics + logs), non-blocking log pipeline via OpenTelemetry SDK, shutdown flush.
- **MediatR bridge**: `Mediana.MediatR` — runs existing MediatR handlers with zero code changes.
- **Engineering**: 17 ADRs (docs/superpowers/specs), CI gates: union branch coverage ≥95% (both TFM assets), Stryker mutation score ≥90%, allocation gate (0 B/call), D14 dependency audit (zero non-Microsoft in core), AOT build.

### Performance (measured vs MediatR 14.2, methodology in benchmarks/RESULTS.md)

- Send: **13.6 ns vs 100.3 ns (7.4×)**, 0 B allocations; Query 10×; Publish 8×
- Throughput at 64 threads: **710M vs 24M ops/s (29×)**; linear scaling to CPU cores (MediatR degrades above 16 threads)
- p99.99 latency: **500 ns vs 21–31 µs (42–61×)**; GC pauses 0.00% vs 3.4–3.7%
- RAM: async operation retention ×3.3 less, WorkingSet −62%, core package 68.5 KB vs 265 KB

### Breaking changes (relative to pre-release iterations; 1.0.0 was never published)

- `EnvelopeCodec` moved from `Mediana.Outbox` to `Mediana.Messaging` namespace
- `IOutboxStore.MarkFailed` signature: added `int maxDeliveryAttempts` parameter
- `KafkaDelivery` constructor: now takes `IProducer` for DLQ produce
- `BridgeLoggerFactory` removed (replaced by standard MEL OpenTelemetry provider)
- Middleware family renamed (D17): `IPipelineBehavior` → `IHandlerMiddleware`, `IEventPipelineBehavior` → `IEventMiddleware`, `IStreamPipelineBehavior` → `IStreamMiddleware`, `RequestHandlerDelegate` → `HandlerDelegate`
- `OutboxMessage` gained `DocumentId` (string) and `Parked` (bool) fields
- `MongoOutboxStore.MarkDelivered/MarkFailed` throw → idempotent no-op when `DocumentId` is null

[Unreleased]: https://github.com/artemfomin/Mediana/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/artemfomin/Mediana/releases/tag/v1.0.0
