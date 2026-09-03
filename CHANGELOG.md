# Changelog

Keep a Changelog](https://keepachangelog.com/ru/1.1.0/), SemVer 2.0](https://semver.org/lang/ru/).

## [Unreleased]

## [1.0.0] — 2026-09-02

first public release.

### Added

- ****: `IRequest`/`ICommand<T>`/`IQuery<T>`/`IEvent`/`IStreamQuery<T>`; middlewares /events/`IHandlerMiddleware`, `IEventMiddleware`, `IStreamMiddleware`); `IMediator` on `ValueTask`; zero-alloc singleton-DI-on and scoped-`SendExact` for struct-messages without parallel/sequential events.
- **Source generator** (`Mediana.Generators`): without NativeAOT/trimming), MED001 on command/query/stream-handlers.
- **SPI** (`Mediana.Transport.Abstractions`): envelope (UUIDv7 MessageId, W3C traceparent, partition key), fluent + `Remote`), `IMessageSerializer` (STJ by inbox-deduplication (in-memory), retry-fixed/incremental/exponential + jitter, poison detection → DLQ, consumer-pipeline.
- **transports**: `Mediana.RabbitMQ` (DLX-cycle retry, direct reply-to request/reply, publisher confirms, graceful drain), `Mediana.Kafka` (retry-partition ordering; RPC/not D11), `Mediana.MassTransit` (transport, in Fault-**Transactional outbox (opt-in)**: `Mediana.Outbox` (relay lease/backoff/cleanup) + `.EFCore` (net10.0-only), `.Dapper` (Postgres SKIP LOCKED / SQL Server READPAST), `.MongoDB` (lease-based).
- **telemetry**: `Mediana.Telemetry.OpenTelemetry` — OTLP (traces+metrics+logs), bounded-drop-on-overflow shutdown-flush.
- **MediatR**: `Mediana.MediatR` — MediatR-handlers without ****: 17 ADR (docs/superpowers/specs), CI-union branch coverage ≥95% (TFM-Stryker mutation score ≥90%, B/D14 (not-Microsoft AOT-build.

### Performance (measurements vs MediatR 14.2, in benchmarks/RESULTS.md)

- Send: **13.6 ns vs 100.3 ns (7.4×)**, 0 B Query 10×; Publish 8×
- throughput 64 **710M vs 24M ops/s (29×)**; scaling to MediatR p99.99 **500 ns vs 21–31 µs (42–61×)**; GC-vs 3.4–3.7%
- RAM: retention async-×3.3 WorkingSet −62%, package KB vs 265 KB

[Unreleased]: https://github.com/artemfomin/Mediana/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/artemfomin/Mediana/releases/tag/v1.0.0
