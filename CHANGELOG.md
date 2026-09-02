# Changelog

Формат — [Keep a Changelog](https://keepachangelog.com/ru/1.1.0/), версионирование — [SemVer 2.0](https://semver.org/lang/ru/).

## [Unreleased]

## [1.0.0] — 2026-09-02

Первый публичный релиз.

### Added

- **Ядро диспетчеризации**: иерархия `IRequest`/`ICommand<T>`/`IQuery<T>`/`IEvent`/`IStreamQuery<T>`; мидлвары команд/событий/стримов (`IHandlerMiddleware`, `IEventMiddleware`, `IStreamMiddleware`); `IMediator` на `ValueTask`; zero-alloc singleton-режим (0 DI-обращений на вызов) и scoped-режим с пулом состояний; `SendExact` для struct-сообщений без боксинга; parallel/sequential политики событий.
- **Source generator** (`Mediana.Generators`): регистрация без рефлексии (NativeAOT/trimming), диагностика MED001 на дубликаты command/query/stream-хендлеров.
- **Транспортный SPI** (`Mediana.Transport.Abstractions`): конверт (UUIDv7 MessageId, W3C traceparent, partition key), роутинг-политики (fluent + атрибут `Remote`), `IMessageSerializer` (STJ по умолчанию), inbox-дедупликация (in-memory), retry-движок (fixed/incremental/exponential + jitter, собственный), poison detection → DLQ, consumer-pipeline.
- **Транспорты**: `Mediana.RabbitMQ` (DLX-cycle retry, direct reply-to request/reply, publisher confirms, graceful drain), `Mediana.Kafka` (retry-топики, partition ordering; RPC/стриминг не поддерживаются — спека D11), `Mediana.MassTransit` (транспорт, мост в обе стороны, Fault-совместимость).
- **Transactional outbox (opt-in)**: `Mediana.Outbox` (relay с lease/backoff/cleanup) + провайдеры `.EFCore` (net10.0-only), `.Dapper` (Postgres SKIP LOCKED / SQL Server READPAST), `.MongoDB` (lease-based).
- **Телеметрия**: `Mediana.Telemetry.OpenTelemetry` — полная OTLP (traces+metrics+logs), неблокирующий bounded-конвейер логов (drop-on-overflow со счётчиками), shutdown-flush.
- **Мост MediatR**: `Mediana.MediatR` — существующие MediatR-хендлеры без изменений кода.
- **Инженерное**: 17 ADR (docs/superpowers/specs), CI-гейты: union branch coverage ≥95% (оба TFM-ассета), Stryker mutation score ≥90%, аллокационный гейт 0 B/вызов, аудит D14 (ноль не-Microsoft зависимостей ядра), AOT-сборка.

### Performance (замеры vs MediatR 14.2, методики в benchmarks/RESULTS.md)

- Send: **13.6 ns vs 100.3 ns (7.4×)**, 0 B аллокаций; Query 10×; Publish 8×
- Throughput 64 потока: **710M vs 24M ops/s (29×)**; линейное масштабирование до ядер (MediatR деградирует выше 16 потоков)
- p99.99 латентности: **500 ns vs 21–31 µs (42–61×)**; GC-паузы 0.00% vs 3.4–3.7%
- RAM: удержание async-операций ×3.3 меньше, WorkingSet −62%, пакет ядра 68.5 KB vs 265 KB

[Unreleased]: https://github.com/artemfomin/Mediana/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/artemfomin/Mediana/releases/tag/v1.0.0
