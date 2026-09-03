<div align="center">

# Mediana

**High-performance zero-allocation mediator for .NET 10 / .NET Standard 2.1<br>with pluggable transports: RabbitMQ · Kafka · MassTransit**

[![CI](https://github.com/artemfomin/Mediana/actions/workflows/ci.yml/badge.svg)](https://github.com/artemfomin/Mediana/actions/workflows/ci.yml)
[![Release](https://github.com/artemfomin/Mediana/actions/workflows/release.yml/badge.svg)](https://github.com/artemfomin/Mediana/actions/workflows/release.yml)
[![NuGet](https://img.shields.io/nuget/vpre/Mediana.svg?label=latest%20stable)](https://www.nuget.org/packages/Mediana)
[![Coverage](https://img.shields.io/badge/test%20coverage-95%25%2B%20branch%20(CI%20gate)-brightgreen)](https://github.com/artemfomin/Mediana/blob/main/benchmarks/RESULTS.md)
[![Mutation](https://img.shields.io/badge/mutation%20score-90.65%25%20(gate%20%E2%89%A590%25)-brightgreen)](https://github.com/artemfomin/Mediana/blob/main/benchmarks/RESULTS.md)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

*Zero GC collections · 7–10× faster than MediatR · linear scaling to CPU cores · 0 bytes per dispatch*

[![Buy Me A Coffee](https://img.buymeacoffee.com/button.jpg)](https://buymeacoffee.com/chanter)

</div>

---

## Why Mediana (vs MediatR)

Every number below is a measured result on identical test stands (same handlers/middlewares, Workstation GC),
reproducible with commands from [`benchmarks/RESULTS.md`](benchmarks/RESULTS.md).

| Metric | MediatR 14.2 | Mediana | Advantage |
|---|---:|---:|---:|
| Send, 1 thread (+2 middlewares) | 100.3 ns | **13.6 ns** | **7.4×** |
| Query, 1 thread | — | **9.8 ns** | **10×** |
| Publish (2 handlers) | 174.4 ns | **21.6 ns** | **8×** |
| Allocations per operation | 512 B | **0 B** | ∞ |
| Gen0 collections per 5M ops | 48 | **0** | — |
| GC pauses under load | 3.4–3.7% of time | **0.00%** | — |
| Throughput, 16 threads | 38 M ops/s | **405 M ops/s** | 10.5× |
| Throughput, 64 threads | 24 M ops/s *(degrades)* | **710 M ops/s** | **29×** |
| p99 latency | 1.5 µs | **100 ns** | 15× |
| p99.99 latency | 21–31 µs | **500 ns** | **42–61×** |
| RAM: retention per async op | 606 B | **181 B** | 3.3× |
| RAM: process WorkingSet | 79.5 MB | **30.5 MB** | −62% |
| Core NuGet package size | 265 KB | **68.5 KB** | 3.9× |

**Why it's fast.** The Mediana pipeline is stitched into a single static delegate at registration time
(middlewares are not resolved from DI on every call), dispatch goes by exact type through a
switch/`FrozenDictionary`, `ValueTask` runs without an async state machine on synchronous completion,
and chain state comes from a pool. Zero allocations is not only about speed — it means **flat tail
latency** (MediatR's p99.99+ spikes are GC pauses) and no GC pressure on the rest of your application.

**Honest trade-offs.** MediatR is a mature ecosystem with thousands of projects, plugins and commercial
support; Mediana is a young library. Migration path: the `Mediana.MediatR` package runs your existing
MediatR handlers with zero code changes.

## Features

- **Message hierarchy**: `IRequest` ← `ICommand<T>` / `IQuery<T>` / `IEvent` / `IStreamQuery<T>` —
  semantics drive routing rules
- **Middlewares** for commands/events/streams — nested wrappers with `next`, registration order = execution order
- **Source generator**: reflection-free registration (NativeAOT/trimming friendly), MED001 diagnostic
  on duplicate handlers; opt-in `AddHandlersFromAssembly` for plugin scenarios
- **Zero-alloc modes**: singleton (0 DI lookups per call — for stateless handlers) and
  scoped (pooled chain state); struct messages via `SendExact` without boxing
- **Transports**: RabbitMQ (DLX-cycle retries, direct reply-to, publisher confirms),
  Kafka (retry topics, partition ordering), MassTransit (transport + bridge + Fault format)
- **Reliability**: inbox deduplication, retry engine with backoff (our own, not Polly),
  poison detection → DLQ, **opt-in transactional outbox** (EF Core/Dapper/MongoDB + lease-based relay)
- **Full OTLP telemetry**: traces + metrics + logs in a single call; non-blocking log pipeline
  (bounded channels, drops are counted)
- **Both platforms**: net10.0 and netstandard2.1 with an identical API surface (contract-tested)

## Quick start

```csharp
// dotnet add package Mediana && Mediana.Generators
services.AddMediana(cfg => cfg
    .AddCommandHandler<CreateOrder, OrderCreated, CreateOrderHandler>()
    .AddQueryHandler<GetOrder, OrderDto, GetOrderHandler>()
    .AddEventHandler<OrderCreated, OrderCreatedAuditHandler>()
    .AddStreamHandler<SearchOrders, OrderDto, SearchOrdersHandler>()
    .AddMiddleware<CreateOrder, OrderCreated, ValidationMiddleware>() // wrapper with next
    .UseSingletonHandlers()); // 0 DI lookups per dispatch

var result = await mediator.Send((ICommand<OrderCreated>)new CreateOrder(42));
await mediator.Publish(new OrderCreated(42, "Created"));
await foreach (var row in mediator.Stream((IStreamQuery<OrderDto>)new SearchOrders("q"))) { }

// Or with the source generator — reflection-free, AOT-friendly, MED001 on duplicates:
services.AddMediana(cfg => cfg.AddGeneratedHandlers());
```

## Packages

| Package | Purpose |
|---|---|
| `Mediana.Abstractions` | Contracts (zero dependencies) |
| `Mediana` | In-process dispatcher, DI |
| `Mediana.Generators` | Source generator + diagnostics |
| `Mediana.Transport.Abstractions` | SPI: envelope, routing, inbox, retry |
| `Mediana.RabbitMQ` / `Mediana.Kafka` / `Mediana.MassTransit` | Transport providers |
| `Mediana.Outbox` (+ `.EFCore` / `.Dapper` / `.MongoDB`) | Opt-in transactional outbox + relay |
| `Mediana.Telemetry.OpenTelemetry` | Full OTLP telemetry |
| `Mediana.MediatR` | Bridge for existing MediatR handlers |

## Benchmarks & reproduction

```bash
dotnet run --project benchmarks/Mediana.Benchmarks -c Release -- alloc-check    # 0 B/call (CI gate)
dotnet run --project benchmarks/Mediana.Benchmarks -c Release -- ram-check all  # churn/retention/footprint
dotnet run --project benchmarks/Mediana.Benchmarks -c Release -- load-check all # scaling + p99.99 tails
```

Full tables and methodology: [`benchmarks/RESULTS.md`](benchmarks/RESULTS.md).
CI runs the vs-MediatR comparison on every push to main (job summary in the Actions tab).

## Documentation

- [Specification (17 ADRs)](docs/superpowers/specs/2026-09-01-mediana-design.md) · [Open questions](docs/QUESTIONS.md)
- [Load testing options](docs/load-testing-options.md) · [Release runbook](docs/release.md) · [Maintenance lifecycle](docs/maintenance.md)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) — local verification (`dotnet test`, coverage/mutation/allocation gates), commit conventions, PR process.
Bugs and ideas — via the [issue templates](.github/ISSUE_TEMPLATE/). Vulnerabilities — [SECURITY.md](SECURITY.md), never via public issues.

## Support the project

[![Buy Me A Coffee](https://img.buymeacoffee.com/button.jpg)](https://buymeacoffee.com/chanter)

If Mediana makes your services faster and leaner, consider buying the author a coffee — it keeps the OSS work going.

## License

[MIT](LICENSE) · Copyright © 2026 Artem Fomin

The core has zero third-party (non-Microsoft) dependencies — the dependency audit is enforced in CI.
