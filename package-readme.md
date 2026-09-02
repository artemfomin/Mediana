# Mediana

**High-performance zero-alloc mediator for .NET 10 / .NET Standard 2.1 with pluggable message-broker transports (RabbitMQ, Kafka, MassTransit).**

- **0 allocations** on in-process `Send`/`Publish` (enforced by a CI gate) — the GC never runs
- **7–10× faster than MediatR** in microbenchmarks; linear scaling to CPU cores
- `ICommand` / `IQuery` / `IEvent` / `IStreamQuery` hierarchy with middlewares, source-generated registration (AOT-friendly)
- Opt-in transactional outbox (EF Core / Dapper / MongoDB), inbox deduplication, retry + DLQ
- Full OTLP telemetry (traces + metrics + logs) with a non-blocking pipeline

## Quick start

```csharp
services.AddMediana(cfg => cfg
    .AddCommandHandler<CreateOrder, OrderCreated, CreateOrderHandler>()
    .AddQueryHandler<GetOrder, OrderDto, GetOrderHandler>()
    .AddEventHandler<OrderCreated, AuditEventHandler>()
    .AddStreamHandler<SearchOrders, OrderDto, SearchHandler>()
    .AddMiddleware<CreateOrder, OrderCreated, ValidationMiddleware>()
    .UseSingletonHandlers()); // 0 DI lookups per dispatch for stateless handlers

var result = await mediator.Send((ICommand<OrderCreated>)new CreateOrder(42));
await foreach (var row in mediator.Stream((IStreamQuery<OrderDto>)new SearchOrders("q"))) { }
```

Or with the source generator (reflection-free, NativeAOT-friendly, MED001 on duplicates):
`services.AddMediana(cfg => cfg.AddGeneratedHandlers())`

## Comparison with MediatR (measured, reproducible)

| Metric | MediatR 14.2 | Mediana | Advantage |
|---|---|---|---|
| Send (1 thread, +2 middlewares) | 100.3 ns | 13.6 ns | 7.4× |
| Allocations per operation | 512 B | 0 B | — |
| Throughput, 16 threads | 38 M ops/s | 405 M ops/s | 10.5× |
| Throughput, 64 threads | 24 M ops/s (degrades) | 710 M ops/s | 29× |
| p99.9 latency | 2.9 µs | 300 ns | 10× |
| p99.99 latency | 21–31 µs | 500 ns | 42–61× |
| GC pauses under load | 3.4–3.7% of time | 0.00% | — |
| RAM: retention per async op | 606 B/op | 181 B/op | 3.3× |
| RAM: process WorkingSet | ~79.5 MB | ~30.5 MB | −62% |
| Core package size | 265 KB | 68.5 KB | 3.9× |

Full methodology and tables: [`benchmarks/RESULTS.md`](https://github.com/artemfomin/Mediana/blob/main/benchmarks/RESULTS.md).

## Packages

Core: `Mediana.Abstractions`, `Mediana`, `Mediana.Generators`.
Transports (optional): `Mediana.RabbitMQ`, `Mediana.Kafka`, `Mediana.MassTransit`, `Mediana.Transport.Abstractions`.
Reliability (opt-in): `Mediana.Outbox` (+ `.EFCore` / `.Dapper` / `.MongoDB`).
Extras: `Mediana.Telemetry.OpenTelemetry`, `Mediana.MediatR` (bridge for existing MediatR handlers).

## License

MIT. The core depends on zero third-party (non-Microsoft) libraries.
