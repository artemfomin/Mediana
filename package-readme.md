# Mediana

**Высокопроизводительный zero-alloc медиатор для .NET 10 / .NET Standard 2.1 с подключаемыми транспортами (RabbitMQ, Kafka, MassTransit).**

- **0 аллокаций** на in-process `Send`/`Publish` (проверено CI-гейтом), GC не запускается вовсе
- **7–10× быстрее MediatR** в микробенчмарках; линейное масштабирование до ядер машины
- Иерархия `ICommand` / `IQuery` / `IEvent` / `IStreamQuery` с мидлварами, source-gen регистрация (AOT-совместимо)
- Opt-in transactional outbox (EF Core / Dapper / MongoDB), inbox-дедупликация, retry+DLQ
- Полная OTLP-телеметрия (traces+metrics+logs) с неблокирующим конвейером

## Быстрый старт

```csharp
services.AddMediana(cfg => cfg
    .AddCommandHandler<CreateOrder, OrderCreated, CreateOrderHandler>()
    .AddQueryHandler<GetOrder, OrderDto, GetOrderHandler>()
    .AddEventHandler<OrderCreated, AuditMiddlewareAppliesHere>()
    .AddStreamHandler<SearchOrders, OrderDto, SearchHandler>()
    .AddMiddleware<CreateOrder, OrderCreated, ValidationMiddleware>()
    .UseSingletonHandlers()); // 0 DI-обращений на вызов для stateless-хендлеров

var result = await mediator.Send((ICommand<OrderCreated>)new CreateOrder(42));
await foreach (var row in mediator.Stream((IStreamQuery<OrderDto>)new SearchOrders("q"))) { }
```

Или source-генератором (без рефлексии, NativeAOT-friendly, диагностика MED001 на дубликаты):
`services.AddMediana(cfg => cfg.AddGeneratedHandlers())`

## Сравнение с MediatR (замеры, воспроизводимо)

| Метрика | MediatR 14.2 | Mediana | Выигрыш |
|---|---|---|---|
| Send (1 поток, +2 middlewares) | 100.3 ns | 13.6 ns | 7.4× |
| Аллокации на операцию | 512 B | 0 B | — |
| Throughput 16 потоков | 38 M ops/s | 405 M ops/s | 10.5× |
| Throughput 64 потока | 24 M ops/s (деградация) | 710 M ops/s | 29× |
| p99.9 латентность | 2.9 µs | 300 ns | 10× |
| p99.99 латентность | 21–31 µs | 500 ns | 42–61× |
| GC-паузы под нагрузкой | 3.4–3.7% времени | 0.00% | — |
| RAM: удержание async-операций | 606 B/оп | 181 B/оп | 3.3× |
| RAM: WorkingSet процесса | ~79.5 MB | ~30.5 MB | −62% |
| Размер пакета (ядро) | 265 KB | 68.5 KB | 3.9× |

Полные методики и таблицы: [`benchmarks/RESULTS.md`](https://github.com/artemfomin/Mediana/blob/main/benchmarks/RESULTS.md).

## Пакеты

Ядро: `Mediana.Abstractions`, `Mediana`, `Mediana.Generators`.
Транспорты (опционально): `Mediana.RabbitMQ`, `Mediana.Kafka`, `Mediana.MassTransit`, `Mediana.Transport.Abstractions`.
Надёжность (opt-in): `Mediana.Outbox` (+ `.EFCore` / `.Dapper` / `.MongoDB`).
Прочее: `Mediana.Telemetry.OpenTelemetry`, `Mediana.MediatR` (мост для существующих MediatR-хендлеров).

## Лицензия

MIT. Ядро не зависит от сторонних (не-Microsoft) библиотек.
