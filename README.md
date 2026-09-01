# Mediana

**Высокопроизводительный медиатор для .NET 10 / .NET Standard 2.1 с интеграцией очередей.**

North star: высочайший уровень алгоритмической оптимизации. Вторая метрика: минимум сторонних библиотек в ядре (D14).

## Ключевые свойства

- **Zero-alloc диспетчеризация**: in-process `Send` с behaviors — **0 байт** в steady state (проверено тестами и бенчмарками); Publish sequential — 0; struct-сообщения через `SendExact` — без боксинга.
- **Иерархия сообщений**: `IRequest` ← `ICommand<T>` / `IQuery<T>` / `IEvent` / `IStreamQuery<T>` — семантика задаёт правила роутинга.
- **Транспорты**: RabbitMQ (DLX-cycle retry, direct reply-to, publisher confirms), Kafka (retry-топики, partition ordering; без RPC — осознанно), MassTransit (транспорт + мост в обе стороны + Fault-совместимость).
- **Надёжность**: inbox-дедупликация, retry-движок с backoff+jitter (собственный, не Polly), poison detection, DLQ.
- **Transactional Outbox — opt-in**: отдельные NuGet-пакеты (EF Core / Dapper / MongoDB), ядро без БД-зависимостей.
- **Полная OTLP-телеметрия**: `Mediana.Telemetry.OpenTelemetry` — traces + metrics + logs одним вызовом, полностью асинхронный конвейер (логи не блокируют диспетч).
- **Source generator**: регистрация без рефлексии, MED001 на дубликаты хендлеров, AOT/trimming-совместимость.
- **Обе платформы**: net10.0 и netstandard2.1 с идентичной API-поверхностью (контракт-тесты).

## Быстрый старт

```csharp
services.AddMediana(cfg => cfg
    .AddCommandHandler<CreateOrder, OrderCreated, CreateOrderHandler>()
    .AddQueryHandler<GetOrder, OrderDto, GetOrderHandler>()
    .AddEventHandler<OrderCreated, OrderCreatedAuditHandler>()
    .AddStreamHandler<SearchOrders, OrderDto, SearchOrdersHandler>()
    .AddBehavior<CreateOrder, OrderCreated, ValidationBehavior>()
    .UseSingletonHandlers()); // 0 DI-обращений на вызов для stateless-хендлеров

// Или генератором (без рефлексии):
services.AddMediana(cfg => cfg.AddGeneratedHandlers()); // Mediana.Generators

var result = await mediator.Send((ICommand<OrderCreated>)new CreateOrder(42));
var stream = mediator.Stream((IStreamQuery<OrderDto>)new SearchOrders("q"));
```

## Пакеты

| Пакет | Назначение | TFM |
|---|---|---|
| `Mediana.Abstractions` | Контракты (ноль зависимостей) | net10.0; ns2.1 |
| `Mediana` | In-process диспетчер, DI | net10.0; ns2.1 |
| `Mediana.Generators` | Source generator + MED001 | netstandard2.0 |
| `Mediana.Transport.Abstractions` | SPI, конверт, роутинг, inbox, retry | net10.0; ns2.1 |
| `Mediana.RabbitMQ` | Провайдер RabbitMQ (клиент 7.x) | net10.0; ns2.1 |
| `Mediana.Kafka` | Провайдер Kafka (Confluent) | net10.0; ns2.1 |
| `Mediana.MassTransit` | 3 режима интеграции MassTransit | net10.0; ns2.1 |
| `Mediana.Outbox` (+ .EFCore/.Dapper/.MongoDB) | Opt-in transactional outbox | net10.0; ns2.1 (EF — net10.0) |
| `Mediana.Telemetry.OpenTelemetry` | Полная OTLP-телеметрия | net10.0; ns2.1 |
| `Mediana.MediatR` | Мост для MediatR-хендлеров | net10.0; ns2.1 |

## Инженерный факт (для контрибьюторов)

Вызов generic-делегата из canon-shared generic-контекста (все generic-аргументы — reference-типы) аллоцирует ~24-32Б на вызов; non-generic хопы и value-специализированные инстанциации — ноль. Ядро построено на non-generic мостах для ref-ответов и прямых typed-путях для value-ответов (см. `RequestCallSiteCompositor`, тесты `AllocationBisectTests`).

## Разработка

```
dotnet test tests/Mediana.UnitTests          # ядро + бюджеты аллокаций
dotnet test tests/Mediana.GeneratorTests     # генератор
dotnet test tests/Mediana.ContractTests.Ns21 # идентичность API двух TFM
dotnet test tests/Mediana.InteropTests       # MediatR-мост + телеметрия
dotnet run --project benchmarks/Mediana.Benchmarks -c Release -- alloc-check
dotnet tool run dotnet-stryker               # мутационное тестирование ядра
```

Спецификация: [docs/superpowers/specs/2026-09-01-mediana-design.md](docs/superpowers/specs/2026-09-01-mediana-design.md)
