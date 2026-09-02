<div align="center">

# Mediana

**Высокопроизводительный zero-alloc медиатор для .NET 10 / .NET Standard 2.1<br>с подключаемыми транспортами: RabbitMQ · Kafka · MassTransit**

[![CI](https://github.com/artemfomin/Mediana/actions/workflows/ci.yml/badge.svg)](https://github.com/artemfomin/Mediana/actions/workflows/ci.yml)
[![Release](https://github.com/artemfomin/Mediana/actions/workflows/release.yml/badge.svg)](https://github.com/artemfomin/Mediana/actions/workflows/release.yml)
[![NuGet](https://img.shields.io/nuget/v/Mediana.svg)](https://www.nuget.org/packages/Mediana)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Mutation score](https://img.shields.io/badge/mutation%20score-90.65%25-brightgreen)](benchmarks/RESULTS.md)

*GC не запускается вовсе · 7–10× быстрее MediatR · линейное масштабирование до ядер · 0 байт на операцию*

</div>

---

## Почему Mediana (сравнение с MediatR)

Все цифры — измеренные замеры на равных стендах (идентичные хендлеры/мидлвары, Workstation GC),
воспроизводимые командами из [`benchmarks/RESULTS.md`](benchmarks/RESULTS.md).

| Метрика | MediatR 14.2 | Mediana | Выигрыш |
|---|---:|---:|---:|
| Send, 1 поток (+2 middlewares) | 100.3 ns | **13.6 ns** | **7.4×** |
| Query, 1 поток | — | **9.8 ns** | **10×** |
| Publish (2 хендлера) | 174.4 ns | **21.6 ns** | **8×** |
| Аллокации на операцию | 512 B | **0 B** | ∞ |
| Gen0-коллекций на 5M операций | 48 | **0** | — |
| GC-паузы под нагрузкой | 3.4–3.7% времени | **0.00%** | — |
| Throughput, 16 потоков | 38 M ops/s | **405 M ops/s** | 10.5× |
| Throughput, 64 потока | 24 M ops/s *(деградация)* | **710 M ops/s** | **29×** |
| p99 латентности | 1.5 µs | **100 ns** | 15× |
| p99.99 латентности | 21–31 µs | **500 ns** | **42–61×** |
| RAM: удержание async-операции | 606 B/оп | **181 B/оп** | 3.3× |
| RAM: WorkingSet процесса | 79.5 MB | **30.5 MB** | −62% |
| NuGet-пакет ядра | 265 KB | **68.5 KB** | 3.9× |

**Почему так.** Пайплайн Mediana сшивается в один статический делегат на регистрации
(мидлвары не резолвятся из DI на каждый вызов), диспетч идёт по точному типу через
switch/`FrozenDictionary`, `ValueTask` без async-машины при синхронном завершении,
а состояние цепочек берётся из пула. Zero-alloc означает не только скорость —
это **плоские хвосты латентности** (у MediatR p99.99+ — это GC-паузы) и отсутствие
GC-давления на остальное приложение.

**Честные компромиссы.** MediatR — зрелая экосистема с тысячами проектов, плагинами и
коммерческой поддержкой; Mediana — молодая библиотека. Наш путь миграции:
пакет `Mediana.MediatR` запускает существующие MediatR-хендлеры без изменений кода.

## Возможности

- **Иерархия сообщений**: `IRequest` ← `ICommand<T>` / `IQuery<T>` / `IEvent` / `IStreamQuery<T>` —
  семантика задаёт правила роутинга
- **Мидлвары** (команд/событий/стримов) — вложенные обёртки с `next`, порядок = порядок регистрации
- **Source generator**: регистрация без рефлексии (NativeAOT/trimming), диагностика MED001
  на дубликаты хендлеров; ленивая `AddHandlersFromAssembly` для плагинов
- **Zero-alloc режимы**: singleton (0 обращений к DI на вызов — для stateless-хендлеров) и
  scoped (пул состояний цепочки); struct-сообщения через `SendExact` без боксинга
- **Транспорты**: RabbitMQ (DLX-cycle retry, direct reply-to, publisher confirms),
  Kafka (retry-топики, partition ordering), MassTransit (транспорт + мост + Fault-формат)
- **Надёжность**: inbox-дедупликация, retry-движок с backoff+jitter (собственный, не Polly),
  poison→DLQ, **opt-in transactional outbox** (EF Core/Dapper/MongoDB + relay с lease)
- **Полная OTLP-телеметрия**: traces + metrics + logs одним вызовом; конвейер логов
  неблокирующий (bounded-каналы, потери считаются)
- **Обе платформы**: net10.0 и netstandard2.1 с идентичной API-поверхностью (контракт-тесты)

## Быстрый старт

```csharp
// dotnet add package Mediana && Mediana.Generators
services.AddMediana(cfg => cfg
    .AddCommandHandler<CreateOrder, OrderCreated, CreateOrderHandler>()
    .AddQueryHandler<GetOrder, OrderDto, GetOrderHandler>()
    .AddEventHandler<OrderCreated, OrderCreatedAuditHandler>()
    .AddStreamHandler<SearchOrders, OrderDto, SearchOrdersHandler>()
    .AddMiddleware<CreateOrder, OrderCreated, ValidationMiddleware>() // мидлвар = обёртка с next
    .UseSingletonHandlers()); // 0 DI-обращений на вызов

var result = await mediator.Send((ICommand<OrderCreated>)new CreateOrder(42));
await mediator.Publish(new OrderCreated(42, "Created"));
await foreach (var row in mediator.Stream((IStreamQuery<OrderDto>)new SearchOrders("q"))) { }

// Или генератором — без рефлексии, AOT-совместимо, MED001 на дубликаты:
services.AddMediana(cfg => cfg.AddGeneratedHandlers());
```

## Пакеты

| Пакет | Назначение |
|---|---|
| `Mediana.Abstractions` | Контракты (ноль зависимостей) |
| `Mediana` | In-process диспетчер, DI |
| `Mediana.Generators` | Source generator + диагностики |
| `Mediana.Transport.Abstractions` | SPI: конверт, роутинг, inbox, retry |
| `Mediana.RabbitMQ` / `Mediana.Kafka` / `Mediana.MassTransit` | Провайдеры транспортов |
| `Mediana.Outbox` (+ `.EFCore` / `.Dapper` / `.MongoDB`) | Opt-in transactional outbox + relay |
| `Mediana.Telemetry.OpenTelemetry` | Полная OTLP-телеметрия |
| `Mediana.MediatR` | Мост существующих MediatR-хендлеров |

## Бенчмарки и воспроизведение

```bash
dotnet run --project benchmarks/Mediana.Benchmarks -c Release -- alloc-check    # 0 B/вызов (CI-гейт)
dotnet run --project benchmarks/Mediana.Benchmarks -c Release -- ram-check all  # churn/retention/footprint
dotnet run --project benchmarks/Mediana.Benchmarks -c Release -- load-check all # масштабирование + хвосты p99.99
```

Полные таблицы и методики: [`benchmarks/RESULTS.md`](benchmarks/RESULTS.md).
CI гоняет vs-MediatR сравнение на каждый пуш в main (job summary в Actions).

## Документация

- [Спецификация (17 ADR)](docs/superpowers/specs/2026-09-01-mediana-design.md) · [Открытые вопросы](docs/QUESTIONS.md)
- [Нагрузочное тестирование: варианты](docs/load-testing-options.md) · [Регламент релизов](docs/release.md) · [Цикл поддержки](docs/maintenance.md)

## Участие

см. [CONTRIBUTING.md](CONTRIBUTING.md) — локальная проверка (`dotnet test`, гейты покрытия/мутаций/аллокаций), конвенции коммитов, процесс PR.
Ошибки и идеи — через [шаблоны issues](.github/ISSUE_TEMPLATE/). Уязвимости — [SECURITY.md](SECURITY.md), не через публичные issues.

## Лицензия

[MIT](LICENSE) · Copyright © 2026 artemfomin

Ядро не зависит от сторонних (не-Microsoft) библиотек — аудит зависимостей встроен в CI.
