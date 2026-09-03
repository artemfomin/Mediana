# Mediana — дизайн-спецификация v1

Дата: 2026-09-01
Статус: утверждён в брейнсторминге (батчи 1–3 подтверждены пользователем)
North star: **высочайший уровень алгоритмической оптимизации** — производительность приоритетнее удобства там, где есть конфликт.

---

## 1. Обзор

Mediana — библиотека паттерна медиатор для .NET 10 с собственной семантикой API (не клон MediatR), подключаемыми транспортами сообщений (RabbitMQ, Kafka, MassTransit и др.) и полным стеком надёжности доставки, где transactional outbox — строго opt-in опция через отдельный NuGet-пакет.

### Non-goals (v1)

- Саги / процесс-менеджеры — не делаем; потребность закрывает мост MassTransit (саги в MassTransit, диспетч в Mediana).
- Поддержка .NET Framework 4.x (netstandard2.0) — вне охвата: тяжёлые полифиллы противоречат north star.
- Прозрачный автоматический ремоутинг «нет локального хендлера → уходим в очередь» — сознательно: скрытые сетевые вызовы ломают семантику исключений и латентность. Только явная политика роутинга.
- Собственный RPC-протокол поверх TCP — только очереди.

---

## 2. Журнал решений (decision log)

| # | Решение | Обоснование |
|---|---------|-------------|
| D1 | Собственный API + отдельный пакет `Mediana.MediatR` (адаптер MediatR-хендлеров) | Свобода оптимизаций без наследования компромиссов MediatR; миграция существующего кода без изменений |
| D2 | **Обе версии — полные**: net10.0 и netstandard2.1 реализуются параллельно для всех пакетов (где позволяет клиентская библиотека, см. D13), с идентичной API-поверхностью, одинаковыми namespace и именами типов; распространяются как мульти-таргет ассеты внутри **одних и тех же NuGet-пакетов** (единые ID) | Разные проекты команды используют разные рантаймы; net10.0-ассет задействует все доступные оптимизации (FrozenDictionary — ~47% быстрее lookup чем Dictionary, System.Threading.Lock, GetAlternateLookup, R2R), ns2.1-ассет — рукописные эквиваленты там, где API нет. Одинаковые имена пакетов и типов = максимальная совместимость: отдельные ID на TFM создавали бы конфликт типов при сборке двух веток в один граф зависимостей. Замечание о «40+%»: это микро-бенчмарк lookup'а; на end-to-end Send выигрыш скромнее, т.к. source-gen диспетч убирает lookup с горячего пути; net10-ассет дополнительно выигрывает на fallback-путях, async-инфраструктуре и startup (R2R) |
| D13 | Транспортные/хранящие пакеты мульти-таргетятся с закреплением мажоров клиентских библиотек per TFM: RabbitMQ — net10.0: RabbitMQ.Client 7.x, ns2.1: 6.x (различие API — тонкий слой адаптации внутри пакета); Kafka — Confluent.Kafka единый API (мажор проверить в плане); MassTransit 8.x — поддерживает ns2.1-хосты; Dapper/Mongo — ns2.1 ок; **EF Core-провайдер — net10.0-only** (EF Core 6+ не таргетирует ns2.1; ns2.1-потребители outbox используют Dapper/Mongo-провайдеры) | Полный охват «обеих версий» без жертвы совместимости API; единственное исключение (EF) задокументировано явно |
| D14 | **Минимум сторонних библиотек — вторая метрика после north star.** Ядро (Abstractions, Mediana, Transport.Abstractions, Generators, релей-логика Outbox) — только собственный код; внешние зависимости ядра ограничены пакетами Microsoft и только там, где это структурно неизбежно (MEDI-абстракции для DI, Roslyn для генератора, STJ как сериализатор по умолчанию). Всё стороннее допускается только в спутниковых пакетах, где SDK и есть суть пакета (клиенты очередей, DB-провайдеры, сериализатор-провайдеры MessagePack/protobuf) | Контролируемая поверхность риска и перф: ноль транзитивных сюрпризов в ядре. Собственные реализации: retry-политики и backoff (не Polly), пулы объектов/IVTS (не ObjectPooling), UUIDv7 на ns2.1 (на net10.0 — `Guid.CreateVersion7`), планировщик relay. Метрика автоматизирована CI-гейтом (§12.6) |
| D15 | **Полная OTLP-телеметрия.** Инструментация — в ядре, через BCL (`ActivitySource("Mediana")`, `Meter("Mediana")`, `ILogger`), ноль зависимостей и ноль затрат при выключенном сборщике (no-op Activity/Meter API). Готовый OTLP-экспорт — спутниковый пакет `Mediana.Telemetry.OpenTelemetry` (net10.0 + ns2.1): один вызов включает OTel SDK для всех трёх сигналов (traces + metrics + logs) с OTLP-экспортёром; атрибуты соответствуют OTel messaging semantic conventions; настройка — `OTEL_EXPORTER_OTLP_*` env + fluent-опции. Конвейер **полностью асинхронный**: inline — только запись в память, весь I/O фоновый, логи и экспорт не блокируют путь диспетчеризации (bounded-очереди, drop-on-overflow со счётчиками, flush при shutdown — §11.4) | Готовая полная телеметрия из коробки без нарушения D14 и D16; стандартная совместимость с коллекторами (Tempo/Jaeger/OTel Collector) |
| D16 | **Идеальный перформанс во всех режимах диспетчеризации, не только Send.** Известные in-process слабости MediatR закрыты архитектурно (таблица контр-мер §5.4); бюджеты аллокаций §12 расширены на Publish (sequential/parallel), Stream и конкурентные режимы; бенчмарк-матрица гоняет все режимы против MediatR 12.x в CI | MediatR исторически терял in-process именно на per-call резолве behaviors из DI, пересборке пайплайна на каждый вызов и Task-аллокациях; у Mediana эти пути отсутствуют по построению, что фиксируется абсолютными бюджетами |
| D3 | Интеграция с очередями: явная политика роутинга (локально / очередь / оба) + подключаемые транспорт-провайдеры; MassTransit — и как транспорт, и как мост | Предсказуемость + расширяемость |
| D4 | Полный стек надёжности в v1: retry, DLQ, poison detection, inbox — в транспортном уровне ядра; **outbox — opt-in через отдельные NuGet-пакеты** | Ядро без зависимостей на БД; transactional-гарантии — осознанный выбор потребителя (по требованию пользователя) |
| D5 | Функциональный объём ядра: parity MediatR + стриминг (`IAsyncEnumerable`), без саг | Стриминг дёшев на ns2.1+, саги — дублирование компетенции MassTransit |
| D6 | Ядро диспетчеризации: гибрид — source-gen статический fast-path + опциональная runtime-регистрация (copy-on-write) | Максимальная скорость стандартного пути + escape hatch для плагинов |
| D7 | Иерархия сообщений: общий корень `IRequest`; `ICommand`/`IQuery`/`IEvent`/`IStreamQuery` — наследники | Гибкость для generic-ограничений инфраструктуры (по требованию пользователя) при сохранении семантики роутинга |

Решения, принятые без отдельного голосования (можно оспорить на ревью спеки):

| # | Решение | Обоснование |
|---|---------|-------------|
| D8 | DI — только `Microsoft.Extensions.DependencyInjection` | Стандарт экосистемы; keyed services доступны пакетом и на ns2.1 |
| D9 | Сериализация по умолчанию — System.Text.Json source-gen; провайдеры MessagePack и protobuf подключаемые, выбор per message type | Zero-reflection + выбор по перф-бюджету |
| D10 | `MessageId` — UUIDv7 (net10.0: `Guid.CreateVersion7()`; ns2.1: собственная реализация, D14) | Sortable → индекс-friendly для outbox/inbox |
| D11 | Удалённый стриминг в v1 — только RabbitMQ (chunked reply frames) и MassTransit; Kafka — нет (documented limitation) | Kafka не предназначен для streaming reply; fetch-loop анти-паттерн |
| D12 | OpenTelemetry-first наблюдаемость; ошибки удалённого Send — `RemoteExecutionException` | Стандарт отрасли; MassTransit-совместимые Fault-события |

---

## 2.1. Дополнение к журналу решений (пост-ревью реализации)

| # | Решение | Обоснование |
|---|---------|-------------|
| D17 | Семейство behaviors переименовано в **Middleware** (выбор пользователя): IPipelineBehavior → IHandlerMiddleware, IEventPipelineBehavior → IEventMiddleware, IStreamPipelineBehavior → IStreamMiddleware, делегат RequestHandlerDelegate<,> → HandlerDelegate<,>, конфиг-методы Add*Behavior → Add*Middleware | Полное совпадение имён с MediatR вызывало CS0104-неоднозначность при совместных ссылках; Middleware — универсальная ментальная модель (ASP.NET Core/MassTransit), ноль коллизий. Namespace Mediana.Pipeline сохранён |

## 3. Структура решения и пакеты

```
Mediana.sln
├── src/
│   ├── Mediana.Abstractions/            # net10.0 + ns2.1. Контракты сообщений/хендлеров,
│   │                                    #   envelope, метаданные. Ноль внешних зависимостей.
│   ├── Mediana/                         # net10.0 + ns2.1. In-process диспетчер, пайплайны,
│   │                                    #   runtime-регистрация, DI-интеграция, роутинг-ядро.
│   ├── Mediana.Generators/              # netstandard2.0 (генераторы так таргетятся).
│   │                                    #   Incremental source generator + анализаторы.
│   ├── Mediana.Transport.Abstractions/  # net10.0 + ns2.1. SPI транспортов: ITransport,
│   │                                    #   publisher/consumer, топология, capabilities,
│   │                                    #   IInboxStore + in-memory реализация.
│   ├── Mediana.RabbitMQ/                # net10.0 (клиент 7.x) + ns2.1 (клиент 6.x, слой адаптации).
│   ├── Mediana.Kafka/                   # net10.0 + ns2.1 (Confluent.Kafka, мажор — в плане).
│   ├── Mediana.MassTransit/             # net10.0 + ns2.1 (MassTransit 8.x).
│   ├── Mediana.Outbox/                  # net10.0 + ns2.1. Ядро transactional outbox + relay
│   │                                    #   (opt-in); DB-реализации inbox/outbox — в пакетах ниже.
│   ├── Mediana.Outbox.EFCore/           # net10.0-only (EF Core 6+ не таргетирует ns2.1; D13).
│   ├── Mediana.Outbox.Dapper/           # net10.0 + ns2.1. Dapper/SQL провайдер (opt-in).
│   ├── Mediana.Outbox.MongoDB/          # net10.0 + ns2.1. MongoDB провайдер (opt-in).
│   ├── Mediana.Telemetry.OpenTelemetry/ # net10.0 + ns2.1. Готовый OTLP-экспорт: OTel SDK для
│   │                                    #   traces/metrics/logs Mediana, семантические конвенции.
│   └── Mediana.MediatR/                 # net10.0 + ns2.1. Адаптер MediatR 12.x контрактов.
├── tests/
│   ├── Mediana.UnitTests/               # ядро, реестр, пайплайны, конверт, retry-политики
│   ├── Mediana.IntegrationTests/        # Testcontainers: RabbitMQ, Kafka, SQL, Mongo
│   ├── Mediana.InteropTests/            # Mediana ⇄ MassTransit, MassTransit-envelope
│   ├── Mediana.AotTests/                # NativeAOT publish + trimming smoke
│   └── Mediana.ContractTests.Ns21/      # контрактные тесты идентичности API-поверхности
│                                        #   обоих ассетов + тесты ядра против ns2.1-ассета
├── benchmarks/
│   └── Mediana.Benchmarks/              # BenchmarkDotNet: dispatch, serialization, e2e
└── docs/
```

Правила multi-target (D2/D13): каждый пакет собирается одним csproj'ом в два ассета; публичная API-поверхность ассетов идентична (проверяется контрактным тестом на публичные типы/члены); выбор оптимизаций — через `#if NET10_0` внутри реализации, не через раздельные типы. Хост-приложение всегда получает один ассет — конфликтов типов в графе зависимостей нет.

Правила зависимостей (D14 — минимальный сторонний след): `Abstractions` не ссылается ни на что; `Mediana` — только `Abstractions` + MEDI-абстракции; `Transport.Abstractions` — без внешних зависимостей (in-memory inbox, контракты SPI); `Generators` — только Roslyn; outbox-relay — только собственный код; транспортные пакеты ссылаются на `Mediana.Transport.Abstractions` и свой клиентский SDK; outbox-провайдеры — на `Transport.Abstractions` и свой DB SDK. Сторонние SDK допускаются исключительно в спутниковых пакетах, где SDK и есть суть пакета. Пакет `Mediana.Outbox` (и его DB-провайдеры) **не требуется** для работы без transactional-гарантий: без него удалённая публикация идёт напрямую в транспорт с retry/DLQ, но без атомарности с бизнес-транзакцией.

---

## 4. Ядро API

### 4.1 Иерархия сообщений

```csharp
public interface IRequest { }
public interface IRequest<TResponse> : IRequest { }

public interface ICommand : IRequest { }
public interface ICommand<TResponse> : IRequest<TResponse> { }
public interface IQuery<TResponse> : IRequest<TResponse> { }
public interface IEvent : IRequest { }
public interface IStreamQuery<TRow> : IRequest { }
```

Маркеры — compile-time only: не влияют на диспетч и производительность. Роутинг различает подтипы (§6).

### 4.2 Хендлеры

```csharp
public interface ICommandHandler<in TCommand, TResponse> where TCommand : ICommand<TResponse>
{
    ValueTask<TResponse> Handle(TCommand command, CancellationToken ct);
}

public interface IQueryHandler<in TQuery, TResponse> where TQuery : IQuery<TResponse>
{
    ValueTask<TResponse> Handle(TQuery query, CancellationToken ct);
}

public interface IEventHandler<in TEvent> where TEvent : IEvent
{
    ValueTask Handle(TEvent @event, CancellationToken ct);
}

public interface IStreamHandler<in TQuery, TRow> where TQuery : IStreamQuery<TRow>
{
    IAsyncEnumerable<TRow> Handle(TQuery query, CancellationToken ct);
}
```

Ограничения (валидируются генератором): у `ICommand`/`IQuery` — ровно один хендлер на тип сообщения в графе; у `IEvent` — сколько угодно; сообщение с remote-политикой должно иметь stable-контракт (serializable).

### 4.3 Точка входа

```csharp
public interface IMediator
{
    ValueTask<TResponse> Send<TResponse>(ICommand<TResponse> command, CancellationToken ct = default);
    ValueTask<TResponse> Send<TResponse>(IQuery<TResponse> query, CancellationToken ct = default);
    ValueTask Publish<TEvent>(TEvent @event, CancellationToken ct = default) where TEvent : IEvent;
    IAsyncEnumerable<TRow> Stream<TRow>(IStreamQuery<TRow> query, CancellationToken ct = default);

    // Zero-boxing escape hatch для struct-сообщений на горячих путях
    // (симметричная перегрузка есть и для IQuery<TResponse>)
    ValueTask<TResponse> SendExact<TCommand, TResponse>(TCommand command, CancellationToken ct = default)
        where TCommand : ICommand<TResponse>;
}
```

Семантика:

- `Send` (локальный) — исключение хендлера летит вызывающему как есть (ожидание MediatR-пользователей).
- `Publish` — диспетчеризация всем локальным хендлерам; политика per event type: `Sequential` (по умолчанию; первый бросок прерывает цепочку) или `Parallel` (барьер: все стартуют, агрегированная ошибка `AggregateException` по завершении).
- `Stream` — лениво; отмена по `ct` останавливает источник.
- Токен отмены пробрасывается всюду без копий.

### 4.4 Пайплайн

```csharp
public interface IHandlerMiddleware<TRequest, TResponse> where TRequest : IRequest<TResponse>
{
    ValueTask<TResponse> Handle(TRequest request, HandlerDelegate<TResponse> next, CancellationToken ct);
}

public delegate ValueTask<TResponse> HandlerDelegate<in TRequest, TResponse>(TRequest request, CancellationToken ct);

// Пайплайн событий (IEvent не имеет ответа — отдельный контракт)
public interface IEventMiddleware<in TEvent> where TEvent : IEvent
{
    ValueTask Handle(TEvent @event, EventHandlerDelegate next, CancellationToken ct);
}
public delegate ValueTask EventHandlerDelegate<in TEvent>(TEvent @event, CancellationToken ct) where TEvent : IEvent;

// Сахар, реализованный сам как behavior (нулевая цена композиции)
public interface IPreProcessor<in TRequest> where TRequest : IRequest { ValueTask Process(TRequest r, CancellationToken ct); }
public interface IPostProcessor<in TRequest, in TResponse> where TRequest : IRequest<TResponse> { ValueTask Process(TRequest r, TResponse response, CancellationToken ct); }
```

Порядок: глобальные behaviors (регистрация по порядку) → per-message behaviors → pre-processors → handler → post-processors. Для событий — аналогичная цепочка из `IEventMiddleware` (порядок: глобальные → per-event → хендлеры). Порядок фиксирован при построении реестра; пайплайн сшивается в один статический делегат через scoped-фабрики один раз, не на вызове (§5.1). Behaviors применяются и к локальному диспетчу, и к консьюмеру из очереди (единая семантика кросс-каттинга).

Стриминг: behaviors для `IStreamQuery` — отдельный контракт `IStreamMiddleware<TQuery, TRow>` (композиция через `IAsyncEnumerable`, обёртки не аллоцируют при синхронном движении курсора).

---

## 5. Диспетчеризация (гибрид D6)

### 5.1 Статический fast-path (source generator)

Генератор на компиляции строит по всем хендлерам в сборке:

- `MedianaRegistrar` — partial-методы регистрации хендлеров в DI (без рефлексии);
- switch-диспетчер по точному типу сообщения: `RuntimeTypeHandle`-switch → прямые вызовы зарегистрированных фабрик. O(1) без хэширования строк, инлайнится JIT;
- сшитые пайплайны per (сообщение × хендлер): behaviors разрешаются в делегат-цепочку один раз при построении реестра (через scoped-фабрики), не на каждом вызове;
- конфигурацию топологий из атрибутов роутинга (§6) — JSON-манифест для `ITransport.BuildTopology`;
- STJ source-gen контексты для конверта и payload-контрактов.

### 5.2 Runtime-регистрация (opt-in escape hatch)

`cfg.AddRuntimeHandlers(assemblies)` — явное включение. Принцип freeze-on-first-dispatch, copy-on-write: реестр иммутабелен; добавление строит новую копию и публикует через `Volatile.Write`; читатели никогда не лочатся. AOT-статус сформулирован явно: runtime-режим не использует Reflection.Emit; диспетчеризация через открытые generic-типы работает под NativeAOT при `DynamicallyAccessedMemberTypes`-аннотациях на сборках-плагинах (требование документируется); Roslyn-компиляция делегатов — необязательное ускорение, только на JIT-хостах.

### 5.3 Аллокационная модель (бюджет: 0 байт на локальном Send)

| Стадия | Механизм | Аллокации |
|---|---|---|
| Резолв хендлера | switch по точному типу, статические фабрики | 0 |
| Пайплайн | заранее сшитый статический делегат; контекстов/списков/замыканий нет | 0 |
| Async | `ValueTask`; при синхронном завершении хендлера state machine не создаётся | 0 |
| Реальная асинхронность | кольцевой пул `IValueTaskSource` на базе `ManualResetValueTaskSourceCore` (доступен и в ns2.1) | 0 в steady state (пул) |
| Сообщение | class-record по конвенции; struct — через `SendExact` без боксинга | 0 |

Реестры: net10.0 — `FrozenDictionary` (~47% быстрее lookup чем Dictionary); ns2.1 — самописный immutable bucket-массив по `RuntimeTypeHandle` (на реальных N реестра сравнимо).

Хосты консьюмеров: `BackgroundService`, backpressure через `System.Threading.Channels`, семафор concurrency, graceful drain при shutdown (stop consume → дождаться in-flight → ack/nack).

### 5.4 Слабые места MediatR и контр-меры (D16)

| Слабость MediatR in-process | Контр-мера Mediana |
|------------------------------|--------------------|
| Behaviors резолвятся из DI **на каждый Send** (N service-lookup'ов на вызов) | Пайплайн сшит в статический делегат один раз при построении реестра; на вызове — ноль обращений к DI (для singleton-режима, см. ниже) или ровно один lookup хендлера (scoped-режим) |
| Пайплайн (цепочка делегатов) собирается на каждый вызов | Заранее сшитая цепочка per (сообщение × хендлер); никаких замыканий и аллокаций на вызове |
| `Task<TResponse>` — гарантированная аллокация Task на каждый вызов | `ValueTask<T>` + синхронный fast-path без state machine; реальная асинхронность — pooled `IValueTaskSource` (0 в steady state) |
| `Publish` резолвит всех handlers и строит делегаты на каждую публикацию | Pre-stitched массив скомпилированных invoker'ов per event type; публикация — обход массива, 0 аллокаций (sequential) |
| `CreateStream` аллоцирует итераторную машинерию на каждый вызов | Проброс `IAsyncEnumerable` хендлера напрямую; без stream-behaviors — ноль обёрток, с behaviors — композит без пошаговых аллокаций |
| Рефлексивное сканирование сборок на старте | Source-gen регистрация; startup-стоимость — генерация при компиляции, не в рантайме |
| Словарные lookup'и на каждый Send/TypedHandler | Switch-диспетч по точному типу (source-gen), fallback-словарь только для runtime-зарегистрированных |

**Lifetime-политика хендлеров** (перф-ручка с сохранением корректности): по умолчанию `Scoped` — хендлер резолвится из текущего scope на каждый вызов (корректно для зависимостей типа DbContext; цена — один service-lookup на вызов). Opt-in `cfg.UseSingletonHandlers()` — хендлеры без scoped-зависимостей инстанцируются один раз и вызываются напрямую: ноль service-lookup'ов на вызове. Генератор статически проверяет, что singleton-режим не применяется к хендлерам со scoped-зависимостями (диагностика-ошибка).

---

## 6. Роутинг

Источник истины — fluent-конфигурация; атрибут — сахар для простых случаев; без политики сообщение локальное.

```csharp
services.AddMediana(cfg => {
    cfg.Route<CreateOrder>().ToQueue("orders");                       // command → конкурентная очередь
    cfg.Route<OrderCreated>().FanOut(Topic.Pattern("order.{type}")); // event → fan-out
    cfg.Route<GetOrder>().Remote(timeout: TimeSpan.FromSeconds(5));   // query → request/reply
    cfg.Route<ReserveStock>().LocalAndRemote("stock");                // оба: локально + в очередь
});
```

```csharp
[Remote("orders", Transport = "rabbit")]
public sealed record CreateOrder(Guid OrderId, ...) : ICommand<OrderId>;
```

Правила по семантике:

- **Command** → одна очередь, конкурентные консьюмеры (load-balancing), у которой ровно один хендлер-тип на ноде.
- **Event** → exchange/topic с fan-out: каждый подписчик — своя очередь/подписка; доставка at-least-once каждому.
- **Query** → request/reply: correlation id, таймаут-политика per route, `RemoteTimeoutException` по истечении.
- `LocalAndRemote` — диспетч локально и публикация в очередь (для event — natural fan-out; для command — задокументированный компромисс: два выполнения, только для сценариев типа аудит-зеркал; генератор выдаёт warning-диагностку).

Политики доставки per route: `Direct` (без outbox-пакета — по умолчанию) или `Outbox` (требует установленного пакета Mediana.Outbox; без него конфигурация падает с внятной ошибкой на старте).

---

## 7. Конверт и wire-формат

```
Envelope {
  EnvelopeVersion: int,                  // эволюция только additive
  MessageId: UUIDv7,                     // sortable, дедупликация inbox
  CorrelationId: UUID?,                  // сквозная корреляция цепочек
  CausationId: UUID?,                    // messageId сообщения-причины
  MessageType { FullName, TypeVersion, ContractHash },
  Timestamp: DateTimeOffset,
  SourceEndpoint: string,
  TraceParent: string?,                  // W3C, сквозные трейсы
  Headers: bag<string,string>,           // user + системные (partition key, reply-to...)
  Payload: bytes
}
```

- Сериализатор выбирается per message type (fluent: `cfg.UseMessagePack<CreateOrder>()`); конверт всегда бинарно-компактен (обёртка поверх payload-bytes, STJ Utf8 source-gen для JSON-режима конверта).
- `ContractHash` — детекция несовместимого контракта на приёме → poison без retry.
- Эволюция контрактов: только additive-поля; правила обязательности полей — через сериализатор-specific настройки, задокументированные per provider.
- PartitionKey (optional, из `IPartitioned { string PartitionKey { get; } }` на сообщении) → Kafka partition key / RabbitMQ routing-ключ по соглашению: ordering per key.

---

## 8. Транспортный SPI и провайдеры

```csharp
public interface ITransport
{
    string Name { get; }
    TransportCapabilities Capabilities { get; }
    ValueTask BuildTopology(TopologyManifest manifest, CancellationToken ct);  // идемпотентный declare
    ValueTask<ITransportPublisher> CreatePublisher(CancellationToken ct);
    IConsumerHostFactory CreateConsumers(IReadOnlyList<ConsumerEndpoint> endpoints);
}

public interface ITransportPublisher
{
    ValueTask Publish(Envelope envelope, PublishOptions options, CancellationToken ct);
    // PublishOptions: confirmDelivery (для outbox-relay), partitionKey, headers-merge
}
```

### 8.1 RabbitMQ (`Mediana.RabbitMQ`)

- Exchange: direct (command/query) или topic (event, паттерн из роуты); очереди + bindings из манифеста.
- Dead-letter: DLX на каждую очередь → `<queue>.dlq`; poison и retry-исчерпание идут туда с заголовками причины.
- Retry: nack с requeue=false → DLX-cycle c TTL-очередями (`<queue>.retry.<delay>`), задержки из retry-политики.
- Request/reply: **direct reply-to** (без временных очередей); таймаут на клиенте; streaming — chunked frames + completion/error frame по тому же reply-to.
- Надёжность публикации: publisher confirms (opt-in per route; обязателен при outbox-режиме).
- Топология объявляется идемпотентно на старте и переиспользуется (кэш declare).

### 8.2 Kafka (`Mediana.Kafka`)

- Топики из роуты; command → топик + consumer group (конкурентность), event → топик на сервис-подписчик (group per subscriber).
- Retry-паттерн retry-топиков: `topic.retry.5s`, `topic.retry.30s` → `topic.dlq`; non-blocking retries.
- Ordering: partition key из PartitionKey сообщения (или MessageId); документируем per-key ordering.
- Request/reply и streaming — не поддерживаются (D11); конфигурация Query/StreamQuery на kafka-транспорте → диагностическая ошибка на старте.

### 8.3 MassTransit (`Mediana.MassTransit`) — три режима

1. **Транспорт**: Mediana-роуты публикуются через MassTransit `IBus`/`IRequestClient` — пользователь получает saga-экосистему, шедулер и конфигурацию MassTransit; Mediana использует выбранный MassTransit-транспорт (RabbitMQ/Azure Service Bus/...).
2. **Мост в Mediana**: MassTransit-консюмеры (`cfg.AddMedianaDispatch()` на receive endpoint) диспатчат входящие MassTransit-сообщения в локальный Mediana-пайплайн — behaviors, retry, идемпотентность применяются единообразно.
3. **MassTransit-envelope режим**: Mediana издаёт конверт в формате MassTransit (messageType envelope) — внешние MassTransit-сервисы потребляют наши сообщения без библиотек Mediana; Fault-события публикуются в MassTransit Fault-формате.

Взаимная изоляция: собственные outbox/retry Mediana при MassTransit-транспорте по умолчанию делегируются MassTransit (его outbox/ retry), чтобы не задваивать механизмы; переключаемо.

---

## 9. Надёжность доставки

### 9.1 Inbox (в транспортном уровне, всегда включён для remote-консюмеров)

- Дедупликация `(MessageId, HandlerIdentity)`; хранилище — интерфейс `IInboxStore` с реализациями в outbox-пакетах БД **и** лёгкой in-memory (для dev/тест; документируем ограничения: не переживает рестарт).
- Запись «обрабатывается» до хендлера (unique constraint побеждает гонку двойной доставки), статус → «обработано» после успеха; коллизия → skip с метрикой.

### 9.2 Retry-политики

- Per message type: `Fixed / Incremental / Exponential (+jitter)`, MaxAttempts; два контура — in-process (transient-ошибки, без редоставки) и transport-level (redelivery по механизмам §8). По умолчанию: Exponential 50ms→5s, 5 попыток in-process, дальше транспортный контур. Движок retry/backoff/jitter — собственная реализация (D14), не Polly.

### 9.3 DLQ и poison detection

- Исчерпание retry → dead-letter родным механизмом транспорта; конверт сохраняется целиком, fingerprint ошибки (тип+stack-hash) в заголовках.
- Poison (десериализация, ContractHash mismatch, известные non-retryable) → DLQ сразу, без retry, алерт-метрика `mediana_poison_total`.

### 9.4 Transactional Outbox — **opt-in через отдельный NuGet** (D4)

- `Mediana.Outbox` — ядро: перехват бизнес-транзакции (EF Core `SaveChangesInterceptor` / Dapper-транзакция / Mongo session через соответствующие провайдер-пакеты), запись исходящих конвертов в ту же транзакцию.
- Фоновый relay: батч-выборка `FOR UPDATE SKIP LOCKED` (SQL) / lease (Mongo), publisher confirms, экспоненциальный backoff при недоступности транспорта, политика cleanup по возрасту.
- Семантика честно документируется: at-least-once доставка + inbox на стороне хендлера = effectively-once выполнение.
- **Без пакета**: роуты с `Direct`-политикой публикуют напрямую (retry/DLQ работают, атомарности с бизнес-транзакцией нет). Конфигурация `Outbox`-политики без установленного пакета → понятная ошибка старта с именем NuGet-пакета.

---

## 10. Стриминг

- Локальный: `IAsyncEnumerable` от хендлера через `IMediator.Stream`, behaviors через `IStreamMiddleware` (композиция без аллокаций на синхронном движении курсора).
- Удалённый: RabbitMQ chunked reply-frames + completion/error frame (D11); MassTransit — через его колбэки, где применимо. Kafka — нет.
- Backpressure: рамки тянутся с consumer-prefetch; отмена (`ct`) → cancel-frame гасит серверный курсор.

---

## 11. Наблюдаемость и семантика ошибок (D15 — полная OTLP-телеметрия)

### 11.1 Инструментация ядра (ноль зависимостей, ноль затрат при выключенном сборщике)

Все сигналы через BCL API (`ActivitySource`/`Meter`/`ILogger`): без слушателей `Activity` API — no-op без аллокаций (гарантия north star); метрики пишутся через reusable теги-объекты.

**Трейсы (ActivitySource "Mediana") — полный инвентарь:**

| Span | Где | Ключевые атрибуты (OTel messaging semconv) |
|------|-----|--------------------------------------------|
| `dispatch {MessageType}` | локальный Send/Stream | `messaging.message.id`, `messaging.system`="inproc" |
| `publish {MessageType}` | публикация (direct или через outbox) | `messaging.destination.name`, `messaging.system`, partition key |
| `consume {MessageType}` | приём и диспетч хендлером | + `messaging.destination.name` очереди/топика |
| `request.send {MessageType}` | удалённый Send, сторона клиента | correlation, destination, timeout |
| `request.handle {MessageType}` | удалённый Send, сторона консьюмера | связан через traceparent конверта |
| `outbox.relay` | батч relay | batch size, taken/sent/skipped |
| `inbox.dedup` | проверка дедупликации | hit/miss |

Сквозная трассировка: `traceparent` (W3C TraceContext) в конверте — локальный→очередь→хендлер цепочка одним trace; `CorrelationId`/`CausationId` в атрибутах каждого span'а.

**Метрики (Meter "Mediana"):** dispatch duration histogram (по видам command/query/event/stream), in-flight count, publish/consume duration, consumer lag, retry attempts counter (по контурам), DLQ rate, `mediana_poison_total`, outbox lag/age/batch size, request/reply duration + timeout counter, stream rows counter.

**Логи:** `ILogger` с семантическими ключами `message.type`, `message.id`, `correlation.id`, `causation.id`, `transport`, `endpoint`; ambient log-scope из конверта.

### 11.2 Пакет Mediana.Telemetry.OpenTelemetry (готовый OTLP-экспорт)

```csharp
builder.Services.AddMedianaOpenTelemetry(otel => {
    otel.WithOtlpExporter()                    // gRPC/HTTP, env OTEL_EXPORTER_OTLP_ENDPOINT/*
        .WithTraces(t => t.SetSampler(new ParentBasedTraceIdRatio(0.1)))
        .WithMetrics(m => m.AddDeltaTemporality())
        .WithLogs();                           // bridge ILogger → OTLP logs
});
```

- Подключает OTel SDK только к сигналам Mediana (не захватывает чужие источники — их приложение добавляет само); либо режим `AddToExisting(sdk)` для композиции с уже настроенным OTel.
- Атрибуты уже соответствуют OTel messaging semantic conventions — коллекторы и дашборды понимают без маппинга.
- OTLP exporter: env-конфигурация стандартная (`OTEL_EXPORTER_OTLP_ENDPOINT`, `..._PROTOCOL`, `..._HEADERS`), ресурсы — `OTEL_SERVICE_NAME` + `service.namespace`/`service.version` по умолчанию из хоста.
- Зависимость OpenTelemetry SDK — только в этом спутниковом пакете (D14 не нарушен).

### 11.4 Асинхронный телеметрический конвейер (никакой записи, задерживающей путь диспетчеризации)

Принцип: **inline на горячем пути — только дешёвая запись в память; весь I/O — фоновый**. Ни один вызов диспетчеризации не ожидает ни сети, ни диска, ни очереди телеметрии.

1. **Guard-условия до сборки данных**: перед созданием span'а — `ActivitySource.HasListeners()`/`IsEnabled(sampling)`; метрики — через reusable теги-объекты. Нет слушателей / sampled-out → ноль аллокаций и ноль работы (бюджет §12 сохраняется).
2. **Span'ы/метрики**: запись только в память процессора OTel (`BatchExportProcessor`): bounded-очередь, фоновая доставка по расписанию и по размеру батча, inline-код не ждёт экспорта.
3. **Логи (ILogger-bridge)**: собственный bounded-канал (`System.Threading.Channels`, lock-free) + фоновый drain → OTLP batch-процессор. Переполнение очереди — **drop без блокировки**: политика по умолчанию `DropNewest` (настраивается `DropOldest`/`Block` — `Block` документирован как анти-паттерн для горячего пути); потерянные записи считаются метрикой `mediana_telemetry_dropped_total` (разбивка по сигналам).
4. **Drop-политика экспортёра**: bounded queue OTLP-экспортёра при недоступном коллекторе тоже не блокирует вызовы; потери считаются (`mediana_telemetry_export_dropped_total`), экспоненциальный backoff ретраев доставки.
5. **Graceful shutdown**: финальный flush с таймаутом (по умолчанию 5 сек, настраивается) — хвост телеметрии не теряется при штатной остановке; при таймауте — счётчик недосланного.
6. **Проверяемость**: (а) тест латентности — диспетчеризация с заблокированным OTLP-endpoint'ом не отличается по латентности от выключенной телеметрии; (б) тест переполнения — bounded-очередь с медленным drain не блокирует producer'а, счётчик dropped растёт; (в) shutdown-flush — все записанные до остановки события доставлены тестовому приёмнику.

### 11.3 Семантика ошибок

- Локальный `Send` — исключение летит вызывающему как есть (ожидание MediatR-пользователей); span получает status=ERROR + `exception.*` события.
- Удалённый `Send` — `RemoteExecutionException { RemoteErrorType, Message, Details, Envelope }`.
- События — Fault-событие (в т.ч. MassTransit Fault-формат) + retry-контур; DLQ-события несут fingerprint ошибки в атрибутах.

---

## 12. Производительность: бюджеты и CI-гейты

Зафиксированные контракты (BenchmarkDotNet, `MemoryDiagnoser`, CI-джоба на PR):

1. In-process `Send` (пайплайн 2 behaviors, sync-completion handler): **0 байт** аллокаций, обе платформы (net10.0 и ns2.1-ассет); в singleton-режиме хендлеров — также 0 обращений к DI на вызов.
2. In-process `Send` (async handler): 0 байт в steady state (пул IVTS), латентность не хуже MediatR; цель ≥2× throughput на высококонкурентных async-путях.
3. In-process `Publish` sequential (1–8 хендлеров): **0 байт** аллокаций на публикацию.
4. In-process `Publish` parallel (1–8 хендлеров): ≤ 1 малой аллокации на хендлер в steady state (координация барьера — pooled waiter'ы; бюджет абсолютный).
5. In-process `Stream`: 0 байт на движение курсора при отсутствии stream-behaviors; ≤ 1 малой аллокации на строку при их наличии.
6. Десериализация + диспетч консьюмера: бюджет ≤ 1.2× стоимости сериализации payload.
7. Outbox-путь: конверт + буферы ≤ 1 KB baseline аллокаций на сообщение.
8. CI-гейт: benchmark-диф между main и PR; регрессия > 5% на любом зафиксированном бюджете → red build; аллокационные бюджеты — абсолютный гейт.
9. CI-гейт зависимостей (D14): автоматический аудит деклараций пакетов ядра (Abstractions, Mediana, Transport.Abstractions, Generators, Outbox) — **ноль не-Microsoft внешних зависимостей**; появление новой зависимости Microsoft-пакета требует явного approve в PR (список разрешённых ведётся в CI-конфиге); спутниковые пакеты аудируются на отсутствие неожиданных транзитивных зависимостей.

Бенчмарк-матрица (D16 — все режимы против MediatR 12.x в CI): Send sync/async, Publish sequential/parallel (1–8 хендлеров), Stream, сериализация (STJ/MessagePack), конверт, e2e через Testcontainers-RabbitMQ (throughput конкурентных консьюмеров). Отдельный сценарий: конкурентный Send (много потоков, scoped- и singleton-режимы) — проверка отсутствия contention на реестре.

---

## 13. Тестирование

- **Unit** (≥90% ядра): диспетч, реестр (включая copy-on-write гонки — stress-тесты), пайплайны, конверт, retry-политики, poison detection. TDD: RED→GREEN для ядра диспетча и пайплайнов.
- **Integration** (Testcontainers): RabbitMQ/Kafka — топология, request/reply, retry/DLQ, inbox против двойной доставки; outbox против Postgres/SQL Server/Mongo — атомарность, relay, SKIP LOCKED конкурентность.
- **Телеметрия**: интеграционный тест с in-process OTLP-приёмником (test HTTP/gRPC endpoint) — полный обход трейса локальный→очередь→хендлер одним trace, наличие всех span'ов §11.1, метрики после сбора, no-op путь без слушателей (аллокационный тест телеметрии); асинхронность конвейера §11.4 — латентность диспетчеризации при заблокированном OTLP-endpoint'е, drop-политика при переполнении, shutdown-flush.
- **Интероп**: Mediana⇄MassTransit обе стороны; MassTransit-envelope режим; адаптер MediatR-хендлеров.
- **AOT/trimming**: NativeAOT publish smoke + `TreatWarningsAsErrors` на trimming-аннотациях; оба ассета.
- **ContractTests.Ns21**: (а) набор тестов ядра исполняется против ns2.1-ассета (включая reflection-free сценарии); (б) контрактный тест идентичности публичной API-поверхности двух ассетов (сравнение экспортированных типов/членов через reflection на собранных сборках).
- Событийная конкурентность: детерминированные тесты Parallel/Sequential политик с virtual time для retry.

---

## 14. DX: генератор и анализаторы

Incremental source generator (netstandard2.0, регистрация как analyzer + generator в одном пакете `Mediana.Generators`):

- Диагностики-ошибки: два хендлера команды; хендлер без сообщения; remote-роут сообщения без serializable-контракта; неизвестный транспорт в атрибуте; Query/StreamQuery на kafka-транспорте; `LocalAndRemote` для command — warning.
- Генерирует: DI-registrar, switch-диспетчер, сшитые пайплайны, JSON-манифест топологий, STJ-конверты. Сгенерированный код идентичен для обоих TFM (кроме реестра — ветка по `#if NET10_0`).
- Стабильный naming/formatting сгенерированного кода (`EmitCompilerGeneratedFiles` для ревью).

---

## 15. Версионирование и совместимость

- SemVer 2.0; пакеты транспортов и outbox версионируются синхронно с ядром в рамках мажора 1.x.
- Wire-формат конверта: `EnvelopeVersion`, эволюция только additive; старые читатели игнорируют неизвестные поля.
- `Mediana.MediatR` поддерживает контракты MediatR 12.x (`IRequestHandler<,>`, `INotificationHandler<>`, `IHandlerMiddleware<,>`) через адаптерную регистрацию `cfg.AddMediatRHandlers(assemblies)` — хендлеры оборачиваются в нативные Mediana-хендлеры, участвуют в общих пайплайнах.
- Минимальные версии клиентов фиксируются в csproj как диапазоны с нижней границей (RabbitMQ.Client 7.x, Confluent.Kafka 2.x, MassTransit 8.x+) — точные нижние границы фиксируются на этапе плана реализации по актуальным стабильным версиям.

---

## 16. Риски и открытые вопросы

| Риск | Митигция |
|------|----------|
| Сложность incremental generator (кэши, пересборки) | Ранний спайк-прототип генератора в начале реализации; канареечный тест на incremental behavior |
| Zero-alloc при исключениях (exception path аллоцирует неизбежно) | Бюджет задаётся только на happy path; exception-path — отдельный мягкий бюджет |
| Гонки copy-on-write реестра | Stress-тесты + модель review; immutable snapshot семантика |
| ns2.1-деградация на легаси-хостах (Unity/Mono) | Честная документация; benchmark-запуск на соответствующих хостах вне CI-гейтов (best effort) |
| MassTransit envelope-режим: тонкости формата | Интероп-тесты против реального MassTransit контракта; фикстуры с образцами конвертов |
| Диапазоны версий клиентских библиотек | Решение фиксируется в плане реализации (D13 — рамки уже заданы) |
| Расхождение API RabbitMQ.Client 6.x/7.x — дублирование транспортного кода на ns2.1-ассете | Тонкий слой адаптации внутри Mediana.RabbitMQ: вся логика протокола/топологии/retry общая, per-TFM только обёртки клиента; контрактные тесты идентичны поведения |

Открытые вопросы к плану реализации: точные нижние версии клиентских библиотек; схема SQL-таблиц outbox/inbox (миграции EF); политика cleanup relay; поддержка `required`-членов в ns2.1-ассете (избегаем в public API).

---

## 17. Вехи v1 (порядок реализации детализируется в плане)

1. **M1 Ядро**: Abstractions + диспетчер (source-gen + runtime), пайплайны, DI, бенчмарк-каркас, бюджеты §12. Каждый milestone закрывает **оба ассета** (net10.0 и ns2.1) одновременно, включая контрактный тест идентичности API-поверхности.
2. **M2 Роутинг и конверт**: роутинг-политики, конверт, STJ source-gen, сериализаторный SPI.
3. **M3 Транспортный SPI + RabbitMQ**: publisher/consumer, топология, retry/DLQ, request/reply, streaming, in-memory inbox.
4. **M4 Kafka**: топики, retry-топики, ordering.
5. **M5 MassTransit**: транспорт, мост, envelope-режим, интероп-тесты.
6. **M6 Надёжность**: poison detection, DB-backed inbox, opt-in Outbox + EF/Dapper/Mongo провайдеры, relay.
7. **M7 MediatR-адаптер, OTLP-пакет телеметрии, документация, релизная подготовка**. (Инструментация ядра §11.1 поставляется инкрементально вместе с M1–M6, не откладывается на M7.)
