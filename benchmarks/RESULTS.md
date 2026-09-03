# Mediana Benchmarks — Baseline

Дата: 2026-09-01. Конфигурация: ShortRun (3 итерации), net10.0, Release.
Полная таблица: см. обновления после CI-прогонов (benchmark-diff gate >5%).

## Dispatch (Send: команда + 2 pass-through behaviors; Publish: 2 хендлера)

| Method | Mean | Ratio | Allocated | Alloc Ratio |
|---|---|---|---|---|
| MediatR_Send | 100.3 ns | 1.01 | 512 B | 1.00 |
| **Mediana_Send** | **13.6 ns** | **0.14 (7.4× быстрее)** | **— (0 B)** | **0.00** |
| **Mediana_Query** | **9.8 ns** | **0.10 (10× быстрее)** | **— (0 B)** | **0.00** |
| MediatR_Publish | 174.4 ns | 1.75 | 1032 B | 2.02 |
| **Mediana_Publish** | **21.6 ns** | **0.22 (8× быстрее)** | **— (0 B)** | **0.00** |

Send-with-response: value-typed; Publish: 2 sequential handlers. ShortRun job, 3 итерации.

## Методика воспроизведения

```
dotnet run --project benchmarks/Mediana.Benchmarks -c Release -- alloc-check   # аллокации
dotnet run --project benchmarks/Mediana.Benchmarks -c Release -- -f "*Dispatch*" --job short
```

## Примечания

- Canon-shared generic-контексты (все-ref generic-аргументы) несут ~24-32Б/вызов налога CLR
  на invoke generic-делегата; ядро обходит через non-generic хопы (см. README «Инженерный факт»).
- Async-хендлеры: стоимость async-инфраструктуры ≤700Б/вызов (Task.Yield), документированный бюджет.

## Сравнение размера пакета: MediatR 14.2.0 vs Mediana 1.0.0 (ядро)

| Метрика | MediatR 14.2.0 | Mediana ядро (Abstractions + Mediana + Generators) |
|---|---|---|
| NuGet-пакет (nupkg) | 265.1 KB (5 TFM-ассетов) | 68.5 KB (Abstractions 9.2 + Mediana 47.2 + Generators 12.1) |
| DLL на один TFM | 100.9 KB (MediatR 94.2 + Contracts 6.7) | 59.4 KB (Abstractions 8.7 + Mediana 50.7) |
| Строк кода ядра | — | ~2 600 (2 197 ядро + 374 генератор) |
| Зависимости | MediatR.Contracts, MEDI.Abstractions | MEDI.Abstractions, Logging.Abstractions, DiagnosticSource (Microsoft-only, D14) |
| Функционал | только in-process | in-process zero-alloc + генератор + роутинг SPI (транспорты — опциональные пакеты) |

Ядро Mediana ≈ 3.9× меньше по nupkg (≈1.7× по сумме DLL одного TFM) при 7–10× скорости и нулевых аллокациях.

## RAM: Mediana vs MediatR (2026-09-02, `ram-check`, 3 стабильных повтора)

### Churn — 1 000 000 sync-операций (Send + 2 pass-through middlewares)

| Библиотека | Время | Gen0-коллекций | Аллокировано |
|---|---|---|---|
| MediatR | 235–262 мс | **10** | 508–543 B/оп |
| **Mediana** | **58–60 мс** | **0** | **0 B/оп** |

Нулевые аллокации → нулевые Gen0-коллекции → GC не работает вовсе; у MediatR 10 циклов сборки на каждый миллион операций.

### Retention — 20 000 удерживаемых async-операций (полная GC до замера)

| Библиотека | Удержано в куче | На операцию |
|---|---|---|
| MediatR | 11.56 MB | 606 B/оп |
| **Mediana** | **3.45 MB** | **181 B/оп** (×3.3 меньше) |

181 B/оп у Mediana — собственная async-машинерия хендлера (одинаковая у обеих сторон);
~425 B/оп у MediatR — доп. обёртки пайплайна, удерживаемые каждой незавершённой операцией.

### Footprint — изолированные процессы (прогрев + 200k операций + полная GC)

| Метрика | MediatR | Mediana | Δ |
|---|---|---|---|
| Managed heap | 115.2 KB | 74.0 KB | −36% |
| WorkingSet | ~79 500 KB | ~30 500 KB | **−62%** |
| PrivateMemory | ~56 400 KB | ~10 500 KB | **−81%** |

Основная дельта footprint — нативная память (JIT-код и generic-инстанциации MediatR-обёрток,
рефлексивный скан сборки при регистрации); managed-куча у обеих мала.
Оговорка честности: процесс MediatR дополнительно тянет инфраструктуру логирования (AddLogging) —
вклад мал относительно наблюдаемой дельты в десятки MB.

Воспроизведение: `dotnet run --project benchmarks/Mediana.Benchmarks -c Release -- ram-check all` и
`... -- ram-check footprint mediatr|mediana` (отдельные процессы).

## LOAD: Mediana vs MediatR (2026-09-02, `load-check`, Workstation GC, 3 стабильных прогона)

### Масштабирование по потокам (Send + 2 middlewares, 3 c на конфиг)

| Потоки | MediatR ops/s | Mediana ops/s | Mediana × |
|---|---|---|---|
| 1 | 8.1–8.4 M | 26–28 M | 3.1–3.4× |
| 2 | 10.0–10.4 M | 51–52 M | 5.0–5.2× |
| 4 | 16.0–16.5 M | 104 M | 6.3–6.5× |
| 8 | 27.7–28.3 M | 202–207 M | 7.3× |
| 16 | 37.9–38.8 M | 397–405 M | 10.3–10.6× |
| 32 | 26.3–27.0 M | 622–705 M | 23–26× |
| 64 | 23.6–23.7 M | 684–710 M | 29–30× |

Ключевые наблюдения:
- **Mediana масштабируется почти линейно до 16 потоков** (405M ≈ 15× от однопоточных 27M) и выходит на плато ≈ логическим ядрам машины (~700M).
- **MediatR деградирует выше 16 потоков**: 38M на 16 → 24–27M на 32/64 (ниже собственного 8-поточного уровня) — конкуренция за transient-резолв behaviors из DI на каждый Send.

### Хвостовые латентности (5M операций, 8 потоков, пер-оп Stopwatch)

| Метрика | MediatR | Mediana | Разрыв |
|---|---|---|---|
| ops/s (с замером) | 25.2 M | 116.8–118.3 M | 4.6× |
| p50 | 200 ns | 0 ns (суб-тик) | — |
| p99 | 1.5 µs | 100 ns | 15× |
| p99.9 | 2.9–3.0 µs | 300 ns | 10× |
| p99.99 | 21–31 µs | 500 ns | **42–61×** |
| max | 0.25–5.4 ms (GC-пауза) | 56–117 µs | **21–96×** |
| Gen0-коллекций | 48 | **0** | — |
| GC-паузы | 6.7–7.2 ms (3.4–3.7% времени) | **0.0 ms (0.00%)** | — |
| Аллокации | 488 B/оп | **0 B/оп** | — |

Хвосты MediatR — прямые GC-паузы (488 Б/оп × 5M ≈ 2.4 ГБ аллокаций за окно → 48 Gen0);
у Mediana GC не запускается ни разу — хвост плоский (max 117 мкс — шедулинг потоков, не GC).
max у MediatR вариативен (0.25–5.4 мс между прогонами) — зависит от того, попала ли BGC-пауза в окно.

### Методика

- Стороны гоняются последовательно в одном процессе, полная GC между фазами (изоляция GC-эффектов).
- Workstation GC зафиксирован в csproj (типичный сервис без Server GC); ConcurrentGC вкл.
- Пер-оп Stopwatch-тайминг (~40–60 нс оверхеда) идентичен обеим сторонам; sync-хендлеры обеих сторон.
- Прогрев 200k операций на сторону (tiered JIT); барьер-старт всех потоков.
- Воспроизведение: `dotnet run --project benchmarks/Mediana.Benchmarks -c Release -- load-check all`
  (`scaling` / `tails` по отдельности; CSV-строки масштабирования печатаются для графиков).
