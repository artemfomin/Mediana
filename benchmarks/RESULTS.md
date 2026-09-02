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
