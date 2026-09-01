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
