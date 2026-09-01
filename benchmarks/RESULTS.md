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
