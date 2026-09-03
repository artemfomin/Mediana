# Mediana Benchmarks — Baseline

configuration: ShortRun (3 net10.0, Release.
CI-benchmark-diff gate >5%).

## Dispatch (Send: command + 2 pass-through behaviors; Publish: 2 | Method | Mean | Ratio | Allocated | Alloc Ratio |
|---|---|---|---|---|
| MediatR_Send | 100.3 ns | 1.01 | 512 B | 1.00 |
| **Mediana_Send** | **13.6 ns** | **0.14 (7.4× ** | **— (0 B)** | **0.00** |
| **Mediana_Query** | **9.8 ns** | **0.10 (10× ** | **— (0 B)** | **0.00** |
| MediatR_Publish | 174.4 ns | 1.75 | 1032 B | 2.02 |
| **Mediana_Publish** | **21.6 ns** | **0.22 (8× ** | **— (0 B)** | **0.00** |

Send-with-response: value-typed; Publish: 2 sequential handlers. ShortRun job, 3 ## methodology ```
dotnet run --project benchmarks/Mediana.Benchmarks -c Release -- alloc-check # allocations
dotnet run --project benchmarks/Mediana.Benchmarks -c Release -- -f "*Dispatch*" --job short
```

## Canon-shared generic-all-ref generic-~24-32/CLR
 on invoke generic-non-generic README «»).
- Async-handlers: async-≤700/Task.Yield), ## package: MediatR 14.2.0 vs Mediana 1.0.0 (| metric | MediatR 14.2.0 | Mediana Abstractions + Mediana + Generators) |
|---|---|---|
| NuGet-package (nupkg) | 265.1 KB (5 TFM-| 68.5 KB (Abstractions 9.2 + Mediana 47.2 + Generators 12.1) |
| DLL on TFM | 100.9 KB (MediatR 94.2 + Contracts 6.7) | 59.4 KB (Abstractions 8.7 + Mediana 50.7) |
| | — | ~2 600 (2 197 + 374 |
| dependencies | MediatR.Contracts, MEDI.Abstractions | MEDI.Abstractions, Logging.Abstractions, DiagnosticSource (Microsoft-only, D14) |
| | only in-process | in-process zero-alloc + + SPI (transports — packages) |

Mediana ≈ 3.9× by nupkg (≈1.7× by DLL TFM) on 7–10× and ## RAM: Mediana vs MediatR (2026-09-02, `ram-check`, 3 ### Churn — 1 000 000 sync-Send + 2 pass-through middlewares)

| library | | Gen0-| |
|---|---|---|---|
| MediatR | 235–262 | **10** | 508–543 B/|
| **Mediana** | **58–60 ** | **0** | **0 B/** |

allocations → Gen0-→ GC not MediatR 10 builds on each ### Retention — 20 000 async-GC to | library | in | on |
|---|---|---|
| MediatR | 11.56 MB | 606 B/|
| **Mediana** | **3.45 MB** | **181 B/** (×3.3 |

181 B/Mediana — async-~425 B/MediatR — ### Footprint — + 200k + GC)

| metric | MediatR | Mediana | Δ |
|---|---|---|---|
| Managed heap | 115.2 KB | 74.0 KB | −36% |
| WorkingSet | ~79 500 KB | ~30 500 KB | **−62%** |
| PrivateMemory | ~56 400 KB | ~10 500 KB | **−81%** |

footprint — memory (JIT-and generic-MediatR-builds on managed-MediatR AddLogging) —
in MB.

`dotnet run --project benchmarks/Mediana.Benchmarks -c Release -- ram-check all` and
`... -- ram-check footprint mediatr|mediana` (## LOAD: Mediana vs MediatR (2026-09-02, `load-check`, Workstation GC, 3 ### scaling by Send + 2 middlewares, 3 c on | threads | MediatR ops/s | Mediana ops/s | Mediana × |
|---|---|---|---|
| 1 | 8.1–8.4 M | 26–28 M | 3.1–3.4× |
| 2 | 10.0–10.4 M | 51–52 M | 5.0–5.2× |
| 4 | 16.0–16.5 M | 104 M | 6.3–6.5× |
| 8 | 27.7–28.3 M | 202–207 M | 7.3× |
| 16 | 37.9–38.8 M | 397–405 M | 10.3–10.6× |
| 32 | 26.3–27.0 M | 622–705 M | 23–26× |
| 64 | 23.6–23.7 M | 684–710 M | 29–30× |

**Mediana to 16 ** (405M ≈ 15× from M) and on ≈ ~700M).
- **MediatR **: 38M on 16 → 24–27M on 32/64 (transient-behaviors from DI on each Send.

### M Stopwatch)

| metric | MediatR | Mediana | |
|---|---|---|---|
| ops/s (| 25.2 M | 116.8–118.3 M | 4.6× |
| p50 | 200 ns | 0 ns (| — |
| p99 | 1.5 µs | 100 ns | 15× |
| p99.9 | 2.9–3.0 µs | 300 ns | 10× |
| p99.99 | 21–31 µs | 500 ns | **42–61×** |
| max | 0.25–5.4 ms (GC-| 56–117 µs | **21–96×** |
| Gen0-| 48 | **0** | — |
| GC-| 6.7–7.2 ms (3.4–3.7% | **0.0 ms (0.00%)** | — |
| allocations | 488 B/| **0 B/** | — |

MediatR — GC-/× 5M ≈ 2.4 → 48 Gen0);
Mediana GC not max 117 not GC).
max MediatR –5.4 from BGC-in ### methodology

- in GC GC-Workstation GC in csproj (without Server GC); ConcurrentGC Stopwatch-~40–60 sync-handlers k on tiered JIT); `dotnet run --project benchmarks/Mediana.Benchmarks -c Release -- load-check all`
 (`scaling` / `tails` by CSV-for 