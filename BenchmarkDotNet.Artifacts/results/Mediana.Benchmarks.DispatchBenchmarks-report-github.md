```

BenchmarkDotNet v0.15.8, Windows 11 (10.0.26200.9168/25H2/2025Update/HudsonValley2)
AMD Ryzen 9 9950X3D 4.30GHz, 1 CPU, 32 logical and 16 physical cores
.NET SDK 10.0.302
  [Host]   : .NET 10.0.10 (10.0.10, 10.0.1026.32716), X64 RyuJIT x86-64-v4
  ShortRun : .NET 10.0.10 (10.0.10, 10.0.1026.32716), X64 RyuJIT x86-64-v4

Job=ShortRun  IterationCount=3  LaunchCount=1  
WarmupCount=3  

```
| Method          | Mean       | Error      | StdDev    | Ratio | RatioSD | Gen0   | Allocated | Alloc Ratio |
|---------------- |-----------:|-----------:|----------:|------:|--------:|-------:|----------:|------------:|
| MediatR_Send    | 100.306 ns | 169.287 ns | 9.2792 ns |  1.01 |    0.12 | 0.0101 |     512 B |        1.00 |
| Mediana_Send    |  13.559 ns |  26.260 ns | 1.4394 ns |  0.14 |    0.02 |      - |         - |        0.00 |
| Mediana_Query   |   9.824 ns |   9.892 ns | 0.5422 ns |  0.10 |    0.01 |      - |         - |        0.00 |
| MediatR_Publish | 174.358 ns |  70.411 ns | 3.8595 ns |  1.75 |    0.15 | 0.0205 |    1032 B |        2.02 |
| Mediana_Publish |  21.629 ns |  26.298 ns | 1.4415 ns |  0.22 |    0.02 |      - |         - |        0.00 |
