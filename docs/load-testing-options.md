# Mediana — ( )

> : . — v1.

## ()

1. **In-process throughput**: Send/Publish/Stream ops/sec 1/8/64 , scoped vs singleton .
2. ** **: / Gen0/1/2 ( §12).
3. ** throughput**: publish/consume RabbitMQ/Kafka (msg/sec), p50/p95/p99 end-to-end.
4. ****: backpressure (bounded channels), draining shutdown, retry-storm .
5. **Outbox-**: relay throughput, lag , relay.

## 

### A. BenchmarkDotNet ( ) — 
- ** **: 1-2 ; ThreadingDiagnoser .
- ****: , , .
- ****: .
- ****: — (`benchmarks/`), .

### B. NBomber — .NET
- ** **: 1, 3, 4; «HTTP- → Mediana → » .
- ****: .NET, , latency-, RSS-.
- ****: ( , — D14 ).
- ****: 3-4: `loadtests/Mediana.LoadTests.NBomber`.

### C. k6 (Grafana) — 
- ** **: 3 HTTP- Mediana + OTLP.
- ****: JS, , Grafana; .
- ****: HTTP ( in-process); harness-.
- ****: e2e- « ».

### D. Testcontainers- + harness
- ** **: 3-5 ; (retry-storm, relay).
- ****: , IntegrationTests; OTLP- — « ».
- ****: ; .
- ****: : `loadtests/Mediana.LoadTests.Harness` (xUnit- runner + OTLP-).

### E. Crank (Microsoft) — 
- ****: -, .
- ****: .
- ****: open-source .

## ()

1. ** 1** ( ): BenchmarkDotNet- Send/Publish (ThreadingDiagnoser), baseline `benchmarks/RESULTS.md`.
2. ** 2**: Testcontainers-harness ( D) RabbitMQ/Kafka throughput+latency 1k/10k/100k msg.
3. ** 3**: NBomber- (API → → → ) OTLP-.
4. ** 4**: k6 API-.

## §12

 (GC.GetTotalAllocatedBytes ) — , ; CI- benchmark-diff (>5%) 1.
