# Mediana — without > task implementation — v1.

## what goals)

1. **In-process throughput**: Send/Publish/Stream ops/sec on 1/8/64 scoped vs singleton ****: /and Gen0/1/2 **throughput**: publish/consume RabbitMQ/Kafka (msg/sec), latency p50/p95/p99 end-to-end.
4. ****: backpressure (bounded channels), draining on shutdown, retry-storm **Outbox-**: relay throughput, lag on relay.

## ### A. BenchmarkDotNet (already in **what **: goals 1-2 ThreadingDiagnoser for ****: already allocations ****: not ****: level — already `benchmarks/`), ### B. NBomber — NET
- **what **: goals 1, 3, 4; «HTTP-→ Mediana → queue» ****: NET, latency-RSS-metrics.
- ****: still dependency (only in not in D14 not ****: for project `loadtests/Mediana.LoadTests.NBomber`.

### C. k6 (Grafana) — external **what **: target 3 HTTP-Mediana + metrics from OTLP.
- ****: JS, integration Grafana; ****: HTTP (not in-process); harness-****: for e2e-«how ».

### D. Testcontainers-+ harness
- **what **: goals 3-5 retry-storm, relay).
- ****: those what in IntegrationTests; metrics our OTLP-package — «».
- ****: own ****: how primary for transports: `loadtests/Mediana.LoadTests.Harness` (xUnit-runner + OTLP-metrics).

### E. Crank (Microsoft) — ****: ****: for ****: to open-source ## **** (BenchmarkDotNet-Send/Publish (ThreadingDiagnoser), baseline in `benchmarks/RESULTS.md`.
2. ****: Testcontainers-harness (D) for RabbitMQ/Kafka throughput+latency k/10k/100k msg.
3. ****: NBomber-API → mediator → queue → OTLP-****: k6 only if public API-## metrics (GC.GetTotalAllocatedBytes by how and in CI-benchmark-diff (>5%) 