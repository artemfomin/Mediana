# ( )

> , . ; .

## Q1. (Testcontainers) — Docker ?
**:** RabbitMQ/Kafka/SQL/Mongo Testcontainers.
** :** unit/generator/interop- (in-memory/in-process); `[Trait("Category","RequiresDocker")]` - Docker-.
**:** .

## Q2. CI (GitHub Actions?)
**:** CI- (coverage/mutation/benchmark-diff/dependency-audit).
**:** `.github/workflows/ci.yml` + `scripts/verify.ps1` .

## Q3. outbox- v1
**:** EF Core (net10-only), Dapper (Postgres/SQL Server ), MongoDB.
**:** Dapper- Postgres + SQL Server; .

## Q4. namespace : `Mediana` `Mediana.Messaging`?
**:** `Mediana.Messaging` (), `Mediana` (), `Mediana.Transports.*` ().

## Q5. : Stryker- .
**:** Stryker (Abstractions+Mediana+Transport.Abstractions+Outbox) score ≥90%; / — integration-; .

## Q9. : 90.65% 
: ~30 killer- ( , behaviors, ) + Stryker- fallback- (fast/slow — CallSiteBranchTests; ). — Stryker equivalent mutants.

## Q10. (2026-09-02): branch coverage ≥95% 
UNION : Mediana 95.1%, Abstractions 100%, Transport.Abstractions 95.2%, Outbox 100%.
 scripts/check-coverage.ps1 (union, 95%) CI -.
 — (per-instantiation generics: value- ref- ).

## Q8. MediatR-: bridge MediatR IPipelineBehavior Mediana
 MediatRBridge (/, scan, DI). MediatR-behaviors → Mediana behaviors : roadmap (v1.x). , .

## Q7 ( ). RabbitMQ.Client 7.2.2 netstandard2.0-
D13 : Mediana.RabbitMQ 7.2.2 TFM (6.x- ).
 — , 6.x-.

## Q6. NuGet- 2026-09-01.
 (., MassTransit . ) — .
