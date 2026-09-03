# OSS ( )

## issues

 ( `triage`):

| | SLA | |
|---|---|---|
| `bug` + | 7 | / → `confirmed` → hotfix |
| `bug` + | 2 , 14 | `needs-info` → close `stale` |
| `enhancement` | 14 | (Discussions ) → `accepted` / `declined` ( ) |
| `question` | 7 | → Discussion |
| security () | 72 | [SECURITY.md](../SECURITY.md) |

: security > correctness- (/) > /CI > .

## (Dependabot)

- PR ( . [dependabot.yml](../.github/dependabot.yml)).
- Microsoft/testing- — CI.
- **-** (BCL, RabbitMQ.Client, Confluent, MassTransit, STJ): :
 `alloc-check` (0 B) → `load-check all` ( `benchmarks/RESULTS.md`);
 > 5% — issue .
- — : API- + .

## 

1. `alloc/ram/load-check` + .
2. job- `vs-mediatr-logs` ( main).
3. — PR c `perf(...)`, `RESULTS.md` ( ≥3 ).
4. - (alloc 0 B, coverage 95, mutation 90) ADR .

## 

- — feature- → PR `main`; linear history.
- — (. [release.md](release.md)).
- Hotfix/security: `support/<major>.x` ( ).

## 

- CoC — [CODE_OF_CONDUCT.md](../CODE_OF_CONDUCT.md); — .
- : PR , (. [CONTRIBUTING.md](../CONTRIBUTING.md)); first-time contributors — + .
- (≥3 PR) — .

## ( )

- : `dotnet list package --vulnerable --include-transitive` ; EOL-.
- : [docs/QUESTIONS.md](QUESTIONS.md) ( — ), RESULTS.md SDK.
