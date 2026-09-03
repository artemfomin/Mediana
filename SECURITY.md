# Security Policy

## Supported Versions

| Version | Status |
|---|---|
| 1.0.x | active support |
| < 1.0 | unsupported |

## Reporting a Vulnerability

**Do not create a public issue for security vulnerabilities.**

**Preferred:** [GitHub Private Vulnerability Reporting](https://github.com/artemfomin/Mediana/security/advisories/new) —
this is the fastest and most secure channel; it keeps the report private until a fix is released.

**Alternative:** contact the maintainer via the email listed on the [GitHub profile](https://github.com/artemfomin).

### What to include

- Affected package(s) and version(s)
- Description of the vulnerability and its impact
- Steps to reproduce or a proof-of-concept
- Any mitigations you've already applied

### Response timeline

| Stage | Target |
|---|---|
| First response | within 72 hours |
| Triage & severity assessment | within 7 days |
| Fix for Critical | within 7 days of triage |
| Fix for High | within 14 days of triage |
| Coordinated disclosure | fix release → GitHub Security Advisory → NuGet update |

Credit will be given in the advisory unless you prefer otherwise.

## Scope

- All `Mediana.*` packages from this repository
- Dependency vulnerabilities affecting our packages
- CI/CD pipeline security (report via PVR, not by testing on live runners)

## Out of scope

- DoS attacks against consumer applications without a specific vector through this library
- User-side broker misconfiguration
- Social engineering

## Dependency security

- CI runs `dotnet list --vulnerable --include-transitive` on every push (gating)
- `NuGetAudit=true; NuGetAuditMode=all; NuGetAuditLevel=low` — SDK-level audit during restore
- Dependabot opens weekly dependency-update PRs
- Core packages (Abstractions, Mediana, Transport.Abstractions, Outbox) are gated to zero non-Microsoft dependencies
