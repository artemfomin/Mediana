# ## model

- SemVer 2.0, source **git-tag** `v<version>` (`v1.0.0`, prerelease: `v1.1.0-beta.1`).
- `VersionPrefix` in `Directory.Build.props` = version; tag **** (`-p:Version=<tag>`).
- github/workflows/release.yml](../.github/workflows/release.yml).

## thread release

1. **** (in feature or main):
 - all CI build, tests coverage ≥95%, Stryker ≥90%, alloc-gate, D14-CHANGELOG.md](../CHANGELOG.md) — `[<version>]` (Added/Fixed/Changed + if if `ram-check`/`load-check`, results in `benchmarks/RESULTS.md`.
2. **Merge in main** — PR (CI, approve).
3. **tag**: `git tag v1.X.Y && git push origin v1.X.Y`.
4. **pipeline release.yml** (`verify` — all on `pack` — 13 + snupkg), **** (license/icon/readme pause on `publish-nuget` — push on nuget.org (secret `NUGET_API_KEY`; without warning + packages in release `github-release` — GitHub Release **vs MediatR** and nupkg.
5. **release**: `VersionPrefix` → version; CHANGELOG `[Unreleased]` Discussions) — ## / security-fix

- SUPPORT.md](../SUPPORT.md) and [SECURITY.md](../SECURITY.md): security-in for branch `support/1.x`, tag `v1.0.<n+1>`.

## release in Secrets: `NUGET_API_KEY` (Settings → Secrets → Actions); environment `nuget` Settings → Environments).
- [ ] Settings → Actions → Workflow permissions: read/write (for + GitHub Releases.
- [ ] Branch protection `main`: require CI-jobs + 1 approval + linear history.
- [ ] Discussions from issue-