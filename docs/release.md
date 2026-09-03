# 

## 

- — SemVer 2.0, — **git-** `v<version>` ( `v1.0.0`, prerelease: `v1.1.0-beta.1`).
- `VersionPrefix` `Directory.Build.props` = ; **** (`-p:Version=<tag>`).
- : [.github/workflows/release.yml](../.github/workflows/release.yml).

## 

1. **** ( feature main):
 - CI (build, , coverage ≥95%, Stryker ≥90%, alloc-gate, D14-);
 - [CHANGELOG.md](../CHANGELOG.md) — `[<>]` (Added/Fixed/Changed + -, );
 - — `ram-check`/`load-check`, `benchmarks/RESULTS.md`.
2. **Merge main** — PR ( CI, . 1 approve).
3. ****: `git tag v1.X.Y && git push origin v1.X.Y`.
4. ** release.yml** ():
 - `verify` — ;
 - `pack` — 13 (+ snupkg), ** ** (license/icon/readme — pause );
 - `publish-nuget` — push nuget.org ( secret `NUGET_API_KEY`; — warning + , );
 - `github-release` — GitHub Release - **vs MediatR** nupkg.
5. ** **: `VersionPrefix` → ; CHANGELOG `[Unreleased]` ; (Discussions) — .

## / security-fix

- . [SUPPORT.md](../SUPPORT.md) [SECURITY.md](../SECURITY.md): security- — `support/1.x`, - `v1.0.<n+1>`.

## - 

- [ ] Secrets: `NUGET_API_KEY` (Settings → Secrets → Actions); environment `nuget` (Settings → Environments).
- [ ] Settings → Actions → Workflow permissions: read/write ( ) + GitHub Releases.
- [ ] Branch protection `main`: require CI-jobs + 1 approval + linear history.
- [ ] Discussions ( issue-).
