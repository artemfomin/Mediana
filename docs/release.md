# Регламент релизов

## Модель

- Версионирование — SemVer 2.0, источник истины — **git-тег** `v<version>` (например `v1.0.0`, prerelease: `v1.1.0-beta.1`).
- `VersionPrefix` в `Directory.Build.props` = ожидаемая следующая версия; тег **переопределяет** (`-p:Version=<tag>`).
- Публикация полностью автоматизирована: [.github/workflows/release.yml](../.github/workflows/release.yml).

## Поток релиза

1. **Подготовка** (в ветке feature или main):
   - все гейты CI зелёные (build, тесты обоих ассетов, coverage ≥95%, Stryker ≥90%, alloc-gate, D14-аудит);
   - [CHANGELOG.md](../CHANGELOG.md) — заполнена секция `[<версия>]` (Added/Fixed/Changed + перф-цифры, если менялись);
   - если менялся диспетч — прогнаны `ram-check`/`load-check`, результаты в `benchmarks/RESULTS.md`.
2. **Merge в main** — через PR (обязательные проверки CI, мин. 1 approve).
3. **Тег**: `git tag v1.X.Y && git push origin v1.X.Y`.
4. **Пайплайн release.yml** (автоматически):
   - `verify` — все гейты повторно на теге;
   - `pack` — 13 пакетов с версией тега (+ snupkg), **проверка метаданных** (license/icon/readme зашиты — упакpause при расхождении);
   - `publish-nuget` — push на nuget.org (нужен secret `NUGET_API_KEY`; без ключа — warning + пакеты в артефакте, релиз продолжается);
   - `github-release` — GitHub Release с телом-сводкой **vs MediatR** и вложенными nupkg.
5. **После релиза**: `VersionPrefix` → следующая ожидаемая версия; секция CHANGELOG `[Unreleased]` заново наверх; анонс (Discussions) — опционально.

## Прекращение поддержки / security-fix

- См. [SUPPORT.md](../SUPPORT.md) и [SECURITY.md](../SECURITY.md): security-фиксы бэкпортируются в последнюю минорную ветку предыдущего мажора — для неё создаётся ветка `support/1.x`, фикс-тег `v1.0.<n+1>`.

## Чек-лист первого релиза в новом репозитории

- [ ] Secrets: `NUGET_API_KEY` (Settings → Secrets → Actions); environment `nuget` создан (Settings → Environments).
- [ ] Settings → Actions → Workflow permissions: read/write (для создания релизов) + разрешить GitHub Releases.
- [ ] Branch protection `main`: require CI-jobs + 1 approval + linear history.
- [ ] Discussions включены (ссылка из issue-шаблона).
