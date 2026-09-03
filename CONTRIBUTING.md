# Участие в Mediana

Спасибо за интерес к проекту! Перед вкладом прочитайте [README](README.md) (особенно раздел сравнения — чтобы понимать north star проекта) и этот файл.

## North star проекта

**Высочайший уровень алгоритмической оптимизации** и минимум сторонних (не-Microsoft) зависимостей в ядре. Любой PR, добавляющий аллокации на горячий путь или стороннюю зависимость в ядро, будет отклонён, если выигрыш не обоснован измерениями. Аналогично: вторая метрика — [аудит зависимостей в CI](.github/workflows/ci.yml) не должен падать.

## Окружение

- .NET SDK 10.0.302+ (`global.json` фиксирует)
- Для интеграционных тестов: Docker (Testcontainers) — без Docker они автоматически пропускаются
- Для NativeAOT-сборки локально: VS C++ workload (в CI есть)

## Локальная проверка перед PR (все гейты CI)

```bash
dotnet build -c Release                    # 0 ошибок, 0 предупреждений (warnings as errors)
dotnet test tests/Mediana.UnitTests        # ядро + аллокационные бюджеты
dotnet test tests/Mediana.UnitTests.Ns21   # те же тесты против ns2.1-ассета
dotnet test tests/Mediana.GeneratorTests   # source generator
dotnet test tests/Mediana.ContractTests.Ns21 # идентичность API двух TFM
dotnet test tests/Mediana.InteropTests     # MediatR-мост + телеметрия

# Гейты
powershell -File scripts/check-coverage.ps1        # union branch coverage >= 95%
dotnet tool restore
dotnet tool run dotnet-stryker                     # mutation score >= 90%
dotnet run --project benchmarks/Mediana.Benchmarks -c Release -- alloc-check  # 0 B/вызов

# Бенчмарки (если трогали диспетч)
dotnet run --project benchmarks/Mediana.Benchmarks -c Release -- load-check all
dotnet run --project benchmarks/Mediana.Benchmarks -c Release -- ram-check all
```

Результаты бенчмарков фиксируйте в `benchmarks/RESULTS.md` (диапазоны нескольких прогонов + методика).

## Правила кода

- C# latest, `Nullable` + `TreatWarningsAsErrors` — включены глобально
- Горячий путь диспетча: без аллокаций; внимательно к canon-generic контекстам (см. `RequestCallSiteCompositor` и тесты `AllocationBisectTests`) — invoke generic-делегата из canon-контекста аллоцирует ~24–32 Б
- Публичный API ядра — без `required`-членов (ns2.1), без рефлексии на горячем пути; рефлексивные пути помечать `[RequiresDynamicCode]`/`[RequiresUnreferencedCode]`
- Эквивалентные мутанты помечаются официальными `// Stryker disable` с обоснованием (см. существующие примеры)
- Тесты: behavioral-тесты для логики, аллокационные бюджеты для перфа; новые ветки ядра закрывайте тестами до 95% union

## Коммиты и PR

- Формат коммитов: `type(scope): message` — типы `feat|fix|perf|refactor|test|docs|chore|bench`; breaking changes — `!` и раздел BREAKING CHANGE в теле
- Один PR — одна логическая правка; тесты обязательны
- PR-шаблон: [.github/PULL_REQUEST_TEMPLATE.md](.github/PULL_REQUEST_TEMPLATE.md)
- Обновление `CHANGELOG.md` — для пользовательских изменений

## Репорты о производительности

Если вы нашли регрессию: приложите вывод `alloc-check`/`load-check`/`ram-check` + `.NET`-версию и конфигурацию машины. Измерения без методики не принимаются — методики в [benchmarks/RESULTS.md](benchmarks/RESULTS.md).
