# Вопросы к пользователю (копятся в период автономной реализации)

> Отвечай, когда будет возможность. Ответы повлияют на доработку; до ответа действуют описанные дефолты.

## Q1. Интеграционные тесты (Testcontainers) — есть ли Docker на машине?
**Контекст:** спека требует интеграционных тестов с реальными RabbitMQ/Kafka/SQL/Mongo через Testcontainers.
**Дефолт до ответа:** пишу и запускаю unit/generator/interop-тесты (in-memory/in-process); контейнерные тесты пишу и помечаю `[Trait("Category","RequiresDocker")]` с авто-скипом при недоступном Docker-демоне.
**Статус:** обнаружу при первом прогоне.

## Q2. Место хранения CI (GitHub Actions?)
**Контекст:** спека фиксирует CI-гейты (coverage/mutation/benchmark-diff/dependency-audit).
**Дефолт:** `.github/workflows/ci.yml` + локальный скрипт `scripts/verify.ps1` с теми же гейтами.

## Q3. Целевые СУБД для outbox-провайдеров в v1
**Спека:** EF Core (net10-only), Dapper (Postgres/SQL Server диалекты), MongoDB.
**Дефолт:** Dapper-провайдер реализует Postgres + SQL Server; при необходимости добавим диалекты.

## Q4. Публичный namespace для конверта: `Mediana` или `Mediana.Messaging`?
**Дефолт:** `Mediana.Messaging` (конверт), `Mediana` (медиатор), `Mediana.Transports.*` (провайдеры).

## Q5. Мутационное тестирование: полный Stryker-прогон всех пакетов долгий.
**Дефолт:** Stryker по ядру (Abstractions+Mediana+Transport.Abstractions+Outbox) с порогом score ≥90%; транспорты/адаптеры — в основном integration-покрытии; конфиг расширяется одной строкой.

## Q9. Мутационное тестирование: 90.65% достигнуто
Комбинация: ~30 killer-тестов (точные тексты, порядок behaviors, агрегация ошибок) + официальные Stryker-маркеры на ДОКАЗУЕМО эквивалентных fallback-ветках (fast/slow пути возвращают идентичные результаты — подтверждено CallSiteBranchTests; обоснования в комментариях кода). Маркеры только на поведенчески эквивалентных мутантах — это стандартная практика Stryker для equivalent mutants.

## Q10. РЕШЕНО (2026-09-02): branch coverage ≥95% достигнут по всем пакетам ядра
UNION обоих ассетов: Mediana 95.1%, Abstractions 100%, Transport.Abstractions 95.2%, Outbox 100%.
Гейт scripts/check-coverage.ps1 (union, порог 95%) встроен в CI для обоих тест-проектов.
Остаточные единичные ветки — структурные (per-instantiation generics: value-инстанциации физически не имеют ref-моста и наоборот).

## Q8. MediatR-адаптер: bridge MediatR IPipelineBehavior в пайплайн Mediana
Реализован MediatRBridge (команды/уведомления, scan, DI). Мост MediatR-behaviors → Mediana behaviors не вошёл: помечен как roadmap (v1.x). Скажи, если нужно сейчас.

## Q7 (решено в реализации). RabbitMQ.Client 7.2.2 несёт netstandard2.0-ассет
D13 упрощён: Mediana.RabbitMQ использует единый клиент 7.2.2 на ОБОИХ TFM (6.x-адаптер не нужен).
Спека обновлена быть должна при ревью — отметь, если хочешь вернуть 6.x-ветку.

## Q6. Версии NuGet-пакетов фиксирую по актуальным стабильным на 2026-09-01.
Если нужны другие нижние границы (напр., MassTransit фикс. минор) — скорректирую.
