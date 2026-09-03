# Политика поддержки

## Поддерживаемые рантаймы

| Платформа | Статус |
|---|---|
| .NET 10 (LTS) | полный приоритет |
| .NET 8/9 (через netstandard2.1-ассет) | полная поддержка |
| netstandard2.1-хосты (Unity, legacy Core) | поддержка ядра (без транспортов, см. [D13 спеки](docs/superpowers/specs/2026-09-01-mediana-design.md)) |
| .NET Framework | не поддерживается |

## Жизненный цикл

- **Патчи** (1.0.x): багфиксы и безопасность — без breaking changes; по мере поступления, критичные — в течение дней
- **Миноры** (1.x): новые возможности без breaking changes API; обратная совместимость гарантируется контрактом-тестами (включая идентичность двух TFM-ассетов)
- **Мажоры**: breaking changes с руководством по миграции в CHANGELOG; анонс минимум за один минор до сноса API (deprecation-атрибуты)

## Что значит «поддержка»

- Реакция на issues по регламенту триажа: [docs/maintenance.md](docs/maintenance.md)
- Security-фиксы — backport в последнюю минорную ветку предыдущего мажора (см. [SECURITY.md](SECURITY.md))
- Обновление зависимостей спутниковых пакетов — через Dependabot (автоматические PR еженедельно)

## Ограничения поддержки

- Сторонние клиентские библиотеки (RabbitMQ.Client, Confluent.Kafka, MassTransit, EF Core, Dapper, MongoDB.Driver, OpenTelemetry) поддерживаем в диапазонах, указанных в [Directory.Packages.props](Directory.Packages.props); их собственные ETL-циклы — вне нашего контроля
- Экспериментальные ветки/пре-релизы — best effort
