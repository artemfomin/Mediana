using System.Data.Common;
using Dapper;
using Mediana.Outbox;

namespace Mediana.Outbox.Dapper;

/// <summary>
/// Dapper-провайдер outbox: ADO-агностичен (пользователь поставляет фабрику DbConnection:
/// Npgsql/SqlConnection/...) — ноль ADO-зависимостей (D14). Диалекты: Postgres/SqlServer.
/// Конкурентные relay: FOR UPDATE SKIP LOCKED (Postgres) / READPAST (SqlServer).
/// </summary>
public sealed class DapperOutboxStore : IOutboxStore
{
    public enum SqlDialect
    {
        Postgres,
        SqlServer,
    }

    private readonly Func<DbConnection> _connectionFactory;
    private readonly SqlDialect _dialect;

    public DapperOutboxStore(Func<DbConnection> connectionFactory, SqlDialect dialect = SqlDialect.Postgres)
    {
        _connectionFactory = connectionFactory;
        _dialect = dialect;
    }

    /// <summary>DDL создания таблицы outbox (миграции запускает приложение).</summary>
    public string GetCreateTableSql(string table = "mediana_outbox")
    {
        return _dialect == SqlDialect.Postgres
            ? $"""
               CREATE TABLE IF NOT EXISTS {table} (
                   sequence BIGSERIAL PRIMARY KEY,
                   message_id UUID NOT NULL,
                   destination TEXT NOT NULL,
                   transport TEXT,
                   envelope_bytes BYTEA NOT NULL,
                   created_at TIMESTAMPTZ NOT NULL,
                   lease_until BIGINT NOT NULL DEFAULT 0,
                   delivery_attempts INT NOT NULL DEFAULT 0,
                   delivered_at TIMESTAMPTZ,
                   last_error TEXT,
                   parked BOOLEAN NOT NULL DEFAULT FALSE
               );
               CREATE INDEX IF NOT EXISTS idx_{table}_lease ON {table} (lease_until) WHERE delivered_at IS NULL AND parked = FALSE;
               """
            : $"""
               IF OBJECT_ID(N'{table}', N'U') IS NULL
               CREATE TABLE {table} (
                   sequence BIGINT IDENTITY PRIMARY KEY,
                   message_id UNIQUEIDENTIFIER NOT NULL,
                   destination NVARCHAR(400) NOT NULL,
                   transport NVARCHAR(100),
                   envelope_bytes VARBINARY(MAX) NOT NULL,
                   created_at DATETIMEOFFSET NOT NULL,
                   lease_until BIGINT NOT NULL DEFAULT 0,
                   delivery_attempts INT NOT NULL DEFAULT 0,
                   delivered_at DATETIMEOFFSET NULL,
                   last_error NVARCHAR(4000),
                   parked BIT NOT NULL DEFAULT 0
               );
               """;
    }

    public async ValueTask AddRange(IEnumerable<OutboxMessage> messages, CancellationToken cancellationToken)
    {
        await using var connection = _connectionFactory();
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

        foreach (var message in messages)
        {
            await connection.ExecuteAsync(
                new CommandDefinition(
                    "INSERT INTO mediana_outbox (message_id, destination, transport, envelope_bytes, created_at, lease_until, delivery_attempts) " +
                    "VALUES (@MessageId, @Destination, @Transport, @EnvelopeBytes, @CreatedAt, @LeaseUntil, @DeliveryAttempts)",
                    message,
                    transaction: transaction,
                    cancellationToken: cancellationToken)).ConfigureAwait(false);
        }

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<IReadOnlyList<OutboxMessage>> LeaseBatch(int batchSize, long leaseUnixMs, CancellationToken cancellationToken)
    {
        await using var connection = _connectionFactory();
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

        var lockingSelect = _dialect == SqlDialect.Postgres
            ? "SELECT * FROM mediana_outbox WHERE delivered_at IS NULL AND parked = FALSE AND lease_until < @now FOR UPDATE SKIP LOCKED LIMIT @batch"
            : "SELECT TOP (@batch) * FROM mediana_outbox WITH (READPAST, UPDLOCK) WHERE delivered_at IS NULL AND parked = 0 AND lease_until < @now";

        var rows = (await connection.QueryAsync<OutboxRow>(
            new CommandDefinition(
                lockingSelect,
                new { now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), batch = batchSize },
                transaction: transaction,
                cancellationToken: cancellationToken)).ConfigureAwait(false)).ToList();

        if (rows.Count > 0)
        {
            var ids = string.Join(",", rows.Select(r => r.sequence));
            await connection.ExecuteAsync(
                new CommandDefinition(
                    $"UPDATE mediana_outbox SET lease_until = @lease WHERE sequence IN ({ids})",
                    new { lease = leaseUnixMs },
                    transaction: transaction,
                    cancellationToken: cancellationToken)).ConfigureAwait(false);
        }

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return rows.Select(ToMessage).ToList();
    }

    public async ValueTask MarkDelivered(OutboxMessage message, CancellationToken cancellationToken)
    {
        await using var connection = _connectionFactory();
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await connection.ExecuteAsync(
            new CommandDefinition(
                "UPDATE mediana_outbox SET delivered_at = @now WHERE sequence = @sequence",
                new { now = DateTimeOffset.UtcNow, sequence = message.Sequence },
                cancellationToken: cancellationToken)).ConfigureAwait(false);
    }

    public async ValueTask MarkFailed(OutboxMessage message, string error, CancellationToken cancellationToken)
    {
        // OB-08 fix: экспоненциальный backoff вместо lease_until=0 (tight loop fix)
        // OB-02 fix: парковка при исчерпании MaxDeliveryAttempts (default 10)
        var truncatedError = error is { Length: > 4000 } ? error[..4000] : error;
        var backoffMs = Math.Min(Math.Pow(2, message.DeliveryAttempts) * 1000, 300_000);
        var leaseUntil = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + (long)backoffMs;
        var parked = message.DeliveryAttempts >= 10;

        await using var connection = _connectionFactory();
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await connection.ExecuteAsync(
            new CommandDefinition(
                "UPDATE mediana_outbox SET delivery_attempts = delivery_attempts + 1, " +
                "last_error = @truncatedError, lease_until = @leaseUntil, " +
                "parked = @parked " +
                "WHERE sequence = @sequence",
                new { truncatedError, leaseUntil, parked, sequence = message.Sequence },
                cancellationToken: cancellationToken)).ConfigureAwait(false);
    }

    public async ValueTask<int> CleanupOlderThan(TimeSpan age, CancellationToken cancellationToken)
    {
        await using var connection = _connectionFactory();
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        return await connection.ExecuteAsync(
            new CommandDefinition(
                "DELETE FROM mediana_outbox WHERE delivered_at IS NOT NULL AND delivered_at < @cutoff",
                new { cutoff = DateTimeOffset.UtcNow - age },
                cancellationToken: cancellationToken)).ConfigureAwait(false);
    }

    private static OutboxMessage ToMessage(OutboxRow row)
        => new()
        {
            Sequence = row.sequence,
            MessageId = row.message_id,
            Destination = row.destination,
            Transport = row.transport,
            EnvelopeBytes = row.envelope_bytes,
            CreatedAt = row.created_at,
            LeaseUntil = row.lease_until,
            DeliveryAttempts = row.delivery_attempts,
            LastError = row.last_error,
        };

    private sealed class OutboxRow
    {
        public long sequence { get; set; }
        public Guid message_id { get; set; }
        public string destination { get; set; } = "";
        public string? transport { get; set; }
        public byte[] envelope_bytes { get; set; } = [];
        public DateTimeOffset created_at { get; set; }
        public long lease_until { get; set; }
        public int delivery_attempts { get; set; }
        public string? last_error { get; set; }
        public bool parked { get; set; }
    }
}
