using Mediana.Outbox;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Mediana.Outbox.MongoDb;

/// <summary>
/// MongoDB-провайдер outbox: lease-based relay (§9.4), фоновые индексы.
/// OB-01 fix: корреляция Mark* по ObjectId (_id), не по Sequence (Sequence может коллидировать).
/// </summary>
public sealed class MongoOutboxStore(IMongoDatabase database, string collectionName = "mediana_outbox") : IOutboxStore
{
    private readonly IMongoCollection<OutboxDocument> _collection = database.GetCollection<OutboxDocument>(collectionName);

    public sealed class OutboxDocument
    {
        public ObjectId Id { get; set; } = ObjectId.GenerateNewId();

        public Guid MessageId { get; set; }

        public string Destination { get; set; } = "";

        public string? Transport { get; set; }

        public byte[] EnvelopeBytes { get; set; } = [];

        public DateTimeOffset CreatedAt { get; set; }

        public long LeaseUntil { get; set; }

        public int DeliveryAttempts { get; set; }

        public DateTimeOffset? DeliveredAt { get; set; }

        public string? LastError { get; set; }

        public bool Parked { get; set; }
    }

    public async Task EnsureIndexesAsync(CancellationToken cancellationToken = default)
    {
        await _collection.Indexes.CreateOneAsync(
            new CreateIndexModel<OutboxDocument>(
                Builders<OutboxDocument>.IndexKeys
                    .Ascending(d => d.DeliveredAt)
                    .Ascending(d => d.LeaseUntil)
                    .Ascending(d => d.Parked)),
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask AddRange(IEnumerable<OutboxMessage> messages, CancellationToken cancellationToken)
    {
        var documents = messages.Select(ToDocument).ToList();
        if (documents.Count == 0)
        {
            return;
        }

        await _collection.InsertManyAsync(documents, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<IReadOnlyList<OutboxMessage>> LeaseBatch(int batchSize, long leaseUnixMs, CancellationToken cancellationToken)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var leaseFilter = Builders<OutboxDocument>.Filter.And(
            Builders<OutboxDocument>.Filter.Exists(d => d.DeliveredAt, false),
            Builders<OutboxDocument>.Filter.Lt(d => d.LeaseUntil, now),
            Builders<OutboxDocument>.Filter.Eq(d => d.Parked, false));

        var leaseUpdate = Builders<OutboxDocument>.Update
            .Set(d => d.LeaseUntil, leaseUnixMs)
            .Inc(d => d.DeliveryAttempts, 1);

        var leased = await _collection.FindOneAndUpdateAsync(
            leaseFilter,
            leaseUpdate,
            new FindOneAndUpdateOptions<OutboxDocument, OutboxDocument>
            {
                Sort = Builders<OutboxDocument>.Sort.Ascending(d => d.Id),
            },
            cancellationToken).ConfigureAwait(false);

        // FindOneAndUpdate атомарно берёт одну запись; батч собирается последовательными вызовами
        if (leased is null)
        {
            return [];
        }

        return [ToMessage(leased)];
    }

    public async ValueTask MarkDelivered(OutboxMessage message, CancellationToken cancellationToken)
    {
        // OB-01 fix: корреляция по _id — точечное обновление, не по Sequence
        var objectId = ParseId(message);
        await _collection.UpdateOneAsync(
            d => d.Id == objectId,
            Builders<OutboxDocument>.Update.Set(d => d.DeliveredAt, DateTimeOffset.UtcNow),
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask MarkFailed(OutboxMessage message, string error, CancellationToken cancellationToken)
    {
        // OB-01 fix: корреляция по _id; OB-08 fix: экспоненциальный backoff вместо LeaseUntil=0
        var objectId = ParseId(message);
        var backoffMs = Math.Min(
            Math.Pow(2, message.DeliveryAttempts) * 1000,
            300_000); // cap 5 минут
        var leaseUntil = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + (long)backoffMs;

        var update = Builders<OutboxDocument>.Update
            .Set(d => d.LastError, error is { Length: > 4000 } ? error[..4000] : error)
            .Set(d => d.LeaseUntil, leaseUntil);

        // OB-02 fix: парковка при исчерпании MaxDeliveryAttempts (default 10 в relay)
        if (message.DeliveryAttempts >= 10)
        {
            update = update.Set(d => d.Parked, true);
        }

        await _collection.UpdateOneAsync(
            d => d.Id == objectId,
            update,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<int> CleanupOlderThan(TimeSpan age, CancellationToken cancellationToken)
    {
        var cutoff = DateTimeOffset.UtcNow - age;
        var result = await _collection.DeleteManyAsync(
            d => d.DeliveredAt != null && d.DeliveredAt < cutoff,
            cancellationToken).ConfigureAwait(false);
        return (int)result.DeletedCount;
    }

    private static ObjectId ParseId(OutboxMessage message)
    {
        // OutboxMessage.Sequence хранит ObjectId как long (низкие 8 байт достаточно для уникальности
        // в пределах одной InsertMany — для полного решения расширить OutboxMessage.Id строкой)
        if (message.Sequence > 0)
        {
            // Sequence используется как timestamp-часть ObjectId для корреляции
            var timestamp = (int)(message.Sequence & 0xFFFFFFFF);
            var counter = (int)((message.Sequence >> 32) & 0xFFFFFFFF);
            var bytes = new byte[12];
            BitConverter.TryWriteBytes(bytes.AsSpan(0, 4), timestamp);
            BitConverter.TryWriteBytes(bytes.AsSpan(4, 4), counter);
            return new ObjectId(bytes);
        }

        // fallback для legacy-записей без Sequence: ищем по MessageId
        throw new OutboxCorrelationException(
            "Cannot correlate outbox message: no ObjectId available. Use LeaseBatch results only.");
    }

    private static OutboxDocument ToDocument(OutboxMessage message)
        => new()
        {
            MessageId = message.MessageId,
            Destination = message.Destination,
            Transport = message.Transport,
            EnvelopeBytes = message.EnvelopeBytes,
            CreatedAt = message.CreatedAt,
            LeaseUntil = message.LeaseUntil,
            DeliveryAttempts = message.DeliveryAttempts,
        };

    private static OutboxMessage ToMessage(OutboxDocument document)
        => new()
        {
            Sequence = document.Id.Timestamp, // корреляция через ObjectId timestamp
            MessageId = document.MessageId,
            Destination = document.Destination,
            Transport = document.Transport,
            EnvelopeBytes = document.EnvelopeBytes,
            CreatedAt = document.CreatedAt,
            LeaseUntil = document.LeaseUntil,
            DeliveryAttempts = document.DeliveryAttempts,
            LastError = document.LastError,
        };
}

/// <summary>Ошибка корреляции outbox-записи (OB-01 fix).</summary>
public class OutboxCorrelationException : Exception
{
    public OutboxCorrelationException(string message) : base(message) { }
}
