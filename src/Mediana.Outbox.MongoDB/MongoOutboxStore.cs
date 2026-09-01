using Mediana.Outbox;
using MongoDB.Driver;

namespace Mediana.Outbox.MongoDb;

/// <summary>MongoDB-провайдер outbox: lease-based relay (§9.4), фоновые индексы.</summary>
public sealed class MongoOutboxStore(IMongoDatabase database, string collectionName = "mediana_outbox") : IOutboxStore
{
    private readonly IMongoCollection<OutboxDocument> _collection = database.GetCollection<OutboxDocument>(collectionName);

    public sealed class OutboxDocument
    {
        public MongoDB.Bson.ObjectId Id { get; set; } = MongoDB.Bson.ObjectId.GenerateNewId();

        public long Sequence { get; set; }

        public Guid MessageId { get; set; }

        public string Destination { get; set; } = "";

        public string? Transport { get; set; }

        public byte[] EnvelopeBytes { get; set; } = [];

        public DateTimeOffset CreatedAt { get; set; }

        public long LeaseUntil { get; set; }

        public int DeliveryAttempts { get; set; }

        public DateTimeOffset? DeliveredAt { get; set; }

        public string? LastError { get; set; }
    }

    public async Task EnsureIndexesAsync(CancellationToken cancellationToken = default)
    {
        await _collection.Indexes.CreateOneAsync(
            new CreateIndexModel<OutboxDocument>(
                Builders<OutboxDocument>.IndexKeys
                    .Ascending(d => d.DeliveredAt)
                    .Ascending(d => d.LeaseUntil)),
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
            Builders<OutboxDocument>.Filter.Lt(d => d.LeaseUntil, now));

        var leaseUpdate = Builders<OutboxDocument>.Update
            .Set(d => d.LeaseUntil, leaseUnixMs)
            .Inc(d => d.DeliveryAttempts, 1);

        var leased = await _collection.FindOneAndUpdateAsync(
            leaseFilter,
            leaseUpdate,
            new FindOneAndUpdateOptions<OutboxDocument, OutboxDocument>
            {
                Sort = Builders<OutboxDocument>.Sort.Ascending(d => d.Sequence),
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
        await _collection.UpdateOneAsync(
            d => d.Sequence == message.Sequence,
            Builders<OutboxDocument>.Update.Set(d => d.DeliveredAt, DateTimeOffset.UtcNow),
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask MarkFailed(OutboxMessage message, string error, CancellationToken cancellationToken)
    {
        await _collection.UpdateOneAsync(
            d => d.Sequence == message.Sequence,
            Builders<OutboxDocument>.Update
                .Set(d => d.LastError, error)
                .Set(d => d.LeaseUntil, 0),
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
            Sequence = document.Sequence,
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
