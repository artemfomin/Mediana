# Mediana - Security Audit: Persistence / Outbox

Scope: src/Mediana.Outbox, src/Mediana.Outbox.Dapper, src/Mediana.Outbox.EFCore, src/Mediana.Outbox.MongoDB (with behaviour evidence in tests/Mediana.UnitTests/OutboxTests.cs). Read-only audit against HEAD 5be92c5.

---

## Findings

### [High] MongoDB relay never allocates Sequence - MarkDelivered/MarkFailed target the wrong document; infinite redelivery + silent message loss

**Location**: src/Mediana.Outbox.MongoDB/MongoOutboxStore.cs:113-123 (ToDocument), :57-84 (LeaseBatch), :86-102 (MarkDelivered/MarkFailed).

**Description**: OutboxDocument.Sequence (line 15) is a long initialised to 0. ToDocument (:113-123) never assigns Sequence, and there is no counter collection, no findAndModify on a sequence document, and no post-insert allocation. All persisted documents therefore have Sequence == 0. LeaseBatch (:57-84) atomically leases exactly one document via FindOneAndUpdateAsync and returns [ToMessage(leased)] (verified: at most one doc per call, :78-83). The returned OutboxMessage.Sequence == 0. MarkDelivered (:88-91) and MarkFailed (:96-101) issue UpdateOneAsync(d => d.Sequence == message.Sequence, ...), i.e. d.Sequence == 0. Because every doc has Sequence == 0, the driver picks an arbitrary matching doc, which is almost never the leased one when the backlog has 2+ pending rows.

**Exploit / impact**: 
1. Backlog > 1: LeaseBatch leases doc A (LeaseUntil > now, DeliveryAttempts++). Relay publishes A successfully. MarkDelivered(A) updates some other doc B (still pending, LeaseUntil == 0) - B is silently marked DeliveredAt and dropped without ever being published (data loss). A is never marked; after LeaseDuration (2 min default) it is re-leased and re-published - duplicate delivery indefinitely.
2. MarkFailed on a poisoned A resets LeaseUntil = 0 on an arbitrary pending doc, taking it out of the leased state prematurely; two relays can concurrently work on the same doc - double delivery.
3. CleanupOlderThan would delete the falsely-marked B rows and preserve poisoned A rows forever.

Deterministic once 2+ pending rows exist; not mitigated by MaxDeliveryAttempts (dead code - OB-02). Impact: broker flood, message loss, unbounded log growth, DoS via duplicates.

**Recommendation**: Allocate Sequence before insert - either drop Sequence and use ObjectId Id as the correlation key (MarkDelivered/MarkFailed filter by Id), or implement a mediana_outbox_counters document with FindOneAndUpdate($inc:{seq:1}) and set the value in ToDocument. Add a unique index on Sequence (or Id) to fail-fast. Add a regression test with 2+ pending docs (current tests use a single doc, hiding the bug).

---

### [High] MaxDeliveryAttempts option is dead code - poison messages retried forever (DoS: log/broker/store flood)

**Location**: src/Mediana.Outbox/OutboxRelay.cs:104 (option), :126-171 (loop), :174-189 (Deliver). Verified by grep across repo: only the declaration site references it.

**Description**: OutboxRelayOptions.MaxDeliveryAttempts (default 10) is never read. Deliver catches every exception and calls store.MarkFailed(message, ex.Message, ...), which (Dapper/EF) increments delivery_attempts and (all stores) resets lease_until = 0. On the next poll (PollInterval = 1 s by default) the same row is immediately re-leased and re-published. A single poison message causes broker RPS proportional to 1 / PollInterval, unbounded log growth (broker/publisher failure logs plus MarkFailed persisting ex.Message each iteration), and store row-lock churn on a hot lease-update path.

**Exploit**: Attacker who injects one malformed envelope (compromised producer, replay, corrupted BLOB) causes the relay to hammer the broker forever. Combined with OB-01, retries land on random docs.

**Recommendation**: In Deliver, after MarkFailed inspect the current DeliveryAttempts; when >= MaxDeliveryAttempts, either park by setting LeaseUntil = long.MaxValue and LastError = "parked", or move to a mediana_outbox_parked table/collection. Introduce per-message failure backoff LeaseUntil = now + min(2^attempt * FailureBackoff, 1h) so non-parked failures stop hot-looping. Extend MarkFailed signature to accept the new lease/park state consistently across providers.

---

### [Medium] MarkFailed(ex.Message) persists unbounded, potentially credential-bearing driver exceptions with no size cap and no scrubbing

**Location**: src/Mediana.Outbox/OutboxRelay.cs:185-188; Dapper columns DapperOutboxStore.cs:44 (last_error TEXT) and :60 (NVARCHAR(MAX)); EF property EfOutboxStore.cs:29 - no HasMaxLength in the config (:35-46), so mapped to text / nvarchar(max); Mongo OutboxDocument.LastError (MongoOutboxStore.cs:33) - arbitrary BSON string subject to the 16 MB doc limit.

**Description**: The relay stores ex.Message verbatim into last_error. Driver/broker exception messages routinely contain sensitive metadata: Npgsql PostgresException.MessageText can include host, database, SQL fragments and query parameters (honours Include Error Detail); Confluent.Kafka.ProduceException.Message includes broker hostnames and, on SASL failures, SASL mechanism/user; RabbitMQ.Client exceptions can carry vhost/user identifiers; SqlException.Message includes server name and login username. Nothing is redacted and there is no maximum length - a pathological driver exception (or an attacker who can influence the payload -> deserialization exception echoing part of the payload) can grow the row unbounded, DoS-ing storage. OutboxMessage.LastError is a public init property (OutboxRelay.cs:29) rehydrated by all three stores; any diagnostic/admin surface reading it re-exposes the raw driver message.

**Exploit**: Ops dashboards / operators querying mediana_outbox for troubleshooting inadvertently disclose host/user/topology metadata. In shared-hosting scenarios (multi-tenant DB, ops from a different security zone) this crosses trust boundaries. Second-order: a malicious envelope crafted so its serialization error contains a large blob inflates last_error on every retry (interacts with OB-02) until table bloat triggers vacuum stalls or Mongo 16 MB doc rejection (which then throws inside MarkFailed, feeding the outer catch and looping again).

**Recommendation**: Truncate at the store level (ex.Message.Substring(0, min(len, 4000))) and set HasMaxLength(4000) in EF configuration and NVARCHAR(4000) in DDL. Store ex.GetType().FullName plus a short truncated message rather than raw driver text; keep detailed text only in structured logs behind an opt-in LogFailureDetails flag. Document that LastError must be treated as sensitive.

---

### [Medium] Relay Deliver swallows OperationCanceledException and records shutdown as a delivery failure (spurious MarkFailed, inflated attempts)

**Location**: src/Mediana.Outbox/OutboxRelay.cs:174-189.

**Description**: The catch (Exception ex) at :185 also catches OperationCanceledException raised by publisher.Publish(..., stoppingToken) during host shutdown. It then calls store.MarkFailed(message, ex.Message, cancellationToken) with the already-cancelled token, so the store update throws (Dapper: OpenAsync(cancellationToken)) - the throw escapes Deliver (no local catch), bubbles through the foreach in ExecuteAsync (:145-148), and is caught by the cycle-level handler (:156) which logs "Outbox relay cycle failed" and backs off. Net effect: attempt counter bumped for a message that was never actually rejected by the broker; across restarts, poison thresholds trip prematurely.

**Recommendation**: Rethrow OCE in Deliver (if (ex is OperationCanceledException && cancellationToken.IsCancellationRequested) throw;) before MarkFailed. Wrap MarkFailed in a fallback catch so cancellation cannot leak from the cleanup path.

---

### [Medium] CleanupAge option is not wired into the relay loop - all delivered rows retained forever (silent storage growth / GDPR retention drift)

**Location**: src/Mediana.Outbox/OutboxRelay.cs:109-110 (option), ExecuteAsync :126-171 (never calls store.CleanupOlderThan). Verified by grep: three CleanupOlderThan implementations and one interface method; no caller in the repository.

**Description**: OutboxRelayOptions.CleanupAge defaults to 7 days and is documented as the retention window, but the relay loop never invokes store.CleanupOlderThan. Delivered rows accumulate indefinitely (each includes full envelope, payload bytes, headers). For a project claiming GDPR compatibility this becomes a right-to-erasure issue; for SqlServer/Postgres providers the table grows without bound. Users reading the config assume retention is enforced.

**Recommendation**: Add a low-frequency cleanup pass to ExecuteAsync (invoke CleanupOlderThan(CleanupAge.Value, ...) every N minutes when CleanupAge is not null). Log the number of deleted rows. Document retention semantics per provider.

---

### [Medium] DapperOutboxStore.GetCreateTableSql(string table) interpolates the table name into DDL - public API accepts caller-supplied string with no validation

**Location**: src/Mediana.Outbox.Dapper/DapperOutboxStore.cs:30-63.

**Description**: `table` is interpolated raw into both branches (CREATE TABLE {table}, CREATE INDEX idx_{table}_lease, IF OBJECT_ID(N'{table}', ...)). The method is public and its return value is intended for execution as SQL. If a caller passes a value derived from configuration, environment or a multi-tenant identifier without an allowlist (not documented as required), an attacker controlling that identifier obtains SQL execution during migration (e.g., "x; DROP TABLE users; --" on Postgres, "foo]) DROP TABLE users --" on SQL Server). Identifiers cannot be parameterised, and the API also offers no quoting. Notably, the runtime queries in AddRange/LeaseBatch/MarkDelivered/MarkFailed/CleanupOlderThan hardcode mediana_outbox, so a custom name passed to GetCreateTableSql is silently ignored at runtime - confirming the API only makes sense when `table` is a trusted constant.

**Recommendation**: Drop the `table` parameter (runtime assumes the default), or validate against ^[A-Za-z_][A-Za-z0-9_]{0,62}$, apply provider-appropriate quoting (double-quotes for Postgres, square brackets for SqlServer) and propagate the chosen name into all runtime SQL. Document that the returned SQL must only be executed by a trusted migration path.

---

### [Medium] EFCore OutboxSaveChangesInterceptor.TakePending() is called during SavingChanges - pending list drained even if the outer SaveChanges throws or is retried; silent outbox loss

**Location**: src/Mediana.Outbox.EFCore/EfOutboxStore.cs:53-103, calling OutboxCollector.TakePending() at OutboxRelay.cs:69-74.

**Description**: Both SavingChanges and SavingChangesAsync invoke collector.TakePending(), which clears the collector's internal _pending list before returning it. Entries are then Add-ed to the DbContext. If SaveChanges subsequently fails (concurrency violation, DB unavailable, ExecuteUpdateAsync conflict on the same transaction, etc.), the pending outbox entries are already gone from the collector. A retry loop that calls SaveChanges again sees an empty collector and commits domain changes without outbox rows - messages silently dropped, atomicity guarantee advertised in section 9.4 broken.

Additional edge cases:
- SavedChangesFailed/SavingChangesFailedAsync are not overridden - no compensation.
- If two DbContexts share the scope-lifetime collector, the second SaveChanges sees an empty pending list - outbox messages destined for the second context's SaveChanges are attached to the first.
- EnvelopeBytes = message.EnvelopeBytes shares the byte[] reference - if a caller mutates it after collector.Add, the persisted row silently changes (defence-in-depth concern only).

**Recommendation**: Do not drain the collector until after SaveChanges succeeds. Snapshot on SavingChanges (leave _pending untouched), clear on SavedChanges, restore on SaveChangesFailed. Implement OutboxCollector.BeginFlush() returning IDisposable to make this explicit. Add a regression test: SaveChanges throwing after interceptor runs must not lose pending envelopes.

---

### [Low] Per-message MarkFailed immediately resets LeaseUntil = 0 - tight busy loop when broker is degraded (no per-message backoff)

**Location**: Dapper DapperOutboxStore.cs:134, EF EfOutboxStore.cs:161, Mongo MongoOutboxStore.cs:100. Relay loop OutboxRelay.cs:126-171.

**Description**: Cycle-level FailureBackoff (:150,:168) only fires when a call throws out of LeaseBatch / publisherFactory. When individual Deliver calls fail (broker rejects publish), MarkFailed returns cleanly, the outer try succeeds, backoff is reset, and the next iteration immediately re-leases the same rows (lease_until = 0). With a 1 s PollInterval and 100-row BatchSize, a broker outage causes ~100 rps of failed publishes per relay instance - amplified by the number of instances.

**Recommendation**: In MarkFailed, set LeaseUntil = now + exponentialBackoff(DeliveryAttempts) bounded to e.g. 5 min. Applies to all three providers.

---

### [Low] OutboxMessage.EnvelopeBytes has no size cap - a single large payload can blow past MongoDB's 16 MB doc limit / bloat SQL rows

**Location**: OutboxRelay.cs:20 (byte[] EnvelopeBytes), MongoOutboxStore.cs:23,54 (InsertManyAsync), DapperOutboxStore.cs:39,55 (BYTEA / VARBINARY(MAX)), EF :19,:43 (no HasMaxLength).

**Description**: OutboxCollector.Add accepts any envelope and serialises with default STJ (no MaxDepth, no size limit). Mongo throws at 16 MB but the exception propagates unwrapped from AddRange; because AddRange is called from the EF interceptor path or explicit user code, this can abort the business SaveChanges in a hard-to-diagnose way. SQL providers accept multi-GB rows and silently degrade throughput. No validation, no metric, no logging.

**Recommendation**: Enforce a configurable MaxEnvelopeBytes (e.g., 1 MB default) in OutboxCollector.Add; throw a typed OutboxPayloadTooLargeException with MessageId; expose a counter.

---

### [Info] Multi-tenancy: single relay processes all rows in the store; no tenant column and no destination allowlist

**Location**: OutboxRelay.cs:126-171; DDL/schemas across all three providers.

**Description**: The store has no tenant discriminator. When a single database is shared across tenants, each relay instance leases every eligible row and publishes to its configured transport regardless of which tenant enqueued it. If tenant A's relay is bootstrapped against tenant B's DB (misconfiguration) or tenants share a DB deliberately, cross-tenant delivery is trivial. Destination and Transport columns exist but are chosen by the producer, not validated by the relay.

**Recommendation** (design/docs): document the single-tenant assumption explicitly; provide an OutboxRelayOptions.DestinationFilter predicate (Func<OutboxMessage,bool>) and an example of a tenant-scoped IOutboxStore decorator.

---

### [Info] Cycle exception LogError includes the raw exception - driver connection strings/credentials in stack traces reach the log pipeline

**Location**: OutboxRelay.cs:158.

**Description**: logger?.LogError(ex, "Outbox relay cycle failed; backing off {Backoff}", backoff) writes ex.ToString() including inner exceptions. Npgsql PostgresException.ToString() and RabbitMQ.Client BrokerUnreachableException.ToString() can carry endpoint/user data. Combined with tenant sharing, a log aggregator with lower privileges than the DB layer sees this.

**Recommendation**: Log ex.GetType().FullName, ex.Message truncated and a stable failure code; keep the full stack behind Debug.

---

### [Info] EF FromSqlRaw is Postgres-specific (LIMIT, FOR UPDATE SKIP LOCKED) but not gated on provider

**Location**: EfOutboxStore.cs:129-131.

**Description**: Not a security bug - parameters {0}/{1} are properly parameterised by EF Core (positional parameters converted to DbParameter instances, no string.Format). Noted for completeness: SqlServer users of the EF package silently fail at first lease (SqlException), which fits the pattern above (cycle-level catch, unbounded retry, no visibility).

---

### [Info] Dapper LeaseBatch IN ({ids}) string-join uses strongly-typed longs - safe (checked, not a finding)

**Location**: DapperOutboxStore.cs:104.

`rows` is List<OutboxRow> where OutboxRow.sequence is long (:166). long.ToString() is culture-invariant for decimal digits and cannot produce SQL metacharacters. No injection path.

---

### [Info] Dapper AddRange opens its own transaction - atomicity with domain writes requires the app to enrol the same connection

**Location**: DapperOutboxStore.cs:65-83.

AddRange calls _connectionFactory() and BeginTransactionAsync locally. It is not invoked by an EF-style interceptor for Dapper; callers must ensure the outbox INSERT runs on the same connection/transaction as their domain writes (e.g., wrap both in TransactionScope or share a connection). Design/documentation gap - the README/spec claims atomic outbox but the Dapper provider does not enforce or verify it.

---

## Summary

| ID | Severity | Title |
|----|----------|-------|
| OB-01 | High | MongoDB Sequence never allocated - Mark* target wrong doc; loss + infinite redelivery |
| OB-02 | High | MaxDeliveryAttempts never enforced - poison messages retried forever (DoS) |
| OB-03 | Medium | MarkFailed(ex.Message) persists unbounded, credential-bearing driver text |
| OB-04 | Medium | Deliver catches OCE and records shutdown as delivery failure |
| OB-05 | Medium | CleanupAge never invoked by the relay - retention not enforced |
| OB-06 | Medium | Dapper GetCreateTableSql(table) interpolates identifier into DDL (public API) |
| OB-07 | Medium | EF interceptor drains collector before SaveChanges succeeds - outbox loss on retry |
| OB-08 | Low | Per-message MarkFailed resets lease to 0 - tight loop on broker outage |
| OB-09 | Low | EnvelopeBytes unbounded; Mongo 16 MB / SQL row bloat unmanaged |
| OB-10 | Info | No tenant discriminator; single-tenant assumption undocumented |
| OB-11 | Info | Cycle LogError logs raw driver exceptions (credential leakage risk) |
| OB-12 | Info | EF FromSqlRaw is Postgres-only; silent failure on SqlServer |
| OB-13 | Info | Dapper LeaseBatch IN ({ids}) join uses longs - safe (verified) |
| OB-14 | Info | Dapper AddRange opens local transaction - atomicity relies on caller |

## Checked & OK

- **SQL injection via LeaseBatch IN ({ids})** (DapperOutboxStore.cs:104): `sequence` is a strongly-typed long on OutboxRow (:166); the string.Join cannot inject characters. Safe.
- **EF FromSqlRaw parameterisation** (EfOutboxStore.cs:129-131): {0}/{1} are positional parameters converted to DbParameter instances by EF Core (not string.Format); values are long and int. Safe.
- **Dialect detection**: chosen by the developer via DapperOutboxStore ctor argument (.cs:23), not derived from connection string, headers or config - no dialect-confusion attack surface.
- **MongoDB query construction**: exclusively via typed Builders<OutboxDocument> predicates (:60-66,:88-91,:96-101,:107-109); no BsonDocument.Parse on user input, no $where, no Regex from headers/payload. Safe.
- **Envelope header injection into queries**: headers are only persisted as part of the STJ-encoded EnvelopeBytes blob; no code path queries against header content in Dapper/EF/Mongo. Safe.
- **CleanupOlderThan scope** (all three providers): filters strictly on DeliveredAt IS NOT NULL AND DeliveredAt < cutoff (DapperOutboxStore.cs:145, EfOutboxStore.cs:169, MongoOutboxStore.cs:107-109). Undelivered rows cannot be deleted. Safe (though never invoked - see OB-05).
- **Lease timezone**: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() used throughout (OutboxRelay.cs:135, DapperOutboxStore.cs:98, EfOutboxStore.cs:127, MongoOutboxStore.cs:59); stored as BIGINT unix-ms; timezone-agnostic comparisons. Safe.
- **Postgres SKIP LOCKED / SqlServer READPAST + UPDLOCK**: correct row-lock semantics for competing relays (DapperOutboxStore.cs:91-93, EfOutboxStore.cs:130). Once OB-01 is fixed, at-least-once with no duplicate leasing.
- **EF interceptor transaction boundary**: entries Add-ed inside SavingChanges(Async) participate in the same SaveChanges command batch and therefore the same transaction as domain writes (:53-103 uses context.Set<OutboxEntry>().Add, not a separate connection). Atomic when SaveChanges succeeds; the retry/failure edge case is OB-07.
- **Relay BackgroundService lifecycle**: outer try/catch(Exception) prevents silent death (:156-169); OperationCanceledException on stoppingToken is honoured (:152-155,:163-165). Does not crash the host.
- **No secrets/connection strings in source**: verified - the Outbox packages read no environment variables, no IConfiguration, and hold no credentials.
- **Payload bytes not logged**: no logging of EnvelopeBytes/payload in any of the three providers or the relay (verified by inspection). No inadvertent PII in structured log fields.
