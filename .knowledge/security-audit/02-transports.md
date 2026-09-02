# Mediana Security Audit — Transports & Messaging

Scope: `src/Mediana.Transport.Abstractions`, `src/Mediana.Kafka`, `src/Mediana.RabbitMQ`, `src/Mediana.MassTransit`.
Threat model: broker and producers on it are untrusted (multi-tenant / compromised peer / injected messages). HEAD `5be92c5`, 2026-09-02, read-only audit.

---

## T-01 [Critical] Poison envelope in Kafka delivery constructor silently kills the poll loop (consumer stops)

**Location:** `src/Mediana.Kafka/KafkaTransport.cs:135-150` (PollLoop), `:179-182` (KafkaDelivery ctor field initializer), `:204-214` (EnvelopeCodec.Decode).

**Description.** `KafkaDelivery.Envelope` is initialized with `EnvelopeCodec.Decode(result.Message.Value)` — eager JSON deserialization runs inside `new KafkaDelivery(_consumer, result)` at `KafkaTransport.cs:142`, *before* the delivery is handed to `ConsumerPipeline`. `PollLoop`'s try/catch (`:139-149`) only catches `OperationCanceledException`. Any other exception thrown by `Decode` (JsonException, ArgumentException, OutOfMemoryException, StackOverflowException on deep nesting) propagates out of the `while` loop, silently terminating the poll `Task` started on `:131`. `Start()` returned `Task.CompletedTask` on `:132`, so no observer sees the crash. On subsequent `Stop()` the exception surfaces via `await _pollLoop` (`:159`), but during runtime the consumer is dead — the process appears healthy while consuming nothing.

**Exploit / impact.** Any attacker with publish access to a topic Mediana is subscribed to can send a single malformed message (invalid UTF-8, truncated JSON, or crafted JSON that triggers exceptions in STJ) and take the consumer offline. `ConsumerPipeline` never runs, so the message is never n-acked / DLQ-ed and the offset is never committed — on process restart the same poison message will kill the new consumer immediately. **Result: permanent denial-of-service of the Kafka consumer via one message.**

**Recommendation.**
- Move `EnvelopeCodec.Decode` out of the delivery constructor; decode inside a try/catch owned by the poll loop, and route decode failures to a poison handler (produce to `<topic>.dlq` with the raw payload + reason header, then commit).
- Broaden `PollLoop` catch: on any non-cancellation exception, log, produce to DLQ, `Commit(result)`, and continue. Never let the loop exit unless canceled.
- Add a health probe that surfaces the loop's `Task.Status` so a silent crash is observable.

---

## T-02 [Critical] Poison envelope in RabbitMQ delivery constructor causes infinite unacked-message poison loop (bypasses DLQ)

**Location:** `src/Mediana.RabbitMQ/RabbitMqConsumer.cs:50-62` (OnReceived), `:96-99` (RabbitMqDelivery field initializer), `src/Mediana.RabbitMQ/RabbitMqTransport.cs:249-259` (EnvelopeCodec.Decode).

**Description.** `RabbitMqDelivery.Envelope` is initialized via `EnvelopeCodec.Decode(args.Body.ToArray())` in the delivery ctor (`:99`). `OnReceived` wraps only `_handlerLimiter.Release()` in `finally`; there is **no catch**. When Decode throws, the exception bubbles out of the `ReceivedAsync` event handler; RabbitMQ.Client 7.x logs it via its internal logger and drops it, but the message is never acked/nacked (`autoAck: false` on `:45`). The delivery stays "unacked" until the channel is closed, at which point the broker **requeues** it. The next redelivery hits the same code path → same crash → same requeue. There is no attempt counter on the transport path (retry-count is only added on the explicit `Nack` DLX-cycle path, `:113-136`).

**Exploit / impact.** A single malformed AMQP message causes:
1. Consumption of a channel slot forever (prefetch permanently reduced).
2. Infinite CPU on Decode retries after each channel reconnect.
3. Legitimate messages ordering-blocked until DLQ (never reached).
4. `ConsumerPipeline`'s poison detector (`ConsumerPipeline.cs:35`) is bypassed entirely because pipeline is never entered.

**Recommendation.**
- Wrap the ctor call and handler dispatch inside `OnReceived` in try/catch; on decode failure, `BasicNackAsync(deliveryTag, multiple:false, requeue:false, ...)` so DLX routes it to `<queue>.dlq`.
- Better: publish a poison-report envelope with the raw body to `<queue>.dlq` explicitly and ack the original.
- Consider moving Decode into `ConsumerPipeline` (accept `Func<Envelope>` lazily) so all decode errors traverse the same poison path.

---

## T-03 [Critical] SystemTextJson deserialization uses default limits — memory/CPU DoS via oversized/nested/base64-amplified payloads

**Location:** `src/Mediana.Transport.Abstractions/Messaging/Serialization.cs:31-58`; `src/Mediana.Kafka/KafkaTransport.cs:204-214`; `src/Mediana.RabbitMQ/RabbitMqTransport.cs:249-259`; `src/Mediana.MassTransit/MassTransitTransport.cs:140-150`; `src/Mediana.Outbox/OutboxRelay.cs:80-90`.

**Description.** All four `EnvelopeCodec.Decode` copies and `SystemTextJsonMessageSerializer` construct `JsonSerializerOptions(JsonSerializerDefaults.Web)` with no custom limits:
- `MaxDepth = 64` (default) — attackers can nest exactly to that limit for CPU cost.
- No document size limit — STJ will happily process any body the transport delivers.
- `Envelope.Payload` is a `byte[]` deserialized from JSON base64 → ~1.33x amplification: a 100 MB base64 string in the envelope decodes to a 75 MB byte[]. Combined with `Headers` (unbounded `IReadOnlyDictionary<string,string>`) and `TraceParent` (arbitrary-length string) the process can be OOM'd with a single message.
- `Decode` in RabbitMQ (`RabbitMqTransport.cs:257`) additionally calls `body.ToArray()` allocating a full copy before deserialization.
- MassTransit (`MassTransitTransport.cs:148`) and Kafka copies do the same.

**Exploit / impact.** Untrusted broker peer publishes a single ~500 MB JSON body → consumer process crashes with OOM. Even a 10 MB body containing a base64 `Payload` of 8 MB and 500 KB of `Headers` sustained at N msg/s DoSes memory. Combined with T-01/T-02 this crashes the process or wedges consumption. RabbitMQ frame-max defaults protect against multi-GB messages but 128 MB payloads are trivially achievable.

**Recommendation.**
- Introduce a single `EnvelopeCodec` in `Mediana.Transport.Abstractions` and delete the four duplicates.
- Configure `JsonSerializerOptions { MaxDepth = 32 }` and use `JsonReaderOptions` via `Utf8JsonReader` to enforce max byte length per message (compare `body.Length` to configurable `MaxEnvelopeBytes`, e.g., 1 MB default).
- Reject messages whose transport-level size exceeds the limit before Decode (Kafka `result.Message.Value.Length`, RabbitMQ `args.Body.Length`).
- Add a bound to `Envelope.Headers` count and cumulative string length; validate `Payload.Length` post-decode; reject `Version > EnvelopeVersion.Current + N`.
- On RabbitMQ, use `body.Span` / a pooled buffer instead of `ToArray()` allocation.

---

## T-04 [Critical] Kafka AdminClient and Consumer drop SASL/SSL/security config

**Location:** src/Mediana.Kafka/KafkaTransport.cs:32-36 (BuildTopology AdminClient), :67-68 (CreateConsumerHosts), :102-106 (KafkaConsumerHostFactory), :119-133 (KafkaConsumerHost.Start).

**Description.** KafkaTransport takes a ProducerConfig (:15-19) that a user can fully populate (SASL_SSL, mechanism, credentials, TLS certs, security.protocol, sasl.username, etc.). But:
- BuildTopology :35 builds `new AdminClientConfig { BootstrapServers = _producerConfig.BootstrapServers }` -- only BootstrapServers is copied. All auth/TLS is dropped.
- CreateConsumerHosts :68 passes only `_producerConfig.BootstrapServers` to KafkaConsumerHostFactory.
- KafkaConsumerHost.Start :121-127 builds a ConsumerConfig with only BootstrapServers, GroupId, AutoOffsetReset, EnableAutoCommit=false.

Impact depends on broker listener config:
- Broker listens on PLAINTEXT: admin/consumer connect UNAUTHENTICATED to a broker configured for SASL_SSL by the user. Topic creation and message consumption happen without credentials, bypassing operator security expectations.
- Broker listens only on SASL_SSL: admin/consumer fail with cryptic librdkafka error; topology never built and no messages consumed, but the failure mode gives no hint that config was dropped.
- Worse: consumer joins the group with an anonymous identity -> ACL bypass on brokers that use PLAINTEXT for internal listeners.

**Recommendation.**
- Change KafkaTransport to accept full ClientConfig-derived options (ProducerConfig + ConsumerConfig + AdminClientConfig), or map all ProducerConfig auth/SSL properties (SecurityProtocol, SaslMechanism, SaslUsername, SaslPassword, SslCaLocation, SslCertificateLocation, SslKeyLocation, SslKeyPassword, SslKeystoreLocation, SslKeystorePassword, SslEndpointIdentificationAlgorithm, EnableSslCertificateVerification, etc.) onto both admin and consumer configs.
- Add a startup guard that fails fast if SecurityProtocol is non-plaintext and admin/consumer configs are not explicitly supplied.

---

## T-05 [High] Kafka Nack silently commits offset without producing to DLQ — guaranteed message loss on handler failure

**Location:** src/Mediana.Kafka/KafkaTransport.cs:190-201, referenced by src/Mediana.Transport.Abstractions/Consuming/ConsumerPipeline.cs:54-59.

**Description.** ConsumerPipeline.Process calls `delivery.Nack(requeue: false, redeliveryDelay: null)` when the handler exhausts retries (:58). KafkaDelivery.Nack (:190-200) does `consumer.Commit(result)` and, per the inline comment, assumes "retry/DLQ publishes the upstream loop." There is no such upstream loop. The message is committed and dropped.

**Exploit / impact.** Availability + data loss (silent). An attacker who can cause handler failures (e.g., by sending a message that triggers InvalidOperationException — see T-11) drops it into the void. Legitimate messages that fail transiently past MaxAttempts (5) are also silently lost. README claims "Kafka DLQ" support; this contradicts it.

**Recommendation.**
- Inject an IProducer<string,byte[]> (or the transport's publisher) into KafkaDelivery; on Nack(requeue:false) produce a copy to `<topic>.dlq` including headers mediana.dlx-reason, mediana.original-topic, exception summary; only commit after produce confirms.
- On Nack(requeue:false, redeliveryDelay != null) produce to KafkaTransport.RetryTopicName(topic, delay).
- Fail startup if DLQ producer is not configured.
- Update README to match actual behavior.

---

## T-06 [High] Kafka PollLoop swallows all non-OCE exceptions silently (consumer stops, no health signal)

**Location:** src/Mediana.Kafka/KafkaTransport.cs:135-150, :119-133.

**Description.** Even independent of T-01, PollLoop only catches OperationCanceledException. Any KafkaException, network exception, ConsumeException, ObjectDisposedException, or fatal serialization exception ends the loop. _pollLoop is a fire-and-forget Task.Run whose failure is only observed on Stop(). No log, no metric, no restart.

**Exploit / impact.** Loss of consumption availability; no ops visibility. Combined with T-01 this is trivially remote-triggerable.

**Recommendation.**
- Catch Exception inside the while loop; log with `_logger?.LogError(ex, ...)`; on transient errors retry with backoff, on fatal errors surface via a health probe or crash the process (Environment.FailFast) so orchestrator restarts it.
- Never leave _pollLoop unobserved: add a ContinueWith(t => logger.LogCritical(t.Exception, ...), OnlyOnFaulted).

---

## T-07 [High] RabbitMQ direct reply-to client: predictable correlationId + unbounded reply decode + spoofable reply

**Location:** src/Mediana.RabbitMQ/RabbitMqConsumer.cs:152-208.

**Description.**
- Correlation is `request.MessageId.ToString()` (:166), used as both MessageId and CorrelationId on the outbound message, and reply is matched by exact string equality on CorrelationId (:170).
- On ns2.1 the MessageId is generated by GuidV7 which uses System.Random (GuidV7.cs:12,30,61,66) — predictable. An attacker who knows the process seed (or observes a few earlier IDs) can guess the CorrelationId of an in-flight request.
- The reply handler (:168-176) calls `EnvelopeCodec.Decode(received.Body.ToArray())` unconditionally when CorrelationId matches, with no size limit (see T-03). A malicious peer that shares publish access to the request queue can send an oversized reply to blow up the requester's memory.
- If the target handler never replies, completion.Task is never completed. delayTask fires and RemoteTimeoutException is thrown (:204). The TaskCompletionSource<Envelope> becomes unreachable -> GC eventually collects — no permanent leak, but no per-request cancellation of the underlying consumer subscription except by disposing the channel (await using on :163). Between reply arrival after timeout and channel disposal there is a small window where a reply could still be decoded.
- Reply spoofing: RabbitMQ direct reply-to (amq.rabbitmq.reply-to) is scoped to the requester's connection, so only the process that consumed the request from the target queue can publish a reply that reaches the requester. If the target queue has multiple competing consumers, any of them can respond (first-write-wins). If one of those consumers is malicious/compromised, it can forge an arbitrary reply body — attacker-controlled data flows into the caller under the guise of a legitimate response.

**Exploit / impact.**
- OOM DoS of the request-issuing process by a compromised peer service.
- Reply substitution: a malicious co-consumer on the destination queue crafts a reply that returns attacker-controlled data to the caller (no signature check, no size limit).
- Predictable MessageId on ns2.1 could allow a peer to pre-emptively publish a reply anticipating the correlationId.

**Recommendation.**
- Enforce reply body size limit before Decode.
- Require an application-layer HMAC or shared-secret signature over the reply envelope (or run request/reply only over trusted peers).
- Use Guid.NewGuid() (cryptographic on modern .NET) for the correlationId, not GuidV7 on ns2.1.
- Register the timeout via cancellationToken linked with a CancellationTokenSource(timeout); drop the Task.Delay race.
- Reject replies whose BasicProperties.MessageId / envelope MessageId does not equal the expected correlationId, not just CorrelationId string equality.

---

## T-08 [High] GuidV7 on netstandard2.1 uses non-cryptographic System.Random -> predictable MessageIds enable inbox dedup poisoning

**Location:** src/Mediana.Transport.Abstractions/Messaging/GuidV7.cs:12,30,61,66; src/Mediana.Transport.Abstractions/Inbox/InboxStore.cs:34-49; src/Mediana.Transport.Abstractions/Consuming/ConsumerPipeline.cs:37-43.

**Description.** On non-NET10_0 targets (both consumers and outbox producers built for ns2.1), GuidV7.NewGuid() uses a static System.Random seeded per-process (`Rng = new()` :12). Every random component (randA :61, randB via NextInt64() :26-31, :66) is derived from System.Random.Next() — a non-CSPRNG, ~48-bit state, guessable given a handful of observed outputs. On net10 the code path uses `Guid.CreateVersion7()` which is documented as unpredictable — that path is safe.

ConsumerPipeline.Process uses `delivery.Envelope.MessageId.ToString("N")` as the inbox dedup key (:37). If a legitimate future MessageId can be predicted, an attacker who can publish onto the same topic can pre-populate the inbox with a forged envelope carrying that MessageId, so the legitimate message is treated as a duplicate and dropped (line :41 acks without invoking the handler).

Note also that the message is marked "consumed" (via TryBegin adding to _completed) BEFORE the handler runs; the handler failing does not roll back the dedup entry, so a subsequent legitimate retry with the same MessageId is dropped too — this is the ns2.1 predictability turning into a real dedup-suppression primitive across attempts.

Additionally, both the ns2.1 monotonic sequence counter (_sequence, :11,42,47,67) and _lastTimestamp (:10) are static and process-wide, making offline collision prediction easier once a few IDs are observed.

**Exploit / impact.** Message suppression: any producer (or attacker with topic-write access) that predicts a MessageId can silently drop the corresponding legitimate message from the consumer's inbox. In a multi-tenant broker this becomes a targeted DoS against a specific consumer.

**Recommendation.**
- On ns2.1 use RandomNumberGenerator.GetBytes (System.Security.Cryptography, available on ns2.1) for the random portion.
- Consider adding an HMAC of Envelope fields with a shared secret and validating on the consumer, so a forged envelope cannot pass authentication regardless of MessageId collision.
- Consider computing the inbox key from (MessageId, Timestamp) or including MessageType.ContractHash so pure MessageId collision does not suppress differently-typed messages.

---

## T-09 [High] `mediana.destination` envelope header lets an attacker choose the publishing routing key on RabbitMQ & Kafka when Publish is called without DestinationOverride

**Location:** src/Mediana.RabbitMQ/RabbitMqTransport.cs:197-211; src/Mediana.Kafka/KafkaTransport.cs:78-82.

**Description.** RabbitMqPublisher.Publish and KafkaPublisher.Publish resolve the destination as `options.DestinationOverride ?? envelope.Headers["mediana.destination"]`. The header is inside the envelope, i.e. inside the untrusted payload. Whoever controls the envelope controls the destination.

Currently the only in-repo caller of ITransportPublisher.Publish is OutboxRelay.Deliver (OutboxRelay.cs:179-182), which always sets `DestinationOverride = message.Destination` from the persisted outbox row — so the in-repo attack surface is limited. However:
- ITransportPublisher is public; user code (bridges, republishers, custom relays, tests) is expected to call Publish directly. Any such caller that passes PublishOptions.Default (or forgets DestinationOverride) will silently route by the untrusted header.
- Any bridge (e.g., "receive from broker A, publish to broker B") that re-uses received envelopes as-is will re-publish an attacker-controlled envelope to an attacker-chosen destination — including routing to `<other-queue>.dlq`, control-plane queues, or a queue owned by another tenant.
- Routing key in a RabbitMQ topic exchange can also be crafted to match wildcard bindings on tenant queues.
- No allowlist / no exchange-namespace enforcement (destinations are used raw as routing keys against MedianaExchange).

**Exploit / impact.** Message injection into unintended queues / topics: bypasses RouteRegistry policy, breaks tenant isolation on a shared broker, allows a malicious peer to seed messages into arbitrary Mediana consumers on the same exchange.

**Recommendation.**
- Delete the `envelope.Headers["mediana.destination"]` fallback entirely; require PublishOptions.DestinationOverride (or a policy-resolved destination) to be non-null.
- If the header must be kept for compatibility, validate it against an allowlist configured on the transport (AllowedDestinations) and reject any not on the list.
- On bridge/relay code that re-publishes envelopes, always overwrite `Headers["mediana.destination"]` with a policy-resolved value before calling Publish.

---

## T-10 [Medium] MessageTypeDescriptor.ContractHash never computed or verified — no type-confusion guard on the wire

**Location:** src/Mediana.Transport.Abstractions/Messaging/Envelope.cs:10-19 (definition); grep for ContractHash returns only the property definition — never read, never written.

**Description.** MessageTypeDescriptor.ContractHash is documented as "detection of incompatibility on receive (poison)" but nothing in the codebase computes or verifies it. The publisher stamps only FullName and TypeVersion onto the wire (Kafka :90-92, Rabbit :221-222); the consumer's ConsumerPipeline dispatches by handlerIdentity supplied by the transport plumbing, not by verifying the envelope's declared type against the handler's expected type. Because Envelope.Payload is just byte[] and the handler deserializes it as its own expected type, a message with declared `FullName = A` and a payload that decodes as B will not be caught until the handler-specific decode either succeeds coincidentally, throws, or (worst case) succeeds with confused semantics (JSON is very forgiving — missing fields become defaults; extra fields ignored under Web defaults).

**Exploit / impact.** Type confusion across contract versions or peer-supplied FullName mismatches; silent field truncation; a message sent to queue X for type A may be dispatched as type A even if its body encodes B (or an evolved schema) without any integrity check. Not directly exploitable to RCE, but breaks the "poison" promise the field advertises.

**Recommendation.**
- Compute a stable hash (e.g., SHA-256 of a canonical schema descriptor) at message registration time, ship it in ContractHash, and reject in ConsumerPipeline when the incoming ContractHash is non-null and does not match the registered handler's hash -> route to DLQ as poison.
- Alternatively, drop the field to avoid a false sense of security.

---

## T-11 [Medium] PoisonDetector classifies InvalidOperationException and ArgumentException as poison — attacker-triggerable message loss

**Location:** src/Mediana.Transport.Abstractions/Reliability/Retry.cs:119-126; interacts with ConsumerPipeline.cs:35, :54-59.

**Description.** PoisonDetector.IsPoison returns true for SerializationException, FormatException, InvalidOperationException, ArgumentException, MediatorConfigurationException. ConsumerPipeline uses `!IsPoison(ex)` as the isRetryable predicate, so any of those exceptions is treated as a fatal poison and immediately DLQ'd (or on Kafka: silently dropped, see T-05).

InvalidOperationException and ArgumentException are routinely thrown for transient issues in .NET (disposed HttpClient, canceled channel, Collection was modified, Connection is closed) and for legitimate business-rule guards. An attacker who can influence handler inputs to reach an argument-guard (e.g., a value out of range) causes the message to bypass all retries and land in DLQ (or /dev/null on Kafka).

**Exploit / impact.**
- Data loss on Kafka (via T-05).
- Denial-of-processing: attacker floods handler with edge-case inputs that trigger argument checks; all such messages are one-shot dropped despite being potentially retryable.
- Amplifies T-05 into a remotely-triggerable loss primitive.

**Recommendation.**
- Restrict poison to SerializationException, MediatorConfigurationException, and a narrow list of user-registered poison predicates. Never treat generic InvalidOperationException / ArgumentException as poison.
- Add attempt-counter DLQ escalation after MaxAttempts retries independent of exception type.

---

## T-12 [Medium] Retry jitter never applied — thundering-herd on transient broker failures

**Location:** src/Mediana.Transport.Abstractions/Reliability/Retry.cs:38-66, :76-102; src/Mediana.Transport.Abstractions/Consuming/ConsumerPipeline.cs:47-51.

**Description.** RetryPolicy.DelayFor applies jitter only when `random is not null` (:59). RetryEngine.Execute declares `Random? random = null` (:80) and passes it through to DelayFor unchanged (:95). ConsumerPipeline calls RetryEngine.Execute without providing a Random (:47-51). Consequence: jitter is effectively dead code; N concurrent consumers all retry at identical timestamps on a broker outage, hammering the broker in synchronized bursts.

**Exploit / impact.** Availability degradation of the broker under partial outage (thundering herd). Not directly a security bug, but README claims jitter is on by default — false marketing.

**Recommendation.**
- Instantiate a per-consumer Random (or use Random.Shared on net6+) inside RetryEngine.Execute when the caller did not pass one, and default Jitter=0.2 in RetryPolicy.Default (already 0.2, so just wire the Random).
- Or replace System.Random with RandomNumberGenerator.GetInt32 for extra safety.

---
