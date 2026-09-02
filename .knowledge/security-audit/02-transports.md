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

## T-13 [Medium] RabbitMQ Stop misuses SemaphoreSlim.WaitAsync(int) as permit count — treats as millisecond timeout; connection close is fire-and-forget

**Location:** src/Mediana.RabbitMQ/RabbitMqConsumer.cs:64-79.

**Description.**
- :72 `await _handlerLimiter.WaitAsync(endpoint.MaxConcurrency).ConfigureAwait(false);` — the int overload of WaitAsync is a MILLISECOND TIMEOUT, not a permit count. If MaxConcurrency=1, Stop waits 1 ms for one permit and continues regardless of in-flight handlers. If MaxConcurrency=10, it waits 10 ms. Graceful drain is not actually performed.
- :78 `_connection?.CloseAsync();` — Task returned by CloseAsync is discarded (not awaited). Connection may still be open when DisposeAsync runs `_connection.DisposeAsync()` on :87, potentially racing.

**Exploit / impact.** In-flight messages get the channel closed under them during shutdown, become unacked and are requeued (possibly leading to duplicate processing, or piling up if combined with T-02). Not directly exploitable but worsens availability under normal restart/deploy scenarios.

**Recommendation.**
- To drain, acquire all permits explicitly: loop MaxConcurrency times calling WaitAsync with a real CancellationToken and reasonable timeout.
- Await the CloseAsync task: `await _connection.CloseAsync(...).ConfigureAwait(false);`.

---

## T-14 [Medium] MassTransit fault leaks Environment.MachineName, exception type FullName, and exception message onto the bus

**Location:** src/Mediana.MassTransit/MassTransitTransport.cs:111-131.

**Description.** ToMassTransitFault constructs a dictionary intended to be published to a bus consumed by potentially untrusted peers on shared MassTransit infrastructure:
- `machineName = Environment.MachineName` (:128) discloses internal hostname (infrastructure enumeration).
- `exceptionType = exception.GetType().FullName` (:122) discloses namespaces and internal type names.
- `message = exception.Message` (:123) often contains stack context, connection strings (Cannot connect to server 10.0.1.5:5432 with user X), file paths, PII from request payloads.
- faultedMessageId and faultMessageType.FullName are also leaked.

**Exploit / impact.** Information disclosure on a multi-tenant bus: internal topology, credentials in DB error messages, filesystem paths, and internal type names help attackers plan follow-up attacks.

**Recommendation.**
- Do not include MachineName (or provide an opt-in IncludeHostInfo flag defaulting to false).
- Sanitize exception.Message (truncate + strip patterns matching connection strings / paths / IPs) or replace with a canonical short reason + correlationId to look up in server-side logs.
- Emit only exception.GetType().Name (not FullName), or a stable classification code.

---

## T-15 [Medium] Correlation / traceparent / arbitrary header values are propagated verbatim from untrusted envelope into logs and downstream (log/trace injection)

**Location:** src/Mediana.Transport.Abstractions/Messaging/Envelope.cs:32-47; src/Mediana.RabbitMQ/RabbitMqTransport.cs:213-234 (writes to AMQP BasicProperties.Headers); src/Mediana.Kafka/KafkaTransport.cs:84-94; src/Mediana.Transport.Abstractions/Consuming/ConsumerPipeline.cs:40,:56 (log format strings interpolate {MessageId} — the only field that is currently a Guid).

**Description.**
- Envelope.TraceParent is a nullable string with no format validation; W3C tracecontext defines a strict version-traceid-spanid-flags grammar. Passing an arbitrary string into an activity/link may result in a malformed activity being created downstream (STJ preserves it as-is on serialization).
- Envelope.Headers is `IReadOnlyDictionary<string,string>` with no length limits, no forbidden-character filtering (CR/LF, NUL). When these are copied onto AMQP BasicProperties.Headers (RabbitMqTransport.cs:219-234) or Kafka Headers (KafkaTransport.cs:88-94), a malicious peer can embed CR/LF sequences that appear in downstream broker logs (log injection), and in ILogger structured logs if the app decides to log headers.
- CorrelationId/CausationId are Guids and thus safe. TraceParent is a string and unsafe. Header keys/values are strings and unsafe.
- Right now the pipeline only logs {MessageId} at debug/error (ConsumerPipeline.cs:40,:56), so log-injection is not currently exploited inside Mediana, but any consumer telemetry or MassTransit fault map that copies these fields will leak. ToMassTransitFault (T-14) copies MessageType.FullName unfiltered, which is attacker-controlled since the attacker crafts the envelope — a malicious `MessageType.FullName` containing CR/LF ends up in the fault dictionary published to the bus.

**Exploit / impact.** Log/trace injection when Mediana or downstream code eventually logs any of Envelope.TraceParent, Envelope.Headers, MessageType.FullName, or when these are propagated to distributed-tracing backends; low direct risk today, but any future logging change exposes it.

**Recommendation.**
- Validate TraceParent against the W3C regex `^[0-9a-f]{2}-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$`; reject or drop non-conformant values before publishing/consuming.
- Enforce max length and forbid CR, LF, NUL in every Headers key and value; enforce total dictionary size bound.
- Enforce max length + character allowlist `[A-Za-z0-9._+-]` on MessageType.FullName.
- If logging headers, use structured logging and never format-string interpolate directly.

---

## T-16 [Low] Inbox key uses raw `|` delimiter — collision-prone if handlerIdentity contains `|`

**Location:** src/Mediana.Transport.Abstractions/Inbox/InboxStore.cs:62-63; called from ConsumerPipeline.cs:37.

**Description.** `Key(messageId, handlerIdentity)` = `messageId + "|" + handlerIdentity`. messageId is always a 32-char hex Guid ("N" format) supplied by ConsumerPipeline, so the first component is safe. handlerIdentity is passed by transport-layer callers and is not attacker-controlled today, but the API contract does not forbid `|` in the string. If a future caller passes a handler identity like `A|B` for handler A on delimiter path and `A` for a differently-named handler on subpath `|B`, keys collide -> false-positive dedup -> message drops.

**Exploit / impact.** No current exploit; latent correctness bug becomes relevant if handlerIdentity is ever externally influenced.

**Recommendation.**
- Use a struct key `(string messageId, string handlerIdentity)` in a `Dictionary<(...)>` instead of concatenation.
- Or hash both parts (SHA-256) and use the hex digest.

---

## T-17 [Low] Inbox marks message consumed BEFORE handler runs -> handler failure = at-most-once not at-least-once

**Location:** src/Mediana.Transport.Abstractions/Inbox/InboxStore.cs:34-49 (TryBegin adds to _completed immediately); src/Mediana.Transport.Abstractions/Consuming/ConsumerPipeline.cs:37-59.

**Description.** TryBegin inserts the key into _completed on first call and returns true; if the handler then throws (past retries), the key stays in _completed. On the next redelivery (RabbitMQ requeue or Kafka replay after group rebalance) TryBegin returns false, ConsumerPipeline calls Ack on line :41, and the failed message is silently dropped. The pipeline advertises effectively-once but is really at-most-once for failed handlers.

Note also that on Kafka T-05 the message is already lost; on RabbitMQ it goes to DLQ. In-memory inbox is per-process so a process restart clears state — but within a single process lifetime the semantics diverge from the docstring.

**Exploit / impact.** Silent message loss for messages that failed and were later redelivered within the same process. Combined with T-11 (over-broad poison classification), a well-timed InvalidOperationException loses the message AND poisons the inbox against re-processing.

**Recommendation.**
- Rename TryBegin semantics: only add to _completed on Complete(), and hold an in-flight set to prevent concurrent duplicates. Alternatively, on handler failure, remove the key from _completed.
- Document the semantics explicitly; add tests.

---

## T-18 [Low] InMemoryInboxStore capacity eviction is FIFO by insertion order — recent messages can be evicted while older ones survive

**Location:** src/Mediana.Transport.Abstractions/Inbox/InboxStore.cs:39-45.

**Description.** Under sustained load >=100 000 messages, eviction begins to drop the oldest inbox keys. If Kafka replay includes messages from before that window (offset reset / rewind / group rebalance), they will be re-processed. Not a security issue directly, but effectively-once becomes effectively-once within a sliding window, which is not documented.

**Recommendation.**
- Document the retention window in IInboxStore docstring.
- Consider TTL-based eviction (drop entries older than N minutes) instead of pure size-FIFO.
- Add a metric on eviction rate so operators know when they need a persistent inbox.

---

## T-19 [Low] Envelope.Version is not enforced on decode — future/unknown wire versions accepted silently

**Location:** src/Mediana.Transport.Abstractions/Messaging/Envelope.cs:4-7,:27; four EnvelopeCodec copies never inspect Version.

**Description.** EnvelopeVersion.Current = 1. Decoded envelopes may declare `Version = 99` and be processed. Only additive evolution is planned per the spec, but there is no forward-compat guard; a future breaking change or a malicious version-0 downgrade could pass silently.

**Recommendation.** In EnvelopeCodec.Decode, reject `envelope.Version < 1 || envelope.Version > EnvelopeVersion.Current`. Route as poison.

---

## T-20 [Low] Kafka RetryTopicName interpolates TimeSpan.TotalMilliseconds as double -> floats/locale drift; topic names not validated

**Location:** src/Mediana.Kafka/KafkaTransport.cs:70-71; also src/Mediana.RabbitMQ/RabbitMqTransport.cs:151-152.

**Description.** `topic + ".retry." + delay.TotalMilliseconds + "ms"` — TotalMilliseconds is a double. Under an unusual current-culture setting or fractional delay values you can produce topic names with `,` or `.` decimal separators and scientific notation (`1E-05`), leading to inconsistent topic naming or Kafka forbidden characters (`[^a-zA-Z0-9._-]` are rejected). Also nothing validates the base topic name is safe against injection (e.g., `topic = "../evil"`).

**Recommendation.** Use `((long)delay.TotalMilliseconds).ToString(CultureInfo.InvariantCulture)`. Validate topic/queue names against a strict regex before building topology.

---

## T-21 [Info] EnvelopeCodec duplicated four times with drift risk

**Location:** src/Mediana.Kafka/KafkaTransport.cs:203-214; src/Mediana.RabbitMQ/RabbitMqTransport.cs:248-259; src/Mediana.MassTransit/MassTransitTransport.cs:139-150; src/Mediana.Outbox/OutboxRelay.cs:79-90.

**Description.** Four independent copies with subtle differences (`byte[]` vs `ReadOnlySpan<byte>`, `body.ToArray()` allocation in Rabbit only). A single behavioral fix (limits, version guard, ContractHash validation) must be applied in four places — easy to skip one, creating an inconsistent security posture across transports.

**Recommendation.** Move the single canonical codec into Mediana.Transport.Abstractions/Messaging and have transports depend on it. Add hardening (max size, max depth, version guard) in one place.

---

## T-22 [Info] RabbitMqTransport.CreateConnection ignores CancellationToken

**Location:** src/Mediana.RabbitMQ/RabbitMqTransport.cs:66-69.

**Description.** `internal ValueTask<IConnection> CreateConnection(CancellationToken cancellationToken)` calls `_factory.CreateConnectionAsync()` without forwarding the token. On slow DNS / TCP handshakes the caller cannot cancel. Not a security bug but hampers graceful shutdown and can extend startup DoS windows.

**Recommendation.** Forward the token: `_factory.CreateConnectionAsync(cancellationToken)` (7.x supports it).

---

# Summary

| ID | Severity | Title |
|---|---|---|
| T-01 | Critical | Kafka poll loop dies silently on poison envelope in delivery ctor |
| T-02 | Critical | RabbitMQ poison envelope -> infinite unacked poison loop, bypasses DLQ |
| T-03 | Critical | STJ default limits + base64 amplification -> memory DoS on all 4 codec paths |
| T-04 | Critical | Kafka AdminClient / Consumer drop SASL/SSL config from ProducerConfig |
| T-05 | High | Kafka Nack commits offset without producing to DLQ (message loss) |
| T-06 | High | Kafka PollLoop swallows all non-OCE exceptions silently |
| T-07 | High | Rabbit request/reply: predictable correlationId + unbounded reply decode + spoofable reply |
| T-08 | High | GuidV7 ns2.1 uses System.Random -> predictable MessageId -> inbox dedup poisoning |
| T-09 | High | `mediana.destination` header lets attacker route Publish() when DestinationOverride null |
| T-10 | Medium | ContractHash never computed/verified — type-confusion guard is dead |
| T-11 | Medium | PoisonDetector treats InvalidOperationException/ArgumentException as poison (loss primitive) |
| T-12 | Medium | Retry jitter never applied (thundering herd) |
| T-13 | Medium | RabbitMQ Stop misuses WaitAsync(int) + un-awaited connection close |
| T-14 | Medium | MassTransit fault leaks MachineName + exception type/message |
| T-15 | Medium | Envelope headers/TraceParent/MessageType.FullName propagated unvalidated (log/trace injection) |
| T-16 | Low | Inbox key `|` delimiter collision-prone |
| T-17 | Low | Inbox marks consumed before handler runs (at-most-once on handler failure) |
| T-18 | Low | Inbox FIFO capacity eviction can drop still-relevant keys |
| T-19 | Low | Envelope.Version not enforced on decode |
| T-20 | Low | Retry-topic name uses TimeSpan.TotalMilliseconds as double (culture/format risk) |
| T-21 | Info | EnvelopeCodec duplicated 4x — drift risk |
| T-22 | Info | RabbitMQ CreateConnection ignores CancellationToken |

---

# Checked & OK

- RabbitMQ topology arguments (`x-dead-letter-*`, `x-message-ttl`, `x-queue-mode`) in TopologyProvisioner (RabbitMqTransport.cs:82-165) are hard-coded with values derived from application-supplied TopologyManifest, not from untrusted envelope data. Queue names come from ConsumerEndpoint.Name / PublishDestinations / DeadLetterDestinations — all application-controlled. No injection vector from envelope path.
- Publisher confirms on RabbitMQ: channel is created with `publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true` (RabbitMqTransport.cs:55-57); BasicPublishAsync awaits confirmation. Correct for outbox relay durability.
- Kafka publisher key falls back to `envelope.MessageId.ToString("N")` when neither options.PartitionKey nor envelope.PartitionKey is set (KafkaTransport.cs:86) — sensible; not a security issue.
- MedianaExchange constant is a fixed literal `"mediana"` (RabbitMqTransport.cs:17), not attacker-influenced.
- No hardcoded credentials, connection strings, or secrets in any transport source file (confirmed by inspection of all four transport projects).
- No use of dangerous serializers (BinaryFormatter, Newtonsoft with TypeNameHandling, Json.NET with polymorphism). Only System.Text.Json with reflection-based `Deserialize<Envelope>` — Envelope is a sealed record with concrete typed properties, so no polymorphic gadget-chain risk.
- No `Type.GetType` / `Assembly.Load` / dynamic type resolution on the transport receive path — `MessageType.FullName` is transported but never resolved to a `System.Type`; there is no reflection-based instantiation from wire data.
- No user-controlled URLs, no outbound HTTP from transport packages.
- AMQP MessageId and CorrelationId on RabbitMQ publisher use Guid.ToString() (safe format).
- Kafka topic creation is idempotent: `CreateTopicsException` with `TopicAlreadyExists` is swallowed only when all results are that error (KafkaTransport.cs:53-56) — correct guard.
- Ack/Nack methods on both transports do not throw on second call in normal path; concurrent Ack is not possible because deliveries are single-owned by the consumer loop.
- MassTransit direct publisher (Mode 1) uses MassTransit's own IBus.Publish — inherits MassTransit's security posture for auth/transport (out of scope) and only ships `MedianaWireMessage { MessageId, Destination, Body }`, no unsafe fields.
- InProcessConsumerHost.Deliver guards against calls before Start() (MassTransitTransport.cs:82-90) — good state check.
- RouteRegistry stores only application-configured policies (RouteRegistry.cs:88-92); the RemoteAttribute reflection lookup (:102-103) is over application types, not wire data — safe.
- OutboxRelay.Deliver always sets DestinationOverride from persisted OutboxMessage.Destination (OutboxRelay.cs:181), so the mediana.destination header attack (T-09) does not apply to the only in-repo caller of ITransportPublisher.Publish.
- Handler dispatch does not use `MessageType.FullName` to resolve a Type via reflection anywhere (grepped: FullName is only written to wire headers and copied into the MassTransit fault dict).
