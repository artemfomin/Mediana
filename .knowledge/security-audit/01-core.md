# Mediana core (Mediana / Mediana.Abstractions / Mediana.Generators) - security audit

Scope: src/Mediana, src/Mediana.Abstractions, src/Mediana.Generators, with behaviour evidence from tests/Mediana.GeneratorTests and tests/Mediana.AotTests. Read-only, HEAD 5be92c5. Every finding cites file:line verified in code. Hypotheses not fully verified are marked as unverified hypothesis.

## Findings

### [Medium] Cross-request state leakage via captured next in pooled ChainState
Location: src/Mediana/Dispatch/ChainState.cs:14-66 (shared NextDelegate field at :18 and :27, pooled slot at :21-22, double-next guard at :55-57).

Description. ChainState of TRequest and TResponse is a [ThreadStatic] single-slot pooled object. Its NextDelegate is bound once to the instance in the constructor (:27) - every pipeline invocation for the same closed (TRequest, TResponse) returns the SAME delegate reference to middlewares. On completion Return() (:60-66) sets _pooled = this after clearing Behaviors and Terminal. On the next Take() (:69-103) the same instance is re-Configure-d with a new Behaviors array, new Terminal and Index = 0. If a middleware captured the next delegate and invokes it after its request has completed AND after another request has already been dispatched on that same thread, the call re-enters Next() (:37-58) on the reconfigured state: Index == 0 < Behaviors.Length, so it executes the OTHER tenant Behaviors[0] and/or Terminal with the OLD request instance. The double-next guard only fires when Index > Behaviors.Length for the SAME configuration; after Return() + Take(), Index is reset, so the guard cannot detect this.

Exploit / impact scenario. Middleware author mistakenly persists the next delegate (background retry timer, offloaded work, error-recovery closure). After the original request completes and the state is pooled and reused, the stale next triggers the pipeline of a subsequent, unrelated request - potentially in a different tenant or authorization context. Result: silent execution of the wrong terminal with foreign request data (data leak / privilege confusion). No memory-safety violation, but a functional isolation break.

Recommendation. (a) Version the pooled state: bump an int Version on every Configure, capture it in a per-call next closure, and throw in Next() if versions diverge. (b) Or allocate NextDelegate per Configure (a new closure over a fresh state copy). (c) Document explicitly that consumers MUST NOT capture the pipeline next beyond the awaited task; add a test that asserts stale-next reuse throws.

### [Medium] Public MessageEntry with internal set combined with Unsafe.As of IEventCallSite array - memory-safety invariant not enforced
Location: src/Mediana/Dispatch/MessageEntry.cs:4-33 (public sealed class with internal set on all callsite properties, IReadOnlyList of IEventCallSite EventCallSites at :30); src/Mediana/Mediator.cs:116 reinterprets that IReadOnlyList as IEventCallSite[]; src/Mediana/Properties/AssemblyInfo.cs:3-6 grants unsigned InternalsVisibleTo to 4 test assemblies.

Description. The Unsafe.As cast at Mediator.cs:116 skips CLR type checks. It is safe only because every internal writer today assigns an array: the default = [] at MessageEntry.cs:30 lowers to Array.Empty of IEventCallSite, and the only mutator, MedianaConfiguration.Freeze, uses list.ToArray() at MedianaConfiguration.cs:281. There is no runtime assertion. Two fragility surfaces:

1. Any future internal writer that assigns a non-array IReadOnlyList (e.g. a List directly, ReadOnlyCollection, ImmutableArray boxed as interface) will silently produce a type-safety violation and, on the very next Publish, either AV/heap corruption or an InvalidCastException at first element access (depending on runtime).
2. The InternalsVisibleTo attributes are unsigned. Any assembly with a name matching Mediana.UnitTests, Mediana.GeneratorTests, Mediana.ContractTests.Ns21, or Mediana.UnitTests.Ns21 can assign a non-array to EventCallSites and provoke the same corruption. (An attacker already running code in the process can do worse - the concern is fragility of the invariant, not a standalone attack vector.)

MessageEntry is publicly constructible; external code can create one and pass it to public MessageRegistry.Add, but cannot set EventCallSites (internal setter), so external abuse via the public API is not possible today.

Recommendation. Any of:
- Change EventCallSites type to IEventCallSite[] (still internal set); this removes the need for Unsafe.As entirely and lets the JIT verify the type.
- Or drop Unsafe.As in favour of a hard-cast that the JIT will elide when the field declared type is already the interface.
- Or add Debug.Assert(entry.EventCallSites is IEventCallSite[]) and a comment stating the invariant.
- Consider making MessageEntry itself internal, or making all setters public but the class sealed with init-only setters.

### [Low] lock(this) on internal instances hidden behind public interfaces
Location: src/Mediana/Dispatch/RequestCallSites.cs:147 (CommandCallSite.Slow) and :340 (QueryCallSite.Slow).

Description. CommandCallSite and QueryCallSite are declared internal sealed (RequestCallSites.cs:48, :232). External code cannot obtain a reference via the compile-time public API - they surface only through the public interfaces IObjectCommandCallSite, ITypedCommandCallSite, IObjectQueryCallSite, ITypedQueryCallSite, IUntypedCallSite (Dispatch/CallSites.cs). HOWEVER, MessageEntry.CommandCallSite and QueryCallSite are typed as object? and exposed via a public property with only the setter internal (MessageEntry.cs:21, :24). A consumer holding a MessageRegistry (registered as singleton in DI, ServiceCollectionExtensions.cs:44) can call MessageRegistry.TryGet(t).CommandCallSite and receive the raw CommandCallSite instance. External lock on that instance would contend with the singleton-composition critical section on the first cold call - practical impact is a startup-only performance hazard, not a deadlock (the critical section is short and self-contained).

For consistency, EventCallSite already uses a private _singletonLock = new() (EventCallSite.cs:22, :50); the request call-sites should follow the same pattern.

Recommendation. Introduce private readonly object _singletonLock = new(); in CommandCallSite and QueryCallSite, and replace lock(this) with lock(_singletonLock). Same one-line change in both classes.

### [Low] Silent no-op on Publish of an unregistered event type
Location: src/Mediana/Mediator.cs:109-114.

Description. Publish returns default(ValueTask) when no MessageEntry exists (:110-113). Design comment explicitly matches MediatR semantics. When events are used for cross-cutting security-relevant paths (audit logging, tamper alarms), a typo or missed registration produces a silent success - the audit event is never processed and no exception surfaces.

Recommendation. Optional strict mode on MedianaConfiguration (RequireEventHandlers = true) that throws MediatorConfigurationException at Publish if no entry is present; or, at minimum, an ILogger warning through System.Diagnostics.DiagnosticSource (already referenced in the csproj). Document the current behaviour prominently in the public IMediator XML doc.

### [Low] Middleware that returns without calling next silently substitutes the response
Location: src/Mediana/Dispatch/ChainState.cs:37-58, src/Mediana/Dispatch/RequestCallSites.cs:36-41, src/Mediana/Dispatch/EventCallSite.cs:88-102.

Description. All pipeline composers invoke behavior.Handle(request, next, ct) - the behaviour is free to skip the next call and return any TResponse. This matches MediatR and is by design, but for security-sensitive pipelines (e.g. authorization behaviour expected to gate all downstream), the semantics allow a downstream behaviour to legally bypass the handler with an attacker-crafted response. There is no way to declare a terminal-must-run middleware.

Recommendation. Document explicitly that behaviour ordering IS the trust boundary; ordering is registration order (already stated in MedianaConfiguration.cs:22). Optionally, expose a diagnostics counter incremented on terminal-not-reached (would require version-and-count tracking on ChainState).

### [Low] PublishParallel has no bounded concurrency and no CT check between starts
Location: src/Mediana/Mediator.cs:176-215.

Description. All callSites are started synchronously in a single tight loop (:184-195). For EventDispatchPolicy.Parallel with N handlers, peak concurrency is N. Allocation of new ValueTask[callSites.Length] at :182 is O(N). In-process, N is developer-controlled, so DoS surface is low, but scan-registered graphs (AddHandlersFromAssembly) with attacker-influenced assembly composition could inflate N. Cancellation is not consulted between starts, so once fan-out begins it runs to completion regardless of ct.

Recommendation. Accept an optional MaxDegreeOfParallelism on EventDispatchPolicy.Parallel (per event type) walled off with a small SemaphoreSlim gate; check cancellationToken.IsCancellationRequested inside the start loop (:184) and short-circuit remaining invokes.

### [Low] MessageRegistry.Add is O(N) per registration and unsynchronised
Location: src/Mediana/Dispatch/MessageRegistry.cs:78-102.

Description. Copy-on-write add allocates a new items array of size _items.Length + 1 and scans the old items linearly. Documented (:12-13) as requiring external synchronisation. If a user path performs many runtime registrations in a tight loop (or under user-controlled input), memory pressure is quadratic (N*(N+1)/2 items copied). Not an in-repo bug, but a footgun.

Recommendation. Add a MessageRegistry.AddRange (single copy for many entries) if runtime bulk-add becomes a supported scenario; mark Add as one-off runtime hot-patch in doc.

### [Info] Unsafe.As justification is undocumented at the call site
Location: src/Mediana/Mediator.cs:116.

Description. There is no comment explaining why the reinterpret is sound (readers must chase MessageEntry.cs:30 and MedianaConfiguration.cs:281 to prove it). Combined with the memory-safety finding above, this is the single load-bearing comment missing from a hot path.

Recommendation. Add a comment stating: entry.EventCallSites is always allocated as IEventCallSite[] (see MedianaConfiguration.Freeze); Unsafe.As avoids the interface-array covariance check.

### [Info] Generator emits type names without global-scope prefix
Location: src/Mediana.Generators/MedianaGenerator.cs:125-136 (Fqn) and emissions at :185-194.

Description. Fqn builds namespace.Name or ContainingType.ToDisplayString() plus dot plus symbol.Name. Without a global-scope prefix, a consumer namespace like TestApp.Mediana or MyCompany.System that shadows a top-level name used in the emitted code can produce a compilation error in the generated MedianaRegistrar.g.cs. Ambiguity between the generated namespace Mediana.Generated and a user Mediana.Generated.MyType will surface as a build-time collision, not a security bypass. The generator method signature at :177 uses Mediana.MedianaConfiguration unqualified - same class name defined in a user namespace could shadow it.

There is no code-injection vector via identifiers: Fqn only consumes symbol display strings from the compilation, which cannot contain arbitrary text (Roslyn enforces C# identifier syntax and generic-argument grammar). No user string is ever interpolated raw.

Recommendation. Prefix every emitted type reference with the C# global-scope qualifier; change Fqn to prepend it for top-level types and to emit ContainingType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat) for nested types. Emit a fully-qualified Mediana.MedianaConfiguration in the method signature. Add a test with a user namespace MyApp.Mediana and a duplicate MedianaConfiguration class to lock this in.

### [Info] Generator matches interfaces by exact display string; user-declared Mediana.Handlers.ICommandHandler would be detected
Location: src/Mediana.Generators/MedianaGenerator.cs:85-119.

Description. Matching by display string rather than symbol identity means that if a user declares a namespace named Mediana.Handlers containing an ICommandHandler in TR interface in their own assembly, the generator will treat any class implementing it as a Mediana handler and emit a registration. C# compilation of the generated code will then fail with an ambiguity between the two ICommandHandler interfaces (or with a constraint violation on AddCommandHandler, whose constraint refers to the Mediana one). No runtime security compromise, but a confusing build break.

Recommendation. Compare against the actual INamedTypeSymbol of Mediana.Handlers.ICommandHandler looked up from the compilation via GetTypeByMetadataName with the arity-suffixed metadata name, and use SymbolEqualityComparer.Default.Equals(iface.OriginalDefinition, marker). Same for the other three interfaces.

### [Info] Generator package ships the analyzer DLL as both analyzers/dotnet/cs/ and lib/netstandard2.0/
Location: src/Mediana.Generators/Mediana.Generators.csproj:20-24; verified in artifacts/Mediana.Generators.1.0.0.nupkg (both analyzers/dotnet/cs and lib/netstandard2.0 present, per unzip -l).

Description. DevelopmentDependency=true is set (csproj:8), which prevents transitive flow of runtime references for consumers using PackageReference. Roslyn loads only from analyzers/dotnet/cs. The lib/netstandard2.0 copy exists (per csproj comment :18-19) to satisfy NuGet pack validation. Consumers using legacy packages.config or projects that do not honour developmentDependency would end up with Mediana.Generators.dll as a compile-time reference and, because Microsoft.CodeAnalysis.CSharp is PrivateAssets=all (csproj:16), fail to resolve Roslyn types at runtime - a broken build, not a security issue. No secrets or unrelated code ship in the DLL.

Recommendation. Replace the lib/netstandard2.0 copy with an empty placeholder file (lib/netstandard2.0/_._) which also satisfies NuGet validation without shipping the analyzer assembly twice. Also add IncludeBuildOutput=false and rely on the explicit Pack=true entry for analyzers only.

### [Info] AddHandlersFromAssembly reflection path is annotated for AOT and does not load untrusted assemblies
Location: src/Mediana/MedianaConfiguration.cs:128-209.

Description. The API takes an Assembly supplied by the caller; there is no Assembly.LoadFrom / Type.GetType(string) / probing. GetTypes() (:139) can throw ReflectionTypeLoadException which is not caught, but that is a startup crash, not a leak. MakeGenericType (:205, :177) closes call-site types with arguments already extracted from a real closed interface implementation on the handler (:156) - so the constraints are guaranteed satisfied by construction, and MakeGenericType cannot throw a constraint-violation exception. Activator.CreateInstance calls internal constructors with a two-parameter signature that always exists on the concrete call-site types. RequiresUnreferencedCode and RequiresDynamicCode are present on both AddHandlersFromAssembly (:132-133) and AddScanned (:200). AOT-safety and trimming annotations are complete for the reflection path.

### [Info] Handler exceptions are propagated verbatim on the local path
Location: src/Mediana/Mediator.cs:15-27 and interface doc src/Mediana.Abstractions/IMediator.cs:5-9.

Description. Documented behaviour: local Send propagates the handler exception as-is. Stack traces and any sensitive data in the exception Message will surface to the caller. This is a deliberate design choice matching MediatR and is not the mediator responsibility to sanitize - but consumers building HTTP layers over Mediana MUST wrap and sanitize. Worth calling out in the SECURITY.md and handler-authoring docs.

## Summary

| # | Severity | Title |
|---|----------|-------|
| 1 | Medium | Cross-request state leakage via captured next in pooled ChainState |
| 2 | Medium | Public MessageEntry + Unsafe.As of IEventCallSite array invariant not enforced |
| 3 | Low | lock(this) on internal call-sites reachable via public MessageEntry.CommandCallSite / QueryCallSite |
| 4 | Low | Silent no-op on Publish of unregistered event |
| 5 | Low | Middleware may skip next and substitute a response silently |
| 6 | Low | PublishParallel unbounded concurrency and no CT check |
| 7 | Low | MessageRegistry.Add O(N) per call, no built-in bulk add |
| 8 | Info | Unsafe.As at Mediator.cs:116 undocumented |
| 9 | Info | Generator emits type names without global-scope prefix |
| 10 | Info | Generator matches interfaces by display string (spoofable in user namespace) |
| 11 | Info | Generator nupkg ships analyzer DLL both under analyzers/ and lib/ |
| 12 | Info | AddHandlersFromAssembly reflection path - verified safe and AOT-annotated |
| 13 | Info | Handler exceptions propagated verbatim by design |

## Checked and OK (false-positive prevention)

- Only one Unsafe.* call site in the core (Mediator.cs:116). No unsafe blocks, no stackalloc, no Marshal, no fixed buffers, no pointer arithmetic anywhere in src/Mediana/* or src/Mediana.Abstractions/* (grep confirmed).
- Unsafe.As of IEventCallSite[] reads a field whose only writers are = [] (compiled to Array.Empty of IEventCallSite) at MessageEntry.cs:30 and list.ToArray() at MedianaConfiguration.cs:281. Under the current invariant the reinterpret is sound; MessageEntry mutation from public API is impossible (setter is internal, IVT is only to test assemblies).
- ChainState.Return clears Behaviors and Terminal (ChainState.cs:63-64) before returning to pool - no delegate/type retention beyond configuration. NextDelegate retention is by design (allocation amortization) and does NOT create a leak of DI scope or request instance across normal happy-path flows.
- ChainState.Take clears _pooled = null before use (ChainState.cs:78), so genuine reentrancy (handler synchronously calls mediator.Send for a different (TRequest,TResponse) - different generic means different pool slot; same generic means new instance allocated on demand) does not corrupt the outer request. The reentrancy hazard is limited to captured-next misuse (finding #1).
- RequestCallSites.Slow singleton composition is idempotent under lock+null-check (RequestCallSites.cs:145-158, :338-352). Once _root is set, subsequent readers observe a fully-published value via the lock release/acquire ordering. Non-singleton path allocates fresh behaviours and terminal per call - no cross-request leakage.
- EventCallSite uses a private _singletonLock and double-checked initialization (EventCallSite.cs:22, :50-56, :112-142). _bridge is published under the same lock; publication order (root -> _singletonRoot -> _bridge, EventCallSite.cs:139-140) is safe. No lock(this).
- Guard.NotNull is applied on every IMediator entry point (Mediator.cs:38, :73, :108, :130) before touching a possibly-null message. SendExact uses ReferenceTypeFlag.Value to avoid boxing struct requests (Mediator.cs:144-147).
- MessageRegistry.TryGet performs no allocation (net10 FrozenDictionary; ns2.1 bucket chain), no reflection, and no lock (MessageRegistry.cs:52-72).
- MedianaConfiguration.Freeze validates duplicate command/query/stream handlers (MedianaConfiguration.cs:234-258); event handlers correctly permit multiple (:268-282). Event policy is only applied to entries actually of kind Event (:284-293).
- AddHandlersFromAssembly does NOT load assemblies itself, does NOT accept type-name strings, does NOT invoke handlers via reflection at dispatch time. All MakeGenericType calls close types with arguments extracted from real closed interface implementations, so constraints are pre-satisfied.
- AddHandlersFromAssembly and AddScanned are RequiresUnreferencedCode / RequiresDynamicCode annotated. AOT smoke test (tests/Mediana.AotTests/Program.cs) exercises the fully-generic path (no reflection escape hatch) and passes on ubuntu-latest per CI.
- PublishParallel exception aggregation (Mediator.cs:176-215): sync throws during Invoke are caught and aggregated (:190-194); async faults are caught in the await loop (:201-208); default(ValueTask) slots (from sync-throwing invokes at index i) are safely awaited as no-ops. No exceptions escape unwrapped except the aggregate.
- No hardcoded credentials, connection strings, API keys, endpoints, or private data anywhere in src/Mediana*. No Console.* writes, no Environment.MachineName leak, no Type.GetType(string), no Assembly.Load*, no Expression.Compile, no Emit, no BinaryFormatter, no Newtonsoft, no eval-like path (grep confirmed against context.md inventory).
- Generator determinism: handlers.Collect() produces a compilation-order stable ImmutableArray; Emit iterates in that order; MED001 diagnostic is deterministic per (Kind, MessageTypeFqn) set (MedianaGenerator.cs:141-165). Tests tests/Mediana.GeneratorTests/MedianaGeneratorTests.cs cover command/query/event/stream generation, multiple event handlers allowed, duplicate command producing MED001, non-handler-class ignore, abstract-class ignore.
- Generator open-generic and abstract classes are excluded (MedianaGenerator.cs:66: symbol.IsGenericType || symbol.IsAbstract).
- Generator does not use any file I/O, network I/O, or process spawn; it only reads syntax/semantic model and calls AddSource. EnforceExtendedAnalyzerRules=true is enabled in the csproj (line 11).
- Stray file src/Mediana/Dispa (mentioned in context.md) has no extension and is therefore NOT compiled by the SDK default Compile Include glob. It contains an outdated RequestCallSiteCompositor (references non-existent RequestHandlerDelegate / IPipelineBehavior); if it were compiled it would not build. No runtime effect. Cleanup recommended but not a security issue.
