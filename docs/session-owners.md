# Session resource ownership

`Store::supports_session_owners()` is available on Memory, PostgreSQL schema 13,
and SQLite schema 9. The SQL adapters persist ownership across controller
restarts. Hosts explicitly reserve and begin an owner before starting the
corresponding resource, and report its actual join after cleanup finishes.
This ledger does not automatically register every runtime or factory resource.

`ReserveSessionOwner` binds one resource ID to an exact session, workspace,
directory, component, behavior fingerprint and optional native execution parent.
A preparation can be reserved before a Session or Run exists. The reservation
stores an immutable session context without creating either object. Later
managed creation and ordinary admission must preserve that context. Opaque
workspace and directory values, including newlines, are preserved; NUL cannot
be stored. Session IDs accept bounded opaque Unicode.

Operation IDs have a separate typed ownership namespace. An exact retry returns
its original receipt before checking current state, archive status, leases,
parent authority or context. Changed semantics conflict. Reserve lease duration
is an acquisition preference, excluded from its stable semantic identity.

Only a freshly applied Reserve or ReclaimUnstarted returns private authority.
Receipt lookup, owner snapshots and retries return metadata. After an unknown
acknowledgement, reconcile the original key; an original receipt cannot recover
a lost token. An expired reservation that never began can be explicitly
reclaimed into a new generation. Started work requires explicit uncertainty or
join resolution. Expiry alone never establishes cleanup.

`Begin` requires the exact unexpired authority. `Renew` preserves the public
snapshot and session revision. Other public changes advance the session's public
revision when that Session exists. An unchanged expired authority can still
report its actual `Join`; a rotated generation cannot. Host `ResolveJoined`
records the supplied resolver identity, authority claim and termination reason.
It requires an expired lease and represents the host's explicit cleanup report.

Detached invocation reservations additionally authenticate the actual native
Started attempt, or Started phase and its Running logical call. Store-clock
leases, run and attempt tokens, generation, call binding and original native
Begin receipts/events must agree. Independent Begin witnesses and a Run fence
journal prevent restored mutable rows from reviving an old parent after
revocation or takeover. Ordinary private lease renewal keeps the same issuance.
Legacy executions without an original witness cannot grant a new parent handoff.

SQL operations use the session mutation gate, including when no Session exists.
Each current owner is checked against its original reservation and the
independently latest immutable audit. Context checks corroborate the first
reservation. Identity anti-joins detect missing or substituted current owners.
Capacity is 1,024 non-Joined owners per session, derived from independently latest
audit state. Historical private audit bodies are not scanned to reserve another
owner. Selected current/audit bodies are bounded to 32 KiB before transfer;
context bodies to 32 KiB. Parent Run proof reads transfer at most 16 KiB of actual
metadata, excluding checkpoint and error content, and corroborate indexed
identity, status, token and lease. Corrupt or oversized selected evidence refuses
the operation with a sanitized unknown-storage outcome.

PostgreSQL reads use a read-only Repeatable Read transaction; SQLite reads use a
reader transaction. These metadata reads never return parent credentials, owner
tokens or execution output, and never grant authority or infer resource joins.

`Store::supports_preparation_protocol()` identifies adapters supporting atomic
preparation completion. The ordinary and keyed admission, queue preparation and
admission, and explicit approval/tool continuation claim and Unstarted recovery
paths use this protocol. A preparation retains the original input, operation key,
intent digest and caller revision. Reserve checks that revision before factories
run; Begin grants one fresh preparation. Original receipt lookup precedes fresh
CAS, archive and ingress checks. Changed retries reach the same deterministic
preparation identity and conflict instead of creating another owner.

Preparation ownership publications record their actual before/after session
presence and revision in version 2 private audits. Completion authenticates at
most 64 original publications of that exact owner, allowing its original Reserve
and deterministic Begin; private Renew does not publish a revision. Before a
Session exists, an independent immutable public-publication clock
advances instead of fabricating a public Session revision. Existing SQL audit
ordinals and Memory's append-only publication order also reject foreign changes
after the original Reserve; deleting older history cannot balance the count and
hide a newer foreign publication. SQL ignores a claimed private Renew only after
corroborating its bounded original receipt, publication and routing; malformed or
mismatched evidence refuses completion. An unrelated public change, including
another owner change, invalidates completion. The original request and native semantic digest remain unchanged. The effective
revision exists only inside the transaction that verifies the lineage.

The runtime retains the factory/validation task and observes its actual join.
The joined task supplies the exact native intent. In one Store transaction,
completion validates that intent against the reservation, publishes the
preparation Join, and commits the native successor. A prepared value or dropped
plan is insufficient to establish resource cleanup. Queue and continuation receipts
include the final public revision. Queue preparation therefore needs no later
Join publication that would immediately stale its receipt.

Continuation preparations corroborate their exact original waiting or expired
Unstarted native source before granting preparation authority. Merely finding a
bound Run does not authorize factories or exempt ordinary archived work. The
native commit rechecks eligibility, fence, generation and original caller CAS.
No successor worker is dispatched from a metadata replay or unknown response.

`RuntimeError::PreparationIncomplete` preserves the underlying error and a safe
`PreparationReconciliation` descriptor: exact scope, owner/generation and original
Reserve/Begin/Join operation IDs. `operation_error()` classifies the original
failure. `lookup_preparation_operation` returns the original descriptor without
factories or authority, including after local close. Unknown acknowledgements
must reconcile original native and owner keys. Failure does not automatically
Join the owner: unless all tasks/resources actually joined, its durable Active
or Uncertain state remains a cleanup blocker. An external native writer's winning
receipt cannot conceal an unretired preparation.

This checkpoint does not adopt generic resume or ordinary admission recovery
into the atomic preparation protocol. Their original historical admission
receipt remains historical metadata. Linked session services, contextual
initializer/reaper lifetimes, provider/factory resource transfer and interrupted
settlement callbacks require further ownership integration. Explicit standalone
initialization remains separate. This protocol does not establish complete
session resource coverage, remote cancellation, deletion readiness, or that a
locally empty task registry means no remote work remains.

A host retaining the actual joined completion and its resources may retry a
failed native transaction with that same completion and key, without rerunning
factories. Alternatively, after actual cleanup, an authenticated Join retires
the original preparation and new work needs a new key and captured revision.
Original preparation receipt replay never grants another Begin. The runtime
currently reports failure correlation and drops its retained completion on
error; finite runtime resource disposition and recovery remain required
follow-up work. Native retained-completion tests do not establish runtime
failure recovery. Descriptors alone grant no cleanup authority.

`Store::supports_session_owner_cancellation()` identifies adapters with durable
owner cancellation. `RequestSessionOwnerCancel` targets one exact owner and
generation. `expected_revision: None` requires that the Session is still absent;
`Some(R)` requires the existing Session at R. The host authenticates its caller
and supplies a bounded resolver assertion. Storage records that assertion
privately. One accepted request remains attached to the owner for its lifetime;
its original key replays metadata, while changed semantics conflict. Requests
and observations publish a public revision, or advance the immutable absent
publication clock before a Session exists. Both invalidate preparation lineage,
including when they target the preparation itself.

`pending_session_owner_cancel` authenticates the retained private authority.
`ObserveSessionOwnerCancel` acknowledges that the actual supervisor has signalled
its local cancellation token. It preserves owner state, token and lease. An
expired but unchanged authority may acknowledge that signal; a rotated token or
generation refuses it. Observation grants no right to resume effects, renew a
lease, reclaim ownership or publish a Join. Cancellation also blocks Unstarted
reclaim. Immutable original request and observation audits supplement current
proof checks; selected carriers retain the existing 32 KiB cap. Original-key
receipt replay runs before current-state validation and returns no authority.
Optional cancellation fields are omitted for uncancelled records. Older strict
readers reject cancelled records and the new operations instead of silently
stripping their metadata.

The runtime's retained session-invocation supervisor checks pending cancellation
after Begin acknowledgement and before calling the task factory, then every five
seconds while the task or its child cleanup remains outstanding. It cancels the
actual token before publishing observation and retries an unknown observation
acknowledgement with the same operation key. Task factories never rerun for that
retry. Cooperative return still requires actual task and resource joins before
Join. Noncooperative, killed or remote workers remain durable blockers. A cold
Agent can request cancellation and read safe owner/receipt metadata, including
after local extension close; it cannot signal a remote process by itself.

Preparation checks pending cancellation after fresh Begin acknowledgement and
before its factory task starts. An already pending request returns
`PreparationIncomplete` with the original descriptor and leaves the owner Active.
There is no task or local signal at that boundary, so it publishes neither an
observation nor a Join. Cancellation of already running preparation factories,
retained preparation resources, and linked services requires the later resource
ownership integration. This checkpoint establishes no complete coverage or
deletion readiness.

`Store::supports_preparation_resources()` and
`retain_preparation_resources(original_grant)` provide a separate, independently
owned capacity record before a factory dispatch. The Store derives its child
identity from the original preparation generation, then atomically publishes
its Reserve and Begin. Its immutable source includes the original root
Reserve digest and Begin audit; matching component labels or caller-supplied
child authority alone cannot establish that link. Exact registration replay
returns original metadata only, including after root completion. Unknown
registration acknowledgements never grant replacement authority.

A root using `OWNED_PREPARATION_COMPONENT` requires that authenticated Active
child for native completion. Only the original root lifecycle and that exact
child's Reserve and Begin may contribute to the original caller revision.
Unrelated changes, child cancellation and observation remain conflicts. The
child's private Renew changes no public revision. The atomic native transaction
records the root's exact child handoff in its Join audit. Its distinct native
handoff digest binds the original Join command, source link, and child. Both
adding that private marker to a cleanup Join and removing it from a native Join
invalidate the original receipt proof. Generic cleanup and legacy Join digests
remain unchanged. The child remains
Active after native success, unknown acknowledgement, and metadata replay;
root Joined does not mean resource cleanup. Native receipt replay requires the
original handoff Join rather than a later generic cleanup Join.

A host that really joined failure cleanup may still Join the root without a
native handoff. Such cleanup is not positive resource coverage and cannot
substitute for a native completion handoff. The separate child must itself be
retired through authenticated actual cleanup or an explicit resource transfer;
plan disposal, lease expiry, and missing local tasks establish neither. Strict
optional fields preserve legacy records when absent and refuse incompatible
new records in older readers. This Store protocol does not yet change Runtime
factory ownership or implement its retained completion registry, shared service
leases, or complete session coverage.
