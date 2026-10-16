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

The owned-scope primitives register a waiting `FnOnce` task before storage
activation. `activate_owned_scope` independently authenticates the original
Reserve/Begin sources, exact generation, live owner and any native parent, then
publishes `ActivateScope` as a public revision change. Only freshly acknowledged
Applied produces the nonserializable, single-use local dispatch permit. Receipt
replay and unknown acknowledgement cannot open a gate. The preparation guard
accepts only its authenticated child's exact activation as an additional own
publication; foreign controls remain revision conflicts.

Main-task and cleanup-task observations retain their actual join handles across
interrupted observation. `join_owned_scope` accepts the opaque observation only
after both the main task and the trusted host's sealed-resource cleanup task
have actually joined. Cleanup must include every child and independent dependency
release acknowledgement. Its source-bound digest distinguishes this conclusion
from generic cleanup. Failed cleanup remains an error and retained responsibility;
no timeout, returned value, plan disposal or lease expiry supplies join evidence.
An observed never-opened gate can record exact NoDispatch cleanup after either
activation rollback or committed-but-unacknowledged activation. NoDispatch without
an independently present original activation remains coverage-unknown. The host
must retain exactly one original registration per fresh Reserve/Begin; recreating
a never-opened registration cannot account for an earlier live task. Memory's
independent publication roster rejects an erased known activation. Stronger
cross-adapter proof of a zero-activation history requires a separate protected
initialization/disposition journal; this protocol does not provide that proof.

These primitives are structural evidence, not retroactive ownership coverage.
They do not establish that a legacy Begin never dispatched earlier work, that a
provider declared a scoped bridge in its frozen binding, or that every shared
service was tracked. Generic reservations and cleanup remain coverage-unknown.
The runtime consumes a provider's optional preparation-resource declaration for
ordinary admission, queue preparation/admission and typed continuation claims or
recovery. Its fingerprint is captured before reservation/effects in the original
preparation origin and a reserved, versioned frozen-runtime marker. Completion
requires the exact task-returned intent and registered activation; a generic
joined task cannot substitute. Replay/restoration reject changed declarations,
including a legacy binding gaining a bridge later. Unsupported scoped paths
refuse before factory acquisition. Providers without a declaration keep their
normal legacy behavior and unknown resource coverage.

At most 64 local preparations retain actual completion/resources. Failed or
unknown native completion keeps its returned plan and original opaque proof;
`retry_preparation_completion` retries that intent without factories or business
dispatch. Its result is metadata only. In particular unkeyed replay supplies no
replacement fence/RunHandle: its original Running/Unstarted run remains a blocker
until explicit eligible native reconciliation or abandonment. A plan lifetime
lease only signals the independently retained cleanup supervisor. The child stays
Active until the actual main task and every registered sealed resource join,
followed by the exact durable Join acknowledgement. Cleanup ACK loss retains
those observations for explicit retry, which checks the original Join receipt
before another write. An actually failed cleanup records a stable keyed
MarkUncertain while retaining its failed task result and remaining resources.
Unknown Join acknowledgement never becomes cleanup failure, and reconciliation
never overwrites a committed Joined state. Timeout alone supplies neither
uncertainty nor Join evidence. Running-factory cancellation applies the
actual cooperative scope-close signal before recording the original cancellation
observation; non-cooperative work and failed cleanup remain blockers.

`cleanup_retained_preparation` requests actual scoped cleanup after a failure;
it does not report Joined. `retained_preparation_status` is bounded local metadata,
available alongside original-key reconciliation after ingress closes. Empty local
status does not prove cross-controller or durable quiescence. Queue preparation
receipts retain their historical commit revision; later actual child cleanup is
another public change, so dispatchers must capture a fresh caller CAS for admission.
No business receipt or original input revision is silently refreshed.

This remains partial ownership adoption. Generic resume/admission recovery that
lacks typed preparation, facade initialization before reservation, resolver/provider
binding acquisition, contextual initializer/reaper/shared-service dependencies and
post-settlement callback ownership still need explicit protocols. An original
bridge declaration or a native preparation witness alone does not establish those
joins or certify whole-session coverage. Legacy owner/invocation rows stay unknown;
none of this enables deletion or upgrades old history retroactively.

### Expired keyed admission recovery

Bundled Stores expose `inspect_admission_recovery` as a coherent, read-only
eligibility check. It authenticates the original admission Init, receipt and
frozen execution binding, the current native issuer and fence, an expired
Unstarted lease using the Store clock, and the current session revision. The
returned `AdmissionRecoveryPreparation` carries metadata only. Unstarted Run
carriers are limited to 32 KiB before transfer and must contain no executed
checkpoint or error; recovery never silently drops or resets those fields. Custom adapters
that have not implemented this read return `Unsupported` before Runtime restores
providers or acquires plans.

Runtime uses that captured source to reserve and begin preparation before
restoration. The actual joined preparation commits its original root Join and
native admission claim in one transaction. A version 3 native Run issuance binds
the new fence to that exact original Join and recovery intent. Competing legacy
claim methods cannot pass an active preparation. Source rotation before Reserve
refuses the stale preparation; changes after Reserve must pass the same native
source and revision checks at commit.

Completion retries authenticate the exact old and successor issuances, original
Init and receipt, and original preparation source audits. They return only the
original admission receipt, even after execution starts or the Run settles. That
historical receipt is unchanged and contains no new fence. Unknown completion
ACKs keep the returned plan and resource child in the retained registry until
explicit retry and actual cleanup. Retrying completion never runs the factory
or business operation. A new controller may explicitly recover a still
Unstarted admission only after its current lease expires and a new authenticated
source inspection succeeds.

This protocol covers keyed admission recovery preparation. Generic expired-Run
resume, complete-history verification of continuation claims, normal
provider/service ownership, initializers, reapers and retained callbacks still
require their own adoption. A component resource declaration or native terminal
state does not establish whole-Run resource cleanup or deletion readiness.

### Prepared continuation claim sources

Prepared approval, checkpoint-tool and frontend claims and their Unstarted
recoveries now associate the actual rotated native fence with the original
preparation Join and full original control fingerprint in a version 3 issuance.
Approval claim retains its real Paused issuance followed by the existing
same-token Running status publication; approval recovery and tool/frontend
activation retain their real Running state. Legacy direct controls keep their
original issuance format.

Before committing a fresh prepared continuation, bundled Stores corroborate
its exact original Reserve/Begin/Join audits, adjacent native issuances and
published native control receipt/event in that same transaction. The shared
borrowed historical verifier authenticates those sources without a current Run
or a capability. It treats legacy or unsupported domains as unproven. Neither
this association nor an arbitrary resource declaration certifies resource policy,
physical worker cleanup, whole-Run coverage or deletion readiness. Generic
expired-Run preparation remains a separate follow-up.

The per-claim historical result is not a complete-history verdict. A streaming
reader must also require strictly increasing authenticated native control event
cursors across rotating claims, rejecting duplicate or borrowed later sources.
Status-only same-token publications are separate from rotating claims. Current
historical reports keep noninitial claims unproven until that reader adopts the
source verifier and its chronological checks.
New PostgreSQL 18 and SQLite 14 owner reservations establish a publication cohort
in the same transaction as the actual Reserve. Its original source and monotonic
head anchor an independent transition roster, including private renewals. Existing
owners remain legacy; later mutations cannot assign them this protocol. Service
dependency exposure requires authenticated fresh cohorts for the relevant
preparation and service owners, so missing history refuses exposure.

The roster retains a second exact copy of each bounded private audit (at most
32 KiB) and its digest. SQL compares all historical audit/roster slots and the
monotonic head, while only selected original/head bodies are decoded by the
client. These reads share the existing 32 MiB encoded-carrier proof allowance.
That allowance does not bound database storage growth, server-side history scans,
SQL equality work, or whole-operation memory and execution time. This protocol
adds source integrity; it does not certify physical service or whole-Run cleanup.

Service dependencies additionally require an independently retained scope origin.
Actual new managed creation, fork creation, or the first preparation reservation
in an absent scope records that origin atomically. The origin anchors the full
canonical creation receipt and audit, or the original Reserve publication. A preparation origin survives
when admission creates its Session. Existing session and owner-context rows retain
a legacy marker; neither a new preparation root nor an empty current roster
upgrades them. Any legacy owner prevents fresh service exposure. MemoryStore
starts a fresh independent ledger and does not import persisted legacy state.

Database upgrades require stopped hosts with no retained resources. This protocol
does not establish continuity during a hot upgrade or prove the absence of
resources after arbitrary erasure of all pre-protocol metadata and context.
Legacy service scopes return Unsupported; hosts must not use that refusal to
fall back to an unowned factory.


### Runtime session-service dependencies

An explicitly declared owned preparation receives a grant-bound
`PreparationResourceContext::service_dependencies()` capability. The Runtime
manager retains its supervisor before native registration I/O and admits the
exact dependency task only after fresh native activation and an acknowledgement
from that task's guarded constructor. A preparation capability cannot admit new
tasks after its actual initializer future returns or is dropped. This seals
local ingress; it supplies no main or resource Join evidence. An already
transferred dependency continues independently after the preparation ends.

A later preparation checks the original live dependency under its own current
grant. It never replaces the original source, task, generation or authority.
Unknown registration retains its original opaque control and generation without
running a factory. Metadata replay carries no activation authority and cannot
become a new acquisition. A lost activation ACK keeps the local gate closed.

Contextual session close seals that session's dependency and waits for its
actual acquisition references, dependency task, registered resource tasks and
native Join acknowledgement. A timeout preserves those same joins for retry.
Failed or panicked cleanup remains Uncertain. Releasing session A does not
close a shared/workspace generation or session B's dependency; physical
workspace/service cleanup remains a separate lifecycle.

The built-in Contextual provider still declares no automatic owned preparation
contract. Advanced embeddings can supply an explicit `RunPlanProvider` bridge
and delegate owned acquisition to Contextual. This component owns the session
dependency only: legacy shared mounts, provider pools, arbitrary callbacks,
physical generation coverage and whole-Run deletion readiness remain separate
requirements. A provider-reference release is not a service-shutdown proof.


### Prepared generic expired Run claim

Bundled Stores expose `inspect_expired_run_preparation` for an exact expired
native fence. This pure coherent read authenticates either an original checkpoint
suspension conclusion and event, or a protected begun keyed admission with no
parent reservation. It captures current R, the full original issuer digest,
frozen capsule digest, original source receipt and chosen replacement owner and
lease. Unsupported or ambiguous original sources fail before restoration.
Existing Unstarted parent reservations remain unsupported by this component.
Run carriers are capped before private clone or SQL transfer;
checkpoint suspension inspection accepts at most 64 calls and 64 phases.

Fresh Reserve, Begin and atomic completion revalidate the original native source.
Live prepared roots block competing legacy claims. Actual retained-task join and
original preparation Join commit with the rotated native fence in one transaction.
The v3 successor issuance binds the exact predecessor and original Join/source;
no historical receipt is rewritten to the current revision.

`lookup_expired_run_preparation(session, run, original_issuance)` authenticates the
original source, preparation audits and actual successor without reading current
Run state. It returns bounded metadata only. Completion replay likewise cannot
mint a fence or run factories, including after lost ACKs or later R changes.
This checkpoint supplies native Store primitives only. Runtime generic resume,
successor driver ownership and normal Static/provider/service adoption still need
their own integration; this association does not establish physical cleanup,
whole-Run coverage or deletion readiness.

The current inspection and reconciliation limits apply to individual carriers.
They do not establish one aggregate 32 MiB proof budget or bound the complete
Memory State clone. A future aggregate/history consumer must thread its shared
caller proof budget through every selected source before transfer or cloning.

SQL execution-driver registration and activation carry one bounded proof allowance
through their selected original sources, owner-history checks and publications.
Original audit metadata is compared with its independently protected publication;
a validly encoded replacement or a version downgrade cannot substitute for it.
Reading that original metadata does not validate later current owner state or its
head and cannot issue a grant. Fresh registration additionally requires the
protected preparation-root and child cohorts; activation requires the driver
cohort and the original Begin revision fence.

The initial Runtime driver retains its actual main and resource cleanup tasks
outside custom Store future unwind boundaries. An activation panic leaves its
gate closed; cancellation or a control panic starts real resource closure while
the main still runs. The cleanup task remains alive until the main has actually
joined and a final drain covers scopes registered later. Native Join retries
reuse that same physical evidence and validate the original termination and
creation binding. A Store response carrying another Run's genuine grant cannot
dispatch this Run's work. Cancellation can start resource closure during a
pending control Store call, while the original poll remains retained and is
polled to completion alongside the actual main join. Retirement waits for that
call too; a replacement poll or dropped future is not completion evidence.

A selected execution-driver owner can be classified after its genuine main and
resource-cleanup observations were durably joined. SQL readers require both the
complete protected cohort and the exact protected selected record; coherent
mutable Joined rows cannot supply that evidence. The original admission sources
are checked under the same caller allowance. This component classification does
not settle the admitted Run or establish complete issuer membership, terminal
outcome, callback cleanup or whole-run resource policy.

An expired initial unkeyed Run has an explicit `UnkeyedAdmission` source only
when its original version 2 Witness authenticates the first native issuance,
original preparation Reserve/Begin/Join and linked child receipts, and the full
frozen unkeyed input binding. Missing keyed initialization is never evidence of
that source. Legacy witnesses without the input anchor and bare unowned admissions
remain unsupported. Lost initial admission ACKs replay only admission metadata;
a host may then explicitly inspect the current expired native fence and prepare
recovery using this independently authenticated original source.

History readers authenticate an explicitly unkeyed initial input through its
original v2 preparation witness, actual RootJoin and first native issuer. They
compare the complete retained execution capsule with that original digest under
the caller's proof allowance. A coherent rewrite of the current binding and Run
hashes fails this check. A missing keyed journal never establishes an unkeyed
source, and a missing required binding remains unknown. Original input evidence
is separate from main joins, terminal outcome and whole-run resource policy.

Fresh initial-driver registration and activation require protected current Run
membership on all three native backends. These checks consume the same caller
proof allowance as the original admission and owner cohorts. Legacy inventory
lookup cannot substitute for this fresh-execution guard, and authentic original
metadata replay remains independent of later current history.

Fresh native Runs retain a protected first-source marker, immutable issuance
hashes and a monotonic source head. A bounded history fold checks every slot
against that head before fresh execution. Erasing a later issuance and its
control receipt, then restoring an older Run or attempt, cannot restore dispatch
authority. Legacy Runs without this independent membership proof remain
unproven for fresh execution and complete historical coverage.

Original metadata lookup checks the exact protected slot and its original chain
links. Damage to later history does not prevent replay of an authentic original
receipt. Session and retained pre-session context era markers preserve legacy
identity boundaries; public revisions cannot move backwards, and live identities
cannot be deleted or moved to create a new source era. Permanent tombstone purge
removes dependent source heads and slots in the same transaction. Complete native
source history supplies one history fact; main-task joins and whole-run resource
policy still require their own evidence.

SQL prepared native transactions retain the same proof context through preparation
source verification, root Join publication, initial source publication and later
issuer changes. Standalone native calls create their allowance at the outer entry.
The issuer publisher honors the caller's remaining allowance before reading or
hashing its carriers. Existing native ready/load/provenance helpers still include
legacy readers, so this source allowance does not establish a bound for every
carrier in the entire native transaction.
