# Host-supervised future runs

`Store::supports_future_queue` declares native atomic queue support. A custom
adapter defaults to `HostUnsupported` and must implement the whole transaction
contract before accepting queued input. Current bundled storage requires PostgreSQL 24 and a fresh SQLite 20 file
(`POSTGRES_SCHEMA_VERSION` / `SQLITE_SCHEMA_VERSION` in `crabber-session`).
The Queue tables were introduced in PostgreSQL 10 / SQLite 6. Connect never migrates.

The future-run queue is separate from the inbox. Enqueue creates no Run, message,
provider request or execution handle. It requires an existing session, immutable
workspace identity and the caller's captured `InputEnvelope` revision. The queue
allocates its monotonic per-session sequence transactionally. Pending input is
limited to 64 items and a 32 MiB enqueue budget, with a separate bounded
32 MiB slot reserved for the head's preparation; public pages accept
1–64 entries and enforce a 256 KiB metadata budget.

The host owns dispatch scheduling:

1. Call `Agent::enqueue_session_input_in` for managed defaults, or
   `enqueue_input_in` with an explicit `ExecutionContext` selection.
2. Submit a keyed `QueueAction::Claim` using the latest session revision. Only a
   fresh `QueueOutcome::Applied` contains a private `QueueAuthority`. Save it in
   the dispatcher; a receipt replay never reconstructs it.
3. Call `prepare_queue_in` with that authority and a stable `QueueControl`.
   Defaults, AgentConfig fallback and reflected execution configuration resolve
   at the first successful preparation. Enqueue's implicit profile is not frozen.
4. Call `admit_queue_in` with a separate stable operation ID, live authority and
   the latest session revision. A fresh admission returns `Admission::Started`
   with the actual owned RunHandle. Retain and supervise that handle normally.

Set `QueueControl::admission_lease` to a validated `QueueAdmissionLease` for
host-selected initial run leases. `QueueAdmissionLease::default()` retains 30
seconds; `QueueAdmissionLease::new(Duration::from_secs(60))` selects 60 seconds.
Durations must be positive and at most one hour. Preparation does not use this
policy. Admission persists it atomically with the run; replay preserves the
original deadline and rejects a changed lease under the same operation ID.

`dispatch_queue_in` combines steps 3 and 4 for an unprepared claimed head, using
its prepare receipt revision as the admission precondition. Keep both operation
IDs and the original preparation control to reconcile an unknown response. A
raced management/state mutation returns a revision conflict; the host chooses
when to retry with a new operation and a newly captured revision.

For implicit defaults, absent provider and model values independently use the
first preparing Agent's fallback values. Invalid provider/model combinations
fail normally. Explicit selection and the caller-supplied admission behavior
fingerprint are immutable enqueue intent. Preparation freezes the original
input envelope, provider descriptor, resolved defaults/fallback, execution plan,
configuration and private keyed-admission capsules. Reflected configuration
before preparation may change. Once prepared, ordinary recovery compatibility
checks apply; unavailable or changed implementations do not silently substitute
new behavior.

Only the FIFO head can be claimed or admitted. A live claim is exclusive. An
expired unadmitted head is reclaimed with a new operation/token/generation using
the Store clock, preserving its QueueId, sequence, admission key and prepared
winner. It cannot be skipped. A reclaimed prepared head uses `admit_queue_in`
directly; preparation under a fresh key cannot overwrite its winner.

Committed preparation excludes fresh ordinary admission. An unprepared claim
can lose a race to ordinary work; preparation then refuses until that work
settles. Queue admission keys are reserved against ordinary keyed admission.
Pending continuations have priority over queue claim, preparation and admission.
Archived sessions refuse fresh queue work but permit original-key replay and
administrative cancellation. Existing-session checks provide the integration
point for later tombstones; this slice does not implement deletion.

Every queue mutation atomically advances SessionRevision, records a shared
operation receipt and a session audit without a synthetic Run/event cursor.
Queue admission additionally writes the real admission receipt, retained binding,
Unstarted execution evidence and queue-to-run mapping in that same transaction.
Latest immutable queue receipts authenticate current state, with independent
original enqueue, preparation and claim audits protecting retained evidence.

Unknown claim responses grant no authority. Reconcile the original key and, if
necessary, wait for expiry before explicitly reclaiming. Unknown preparation
responses reconcile the original key and retained winner. Unknown admission
responses reconcile the original admission mapping; replay never starts a worker.
Only `recover_queue_in` may explicitly recover an expired, positively Unstarted
admission under the existing recovery contract. Started or uncertain execution
is never dispatched again automatically.

Cancellation before admission revokes the claim and prevents execution. After
admission it returns the original mapping and does not cancel the Run. Cancel
that Run through the explicit lifecycle controls when required. A closed Agent
refuses fresh enqueue/claim/preparation/admission; original-key metadata replay,
reads and administrative cancellation remain available.

Public `get_queue`, `list_queue` and receipt DTOs contain no input, checkpoint,
owner, lease, token or execution capsule. `queued_intent` is an explicitly private
host API requiring its admitted SessionContext; reading it never grants authority.
Keep private intent and QueueAuthority out of frontend DTOs and logs. No provider,
factory or executor work runs while a database transaction is held.
