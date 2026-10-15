# Configured server-host contracts

The durable-host API is under implementation and is unstable until the public
consumer and dependency tuple are qualified. The currently implemented context
entry points preserve one configured Agent's registrations, store, policy and
prompt settings while selecting an admitted workspace/session and model for each
request. The server owns root authorization and allowed model selection.

`SessionContext` contains immutable session, workspace and directory identity.
`ExecutionContext` combines it with an explicit `Selection`. Identity comparisons
use exact strings: no path normalization, filesystem lookup or process `chdir`.
Empty persisted workspace fields remain empty and are exposed as unavailable by
native tool workspace context; agent defaults never replace them.

Call `Agent::ready` to initialize shared registrations once. A failed partial
mount rolls back the mounts already installed in that initialization attempt.
`prompt_keyed_in` can atomically create a new host-selected session ID with its
first admission. `prompt_in` is the unkeyed entry for an existing session. Both
accept an optional host trace context separately from execution routing.

Retain the original execution context, input and `AdmissionOptions` after an
unknown response. `recover_admission_in` verifies those original semantics before
claiming a retained unstarted admission. An identical terminal retry replays a
receipt and grants no new executor. A changed root fails before extension setup;
the store repeats its identity check inside admission to protect concurrent
creation. A changed selected model under the same key conflicts even when a
caller reuses a stale claimed fingerprint. See [admission receipts](admission-receipts.md)
for canonical hashing, unknown-outcome reconciliation and behavior versioning.

`Scope::Workspace` and `Scope::SessionIn` select shared registrations using the
context supplied to plan acquisition. Scope filtering does not create independent
mutable state. Plan fingerprints use only the ordering of applicable mounts, so
an unrelated session's mount cannot break recovery. Handler identity includes its
owning extension, so changing the order of relevant handlers still changes the
fingerprint. This changes fingerprints for plans containing handlers or legacy
prompt contributors. Finish or settle old paused work with its matching old
binary before upgrading; it must not execute under an incompatible plan.

`close_extensions` now stops ingress even for an agent with no extensions.
Readiness and new prompts return `RuntimeError::HostClosed` once closing starts.
It closes extension resources and leases; it does not join active executions.

Scoped factory instantiation, immutable provider-binding restoration, the common
owned initial/resumed execution lifecycle, typed admission, authoritative controls
and session management remain part of this implementation. Their completion is
required before claiming the durable server-host contract delivered.

Current regression coverage is in `crates/crabber/tests/server_host.rs` and
`crates/crabber-extension/tests/context_plans.rs`: concurrent explicit workspaces,
preserved custom registrations and defaults, exact retry and root drift,
unavailable workspace values, terminal ingress closure, and fingerprint ordering.
