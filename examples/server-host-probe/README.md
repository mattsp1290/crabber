# Offline configured server host

This executable runs real public Agent/Store flows with a scripted local provider.
It needs no credentials, HTTP server, browser, AG-UI dependency or global `chdir`.
The embedding owns the Tokio runtime and injects its Store through `AuditedStore`,
a complete public forwarding adapter. Optional capabilities on an incomplete
adapter remain Unsupported; the probe checks that it cannot start provider work.

```sh
cargo run -p server-host-probe -- memory
cargo run -p server-host-probe --features sqlite -- sqlite /explicit/fresh/host.db
cargo xtask build-fixtures
cargo test -p server-host-probe --features wasm --test shared_wasm
CRABBER_REQUIRE_POSTGRES=1 cargo test -p server-host-probe --features postgres
```

For PostgreSQL, set `CRABBER_TEST_POSTGRES_URL` to a dedicated test database first.
Use a fresh database migrated by this checkout. Migration is explicit.
`connect` does not create or upgrade storage.

The assertions verify actual behavior:

- One configured Agent uses two immutable workspace/session contexts. A custom
  permission policy allows a session-scoped counter tool. Two factory instances
  each execute once, retain separate state, and join their cleanup on close.
- A versioned native tool suspends twice and accepts authenticated, schema-checked
  host answers. Reconstructed Agents resume the exact checkpoint; the effect
  before the first checkpoint runs once. Answer and continuation retries replay
  metadata. Reconstruction here is in one process, not a process-crash proof.
- Frontend descriptors pause twice without backend executors. Reconstructed Agents
  submit authenticated external results, persist the exact tool results, and
  replay both continuation receipts without provider work. Original typed keyed
  input also reconciles through admission and recovery metadata replay.
- A separate fixed Memory host runs keyed and unkeyed pristine library sources
  through parked start, actual join and supervisor drain, then deletes each
  completed session with its current revision and replays that deletion. Any
  genuine returned Seal retry is retained and reconciled on the same handle;
  the normal probe does not inject lost acknowledgements. A third parked task is
  stopped before dispatch and joins without changing its managed session; it is
  not counted among the two completed fixed sessions.
- An explicitly shared extension executes a tool that reads the real
  `shared_host_services` capability. Registration and the service each run once.
- The feature-gated WASM test loads the actual SHA-256-checked local echo
  component through `shared_wasm_extension`, executes it, and checks the exact
  stored result and next model request before closing extensions. `cargo xtask
  check` builds the fixture and runs this test explicitly.
- After close, pure controls fork verified closed history, read imported calls
  across coherent V2 pages, and atomically delete the quiet fork. The original
  generic executed session remains blocked: its scoped cleanup does not certify
  whole-runtime coverage. The separate fixed Memory path verifies its narrower
  supported guarded deletion.

The separate locked [path consumer](../../testdata/server-host-consumer/README.md)
executes these fixtures using only exported interfaces. Native faults, killed
writers and fresh-process continuations are covered by the underlying contracts
and `crates/crabber/tests/server_host` fixtures; this example does not replace
those tests. The original minimal-embed example and its 60-line limit are unchanged.

Normal Agent plan providers still use the legacy preparation coverage path.
Scoped instances joining in this probe must not be interpreted as positive
whole-runtime deletion evidence. The API remains unstable pending the independent
SDK, external Git-only consumer and publication/adoption gates.

The `conformance` integration target also runs the published
`session_management_contract`, `control_contract`, `interaction_contract`,
`attempt_contract` and `phase_contract`
harnesses through the custom adapter on every enabled backend. These exercise
actual transactions, revision conflicts, frozen interactions and metadata replay;
they are not success-returning Store mocks. The historical harness import paths
remain available for existing adapter users.
