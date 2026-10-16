# Offline configured server host

This executable runs real public Agent/Store flows with a scripted local provider.
It needs no credentials, HTTP server, browser, AG-UI dependency or global `chdir`.
The embedding owns the Tokio runtime and injects its Store through `AuditedStore`,
a complete public forwarding adapter. Optional capabilities on an incomplete
adapter remain Unsupported; the probe checks that it cannot start provider work.

```sh
cargo run -p server-host-probe -- memory
cargo run -p server-host-probe --features sqlite -- sqlite /explicit/fresh/host.db
CRABBER_REQUIRE_POSTGRES=1 cargo test -p server-host-probe --features postgres
```

For PostgreSQL, set `CRABBER_TEST_POSTGRES_URL` to a dedicated test database first.
SQLite currently requires a fresh schema-13 file; PostgreSQL currently uses schema
17. Migration is explicit. `connect` does not create or upgrade storage.

The assertions verify actual behavior:

- One configured Agent uses two immutable workspace/session contexts. A custom
  permission policy allows a session-scoped counter tool. Two factory instances
  each execute once, retain separate state, and join their cleanup on close.
- A versioned native tool suspends twice and accepts authenticated, schema-checked
  host answers. Reconstructed Agents resume the exact checkpoint; the effect
  before the first checkpoint runs once. Answer and continuation retries replay
  metadata. Reconstruction here is in one process, not a process-crash proof.
- After close, pure controls fork verified closed history, read imported calls
  across coherent V2 pages, and atomically delete the quiet fork. The original
  executed session remains blocked: this probe does not certify whole-runtime
  cleanup or make retained runs deletable.

The separate locked [path consumer](../../testdata/server-host-consumer/README.md)
executes these fixtures using only exported interfaces. Native faults, killed
writers and fresh-process continuations are covered by the underlying contracts
and `crates/crabber/tests/server_host` fixtures; this example does not replace
those tests. The original minimal-embed example and its 60-line limit are unchanged.

Normal Agent plan providers still use the legacy preparation coverage path.
Scoped instances joining in this probe must not be interpreted as positive
whole-runtime deletion evidence. The API remains unstable pending the independent
SDK, external Git-only consumer and publication/adoption gates.
