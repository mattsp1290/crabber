# Public result-transform probe

This standalone Rust 2024 crate has its own workspace and lockfile. It consumes
only the public `crabber` facade from
`ssh://git@github.com/mattsp1290/crabber.git`, provisionally pinned to
`fb179621d61f57c60a420f674c632f8101179a87`. It has no path dependencies,
workspace test helpers, WASM, provider calls, or runtime credentials. Existing
authorized SSH Git access fetches the private source; `.cargo/config.toml`
uses the Git CLI. No task-specific environment variables are required.

From this directory, run exactly:

```sh
cargo test
```

The two tests in `tests/public_contract.rs` run real `Agent`/`Orchestrator`
execution with public `FakeProvider`, `MemoryStore`, extensions and tools:

- `exact_normalized_binding_reducer_then_final_redactor_protects_next_request`
  rewrites provider input during preparation and binds reduction to the exact
  tool and normalized input, with separate wrong-input and wrong-tool controls.
  Reduction escalates success to an error; the final redactor preserves that
  status and follows reduction even with a lower registration order. The secret
  originates only in executed output. Assertions inspect the full next
  `ModelRequest` through its public `Debug` representation, and compare protected
  content in that request with durable tool results, messages and events.
- `prepare_failure_reports_unavailable_input_and_cannot_downgrade_error`
  checks `PrepareFailed` with explicit `Unavailable { PrepareFailed }` input,
  no execution, and error status retained when both handlers return
  `mark_error: false`.

Callback observations are asserted after the run, outside callbacks whose
panics the runtime contains. This probe is excluded from `cargo xtask check`.

For pre-publication verification, clone the committed candidate into a unique
clean scratch directory, check out its exact full SHA, and run the same command:

```sh
git clone --no-hardlinks /path/to/committed/crabber /path/to/unique-clean-clone
git -C /path/to/unique-clean-clone checkout --detach <candidate-full-40-hex-sha>
cd /path/to/unique-clean-clone/testdata/result-transform-probe
cargo test
```

The dependency remains the pinned GitHub source, independent of the candidate
clone. Record the candidate SHA, source URL, dependency pin, command and test
result alongside the clean-clone log.

The pin is provisional under `docs/embedding.md`, Recorded answer 6. Immediately
before human merge, reset it to the last implementation commit in a final
commit that changes only the manifest revision and lockfile. Later review fixes
make the earlier pin stale. Use a merge commit so the pinned revision stays
reachable from `main`; any alternate tag/publication route requires human
approval. `crabber-8sxd` still owns post-publication clean-clone verification.
Merge, tags/releases, request reply and acceptance remain human gates.
