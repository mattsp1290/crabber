# Prompt contribution public consumer probe

## Source and acceptance

This standalone Rust 2024 workspace uses only public `crabber` facade APIs.
Its `Cargo.toml` and committed `Cargo.lock` must resolve the Crabber dependency
to the published crate-content candidate **C**,
`04e31b1cf7110f44fe1bbc73668111884f291508`. No development path override is used. Authorized Git
access is required; the scripted tests use no provider credentials.

Record the actual checkout revision, `rustc --version`, `cargo --version`, locked
commands/results and resolved Crabber Git SHA with the qualification evidence.
These instructions describe the checks; they do not themselves record a pass.
`--locked` rejects a manifest/lockfile mismatch rather than silently rewriting it.

From the Crabber repository root, `cargo xtask check-probes` is a separate local
static API check. It copies the actual probe source, tests and manifest into
disposable outside-repository workspaces and changes only the disposable
`crabber` dependency to the local checkout path. It seeds resolution from the
workspace `Cargo.lock`; additional registry dependencies may still resolve.
It checks all targets with default features, without running the probes or
fetching their tracked Crabber Git dependency. PostgreSQL cfg is off. With all
dependencies cached it can run with `CARGO_NET_OFFLINE=true`; cold CI may access
the registry without provider credentials. It does not modify the tracked probe
manifests or locks, and is not Git-source, runtime or PostgreSQL acceptance.

## Commands

From `testdata/prompt-contribution-probe/` in the clean maintenance checkout:

```sh
cargo test --locked
cargo tree --locked -i crabber
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
```

This probe has no PostgreSQL feature or database prerequisite. Do not count its
default test run as PostgreSQL coverage. It needs no provider/API credentials.

## Runtime assertions

The runtime test source asserts file refresh on retries and later turns, post-compaction
refresh with internal summaries excluded, session shadowing, deterministic
ordering, concurrent session isolation, cancellation, exact and exceeded byte
bounds, deadline failure with tracked work retaining a semaphore permit, and
sanitized callback error/panic with zero provider calls.

Separate repository runtime coverage R11–R17 checks failure on retry, resume, fingerprint
rejection, static-only plans, proactive compaction, lease loss and model-stream
system ownership. Recovery and empty-prompt edge cases are also tested in the
repository. See [the embedding contract](../../docs/embedding.md#per-attempt-prompt-contributions).

## Clean-checkout verification

The exact dependency pin above identifies the crate code under test. The
repository checkout may be a later maintenance revision whose manifest selects
that pin. Record both the checkout commit and the resolved Crabber dependency
SHA with the actual command results.

Set `PROBE_CHECKOUT_COMMIT` to the reviewed published checkout revision:

```sh
git clone git@github.com:mattsp1290/crabber.git /path/to/unique-clean-clone
git -C /path/to/unique-clean-clone checkout --detach "$PROBE_CHECKOUT_COMMIT"
```

Enter this probe directory and run the locked commands above without a path
override. Inspect the resolved source with `cargo tree --locked -i crabber` and
`cargo metadata --locked --format-version 1`. The local static API check is
separate from these Git-source/runtime results.
