# Prompt contribution public consumer probe

## Current acceptance status

The tracked Git revision and lockfile are historical and are **not current-API
acceptance**. The updated probe source uses APIs missing at that revision,
including the current behavior declaration / JSON executor interfaces. Merely
fetching the existing pin or finding it on `origin/main` does not qualify this
source. The direct probe commands below are acceptance instructions for AFTER
an authorized re-pin to the actual published forty-character candidate SHA;
they are not automated tests of the current historical pin.

From the Crabber repository root, `cargo xtask check-probes` provides separate
local static API coverage. It copies the actual probe source, tests and manifest
to disposable outside-repository workspaces and changes only the disposable
`crabber` dependency to the local checkout path. It seeds resolution from the
workspace `Cargo.lock`; additional registry dependencies may still resolve.
It checks all targets with default features (PostgreSQL cfg is off), without
running the probes or fetching their tracked Crabber Git dependency. With all
required dependencies cached, the check can run with `CARGO_NET_OFFLINE=true`;
a cold CI may access the registry without provider credentials. Tracked manifests,
Git pins and lockfiles remain unchanged by this helper.

That type-check is not runtime, Git-source, clean-consumer or PostgreSQL
acceptance. After publication and authorized re-pinning, run the Git-only probe
commands from a separate clean checkout, without a path override, and record the
actual resolved dependency SHA, commands and results. PostgreSQL runtime proof
still requires its real service prerequisites. No historical-pin runtime success
is claimed for the current source.

This standalone workspace uses only `crabber::…` facade imports and a full Git
revision (currently historical `7a0f573b44944f8f100f60761dd2eec8ff2a65eb`,
incompatible with the updated source). Authorized SSH Git access is required to fetch the private repository.
The tests need no provider, API or database credentials.

After the final published re-pin, run from this directory:

```sh
cargo test --locked
cargo tree -i crabber
cargo fmt --check
cargo clippy --all-targets -- -D warnings
```

The runtime test source asserts file refresh on retries and later turns, post-compaction
refresh with internal summaries excluded, session shadowing, deterministic
ordering, concurrent session isolation, cancellation, exact and exceeded byte
bounds, deadline failure with tracked work retaining a semaphore permit, and
sanitized callback error/panic with zero provider calls.

Runtime-only coverage R11–R17 proves failure on retry, resume, fingerprint
rejection, static-only plans, proactive compaction, lease loss and model-stream
system ownership. Recovery and empty-prompt edge cases are also tested in the
repository. See [the embedding contract](../../docs/embedding.md#per-attempt-prompt-contributions).

Publication uses a content commit followed by a commit that changes only the
Git `rev` and generated lockfile. Run the probe from the merge commit that
publishes that pin; its manifest identifies the revision actually tested.
The content commit's probe manifest carries the previous revision and cannot
compile the new contract by itself. Merge with a merge commit so the content
pin stays an ancestor of `main`. No development path override is committed.
