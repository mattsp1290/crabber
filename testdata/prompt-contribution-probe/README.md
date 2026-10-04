# Prompt contribution public consumer probe

This standalone workspace uses only `crabber::…` facade imports and a full Git
revision. Authorized SSH Git access is required to fetch the private repository.
The tests need no provider, API or database credentials.

Run from this directory:

```sh
cargo test --locked
cargo tree -i crabber
cargo fmt --check
cargo clippy --all-targets -- -D warnings
```

The ten tests prove file refresh on retries and later turns, post-compaction
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
