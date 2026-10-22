# Native extensions consumer

Run from a checkout containing the immutable revision being qualified:

```sh
CRABBER_REV=<pushed-full-sha> ./testdata/extensions-consumer/run.sh
```

The script creates an empty project outside the repository. Set
`CRABBER_EXTENSIONS_CONSUMER_DIR` to choose an empty external destination and
`CRABBER_EXTENSIONS_EVIDENCE_DIR` to copy logs, lockfile, lock hash, metadata,
and dependency trees. It requires Git, Python 3.11–3.14, stable Rust, `/bin/sh`,
`ps`, and `rg`; no provider credentials are used.

Cargo resolves `crabber` and `crabber-extensions` from the crabber repository at
one `CRABBER_REV`. Every resolved `crabber*` package must share that exact Git
source and have one version/source identity. The project has its own workspace
and generates a lock once, then uses `--locked` for subsequent commands.

The script exports six public-API integration targets and their fixtures from
the same immutable revision using `git archive`. These journeys compile against
the consumer's Git dependencies outside the original workspace. Development
path overrides and private source modules are not used. The separate
[standard tools consumer](../standard-tools-consumer) qualifies the combined
runtime, tools, and native extensions tuple.

SQLite restart runs in two processes. Phase 1 commits an active detached owner
and a paused ask, then exits without graceful cleanup to simulate host loss.
Phase 2 answers and continues the ask with replay checks, observes the old owner
still Active, and verifies a fresh local drain/cache cannot claim a join or
resume an old process. Live provider qualification remains separate.
