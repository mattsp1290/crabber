# Standard tools and native extensions consumer

Qualify one pushed revision of the runtime, tools, and native extensions:

```sh
CRABBER_REV=<pushed-full-sha> ./testdata/standard-tools-consumer/check.sh
```

The script materializes an empty project outside the repository and generates
its lockfile once, then uses `--locked` for all following commands. It verifies
that every resolved Crabber package has one version/source identity at that
revision, checks duplicate roots, and runs the complete tools matrix plus the
command-guard and redactor tuple. The tuple checks a denied shell operation,
redaction persisted through a reopened SQLite connection, and successful search.
No provider credentials or network services are used by the journeys.

Git dependencies keep the canonical SSH spelling; public fetches use an HTTPS
transport rewrite. No path dependencies or development patches enter the
consumer. `cargo xtask check` checks a scratch copy against local workspace APIs;
the pushed-revision script proves the Git dependency graph separately.

Requirements: stable Rust, Git, ripgrep >= 14, Python 3.11–3.14, and `/bin/sh`.
Set `CRABBER_TOOLS_CONSUMER_DIR` to choose an empty destination outside the
repository. Set `CRABBER_TOOLS_EVIDENCE_DIR` to retain the generated lockfile,
lock SHA-256, metadata, package identities, dependency tree, and toolchain versions.
The [native extensions consumer](../extensions-consumer/README.md) separately
qualifies its public host, cleanup, and process-restart journeys.
