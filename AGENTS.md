# Working in crabber

## Workspace

This is a Cargo workspace using Rust 2024. Run `cargo xtask check` before handing off changes. It checks formatting, Clippy, workspace tests, and the minimal embedding example's 60-line glue limit once that example exists.

## Beans issue tracker

Use `bn` to inspect and update work tracked under `crabber-<hash>` issue IDs. Keep issue scope and dependencies intact. Record the relevant commit when completing an issue; do not close work owned by another branch or agent.

## Local agent artifacts

Keep `.agents/plans/`, `.agents/reviews/`, and `reviews/` local. They are ignored and must not be committed.

## Secrets and fixtures

Use environment variable names in documentation, never credential values. Tests must run without credentials. Generated WASM binaries under `fixtures/wasm/` are local artifacts and must not be committed.

## Standard tools and native extensions

The `crabber-tools-*` and `crabber-extensions` crates live in this workspace.
They use only the public `crabber` API through path dependencies.
`crabber-extension` defines the extension API; `crabber-extensions` implements
native extensions.

Hosts own workspace admission, permission and environment policies, process uid,
`command_guard` analysis, and `tool_result_redactor` redaction. File operations
run as the host uid; `run_as` applies only to child processes.

Search tests require ripgrep >= 14 on PATH; shell tests require `sh`.
Extensions tests require `python3` 3.11 through 3.14 and `/bin/sh`.
Use `BACKGROUND_JOBS_REQUIRE_SHELL=1 PYTHON_REPL_REQUIRE_PYTHON=1` and, on Linux,
`CRABBER_TOOLS_REQUIRE_LOGIN_PROFILE=1` to require environment-dependent tests.
The workspace gate sets these requirements. It also runs rustdoc with
`RUSTDOCFLAGS='-D warnings'` and Clippy with
`--features crabber-extensions/live-opencode-go`.

## Consumer templates

`testdata/standard-tools-consumer/` and `testdata/extensions-consumer/` are
Git-only templates executed outside the repository. Their manifests must never
gain path dependencies, patches, or alternative source spellings. Runtime HTTPS
fetch rewrites preserve the canonical SSH package source identity.
`cargo xtask check` rewrites Git dependencies to local paths only in disposable
scratch copies for API checks. `testdata/server-host-consumer/` retains its own
existing path-dependency contract.

The former `crabber-tools-*` and `crabber-extensions-*` issue IDs are migrated
with pointers after consolidation reaches main and its acceptance gates pass.
New implementation issues use `crabber-<hash>` IDs.
