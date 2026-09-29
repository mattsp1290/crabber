# Working in crabber

## Workspace

This is a Cargo workspace using Rust 2024. Run `cargo xtask check` before handing off changes. It checks formatting, Clippy, workspace tests, and the minimal embedding example's 60-line glue limit once that example exists.

## Beans issue tracker

Use `bn` to inspect and update work tracked under `crabber-<hash>` issue IDs. Keep issue scope and dependencies intact. Record the relevant commit when completing an issue; do not close work owned by another branch or agent.

## Local agent artifacts

Keep `.agents/plans/`, `.agents/reviews/`, and `reviews/` local. They are ignored and must not be committed.

## Secrets and fixtures

Use environment variable names in documentation, never credential values. Tests must run without credentials. Generated WASM binaries under `fixtures/wasm/` are local artifacts and must not be committed.
