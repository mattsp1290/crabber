# Real-command transcripts

These fixtures were captured on Linux aarch64 from synthetic projects. Temporary
project paths in Cargo/Go output were replaced with `/synthetic/project`; no user
data or credentials are included. Tests assert sentinels and encoded reduction,
not timing, timestamps or exact output from future RTK releases.

- `cargo-test.txt`: standalone Rust 2024 crate, thirty passing arithmetic tests
  and `FAILURE_SENTINEL` asserting `2 + 2 == 5`; captured with `cargo test --offline`.
- `go-test.jsonl`: module `synthetic/fixture`, twenty passing tests logging
  `passed`, and `TestFAILURE_SENTINEL` calling `t.Fatal("FAILURE_SENTINEL")`;
  captured with `go test -json .`.
- `git-diff.txt`: `git init`, stage `example.txt` with lines `line 0` through
  `line 49` and `REMOVED_SENTINEL`, replace the sentinel with `replacement`,
  capture `git diff -- example.txt`.
- `log.txt`: captured stdout of a Python command printing 400 lines of
  `INFO request completed successfully`, with line 201 replaced by
  `ERROR_SENTINEL synthetic failure`.

RTK v0.51.0's official archive was verified against `checksums.txt` before
extracting. Published checksums cover archives; the extension and
`RTK_REDUCER_TEST_SHA256` consume the extracted executable's separately computed
digest. CI uses synthetic executables and never downloads RTK.
