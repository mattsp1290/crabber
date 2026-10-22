# Golden metadata provenance

Source: eino-tools at `c0975d6764b2536faf639efe516a79de6154a55d`.

Extracted in a temporary archive of that revision with the Go program preserved as `crabber-fixtures.go.txt`:
`go run ./cmd/crabber-fixtures <fixture-directory>`. The program called
`fileops.ReadToolInfo`, `WriteToolInfo`, `EditToolInfo`, `ListToolInfo`,
`search.ToolInfo`, and `shell.ToolInfo`, then `info.ToJSONSchema()`, serializing
`name`, `info.Desc` as `description`, and the schema as `parameters`.
The extractor is preserved as inert text in `crabber-fixtures.go.txt`; no repository gate compiles it. These are upstream fixtures, not Rust output.

The second-deliverable `glob.json` was independently extracted at the same
revision using the preserved `crabber-fixtures.go.txt` Go program calling
`glob.ToolInfo()` and `info.ToJSONSchema()` (2026-10-09).

`apply_patch.json` was extracted at the same pinned revision on 2026-10-09 by
calling `applypatch.ToolInfo()` and `info.ToJSONSchema()` in the preserved `crabber-fixtures.go.txt` Go
extractor. The original Go test corpus is copied byte-for-byte into
`crates/applypatch/tests/fixtures/upstream_applypatch_test.go`; `go-cases.json`
extracts its twelve structured patch strings and their setup/expected categories.
Rust runtime tests run every extracted case. Additional Rust cases cover malformed
grammar, overlapping ambiguity, content bounds, cancellation, and commit failures.

`url_fetch.json` was separately extracted from an archive of the same immutable
revision with `urlfetch.ToolInfo()` and `info.ToJSONSchema()` in a temporary Go
program. It is not generated from Rust metadata. Its unchanged upstream prose
mentions Go redirect defaults; the Rust runtime uses the stricter manual policy
documented in `docs/tool-contract.md`.

`tracker_write.json` was extracted on 2026-10-09 from the same immutable Go
revision using `trackerwrite.ToolInfo()` and `info.ToJSONSchema()` in the
preserved `crabber-fixtures.go.txt` extractor. Its parity test compares independently implemented Rust
metadata with the fixture; production does not load the fixture.

`user_interact.json` was extracted on 2026-10-10 from an archive of the same
immutable revision using `userinteract.ToolInfo()` and `info.ToJSONSchema()`.
The extractor also regenerated all ten existing metadata fixtures with no JSON
value differences. The upstream userinteract test file is copied byte-for-byte
into `crates/userinteract/tests/fixtures/upstream_userinteract_test.go`;
`go-cases.json` records every Run case and the source SHA256. CLI cases use a
host prompter; both upstream answer-provided cases use the pending surface,
because the prompter deliberately rejects model-supplied answers.

On 2026-10-11 all eleven per-tool fixtures were regenerated without JSON value
differences from the pinned revision above. `catalog.json` was extracted by
`catalog.Standard(catalog.Options{TrackerWriter: stub})`, where stub implements
`Close(ctx, id, reason string) error`, emitting id, name, binding, retry_safe and
concurrent in Go order. Per-tool accessors are `fileops.{Read,Write,Edit,List}ToolInfo`,
`glob.ToolInfo`, `search.ToolInfo`, `applypatch.ToolInfo`, `shell.ToolInfo`,
`urlfetch.ToolInfo`, `userinteract.ToolInfo`, `trackerwrite.ToolInfo`, followed by
`info.ToJSONSchema()`. No Rust metadata produced the fixture.

SHA-256 of committed `catalog.json`: `a5716b27695fbaf02b94d8c91353ced018b1c07c815938a75ef446609fe4898d`.
SHA-256 of committed inert `crabber-fixtures.go.txt`: `e6b2fe3d53ba5734ba81952f85b2c04e335e1493632e350f60649469599a3727`.
Regenerate in an archive of the pinned Go revision by copying the inert text to
`cmd/crabber-fixtures/main.go`, creating the output directory and running
`go run ./cmd/crabber-fixtures <fixture-directory>` with rg >=14 and sh on PATH.
Fixtures are frozen; detecting upstream drift requires re-extraction.
