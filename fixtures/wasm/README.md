# WASM test components

Run `cargo xtask build-fixtures` at the repository root. It builds all guests
for `wasm32-wasip2` and writes a local `manifest.sha256`. Components are
generated under `fixtures/wasm/target/` and are never committed.

Each fixture uses the public `crabber-guest` SDK. `tool-and-sink` provides a
local WIT world to test a two-role export outside the standard worlds.

Tool-result middleware uses result-transform contract version 2, with no WIT
signature or package-version change. `after-tool-call` receives the tagged
input JSON in `executed-input-json` and the whole
`{ "context": ..., "result": ..., "mark_error": false }` envelope in
`output-json`. A `json` reply must preserve context and return that full
envelope; `unchanged` keeps the result without escalation. Old bare-result
replies fail the host's envelope validation. See the
[exact ABI and cancellation contract](../../docs/embedding.md#tool-result-transform-context).

| Source fixture | Purpose |
| --- | --- |
| [all-in-one](src/all-in-one/src/lib.rs) | Multi-role guest; after-tool redaction preserves the version-2 envelope |
| [redact-middleware](src/redact-middleware/src/lib.rs) | Replaces `secret` inside `result`, preserving context and `mark_error` |
| [spinning-middleware](src/spinning-middleware/src/lib.rs) | Logs `spin-ready`, then spins in after-tool middleware so host tests can observe token-driven epoch interruption |

All WASM result middleware is ordinary phase. `redact-middleware` demonstrates
normal redaction, but cannot guarantee protection after cancellation: mount a
native or JSON final redactor for that guarantee. The spinning guest is test
infrastructure, not an application example. Rebuild compatible guest artifacts
when adopting the new host contract; pin compatible host and guest revisions
when rolling back. Generated components and `manifest.sha256` remain local,
and fixture builds need no provider credentials.
