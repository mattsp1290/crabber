# Crabber guest SDK

Add `crabber-guest` to a Rust `cdylib` crate, call
`crabber_guest::generate!(world: "tool")`, implement the generated role API
trait, and call `crabber_guest::export_extension!` to export the manifest and
component. The [echo fixture](../../fixtures/wasm/src/echo-tool/src/lib.rs)
shows a complete tool. `generate!` also accepts `path: "wit"` for a guest
defined world that combines selected roles.

Build with `cargo build --target wasm32-wasip2 --release`. The `.wasm` output
is a Component Model component. The host uses a SHA-256 digest and an allowed
root when loading it. The [external consumer](../../testdata/external-consumer/)
builds a guest and a host from outside the root Cargo workspace.

Tools target `crabber:extensions@0.2.0`. The `execute` result is a versioned typed
completion envelope. For a JSON-only tool, return
`crabber_guest::json_tool_output(&value)`; this deliberately renders JSON as text.
For mixed content, construct `crabber_guest::ToolOutput` and return
`output.into_envelope().to_string()`. The host validates the version and content;
media requires an explicit kind, MIME type and data/URL/opaque-file source.
