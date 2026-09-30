# WASM test components

Run `cargo xtask build-fixtures` at the repository root. It builds all guests
for `wasm32-wasip2` and writes a local `manifest.sha256`. Components are
generated under `fixtures/wasm/target/` and are never committed.

Each fixture uses the public `crabber-guest` SDK. `tool-and-sink` provides a
local WIT world to test a two-role export outside the standard worlds.
