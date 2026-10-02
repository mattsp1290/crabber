# WASM extension

```sh
cargo xtask build-fixtures
cargo run -p wasm-extension
```

The package enables the facade's `wasm` feature. From its manifest directory it
loads `fixtures/wasm/target/wasm32-wasip2/release/echo_tool.wasm`, canonicalizes the
allowed root, computes the binary's SHA-256, and registers module ID `echo-tool`
with that expected hash, default limits and per-call instances. The fake provider
calls echo with a message, then emits `Done`; output includes
`WASM extension run: Completed`.

The tool crosses the real component extension boundary. Module identity, trusted
hash and allowed-root policy belong to the host. Generated binaries are local
and must not be committed. This fixture path differs from minimal-embed's copied
`fixtures/wasm/echo-tool.wasm`. See [source](src/main.rs), [WIT](../../wit/README.md)
and the [embedding matrix](../../docs/embedding.md). No credentials are needed.
