# crabber

Crabber is a Rust agent runtime under development. The workspace currently contains skeleton crates for the public facade, core types, sessions, runtime, extensions, and providers.

Run the local quality gate with:

```sh
cargo xtask check
```

The command checks formatting, Clippy, and all workspace tests. When the minimal embedding example is added, it also limits the code between its `crabber:glue-start` and `crabber:glue-end` markers to 60 lines.
