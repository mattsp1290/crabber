# Crabber extension contract

`crabber:extensions@0.1.0` defines eight extension roles and a `bundle` world.
Every component exports `manifest-api`. The host detects roles from exported
interfaces and requires `describe().roles` to match exactly.

Payloads that can evolve without breaking the ABI travel as JSON strings.
The host bounds input and output sizes, validates imports, and supplies only
`crabber:host/log` and `crabber:host/state` in addition to the restricted
`wasm32-wasip2` baseline. Guests receive no environment variables, directory
preopens, network access, or process arguments.

Change the package version for a breaking WIT change. Update the three WIT
copies in `wit/`, `crates/crabber-guest/wit/`, and
`crates/crabber-guest-macros/wit/` together. Run `cargo xtask check` to
validate the WIT and build fixture components.
