# Locked public server-host consumer

Run `./testdata/server-host-consumer/check.sh` from the repository root. This is a
separate Cargo root with a checked-in lockfile and explicit local path dependencies.
It constructs an injected custom Store and runs the public offline host journeys:
scoped tools in two contexts, explicit questions and replay, V2 imported history,
and bounded quiet deletion. Assertions check effect counts and adapter use.

It intentionally has no AG-UI/SDK dependency and requires no network provider or
credentials. Its lockfile is independent of the workspace lockfile. `--locked`
prevents silent dependency updates; Cargo may download missing locked dependencies
on the first build. After caching them it can also run with `--offline`.

This is local path-consumer qualification. It does not qualify a published crate,
a Git-only candidate outside this repository, final SDK projection behavior,
transport authentication, or standard-tool adopters. Those remain separate gates.
