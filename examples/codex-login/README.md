# Codex login (manual)

```sh
cargo run -p codex-login -- status
cargo run -p codex-login -- login
cargo run -p codex-login -- logout
```

These commands access the local credential store; login opens a browser OAuth
flow and logout clears credentials. Status can create store/lock directories
while reading. Run them manually, never in CI against a user's real store.
Invoking the example without an argument also starts login. This implementation
adds no new authentication mechanism.

FileCredentialStore uses `$XDG_CONFIG_HOME/crabber/auth.json`, falling back to
`$HOME/.config/crabber/auth.json` (or `./crabber/auth.json` when neither variable
exists). It serializes access with lock files, writes via a temporary file and
atomic rename, and protects credential files with restrictive permissions on
Unix. It is protected local file storage, not an encrypted keyring. Keep those
files private and never copy their values into logs or documentation. See
[auth implementation](../../crates/crabber-auth/src/lib.rs),
[command source](src/main.rs), and [Codex provider usage](../../crates/crabber-providers/README.md).
Offline auth tests use temporary stores and fake OAuth responses; this guide's
manual commands are not part of the workspace verification gate.
