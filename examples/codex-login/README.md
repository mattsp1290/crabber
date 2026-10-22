# Codex login

```sh
cargo run -p codex-login -- status
cargo run -p codex-login -- login
cargo run -p codex-login -- device
cargo run -p codex-login -- profile
cargo run -p codex-login -- catalog --client-version <version>
cargo run -p codex-login -- access-status --file <broker-access-file>
cargo run -p codex-login -- logout
```

| Mode | Stack | Credential source | Audience / scopes | Inference endpoint | Catalog |
| --- | --- | --- | --- | --- | --- |
| Browser loopback PKCE | SIWC | `login` writes `auth.json` profile `siwc` | resource `https://api.openai.com/v1`; `openid profile email offline_access resource.invoke chatgpt.tokens.use.direct` | `https://api.openai.com/v1/responses` | unavailable |
| Device code (unverified live) | Codex CLI | `device` writes `auth.json` profile `codex-cli` | client `app_EMoamEEZ73f0CkXaXp7hrann`; `openid profile email offline_access`, no resource | `https://chatgpt.com/backend-api/codex/responses` with account header | `/models?client_version=` |
| Broker refresh | either | host `CredentialStore` | per stack | per stack | per stack |
| Brokered access-only (unverified live) | Codex CLI | broker-owned `access.json` or host store | inherits broker token | Codex CLI Responses | current access token |

`login`, `device` and `catalog` are manual commands and never run in CI.
Device sign-in opens no listener or browser; it prints a verification URL and
user code, supports Ctrl-C, and replaces any existing stored account/profile.
Codex CLI endpoints are reference-derived and not published by OpenAI; device,
access-only inference and catalog remain **unverified live** until the manual
pre-merge gate in the [provider guide](../../crates/crabber-providers/README.md)
is completed. No ID-token validation is relaxed for this path.

Without an argument this example starts browser login. `status` and `profile`
read the local credential store and may create store/lock directories. `logout`
clears it. `catalog` obtains current credentials through `TokenManager`, then
lists model slugs, default effort and supported efforts without printing tokens.
`access-status --file` reads only the specified broker projection, printing
`available until <unix ms>`, `unavailable`, or a sanitized error.

`FileCredentialStore` uses `$XDG_CONFIG_HOME/crabber/auth.json`, falling back to
`$HOME/.config/crabber/auth.json` (or `./crabber/auth.json` when neither exists).
It serializes access through lock files and writes with atomic rename and
restrictive Unix permissions. This is local file storage, not an encrypted
keyring. Never copy credential values into logs or docs. Saved fields are
`profile`, `client_id`, `host_id`, `account_id`, `access_token`, optional
`refresh_token`, `id_token`, `scopes`, and `expires_unix_ms`.

The read-only `access.json` schema has `schema_version: 1`, `state`, optional
`access`, `account_id`, `expires_unix_ms`, `issued_unix_ms`, and required
`generation`. `available` requires nonempty access and positive expiry;
`unavailable` must omit access/account ID. The broker owns refresh and writes.
Set `CRABBER_CODEX_PROFILE=codex-cli` for inference with these credentials;
SIWC is the default. See the provider guide for error behavior and stack details.

Offline tests use temporary stores, a signed fake IdP and loopback HTTP servers.
The access-status integration test runs only against a temporary unavailable
projection and makes no network requests.

Browser login refuses an existing `codex-cli` registration before opening its
listener; run `codex-login logout` before switching that store back to SIWC.
Authentication responses and JWKS are limited to 1 MiB of received body data.
