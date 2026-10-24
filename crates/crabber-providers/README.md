# Crabber model providers

The real adapters are Cargo features on `crabber-providers` (`anthropic`, `openai`, `codex`, and `opencode-go`). `crabber` re-exports the same features. `AgentBuilder::providers_from_env()` registers the compiled adapters. The minimal embedding example enables all four and accepts `--provider`, `--model`, and OpenCode Go's `--protocol` (`responses`, `messages`, or `chat-completions`). The default no-argument example still uses the scripted provider.

| Provider | Credential source | Protocol |
| --- | --- | --- |
| `anthropic` | `ANTHROPIC_API_KEY` | Messages API |
| `openai` | `OPENAI_API_KEY` | Responses API |
| `opencode-go` | `OPENCODE_GO_API_KEY` | OpenCode Go API; Responses by default |
| `codex` | `auth.json` or host-owned store; configured by `CRABBER_CODEX_PROFILE` | SIWC public Responses or Codex CLI backend |

The Anthropic adapter's default credential stays `ANTHROPIC_API_KEY`. The opt-in
live acceptance tests read `CRABBER_ANTHROPIC_API_KEY` instead and pass it
through `HttpAdapter::with_api_key`.

Every Messages request sends `anthropic-version: 2023-06-01` and no
`anthropic-beta` header. Current Claude models reject non-default `temperature`
with HTTP 400. The runtime sends no `temperature` unless a `crabber/model/stream`
hook sets one.

Codex supports two configured credential stacks. Browser sign-in follows the
[public SIWC flow](https://developers.openai.com/siwc/token-sharing-open-source/sign-in).
Codex CLI endpoints are reference-derived and not published by OpenAI.
Device-code, access-only inference and catalog are **unverified live** until the
pre-merge acceptance below is recorded.

| Mode | Stack | Credential source | Token audience / scopes | Inference endpoint | Catalog |
| --- | --- | --- | --- | --- | --- |
| Browser loopback PKCE | SIWC (`dynamic_agent_client` or issued `oaiapp_*`) | `codex-login login`; `auth.json` profile `siwc` | `resource=https://api.openai.com/v1`; scopes `openid profile email offline_access resource.invoke chatgpt.tokens.use.direct` | `POST https://api.openai.com/v1/responses` via `HttpAdapter::codex` | unavailable |
| Device code (unverified live) | Codex CLI (`app_EMoamEEZ73f0CkXaXp7hrann`) | `codex-login device`; `auth.json` profile `codex-cli` | scopes `openid profile email offline_access`; no `resource`; ID token audience validated against client id | `POST https://chatgpt.com/backend-api/codex/responses` with `ChatGPT-Account-Id` via `HttpAdapter::codex_cli` | `GET …/codex/models?client_version=` (unverified live) |
| Broker refresh | either | host `CredentialStore` with `refresh_lock` and `compare_exchange` | per stack | per stack | per stack |
| Brokered access-only (unverified live) | Codex CLI | `AccessFileCredentialStore` or host store with `refresh_token: None` | inherits broker token | `HttpAdapter::codex_cli`; one 401 retry re-reads the broker store without refreshing | current access token (unverified live) |

`CRABBER_CODEX_PROFILE` configures which stack the stored credential uses:
unset, empty or `siwc` selects SIWC; `codex-cli` selects Codex CLI. The adapters
both register as `codex`, but their endpoint binding fingerprints differ.
Profile mismatches and missing Codex CLI account IDs fail before inference I/O.
Prefer `HttpResolver::try_from_env()`, which rejects invalid profile values.
`AgentBuilder::providers_from_env()` reports these as
`BuildError::InvalidConfig("CRABBER_CODEX_PROFILE")`. Compatibility
`HttpResolver::from_env()` omits only Codex for a typo and keeps other compiled
providers. The minimal embedding example uses this compatibility method, so a
typo fails when selecting the missing provider.

Credentials are written under `${XDG_CONFIG_HOME}/crabber/auth.json`, falling
back to `${HOME}/.config/crabber/auth.json`, with owner-only permissions on Unix.
Serialized fields are `profile`, `client_id`, `host_id`, `account_id`,
`access_token`, optional `refresh_token`, `id_token`, `scopes`, and
`expires_unix_ms`. Legacy files without `profile` parse as `siwc`.
The broker-owned `access.json` schema 1 has `schema_version`, `state`, optional
`access`, optional `account_id`, optional `expires_unix_ms`, optional
`issued_unix_ms`, and required `generation`. `available` requires a nonempty
access token and positive expiry; `unavailable` must omit access/account ID.
Unknown fields and files larger than 65,536 bytes are rejected. This projection
is read-only: refresh, file writes and rotation belong to the broker. The adapter
never logs credential values or request bodies.

## Typed input support

`HttpAdapter::input_support()` and `CustomHttpAdapter::input_support()` report
supported roles, media references and explicit context data without I/O.
Context reaches each protocol as canonical user-role JSON text, separate from
system policy and UI activity. Unsupported roles/media fail before credentials,
header callbacks or network access; media is never silently removed or fetched.
Encoding support is specific to the media source, MIME type and placement:

| Protocol | Image sources | Placements |
| --- | --- | --- |
| Responses | Inline base64, URL, opaque file ID | User messages and function-call output |
| Chat Completions | Inline base64, URL | User messages |
| Messages | Inline base64, URL, opaque file ID | User messages and tool results |

Supported image MIME types are PNG, JPEG, WebP and GIF. Other media kinds,
unsupported roles/sources and malformed references fail before credentials or
HTTP. Source URLs are passed through and never fetched by Crabber; opaque file
IDs remain opaque. `HttpMediaSupport` reports these encoding limits, including
`message_roles`, `tool_results` and `mime_types`; it does not claim that a selected
remote model supports vision. Native typed streamers receive media unchanged.
The wire mapping follows the official [image input guide](https://developers.openai.com/api/docs/guides/images-vision),
[function output guide](https://developers.openai.com/api/docs/guides/function-calling)
and [Chat Completions schema](https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create).
Messages image blocks follow the [Anthropic vision guide](https://platform.claude.com/docs/en/build-with-claude/vision).
A Messages tool result stays a joined string when it holds only text and
becomes a `text`/`image` block array when it carries media, with empty text
blocks dropped. Support is keyed by protocol, so OpenCode Go Messages and
custom Messages adapters advertise the same sources; confirm that the backend
accepts them, especially file IDs. Base64 images are verified against the live API. URL and file
sources are verified only by loopback tests against the documented block shapes.

Agent admission requires an explicit host `behavior_version` or
`behavior_fingerprint`. Built-in HTTP bindings independently capture endpoint,
protocol, implementation version and user agent, excluding credential values.
Opaque custom callbacks/transports require an explicit `VersionedResolver` or
`VersionedAdapter` route/configuration declaration for Agent execution. Direct
adapter streaming remains available for transport tests and host-owned drivers.

## Host-owned custom HTTP providers

Enable the facade's `custom-http` feature (or `crabber-providers/custom-http`
directly). It is independent of `anthropic`, `openai`, `codex`,
`opencode-go`, and `all-providers`:

```toml
[dependencies]
crabber = { version = "0.1", default-features = false, features = ["custom-http"] }
# Needed when the host names public header, status, certificate, identity, or TLS types.
reqwest = { version = "0.12", default-features = false }
```

The public construction surface is `HttpAdapter::custom`, `HttpResolver`, and
`Protocol`, with `AuthScheme`, `ChatTokenField`, `CredentialSource`,
`RequestHeaderHook`, `ResponseObserver`, `ErrorClassifier`, `HttpClientConfig`,
and `HttpProxyConfig`. These are available from `crabber::providers` or the
`crabber_providers` crate root. Public HTTP types are from **reqwest 0.12**;
depend directly on that version rather than relying on Crabber's transitive
dependency.

`HttpClientConfig` deliberately exposes only these transport controls:

- total, connect, and read timeouts;
- idle-pool timeout and maximum idle connections per host;
- an all-protocol `HttpProxyConfig` (optionally with basic authentication), or
  explicit proxy disabling;
- additional root certificates, client identity, minimum/maximum TLS versions,
  and preconfigured TLS state.

Redirects are always disabled. There is intentionally no way to inject a raw
`reqwest::Client`, `ClientBuilder`, or `Proxy`, install default headers, change
the redirect policy, or run a generic builder callback. This finite surface
keeps Crabber in control of credentials and protected headers.

A custom adapter does not fetch a credential during construction, registration,
resolution, streamer build, or its empty model-catalog lookup. Starting an
outbound stream attempt calls `CredentialSource::credential` and requires a
non-empty credential; the source is called again for every retry attempt.
`AuthScheme::Bearer` writes exactly one `Authorization` header whose value is
`Bearer <credential>`, and writes no `x-api-key` header;
`AuthScheme::XApiKey` writes exactly one `x-api-key` header and no
`Authorization` header. Static headers are applied first, then the per-attempt
`RequestHeaderHook` replaces every static value having the same name. Crabber
writes its owned headers last. Both static and hook output reject `authorization`, `x-api-key`,
`content-type`, `user-agent`, and `anthropic-version`; a protected static header
fails construction, and protected hook output fails before that attempt is
sent. Mark any other secret-bearing `HeaderValue` as sensitive so error
sanitization can recognize it.

When a dynamic `CredentialSource` receives a 401, Crabber passes the exact stale
credential to `invalidate`, fetches credentials and hook headers again, and
sends **one** retry. A second 401 is a non-retryable `Auth` error. The source—not
Crabber—owns atomic compare-and-invalidate (so an old 401 cannot clear a newer
generation) and single-flight refresh across concurrent requests. Static
`with_api_key` credentials do not take this dynamic refresh path.

`ResponseObserver::observe` runs exactly once for each received HTTP response,
including a received 401: after its headers arrive and before retry handling,
status classification, or response-body consumption. It is not invoked when an
attempt fails before response headers arrive. The observer receives the raw,
unredacted response `HeaderMap`, which may contain cookies, authentication
challenges, or vendor-specific secrets. Hosts must not indiscriminately log or
export that map and are responsible for applying their own redaction policy.
Non-success errors contain an opaque, UTF-8-safe excerpt bounded to 4096 bytes.
Crabber removes known attempt credentials and sensitive request-header values,
but the gateway may echo other secrets: hosts must apply their own redaction
policy before logging or exporting the error. Transport failures use
stable retryable `Transport` messages: `provider transport connect`,
`provider transport timeout`, or `provider transport body`; they do not include
URLs or credentials.

For `Protocol::ChatCompletions`, `ChatTokenField::MaxTokens` or
`ChatTokenField::MaxCompletionTokens` selects the outgoing token field. An
empty tool list is omitted. `ErrorClassifier` sees status plus the bounded
sanitized excerpt and can choose `ProviderErrorKind` and retryability. Host
classifiers apply only to custom adapters. The terminal second 401 after
dynamic credential refresh is the exception: Crabber bypasses the classifier
and returns non-retryable `Auth`. The built-in Anthropic adapter uses a
crate-private classifier (see Messages stop reasons and stream errors). The
other built-in adapters use status-only classification. A custom
adapter returns no model catalog and never mints a gateway token. Model
discovery and token minting/refresh remain host-owned (normally behind the
host's resolver/catalog and `CredentialSource`). Construction and registration
through `HttpResolver::with_adapter` perform no credential fetch and no network
request.

## Live acceptance record

The subscription checks must be run locally with authorized accounts. Criterion 1 (the existing rows) requires a streamed reply, an executed `echo` tool round trip, and a settled run. Criterion 2 (tool-free Chat Completions) requires streamed text, a completed run, and exactly one successful model operation observed. Criterion 3 (Anthropic Messages) requires all of the following:

- streamed text and exactly one successful model operation for a tool-free prompt
- an `echo` tool round trip that settles `completed`, with two successful model operations
- a catalog that contains the selected model
- a base64 image request that streams text

The record also states the observed `thinking_replayed` value. The SSE fixtures and codec tests do not substitute for this gate.

| Date | Backend | Model | Outcome |
| --- | --- | --- | --- |
| 2026-10-22 UTC | Anthropic Messages (criterion 3), after review fixes | `claude-opus-5-5` | Passed on clean code commit `a30e95de345736048895aa3c928116c1fd5dd98c` (`git status --porcelain` empty), which includes the observable-output retry boundary, unsigned-thinking truncation, and catalog refactor. Command: `cargo test -p crabber --features anthropic --test anthropic_live -- --ignored --nocapture`, with `CRABBER_ANTHROPIC_API_KEY` loaded from the shell profile. 4 passed with the same outcomes as the earlier row. `thinking_replayed=false` again: no thinking block streamed on the tool turn, so live signed-thinking replay remains unexercised (loopback tests prove the replay encoding). |
| 2026-10-22 UTC | Anthropic Messages (criterion 3) | `claude-opus-5-5` | Passed on clean code commit `8c3acd8b364aa9c921609904611a7312d63d8088` (`git status --porcelain` empty). Command: `cargo test -p crabber --features anthropic --test anthropic_live -- --ignored --nocapture`, with `CRABBER_ANTHROPIC_API_KEY` loaded from the shell profile. 4 passed: tool-free text completed with one successful model operation, `echo` settled `completed` with two successful model operations, the catalog listed `claude-opus-5-5`, and the base64 PNG streamed text. `thinking_replayed=false`: the model emitted no thinking block on the tool turn, so signed-thinking replay against the live API was not exercised on this run (loopback tests prove the replay encoding). |
| 2026-10-06 UTC | OpenCode Go, Chat Completions, tool-free (criterion 2) | `deepseek-v4-flash` | Passed on clean code commit `5769fe276de5781abb43c0c317fb825b44afcecd` (`git status --porcelain` empty). Command: `cargo test -p crabber --features opencode-go --test opencode_go_live -- --ignored`, with `OPENCODE_GO_API_KEY` loaded from the environment. Streamed text, completed run, exactly one successful model operation. Body shape is proven by loopback tests; this accepts the current body and does not reproduce the consumer’s earlier empty-tools-array probe. |
| 2026-09-29 EDT | OpenCode Go | `gpt-5.6-luna` | Passed on candidate `ada6eb5`: Responses stream replied, `echo` tool settled, final text `Echo`, process exited 0. Command: `cargo run -p minimal-embed -- --provider opencode-go --protocol responses --model gpt-5.6-luna`. |
| 2026-09-29 EDT | ChatGPT plan / Codex | `gpt-5.5` | Passed on candidate `954146f`: browser OAuth signed in, `auth.json` mode was `0600`, Responses stream replied, `echo` tool settled, final text `Echo tool used.`, process exited 0. Command: `cargo run -p minimal-embed -- --provider codex --model gpt-5.5`. |

Commands:

```sh
cargo run -p codex-login -- login
cargo run -p minimal-embed -- --provider codex --model <account-listed-model-id>
cargo run -p minimal-embed -- --provider opencode-go --protocol responses --model <id-from-GET-models>
cargo test -p crabber --features anthropic --test anthropic_live -- --ignored --nocapture
CRABBER_ANTHROPIC_LIVE_MODEL=claude-sonnet-5-5 cargo test -p crabber --features anthropic --test anthropic_live -- --ignored --nocapture
ANTHROPIC_API_KEY="$CRABBER_ANTHROPIC_API_KEY" cargo run -p minimal-embed -- --provider anthropic --model claude-opus-5-5   # optional manual demo; record it only if run
```

Set `OPENCODE_GO_API_KEY` in the environment before the OpenCode Go command. `CRABBER_ANTHROPIC_API_KEY` comes from the operator's shell profile. `CRABBER_ANTHROPIC_LIVE_MODEL` selects another model from the catalog test's printed ids (default `claude-opus-5-5`). The Anthropic live tests are `#[ignore]`. Plain `cargo test` and CI never run them, and `cargo xtask check` only builds them with `--no-run`. No key value belongs in this record.

## Tool-free Chat Completions

`HttpAdapter::opencode_go(Protocol::ChatCompletions)` omits `tools` and
`tool_choice` when no tools are registered and sends the output cap as
`max_tokens`. `max_completion_tokens` remains a custom HTTP option through
`CustomHttpAdapter::with_chat_token_field`.

## Chat reasoning and stream errors

An admitted reasoning effort is serialized by both Responses and Chat
Completions. Chat sends it as the top-level `reasoning_effort` field, using the
canonical lowercase API values `low`, `medium`, or `high`, and omits the field
when the control is `None`. Messages continues to ignore this control. See
[Reasoning controls and admission](#reasoning-controls-and-admission) for how
the control is selected and retained.

Chat recognizes an explicit stream failure in either of these SSE forms:

- a data-only event whose JSON object has a top-level `error`; this is the
  pinned OpenRouter-compatible fixture;
- a named `event: error` whose data is also a JSON object with a top-level
  `error`; this is Crabber compatibility required by `crabber-r-3l5m`, not a
  claim about the OpenRouter wire format.

The `error` value must be an object with an integer `code` and string
`message`. It may have a `metadata` object, whose optional `error_type` may be
absent, null, or a string. Any violation of that schema produces a
non-retryable `Invalid` error with the fixed message
`invalid Chat stream error`. A valid error produces only the fixed message
`Chat stream error`: Crabber never copies the raw provider message, code, type,
metadata, or unrelated fields into the returned error message or diagnostics.

Classification is exact and case-sensitive. A recognized `error_type` wins
over the numeric code:

| `metadata.error_type` | Kind | Initially retryable |
| --- | --- | --- |
| `rate_limit_exceeded` | `RateLimited` | Yes |
| `authentication`, `permission_denied` | `Auth` | No |
| `context_length_exceeded` | `ContextOverflow` | No |
| `provider_overloaded`, `provider_unavailable` | `Server` | Yes |
| `invalid_request` | `Invalid` | No |

When the type is absent, unknown, or has the wrong case, the integer code is
the fallback: 429 is `RateLimited`; 401 and 403 are `Auth`; 400, 402, 404, 408,
413, and 422 are `Invalid`; 500 through 599 are `Server`; every other integer
is also `Server`. `RateLimited` and `Server` are initially retryable; the other
kinds are not.

An explicit error is terminal. The stream emits exactly one error and
suppresses trailing deltas, usage, `[DONE]`, and completion. Once any
observable output has occurred, including a partial tool call, Crabber removes
retryability to prevent a retry from duplicating effects. A failed Chat attempt
does not commit a successful assistant message or turn, and a partial tool call
is not dispatched or executed.

Opaque Chat provider-state round trips remain unsupported and are tracked
separately from this contract. This contract does not support arbitrary
provider string or scalar envelopes.

## Messages stop reasons and stream errors

These rules apply to every `Protocol::Messages` adapter: built-in Anthropic,
OpenCode Go Messages, and custom Messages adapters. `tools` and `tool_choice`
are omitted only when a turn has no tools and no `tool_use`/`tool_result`
history. The API rejects replayed tool blocks without `tools`, so a resumed
session with tool history still sends `"tools": []`; no live run has yet
confirmed that the API accepts an empty array alongside tool history.
`tool_choice` values `auto`, `any`, and `none` map to their own types,
`required` maps to `any`, and any other value names a tool. Current Claude
models reject forced `any` and named-tool choices with HTTP 400. Empty text
blocks are never sent, because the API rejects them.

| `stop_reason` | Outcome |
| --- | --- |
| `refusal` | Terminal non-retryable `Invalid`, `Messages refusal` |
| `max_tokens` with a `tool_use` block, no text, or a thinking block still awaiting its signature | Terminal non-retryable `Invalid`, `Messages output truncated` |
| `max_tokens` after text, no tool use | Completes with the partial text |
| `model_context_window_exceeded` | Terminal non-retryable `ContextOverflow`, `Messages context window exceeded` |
| `end_turn`, `tool_use`, `stop_sequence`, `pause_turn`, unknown | Completes |

| Stream `error.type` | Kind | Retryable before output |
| --- | --- | --- |
| `overloaded_error`, `api_error`, unknown | `Server` | Yes |
| `rate_limit_error` | `RateLimited` | Yes |
| `authentication_error`, `permission_error` | `Auth` | No |
| `billing_error`, `invalid_request_error`, `not_found_error` | `Invalid` | No |
| `request_too_large` | `ContextOverflow` | No |

Every stream error message is the fixed string `Messages stream error`. The
provider's message is never copied. A stream error can be retryable only before
the first observable output (streamed text or a started tool call) and before
`message_stop`. The adapter stream enforces this for every protocol. Private thinking is never published to observers, so it does not
block a retry. For every protocol, a transport failure (a broken body or a
stream that ends early) is likewise non-retryable once observable output has
been queued. A retry for these errors therefore never duplicates deltas that
observers have already received.

`ContextOverflow` is the exception. When it arrives after output (from
`model_context_window_exceeded` or a `request_too_large` stream error), the
runtime still compacts the session and re-runs the turn by default.
Observers then see the failed attempt's partial deltas followed by the re-run's
deltas. A failed attempt persists no assistant message and its token usage is
not counted, but live deltas already published are not rolled back. This
intentionally differs from the Chat codec, which treats every `finish_reason`
other than `tool_calls` as a completion.

For the built-in Anthropic adapter only, the following HTTP rejections are
non-retryable `ContextOverflow`:

- HTTP 413
- HTTP 400 whose sanitized text contains `prompt is too long`, `context limit`,
  or `context window` (best-effort, case-insensitive text match)

Everything else, including billing's HTTP 402 (non-retryable `Invalid`), uses
the status taxonomy.

## Anthropic model catalog

`HttpAdapter::anthropic().models()` sends one `GET /v1/models?limit=1000` with
`x-api-key` and `anthropic-version`. It maps `max_input_tokens` to
`context_limit`, where `0` means unknown, as for OpenCode Go. It reads a single
page and ignores `has_more`. A malformed base URL fails as an invalid setting
before any request. Failures are redacted and status-classified. Custom
adapters, including one named `anthropic`, return an empty catalog.

## Host product token in User-Agent

`HttpAdapter::try_with_user_agent_product("crabber-channels/0.1.0")` sets
`User-Agent: crabber-channels/0.1.0 crabber/0.1`. The same method is available
on `CustomHttpAdapter`. It accepts one `name` or `name/version` product: each
part contains 1–64 ASCII HTTP token characters (letters, digits, and
`!#$%&'*+-.^_` plus backtick, `|`, and `~`). Whitespace, comments, extra slashes,
controls, and non-ASCII bytes are invalid. The name `crabber` is reserved,
ignoring ASCII case. Repeated calls replace the earlier host product.

Rejection is a non-retryable `Invalid` error with the message
"invalid provider setting", without echoing the input. The composed header
applies to streams and the OpenCode Go model catalog. The default is
`crabber/0.1`; custom static headers and hooks still cannot set `user-agent`.

Tool-free live acceptance command (load `OPENCODE_GO_API_KEY` into the environment):

```sh
cargo test -p crabber --features opencode-go --test opencode_go_live -- --ignored
```

## Experimental durable binding primitives

`ProviderBindingDescriptor` in `crabber-core` records a provider/model route,
backend identity, behavior version, nonsecret configuration fingerprint, and
known token limits. Construction and deserialization validate the same bounds.
Native hosts own truthful version declarations; the library cannot inspect
opaque callbacks or guarantee that remote model aliases stay immutable.

`Resolver::bind` and `ProviderAdapter::bind` return a `BoundProvider` holding one
implementation and descriptor. `restore_binding` rejects changed descriptors
before acquisition. Credential refresh within that implementation does not
change binding identity. Undeclared bindings return typed `Unavailable` rather
than assuming provider/model strings identify equivalent behavior.

Use `VersionedResolver` to declare an immutable allowlist of routes over a host
resolver, or `VersionedAdapter` for an extension-provided adapter. Each declaration
must cover endpoint/gateway routing, protocol, opaque hooks and other behavior
settings without including credentials, credential values or headers. These
wrappers cannot verify falsely unchanged native configuration. `FakeProvider`
declares its built-in deterministic fixture contract.

These primitives are unstable. Runtime bound admission and checkpoint restoration
retain and verify the original descriptor, configuration and input before fresh
execution; missing or changed evidence fails closed. This binding does not prove
resource ownership or arbitrary provider effects. See the current
[server-host binding contract](../../docs/server-host.md).

Provider HTTP boundaries emit `crabber.provider.request` and `crabber.provider.models` spans under `crabber::providers`. Request spans end when response headers arrive and record finite status/error categories; URLs, headers and response bodies are excluded. See the [observability guide](../../docs/observability.md).

## Reasoning controls and admission

Reasoning effort is selected through `ModelControls` on `AgentConfig` or
`ExecutionContext::with_model_controls`: `low`, `medium`, or `high`. Responses
serializes an admitted value as `reasoning.effort`; the default preserves Codex
`"reasoning": null` and omits `reasoning` for OpenAI. Chat serialization is
described above; Messages ignores the control. No summary is requested.
Controls occupy index 15 of admission semantics, so changing effort conflicts
with a retained keyed admission. Older 15-element admissions cannot replay.

Device-code, access-only inference and catalog require Matt's live pre-merge
acceptance on this branch: run `codex-login device`,
`codex-login catalog --client-version <version>`, and a Responses stream with
`CRABBER_CODEX_PROFILE=codex-cli`; record the actual result in the live table.
Offline fake-IdP tests do not prove the live audience contract. Changing the
ID-token audience validation requires explicit approval and a separate reviewed
commit.
