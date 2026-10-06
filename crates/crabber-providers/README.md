# Crabber model providers

The real adapters are Cargo features on `crabber-providers` (`anthropic`, `openai`, `codex`, and `opencode-go`). `crabber` re-exports the same features. `AgentBuilder::providers_from_env()` registers the compiled adapters. The minimal embedding example enables all four and accepts `--provider`, `--model`, and OpenCode Go's `--protocol` (`responses`, `messages`, or `chat-completions`). The default no-argument example still uses the scripted provider.

| Provider | Credential source | Protocol |
| --- | --- | --- |
| `anthropic` | `ANTHROPIC_API_KEY` | Messages API |
| `openai` | `OPENAI_API_KEY` | Responses API |
| `opencode-go` | `OPENCODE_GO_API_KEY` | OpenCode Go API; Responses by default |
| `codex` | `crabber` user config `auth.json` from `codex-login` | ChatGPT plan usage via public Responses API |

Codex sign-in follows the current [Sign in with ChatGPT open-source agent flow](https://developers.openai.com/siwc/token-sharing-open-source/sign-in): dynamic client registration, a loopback browser callback, PKCE, ID-token signature and claim validation, and the `chatgpt.tokens.use.direct` grant. Inference uses the [documented public Responses endpoint](https://developers.openai.com/siwc/token-sharing-open-source/models-and-inference) with `store:false` and `stream:true`. The earlier design note's private `chatgpt.com/backend-api/codex/responses` endpoint is superseded by the published flow. The device-code helper is isolated for compatibility testing; no device sign-in is offered for the current SIWC flow.

Credentials are written under `${XDG_CONFIG_HOME}/crabber/auth.json`, or `${HOME}/.config/crabber/auth.json`, with owner-only file permissions on Unix. The stable host identifier is stored in the same directory. The adapter never logs credential values or request bodies.

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
sanitized excerpt and can choose `ProviderErrorKind` and retryability, but it
applies only to
custom adapters. The terminal second 401 after dynamic credential refresh is
the exception: Crabber bypasses the classifier and returns non-retryable `Auth`.
Built-in provider classification is unchanged. A custom
adapter returns no model catalog and never mints a gateway token. Model
discovery and token minting/refresh remain host-owned (normally behind the
host's resolver/catalog and `CredentialSource`). Construction and registration
through `HttpResolver::with_adapter` perform no credential fetch and no network
request.

## Live acceptance record

The subscription checks must be run locally with authorized accounts. Criterion 1 (the existing rows) requires a streamed reply, an executed `echo` tool round trip, and a settled run. Criterion 2 (tool-free Chat Completions) requires streamed text, a completed run, and exactly one successful model operation observed. The SSE fixtures and codec tests do not substitute for this gate.

| Date | Backend | Model | Outcome |
| --- | --- | --- | --- |
| 2026-10-06 UTC | OpenCode Go, Chat Completions, tool-free (criterion 2) | `deepseek-v4-flash` | Passed on clean code commit `5769fe276de5781abb43c0c317fb825b44afcecd` (`git status --porcelain` empty). Command: `cargo test -p crabber --features opencode-go --test opencode_go_live -- --ignored`, with `OPENCODE_GO_API_KEY` loaded from the environment. Streamed text, completed run, exactly one successful model operation. Body shape is proven by loopback tests; this accepts the current body and does not reproduce the consumer’s earlier empty-tools-array probe. |
| 2026-09-29 EDT | OpenCode Go | `gpt-5.6-luna` | Passed on candidate `ada6eb5`: Responses stream replied, `echo` tool settled, final text `Echo`, process exited 0. Command: `cargo run -p minimal-embed -- --provider opencode-go --protocol responses --model gpt-5.6-luna`. |
| 2026-09-29 EDT | ChatGPT plan / Codex | `gpt-5.5` | Passed on candidate `954146f`: browser OAuth signed in, `auth.json` mode was `0600`, Responses stream replied, `echo` tool settled, final text `Echo tool used.`, process exited 0. Command: `cargo run -p minimal-embed -- --provider codex --model gpt-5.5`. |

Commands:

```sh
cargo run -p codex-login -- login
cargo run -p minimal-embed -- --provider codex --model <account-listed-model-id>
cargo run -p minimal-embed -- --provider opencode-go --protocol responses --model <id-from-GET-models>
```

Set `OPENCODE_GO_API_KEY` in the environment before the OpenCode Go command. No key value belongs in this record.

## Tool-free Chat Completions

`HttpAdapter::opencode_go(Protocol::ChatCompletions)` omits `tools` and
`tool_choice` when no tools are registered and sends the output cap as
`max_tokens`. `max_completion_tokens` remains a custom HTTP option through
`CustomHttpAdapter::with_chat_token_field`.

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
