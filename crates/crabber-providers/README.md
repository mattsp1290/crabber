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

## Live acceptance record

The subscription checks must be run locally with authorized accounts. A pass requires a streamed reply, an executed `echo` tool round trip, and a settled run. The SSE fixtures and codec tests do not substitute for this gate.

| Date | Backend | Model | Outcome |
| --- | --- | --- | --- |
| 2026-09-29 EDT | OpenCode Go | `gpt-5.6-luna` | Passed on candidate `ada6eb5`: Responses stream replied, `echo` tool settled, final text `Echo`, process exited 0. Command: `cargo run -p minimal-embed -- --provider opencode-go --protocol responses --model gpt-5.6-luna`. |
| Pending | ChatGPT plan / Codex | Pending | Unverified against live service |

Commands:

```sh
cargo run -p codex-login -- login
cargo run -p minimal-embed -- --provider codex --model <account-listed-model-id>
cargo run -p minimal-embed -- --provider opencode-go --protocol responses --model <id-from-GET-models>
```

Set `OPENCODE_GO_API_KEY` in the environment before the OpenCode Go command. No key value belongs in this record.
