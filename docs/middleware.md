# Model middleware and host-authorized `AGENTS.md`

Crabber model middleware currently has one capability: it may append bounded text to a model request's system prompt. It cannot replace the system prompt or mutate any other part of a request. The first-party [`AgentsMdExtension`](../crates/crabber-middleware/src/agents_md.rs) recipe reads explicitly configured instruction files through a capability supplied by the embedding host; it never opens the process filesystem.

Run the complete, credential-free embedding:

```sh
cargo run -p agents-md-middleware
```

The [source](../examples/agents-md-middleware/src/main.rs) uses an in-memory resolver and reader, executes one fake-provider prompt, and asserts the exact frame, persisted workspace context, path, byte bound, call counts, and denial of a forged context.

## Registration and frozen identity

Typed `SystemPromptMiddleware` and generic prompt callbacks use the same collector controls: deterministic ordering, scope selection, collision checks, per-callback and aggregate deadlines, cancellation, panic containment, byte limits, and sanitized failure. Contributions are ordered by `(order, registration name)`. Names collide under the same shared registration rules, and global/session scope selection is shared too. A generic/legacy callback never receives a workspace resolver; that capability is available only in the runtime-created typed attempt context. Sharing controls does **not** give the callback forms equal capabilities.

The typed identity fields and contract are stable plan identity: registration name, order, middleware contract version, descriptor kind, descriptor version, and descriptor configuration hash. A descriptor is a trusted native extension's attestation. Crabber validates its shape, but cannot inspect arbitrary native callback behavior or prove that its hash describes that behavior. First-party recipes therefore keep validated configuration, canonical hashing, and implementation version in lockstep.

Strict resume seals the contract version, registration name, order, descriptor kind/version/configuration hash through the frozen plan. It does **not** seal opaque callback behavior, resolver or reader identity, host authorization policy, resolved root, symlink policy, or file contents. Deployments must treat native code and host-policy changes as operational compatibility decisions even when the plan still matches.

## Workspace authority boundary

`WorkspaceContext.directory` (like `workspace_id`) is persisted routing metadata, never authority. It is compared and passed verbatim; it is not a trusted root. At runtime Crabber attenuates the configured resolver to the exact workspace context admitted for that attempt. The host resolver must nevertheless validate **every** context-to-reader mapping and deny unknown or mismatched values. Never turn `directory` directly into filesystem authority.

The reader returned by the host owns rooting, path traversal prevention, symlink policy, access control, and backend selection. It must honor `read_limited(path, max_bytes)`: return at most the bound, and return `TooLarge` without bytes when the object does not fit. A safe adapter should resolve beneath an independently authorized root, apply a documented symlink policy at every component, reject races or escapes, and avoid ambient process-current-directory access.

The recipe skips only `NotFound` for a file configured as optional. A required `NotFound`, a missing resolver, `Denied`, `Io`, `TooLarge`, `InvalidPath`, invalid UTF-8, cancellation, panic, timeout, or collector limit failure aborts the model attempt with a sanitized error. Plan identity and public errors contain no instruction content, path, root, or backend details. System-prompt middleware applies only to agent-turn model requests; internal model requests such as context compaction bypass it and keep their dedicated prompts. There is no content cache: each agent-turn attempt resolves and reads again, so retries and the next agent-turn attempt after compaction observe newly authorized contents.

## Configuration, paths, and framing

`AgentsMdConfig` accepts 1 through 16 unique paths and does not normalize them. Each path is UTF-8 and at most 1,024 bytes; it must be nonempty and relative, have no leading or trailing `/`, empty segment (`//`), `.` or `..` segment, backslash, colon, NUL, or control character. Each file read is bounded to 32 KiB. The shared collector applies its own aggregate contribution limits as well.

Every present file is appended in configured order using this exact UTF-8 frame (with `{bytes}` equal to the body byte length):

```text
## Workspace instructions: {path}
<!-- crabber:agentsmd bytes={bytes} -->
{body}
## End workspace instructions: {path}
```

A newline follows the closing heading. Multiple frames have one additional newline between them. The framing labels content for the model; it is not an authorization mechanism and does not make untrusted instructions safe.

## Host embedding checklist

1. Construct `AgentConfig` with the workspace ID and directory that will be persisted for new sessions; retain an independently trusted mapping from that exact pair to an authorized root or object namespace.
2. Implement `WorkspaceReaderResolver` as allow-list matching. Reject every unrecognized pair with `Denied`, without returning a reader.
3. Implement a read-only `WorkspaceReader` that enforces the unchanged validated relative path, rooting, symlink and access policy, and the supplied bound. Do not use ambient current-directory authority.
4. Mount `AgentsMdExtension` in the intended `Scope` and pass the resolver through `Agent::builder().workspace_reader_resolver(...)`.
5. Keep descriptor version/configuration hashing in lockstep with recipe semantics. Settle or finish incompatible frozen runs before changing plan identity.
6. Test authorized and forged contexts, exact path/bound handling, oversized and malformed content, all error classes, retries, cancellation, and sanitized externally visible failures.
7. Review instruction files as model input, not trusted executable policy. Tool guards and host authorization remain mandatory.

## Deliberately out of scope

Model middleware does not provide arbitrary history/context mutation; tools from middleware or tool injection; store access; direct model/provider dispatch or replacement; dynamic tool search/discovery; summarization/reduction; scratch state; filesystem writes or an ambient process-filesystem adapter; a reader cache; durable middleware events; or a WASM middleware ABI. Those require separate, explicit capability and durability designs rather than expansion of the prompt-text hook.
