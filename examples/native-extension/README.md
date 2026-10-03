# Native extension

```sh
cargo run -p native-extension
```

The existing fake script requests a forbidden `rm -rf` and an allowed `echo hello`
through a registered native shell tool. A prompt section contributes guidance,
a guard denies recursive deletion before execution, ToolResultTransform redacts
the allowed tool's private result field, and EventPublished notifications provide
observer evidence. The main program checks registration, prompt guidance, redacted
content in the next provider request, and observed lifecycle events. Its stdout
test also verifies the forbidden call never executes. The fake script ends with
assistant text `Done`.

The shell tool implements `execute_with_context` and prints the session's
persisted workspace ID and directory from `ToolContext::workspace()`. Both are
`Option`s: a session without one reports `unavailable` instead of a default.
This example keeps `AgentConfig::new`'s defaults, so it prints `default` and
`.`; those are real persisted values, not placeholders.

No shell command is executed: the demo executor returns fake JSON. Registration,
guards, transforms and notifications use the real extension pipeline. See
[src/main.rs](src/main.rs) and the [embedding matrix](../../docs/embedding.md).
