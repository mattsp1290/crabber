# Allowed metadata differences

Only these exact changes are permitted by the parity test. All other names,
descriptions, and schema fields match the extracted upstream fixtures.

```json
[
  {
    "tool": "search",
    "pointer": "/description",
    "upstream": "Search workspace files via ripgrep. Defaults to regex mode and preserves existing pattern/path/timeout behavior. Optional glob filters, literal mode, ignore-case mode, context lines, and match limit are supported. Per-call timeout defaults to 60s and is capped at 600s.",
    "rust": "Search workspace files via ripgrep. Defaults to regex mode and preserves existing pattern/path/timeout behavior. Optional glob filters, literal mode, ignore-case mode, context lines, and match limit are supported. Per-call timeout defaults to 60s and is capped at 600s. The host resolves the ripgrep binary once; configuration files are always disabled.",
    "reason": "Host-owned binary/environment and explicit output policy; overview decisions 8-10 and deviation table."
  },
  {
    "tool": "search",
    "pointer": "/parameters/properties/limit/minimum",
    "upstream": 1,
    "rust": 0,
    "reason": "Zero selects default limit, as specified in 04-search; explicit schema deviation."
  },
  {
    "tool": "shell",
    "pointer": "/description",
    "upstream": "Run a command with host-configured shell policy in the agent's workspace cwd. Captures stdout, stderr, exit code, and duration. Per-call timeout defaults to 60s and is capped at 600s. Stdout/stderr use host-configured per-stream caps (default 256 KiB); oversize output sets truncated=true.",
    "rust": "Run a command with host-configured shell policy in the agent's workspace cwd. Captures stdout, stderr, exit code, and duration. Per-call timeout defaults to 60s and is capped at 600s. Stdout/stderr use host-configured per-stream caps (explicitly supplied by the host); oversize output sets truncated=true.",
    "reason": "Host-owned binary/environment and explicit output policy; overview decisions 8-10 and deviation table."
  },
  {
    "tool": "user_interact",
    "pointer": "/description",
    "upstream": "Ask the user a question and return their answer as a string. In CLI mode, prints the question to stderr and reads a multi-line answer from stdin (terminated by a blank line or EOF). In MCP mode, returns immediately with a pending result — provide the user's answer in a follow-up call by populating the answer field. Does not format questions, validate answers, or offer multiple-choice options.",
    "rust": "Ask the user a question and return their answer as a string. Depending on host configuration, either pauses until the host records the user's answer or returns immediately with a pending result — provide the user's answer in a follow-up call by populating the answer field. Does not format questions, validate answers, or offer multiple-choice options.",
    "reason": "Durable host responses replace stdin/stderr; one description for both surfaces."
  },
  {
    "tool": "user_interact",
    "pointer": "/parameters/properties/answer/description",
    "upstream": "Reserved for the agent loop. Do not populate this field — it will be set by the loop after the user responds. When non-empty, the tool returns this value as the answer immediately.",
    "rust": "Reserved for relaying an answer the host already collected for a pending question; when non-empty, the tool returns it immediately. Rejected on the durable surface. Leave empty when asking.",
    "reason": "Durable surface rejects model-supplied answers."
  }
]
```
