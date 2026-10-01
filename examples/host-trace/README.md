Run `cargo run -p host-trace` without credentials. The MemoryStore journey uses a
fake provider, two parallel tools synchronized at a barrier, an explicit 128-bit
host context and an observer composed with facade broadcasts. It asserts callback
identity and counts and prints only safe run identity and assertion counts. No
global tracing subscriber is installed.

Construct `TraceContext::new(trace_hex, span_hex)` before calling the facade's
`prompt_with_context` or `prompt_keyed_with_context`, or the runtime's
`start_with_context` or `start_keyed_with_context`. Their final argument is
`Option<TraceContext>`; existing context-free methods continue to work. Trace IDs
must have 16 or 32 hexadecimal digits; span IDs must have 16. Zero, other widths,
nonhex identity and unknown serialized fields are rejected. Serialization contains
only `trace_id` and `span_id`. Debug and validation errors redact input. No baggage,
prompts, arguments, credentials or paths belong in context.

Add host observers with `Agent::builder().observer(Arc::new(observer))`. Repeated
calls compose observers. Override `emit_with_context` and
`model_completed_with_context` to inspect the explicit identity. Existing custom
observers implementing `emit` and optionally `model_completed` remain compatible:
the new methods default to those callbacks. Model completion is a separate observer
callback, not an additional facade broadcast. Host callbacks should be fast and
must not panic; callbacks execute synchronously on the run's tasks.

Each execution owns immutable context and carries it through lifecycle, model and
parallel tool tasks. Concurrent sessions do not share context. Identity is transport
metadata excluded from the semantic admission fingerprint: retrying a keyed prompt
with different context returns the original receipt without executing or replacing
its context. This slice does not persist identity or attach new recovery attempt
contexts; durable handoff/recovery and Datadog export field mapping are separate
milestone slices. Hosts own subscriber and exporter initialization. The existing
optional Datadog observer and tracing bridge remain composed with host observers
and broadcasts; the bridge exposes only approved host trace/span identity.
