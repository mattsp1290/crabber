# Operational telemetry host

Run `cargo run -p operational-telemetry -- --check` from a clean committed checkout.
No environment credentials or external services are required. The host runs a
scripted public provider, native echo tool, memory store, composed callbacks and
facade broadcasts against loopback intakes. Assertions exit unsuccessfully if
outcomes, measurements, sample counts, redaction, recovery or local health differ.
The command is included in `cargo xtask check` and therefore the existing Ubuntu
and macOS CI gates. The external consumer compiles and executes the same public
host journey outside the workspace.

The full compiled Git SHA and source root are compared to the actual launching
checkout's root and HEAD. Dirty or untracked source files fail the identity proof.
`build.rs` watches the worktree HEAD, referenced common-directory ref and packed
refs; changing revisions or worktrees with a shared Cargo target rebuilds its
stamp. Direct invocation of a stale binary from a different checkout fails.
Run from the checkout, including when invoking the binary directly.

Fixtures include startup and midstream provider failures, cancellation during
acquisition and consumption, successful and failed native tools followed by a
successful model continuation, repeated text streams, empty/reasoning/no-text
calls, and a replacement lease. A manually controlled monotonic clock produces
known integer millisecond measurements: text model elapsed 50, first token 30,
tool-only model 10, native tool 15, full model/tool run 75. Empty deltas and
reasoning precede text; later text never adds another first-token sample.
The lease fixture uses the public runtime and an injectable wall clock to prove
that local loss observations do not durably settle the replacement owner. Its
real captured observations are forwarded to the intake after virtual-time
fencing checks finish. Other main fixtures export directly from runtime callbacks.

Mock requests are decoded from actual deflate distributions, gzip metrics/logs,
and plain native LLM envelopes. Accepted sample counts are distinguished from
HTTP attempts. Every expected individual measurement is checked against captured
samples, including exact integer values; fractional values fail. Metric dimensions
and finite purpose/reason labels are checked, and sentinel prompt/token/argument/
result/response content must be absent. The fixture rejects a later log stage
then recovers without replaying accepted earlier chunks/stages. A separate 503
fixture proves run completion and broadcasts independent of intake availability.
A synchronous burst on the current-thread runtime deterministically fills a
two-record queue: two accepted, 98 dropped. This supplemental queue stress reuses
an actual captured model measurement; it does not manufacture main runtime outcomes.

Slow intake is first synchronized to an actual stalled request, then Tokio time
is paused and advanced to the configured ten-second control budgets. A timed-out
flush leaves a live worker; shutdown eventually stops it, zeros local depths and
accounts for outstanding drops. Fast loopback requests run with real Tokio time.

For adoption, implement `Observer::operational_completed` or
`operational_completed_in_attempt` and attach observers with repeated
`AgentBuilder::observer`. Call `AgentBuilder::datadog(config)` for export and read
`Agent::export_health()` locally; attaching a standalone `DatadogObserver` with
`observer` lets the host call its own `health()` and `flush()`/`shutdown()`, while
facade health remains `None`. Keep callbacks bounded, nonblocking and free of
content. The sample exposes disabled/configured facade health and standalone
observer health; counters represent observations, not necessarily samples.

[Runtime observation adoption](../../docs/operational-observation.md) specifies
the finite taxonomy, execution/compaction timing boundaries, clock regression,
pause/resume/recovery/abandon semantics, attempt identity and loss-of-fence limits.
[Exporter documentation](../../crates/crabber-obs/README.md) specifies API migration,
health units and cumulative/current fields, explicit 32-name allowlists and
`overflow`, retries and response-loss uncertainty, queue/pending bounds, control
timeouts and eventual shutdown, safe diagnostics, all metric names and units.
No durable schema change is introduced by this example; its store is in-memory.

Distributions use `POST /api/v1/distribution_points`, timestamp plus individual
sample arrays and zlib `Content-Encoding: deflate`. The configured raw JSON limit
is conservative, not an invented v1 service limit; 413 causes chunk splitting.
Existing spans, counters/gauges and logs keep their own transport and names.
Percentile aggregations must be enabled for the distribution metric names in
Datadog. Local intake acceptance is **UNVERIFIED** evidence of live ingestion or
percentile visibility; live verification and alerts remain deferred to
`crabber-jeyl`. Real adoption uses variable names `DD_API_KEY`, `DD_SITE`,
`DD_SERVICE`, `DD_ENV`, `DD_VERSION`, `DD_LLMOBS_ML_APP`; the fixture never reads them.
