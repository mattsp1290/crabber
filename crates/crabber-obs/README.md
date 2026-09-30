# Crabber Datadog export

Enable the `crabber/datadog` feature and call `AgentBuilder::datadog_from_env()`. Set `DD_API_KEY` and optionally `DD_SITE`, `DD_SERVICE`, `DD_ENV`, `DD_VERSION`, and `DD_LLMOBS_ML_APP`. `DD_SITE` defaults to `datadoghq.com`. Exports use the LLM Observability span intake with plain JSON, and the metrics v2 and logs v2 intakes with gzip JSON. `DD_APP_KEY` is only read by `cargo xtask verify-datadog` for ingestion queries.

The original intake acceptance expected gzip for all three signals. In the US3 live gate, LLM Obs rejected the gzip request with HTTP 400 and accepted plain JSON with HTTP 202. The current [HTTP API reference](https://docs.datadoghq.com/llm_observability/instrument/api/) documents JSON for span intake. Metrics and logs continue to use gzip.

The observer allowlists lifecycle fields before queuing. Prompt and completion text, tool arguments and results, reasoning, and headers are never exported. The bounded queue drops observations when full and reports `crabber.export.dropped`. Call `agent.flush().await` before process exit, or `agent.shutdown().await` to flush and stop the worker.

Run the credential-free example with `cargo run -p datadog-export`. The live gate is `DD_SITE=... DD_API_KEY=... DD_APP_KEY=... cargo xtask verify-datadog`; it submits a marker-tagged fake run, requires LLM Obs intake HTTP 202, and queries metrics and logs for up to two minutes. A user must also confirm the spans in the LLM Observability UI.

## Live verification

| Date | Candidate | Site | Intake | Metrics | Logs | Direct span query | UI spans |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-09-30 02:15 UTC | `42fbb96` | `us3.datadoghq.com` | HTTP 202 | Found (query HTTP 200, 1 series) | Found | Not queried | Pending user confirmation |
| 2026-09-30 03:09 UTC | `653571c73461bc88a2781b6c7cf68cba1454f895` | `us3.datadoghq.com` | HTTP 202 | Found (query HTTP 200, status ok, 1 series) | Found | Agent, workflow, and LLM spans found | Pending user confirmation |

The `653571c` live gate `cargo xtask verify-datadog` exited 0 with marker `crabber-1790737749-73371`. A direct `pup llm-obs spans search --query 'verify:crabber-1790737749-73371' --from 1h --summary` found agent, workflow, and LLM spans on trace `16248679833872246374`, with the expected parent IDs and 3.218 ms agent and workflow durations. This is a direct Datadog query result; user confirmation in the browser UI remains pending. The earlier `42fbb96` gate also exited 0 with marker `crabber-1790734554-56395` before the retry and span-timing changes.

References: [LLM Observability HTTP API](https://docs.datadoghq.com/llm_observability/instrument/api/), [Submit metrics](https://docs.datadoghq.com/api/latest/metrics/submit-metrics/), [Send logs](https://docs.datadoghq.com/api/latest/logs/send-logs/), [Search logs](https://docs.datadoghq.com/api/latest/logs/search-logs-post/).
