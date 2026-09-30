# Crabber Datadog export

Enable the `crabber/datadog` feature and call `AgentBuilder::datadog_from_env()`. Set `DD_API_KEY` and optionally `DD_SITE`, `DD_SERVICE`, `DD_ENV`, `DD_VERSION`, and `DD_LLMOBS_ML_APP`. `DD_SITE` defaults to `datadoghq.com`. Exports use the LLM Observability span intake with plain JSON, and the metrics v2 and logs v2 intakes with gzip JSON. `DD_APP_KEY` is only read by `cargo xtask verify-datadog` for ingestion queries.

The original intake acceptance expected gzip for all three signals. In the US3 live gate, LLM Obs rejected the gzip request with HTTP 400 and accepted plain JSON with HTTP 202. The current [HTTP API reference](https://docs.datadoghq.com/llm_observability/instrument/api/) documents JSON for span intake. Metrics and logs continue to use gzip.

The observer allowlists lifecycle fields before queuing. Prompt and completion text, tool arguments and results, reasoning, and headers are never exported. The bounded queue drops observations when full and reports `crabber.export.dropped`. Call `agent.flush().await` before process exit, or `agent.shutdown().await` to flush and stop the worker.

Run the credential-free example with `cargo run -p datadog-export`. The live gate is `DD_SITE=... DD_API_KEY=... DD_APP_KEY=... cargo xtask verify-datadog`; it submits a marker-tagged fake run, requires LLM Obs intake HTTP 202, and queries metrics and logs for up to two minutes. Verify the spans in Datadog's LLM Observability UI or with an authenticated span search.

## Live verification

| Date | Candidate | Site | Intake | Metrics | Logs | Span visibility |
| --- | --- | --- | --- | --- | --- | --- |
| 2026-09-30 02:15 UTC | `42fbb96` | `us3.datadoghq.com` | HTTP 202 | Found (query HTTP 200, 1 series) | Found | Later confirmed by direct query |
| 2026-09-30 03:09 UTC | `653571c` | `us3.datadoghq.com` | HTTP 202 | Found (query HTTP 200, status ok, 1 series) | Found | Agent, workflow, and LLM spans found by direct query |
| 2026-09-30 03:18 UTC | `ceb0924` | `us3.datadoghq.com` | HTTP 202 | Found (query HTTP 200, status ok, 1 series) | Found | Agent, workflow, and LLM spans found by direct query |

The `ceb0924` live gate exited 0 with marker `crabber-1790738310-13253`. The user authorized direct verification with the authenticated `pup` CLI. `pup llm-obs spans search --query 'verify:crabber-1790738310-13253' --from 1h --summary` found linked agent, workflow, and LLM spans on trace `9245911478003124504`; the agent and workflow each covered 1.691 ms. This verifies span visibility through Datadog's read API; a browser UI click was not performed. Earlier gates on `653571c` and `42fbb96` also exited 0 before the final chunk-retry and failed-model observation fixes.

References: [LLM Observability HTTP API](https://docs.datadoghq.com/llm_observability/instrument/api/), [Submit metrics](https://docs.datadoghq.com/api/latest/metrics/submit-metrics/), [Send logs](https://docs.datadoghq.com/api/latest/logs/send-logs/), [Search logs](https://docs.datadoghq.com/api/latest/logs/search-logs-post/).
