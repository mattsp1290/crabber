# Crabber Datadog export

Enable the `crabber/datadog` feature and call `AgentBuilder::datadog_from_env()`. Set `DD_API_KEY` and optionally `DD_SITE`, `DD_SERVICE`, `DD_ENV`, `DD_VERSION`, and `DD_LLMOBS_ML_APP`. `DD_SITE` defaults to `datadoghq.com`. Exports use the LLM Observability span intake, metrics v2 intake, and logs v2 intake. `DD_APP_KEY` is only read by `cargo xtask verify-datadog` for ingestion queries.

The observer allowlists lifecycle fields before queuing. Prompt and completion text, tool arguments and results, reasoning, and headers are never exported. The bounded queue drops observations when full and reports `crabber.export.dropped`. Call `agent.flush().await` before process exit, or `agent.shutdown().await` to flush and stop the worker.

Run the credential-free example with `cargo run -p datadog-export`. The live gate is `DD_SITE=... DD_API_KEY=... DD_APP_KEY=... cargo xtask verify-datadog`; it submits a marker-tagged fake run, requires LLM Obs intake HTTP 202, and queries metrics and logs for up to two minutes. A user must also confirm the spans in the LLM Observability UI.

## Live verification

| Date | Site | Intake | Metrics | Logs | UI spans |
| --- | --- | --- | --- | --- | --- |
| Pending | Pending | Pending | Pending | Pending | Pending |

The schema remains unverified against live Datadog intake until this row is completed.

References: [LLM Observability HTTP API](https://docs.datadoghq.com/llm_observability/instrument/api/), [Submit metrics](https://docs.datadoghq.com/api/latest/metrics/submit-metrics/), [Send logs](https://docs.datadoghq.com/api/latest/logs/send-logs/), [Search logs](https://docs.datadoghq.com/api/latest/logs/search-logs-post/).
