use super::{
    Fields,
    metric_values::{OUTCOMES, error_tag, tag},
};
use crate::{MetricDimensions, dimension};

pub(super) fn known_span(name: &str) -> bool {
    matches!(
        name,
        "crabber.store.transaction"
            | "crabber.store.connect"
            | "crabber.store.migrate"
            | "crabber.provider.request"
            | "crabber.extension.mount"
            | "crabber.extension.close"
            | "crabber.extension.transform_result"
            | "crabber.admission"
            | "crabber.approval.suspend"
            | "crabber.approval.continue"
            | "crabber.wasm.call"
    )
}
pub(super) struct Metric {
    pub(super) distribution: Option<&'static str>,
    pub(super) counter: Option<&'static str>,
    pub(super) tags: Vec<String>,
}
pub(super) fn describe(name: &str, fields: &Fields, dimensions: &MetricDimensions) -> Metric {
    let outcome = || tag(fields, "outcome", "outcome", OUTCOMES);
    let identity = |key: &str, label: &str, allowed: &[String]| {
        format!(
            "{label}:{}",
            dimension(&fields.get(key).unwrap_or_default(), allowed)
        )
    };
    let (distribution, counter, tags) = match name {
        "crabber.store.transaction" => (
            Some("crabber.store.transaction_ms"),
            Some("crabber.store.transactions"),
            transaction_tags(fields),
        ),
        "crabber.store.connect" | "crabber.store.migrate" => (
            Some(if name.ends_with("connect") {
                "crabber.store.connect_ms"
            } else {
                "crabber.store.migrate_ms"
            }),
            None,
            vec![
                tag(fields, "db.system", "db", &["postgresql", "sqlite"]),
                outcome(),
            ],
        ),
        "crabber.provider.request" => (
            Some("crabber.provider.response_start_ms"),
            Some("crabber.provider.requests"),
            provider_tags(fields, dimensions),
        ),
        "crabber.extension.mount" => (
            Some("crabber.extension.mount_ms"),
            None,
            vec![
                identity("extension.id", "extension", &dimensions.extensions),
                tag(
                    fields,
                    "scope",
                    "scope",
                    &["global", "workspace", "session"],
                ),
                outcome(),
            ],
        ),
        "crabber.extension.close" => (
            Some("crabber.extension.close_ms"),
            (fields.get("outcome").as_deref() == Some("timed_out"))
                .then_some("crabber.extension.close_timeouts"),
            vec![
                identity("extension.id", "extension", &dimensions.extensions),
                outcome(),
            ],
        ),
        "crabber.extension.transform_result" => (
            Some("crabber.extension.transform_ms"),
            None,
            vec![identity("tool.name", "tool", &dimensions.tools), outcome()],
        ),
        "crabber.admission" => (
            Some("crabber.admission.elapsed_ms"),
            Some("crabber.admission.outcomes"),
            vec![outcome(), tag(fields, "keyed", "keyed", &["true", "false"])],
        ),
        "crabber.approval.suspend" => (None, Some("crabber.approval.suspensions"), vec![outcome()]),
        "crabber.approval.continue" => (
            None,
            Some("crabber.approval.continuations"),
            vec![outcome()],
        ),
        "crabber.wasm.call" => (
            Some("crabber.wasm.call_ms"),
            None,
            vec![
                identity("extension.id", "extension", &dimensions.extensions),
                outcome(),
            ],
        ),
        _ => (None, None, vec![]),
    };
    Metric {
        distribution,
        counter,
        tags,
    }
}

fn transaction_tags(fields: &Fields) -> Vec<String> {
    let mut tags = vec![
        tag(fields, "db.system", "db", &["postgresql", "sqlite"]),
        tag(
            fields,
            "crabber.store.scope",
            "scope",
            &[
                "unscoped",
                "session",
                "run",
                "lease",
                "admission",
                "events",
                "snapshot",
                "approval",
                "tool_continuation",
                "queue",
                "verify",
            ],
        ),
        tag(fields, "outcome", "outcome", OUTCOMES),
    ];
    if matches!(
        fields.get("outcome").as_deref(),
        Some("begin_failed" | "commit_failed")
    ) {
        tags.push(error_tag(fields));
    }
    tags
}
fn provider_tags(fields: &Fields, dimensions: &MetricDimensions) -> Vec<String> {
    let status = fields
        .get("http.response.status_code")
        .and_then(|v| v.parse::<u16>().ok());
    let class = match status {
        Some(200..=299) => "2xx",
        Some(400..=499) => "4xx",
        Some(500..=599) => "5xx",
        _ if fields.get("outcome").as_deref() == Some("transport_error") => "transport",
        _ => "other",
    };
    let retried = fields
        .get("retry.attempt")
        .and_then(|v| v.parse::<u64>().ok())
        .is_some_and(|v| v > 0);
    vec![
        format!(
            "provider:{}",
            dimension(
                &fields.get("provider.id").unwrap_or_default(),
                &dimensions.providers
            )
        ),
        tag(
            fields,
            "protocol",
            "protocol",
            &[
                "openai",
                "anthropic",
                "openai_compatible",
                "openai_responses",
                "chat_completions",
                "responses",
                "messages",
                "sse",
                "http",
            ],
        ),
        format!("status_class:{class}"),
        format!("retried:{retried}"),
        error_tag(fields),
    ]
}
