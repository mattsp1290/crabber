//! Read API contracts: <https://docs.datadoghq.com/llm_observability/investigate/export_api/>
use super::Failure;
use serde_json::{Value, json};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

#[derive(Clone, Copy)]
pub(super) struct Window {
    from: i64,
    to: i64,
}
impl Window {
    pub fn new(from: i64, to: i64) -> Self {
        Self { from, to }
    }
    pub fn start_rfc3339(self) -> String {
        utc(self.from)
    }
    pub fn end_rfc3339(self) -> String {
        utc(self.to)
    }
    fn overlaps(self, timestamp: i64, interval: i64) -> bool {
        interval > 0
            && timestamp <= self.to.saturating_mul(1000)
            && timestamp.saturating_add(interval) > self.from.saturating_mul(1000)
    }
}
fn utc(seconds: i64) -> String {
    OffsetDateTime::from_unix_timestamp(seconds)
        .expect("valid execution time")
        .format(&Rfc3339)
        .expect("UTC format")
}
pub(super) struct Query {
    pub marker: String,
    pub service: String,
    pub ml_app: String,
    pub window: Window,
}
#[derive(Clone, Debug)]
pub(super) enum Evidence {
    Graph {
        trace: String,
        agent: String,
        workflow: String,
        llm: String,
    },
    Metric {
        name: &'static str,
        points: usize,
    },
    Logs(usize),
}
impl Evidence {
    pub fn print(&self) {
        match self {
            Self::Graph {
                trace,
                agent,
                workflow,
                llm,
            } => println!(
                "spans=visible trace_id={trace} agent_span_id={agent} workflow_span_id={workflow} llm_span_id={llm}"
            ),
            Self::Metric { name, points } => println!("metric={name} finite_points={points}"),
            Self::Logs(count) => println!("logs=visible count={count}"),
        }
    }
}
#[derive(Clone)]
pub(super) struct Span {
    kind: String,
    id: String,
    parent: String,
    trace: String,
    tags: Vec<String>,
}
fn string(value: &Value) -> Result<&str, Failure> {
    value.as_str().ok_or(Failure::Schema)
}
fn id(value: &Value) -> Result<String, Failure> {
    let value = string(value)?;
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(Failure::Schema);
    }
    Ok(value.into())
}
// Datadog may encode integral millisecond timestamps as JSON floating-point numbers.
#[allow(clippy::cast_possible_truncation)] // Exact integer and 2^53 bound checked before conversion.
fn milliseconds(value: &Value) -> Result<i64, Failure> {
    value
        .as_i64()
        .or_else(|| {
            value
                .as_f64()
                .filter(|v| v.is_finite() && v.fract() == 0.0 && v.abs() < 9_007_199_254_740_992.0)
                .map(|v| v as i64)
        })
        .ok_or(Failure::Schema)
}
pub(super) fn span_page(body: &Value) -> Result<Vec<Span>, Failure> {
    body["data"]
        .as_array()
        .ok_or(Failure::Schema)?
        .iter()
        .map(|resource| {
            if resource["type"] != "span" {
                return Err(Failure::Schema);
            }
            let a = &resource["attributes"];
            Ok(Span {
                kind: string(&a["span_kind"])?.into(),
                id: id(&a["span_id"])?,
                parent: string(&a["parent_id"])?.into(),
                trace: id(&a["trace_id"])?,
                tags: a["tags"]
                    .as_array()
                    .ok_or(Failure::Schema)?
                    .iter()
                    .map(|tag| string(tag).map(str::to_owned))
                    .collect::<Result<_, _>>()?,
            })
        })
        .collect()
}
pub(super) fn cursor(body: &Value) -> Result<Option<String>, Failure> {
    match &body["meta"]["page"]["after"] {
        Value::Null => Ok(None),
        Value::String(s) if !s.is_empty() => Ok(Some(s.clone())),
        _ => Err(Failure::Schema),
    }
}
impl Query {
    pub fn span_request(&self, cursor: Option<&str>) -> Value {
        let mut body = json!({"data":{"type":"spans","attributes":{
            "filter":{"from":self.window.start_rfc3339(),"to":self.window.end_rfc3339(),"tags":{"verify":self.marker}},
            "page":{"limit":100},"options":{"include_attachments":false}}}});
        if let Some(cursor) = cursor {
            body["data"]["attributes"]["page"]["cursor"] = json!(cursor);
        }
        body
    }
    pub fn metric_request(&self, metric: &str) -> [(String, String); 3] {
        [
            ("from".into(), self.window.from.to_string()),
            ("to".into(), self.window.to.to_string()),
            (
                "query".into(),
                format!(
                    "{}:{metric}{{verify:{}}}",
                    if metric == "crabber.run.count" {
                        "sum"
                    } else {
                        "avg"
                    },
                    self.marker
                ),
            ),
        ]
    }
    pub fn log_request(&self) -> Value {
        json!({"filter":{"query":format!("@verify_marker:{}",self.marker),
            "from":self.window.start_rfc3339(),"to":self.window.end_rfc3339()},"page":{"limit":100}})
    }
    pub fn graph(&self, spans: &[Span]) -> Option<Evidence> {
        let expected = [
            format!("verify:{}", self.marker),
            format!("service:{}", self.service),
            format!("ml_app:{}", self.ml_app),
        ];
        let spans: Vec<_> = spans
            .iter()
            .filter(|s| expected.iter().all(|tag| s.tags.contains(tag)))
            .collect();
        for agent in spans.iter().filter(|s| s.kind == "agent") {
            for workflow in spans.iter().filter(|s| {
                s.kind == "workflow"
                    && s.parent == agent.id
                    && s.trace == agent.trace
                    && s.id != agent.id
            }) {
                if let Some(llm) = spans.iter().find(|s| {
                    s.kind == "llm"
                        && s.parent == workflow.id
                        && s.trace == agent.trace
                        && s.id != workflow.id
                        && s.id != agent.id
                }) {
                    return Some(Evidence::Graph {
                        trace: agent.trace.clone(),
                        agent: agent.id.clone(),
                        workflow: workflow.id.clone(),
                        llm: llm.id.clone(),
                    });
                }
            }
        }
        None
    }
    pub fn metric(&self, body: &Value, metric: &'static str) -> Result<Option<Evidence>, Failure> {
        if body["status"] != "ok" {
            return Err(Failure::Schema);
        }
        let series = body["series"].as_array().ok_or(Failure::Schema)?;
        let mut points = 0;
        for item in series {
            if item["metric"] != metric {
                continue;
            }
            let interval = milliseconds(&item["interval"])?;
            if interval <= 0 {
                return Err(Failure::Schema);
            }
            // V1 response timestamps AND intervals are milliseconds; query bounds are seconds.
            for point in item["pointlist"].as_array().ok_or(Failure::Schema)? {
                let pair = point
                    .as_array()
                    .filter(|p| p.len() == 2)
                    .ok_or(Failure::Schema)?;
                let timestamp = milliseconds(&pair[0])?;
                if pair[1].as_f64().is_some_and(f64::is_finite)
                    && self.window.overlaps(timestamp, interval)
                {
                    points += 1;
                }
            }
        }
        Ok((points > 0).then_some(Evidence::Metric {
            name: metric,
            points,
        }))
    }
    pub fn logs(&self, body: &Value) -> Result<Option<Evidence>, Failure> {
        let data = body["data"].as_array().ok_or(Failure::Schema)?;
        let count = data
            .iter()
            .filter(|item| {
                let a = &item["attributes"];
                a["service"] == self.service
                    && a["attributes"]["verify_marker"] == self.marker
                    && matches!(
                        a["message"].as_str(),
                        Some(
                            "run settled" | "run admitted" | "tool settled" | "permission decided"
                        )
                    )
            })
            .count();
        Ok((count > 0).then_some(Evidence::Logs(count)))
    }
}
