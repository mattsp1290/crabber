//! APM search contract: <https://docs.datadoghq.com/api/latest/spans/search-spans/>
//! POST /api/v2/spans/events/search; application key needs `apm_read`.
use super::{
    Failure,
    contract::{Evidence, Query},
};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    fmt::Write as _,
};

const REQUIRED: [&str; 4] = [
    "crabber.run",
    "crabber.model_call",
    "crabber.tool_call",
    "crabber.store.transaction",
];

pub(super) fn request(query: &Query, cursor: Option<&str>) -> Value {
    let mut filter = format!("service:{} verify:{}", query.service, query.marker);
    if let Some(env) = &query.env {
        write!(&mut filter, " env:{env}").unwrap();
    }
    let mut request = json!({"data":{"type":"search_request","attributes":{
        "filter":{"from":query.window.start_rfc3339(),"to":query.window.end_rfc3339(),"query":filter},
        "page":{"limit":100},"sort":"timestamp"}}});
    if let Some(cursor) = cursor {
        request["data"]["attributes"]["page"]["cursor"] = json!(cursor);
    }
    request
}

pub(super) struct Span {
    name: String,
    trace: String,
    id: String,
    parent: String,
}
pub(super) fn page(body: &Value, query: &Query) -> Result<Vec<Span>, Failure> {
    body["data"]
        .as_array()
        .ok_or(Failure::Schema)?
        .iter()
        .filter_map(|item| {
            let attributes = &item["attributes"];
            if attributes["service"] != query.service
                || query
                    .env
                    .as_ref()
                    .is_some_and(|env| attributes["env"] != *env)
            {
                return None;
            }
            let marker = format!("verify:{}", query.marker);
            let tagged = attributes["tags"]
                .as_array()
                .is_some_and(|tags| tags.iter().any(|tag| tag == &marker));
            if !tagged && attributes["attributes"]["verify"] != query.marker {
                return None;
            }
            Some((|| {
                if item["type"] != "spans" {
                    return Err(Failure::Schema);
                }
                Ok(Span {
                    name: attributes["resource_name"]
                        .as_str()
                        .ok_or(Failure::Schema)?
                        .to_owned(),
                    trace: trace_hex(attributes["trace_id"].as_str().ok_or(Failure::Schema)?)?,
                    id: span_hex(attributes["span_id"].as_str().ok_or(Failure::Schema)?)?,
                    parent: span_hex(attributes["parent_id"].as_str().ok_or(Failure::Schema)?)?,
                })
            })())
        })
        .collect()
}

pub(super) fn graph(spans: &[Span]) -> Option<Evidence> {
    for run in spans.iter().filter(|span| span.name == REQUIRED[0]) {
        let by_id: HashMap<_, _> = spans
            .iter()
            .filter(|span| span.trace == run.trace)
            .map(|span| (span.id.as_str(), span))
            .collect();
        let has_descendant = |name: &str| {
            spans
                .iter()
                .filter(|span| span.name == name && span.trace == run.trace)
                .any(|span| {
                    let mut parent = span.parent.as_str();
                    let mut seen = HashSet::new();
                    while seen.insert(parent) {
                        if parent == run.id {
                            return true;
                        }
                        let Some(next) = by_id.get(parent) else {
                            return false;
                        };
                        parent = &next.parent;
                    }
                    false
                })
        };
        if REQUIRED[1..].iter().all(|name| has_descendant(name)) {
            return Some(Evidence::Apm {
                trace: run.trace.clone(),
                run: run.id.clone(),
            });
        }
    }
    None
}

/// API decimal IDs and full 128-bit hexadecimal IDs normalize to `OTel` hex.
fn trace_hex(value: &str) -> Result<String, Failure> {
    let parsed = if value.len() == 32 && value.bytes().all(|b| b.is_ascii_hexdigit()) {
        u128::from_str_radix(value, 16).ok()
    } else {
        value.parse::<u128>().ok()
    }
    .filter(|value| *value != 0)
    .ok_or(Failure::Schema)?;
    Ok(format!("{parsed:032x}"))
}
fn span_hex(value: &str) -> Result<String, Failure> {
    // Datadog search returns unsigned decimal span IDs.
    value
        .parse::<u64>()
        .map(|value| format!("{value:016x}"))
        .map_err(|_| Failure::Schema)
}

pub(super) fn layer_request(query: &Query) -> Value {
    json!({"filter":{"query":format!("service:{} verify:{} @crabber.source:layer",query.service,query.marker),
        "from":query.window.start_rfc3339(),"to":query.window.end_rfc3339()},"page":{"limit":100}})
}
pub(super) fn layer_log(body: &Value, query: &Query) -> Result<Option<Evidence>, Failure> {
    for item in body["data"].as_array().ok_or(Failure::Schema)? {
        let attributes = &item["attributes"];
        let fields = &attributes["attributes"];
        if attributes["service"] != query.service
            || fields["crabber.source"] != "layer"
            || !fields["error.kind"].is_string()
        {
            continue;
        }
        let marker = format!("verify:{}", query.marker);
        if fields["verify_marker"] != query.marker
            && !attributes["tags"]
                .as_array()
                .is_some_and(|tags| tags.iter().any(|tag| tag == &marker))
        {
            continue;
        }
        let trace = fields["trace_id"]
            .as_str()
            .filter(|id| id.len() == 32 && id.bytes().all(|b| b.is_ascii_hexdigit()))
            .map(str::to_lowercase);
        let dd_trace = fields["dd.trace_id"]
            .as_str()
            .and_then(|id| trace_hex(id).ok());
        if trace.is_some() || dd_trace.is_some() {
            return Ok(Some(Evidence::LayerLog { trace, dd_trace }));
        }
    }
    Ok(None)
}
pub(super) fn linkage(evidence: &[Evidence]) -> Result<&'static str, Failure> {
    let trace = evidence
        .iter()
        .find_map(|item| {
            if let Evidence::Apm { trace, .. } = item {
                Some(trace)
            } else {
                None
            }
        })
        .ok_or(Failure::Schema)?;
    for item in evidence {
        if let Evidence::LayerLog {
            trace: otel,
            dd_trace,
        } = item
        {
            if otel.as_ref() == Some(trace) {
                return Ok("otel_hex");
            }
            if dd_trace.as_ref() == Some(trace) {
                return Ok("datadog");
            }
        }
    }
    Err(Failure::Schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn query() -> Query {
        Query {
            marker: "fresh".into(),
            service: "test".into(),
            env: Some("test".into()),
            ml_app: "test".into(),
            window: super::super::contract::Window::new(1, 2),
        }
    }
    fn span(name: &str, id: &str, parent: &str, trace: &str) -> Value {
        json!({"type":"spans","attributes":{"service":"test","env":"test","tags":["verify:fresh"],"resource_name":name,"span_id":id,"parent_id":parent,"trace_id":trace}})
    }
    #[test]
    fn apm_requires_marker_scoped_descendants_on_one_trace() {
        let query = query();
        assert_eq!(request(&query, None)["data"]["type"], "search_request");
        let mut body = json!({"data":[span(REQUIRED[0],"1","0","9"),span(REQUIRED[1],"2","1","9"),span(REQUIRED[2],"3","1","9"),span(REQUIRED[3],"4","1","9")]});
        assert!(graph(&page(&body, &query).unwrap()).is_some());
        body["data"][3]["attributes"]["trace_id"] = json!("8");
        assert!(graph(&page(&body, &query).unwrap()).is_none());
        body["data"][3]["attributes"]["trace_id"] = json!("9");
        body["data"][3]["attributes"]["tags"] = json!(["verify:old"]);
        assert!(graph(&page(&body, &query).unwrap()).is_none());
    }
    #[test]
    fn linkage_requires_matching_valid_trace_ids() {
        let trace = "00000000000000000000000000000009".to_owned();
        let apm = Evidence::Apm {
            trace: trace.clone(),
            run: "1".into(),
        };
        assert_eq!(
            linkage(&[
                apm.clone(),
                Evidence::LayerLog {
                    trace: Some(trace),
                    dd_trace: None
                }
            ])
            .unwrap(),
            "otel_hex"
        );
        assert!(
            linkage(&[
                apm,
                Evidence::LayerLog {
                    trace: Some("wrong".into()),
                    dd_trace: None
                }
            ])
            .is_err()
        );
        assert!(trace_hex("secret\n").is_err());
        assert_eq!(trace_hex("9").unwrap(), "00000000000000000000000000000009");
    }
}

#[cfg(test)]
mod log_tests {
    use super::*;
    #[test]
    fn layer_log_requires_marker_source_and_finite_identity() {
        let query = Query {
            marker: "fresh".into(),
            service: "test".into(),
            env: None,
            ml_app: "test".into(),
            window: super::super::contract::Window::new(1, 2),
        };
        let mut body = json!({"data":[{"attributes":{"service":"test","tags":["verify:fresh"],"attributes":{"crabber.source":"layer","error.kind":"provider_server","trace_id":"00000000000000000000000000000009","dd.trace_id":"9"}}}]});
        assert!(layer_log(&body, &query).unwrap().is_some());
        for (key, value) in [
            ("crabber.source", json!("observer")),
            ("error.kind", Value::Null),
            ("trace_id", json!("unsafe\n")),
        ] {
            let mut bad = body.clone();
            bad["data"][0]["attributes"]["attributes"][key] = value;
            if key == "trace_id" {
                bad["data"][0]["attributes"]["attributes"]["dd.trace_id"] = Value::Null;
            }
            assert!(layer_log(&bad, &query).unwrap().is_none(), "{key}");
        }
        body["data"][0]["attributes"]["tags"] = json!(["verify:stale"]);
        assert!(layer_log(&body, &query).unwrap().is_none());
    }
}
