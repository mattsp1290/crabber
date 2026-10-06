use super::*;
use serde_json::{Value, json};
use std::cell::Cell;

fn query() -> Query {
    Query {
        marker: "fresh".into(),
        service: "crabber".into(),
        ml_app: "demo".into(),
        window: Window::new(1_760_000_000, 1_760_000_660),
    }
}
fn span(kind: &str, id: &str, parent: &str) -> Value {
    json!({"type":"span","id":"resource-id-not-span-id","attributes":{
        "span_kind":kind,"span_id":id,"parent_id":parent,"trace_id":"12345",
        "tags":["verify:fresh","service:crabber","ml_app:demo"]}})
}
fn graph() -> Value {
    json!({"data":[span("agent","1","undefined"),span("workflow","2","1"),span("llm","3","2")],
        "meta":{"status":"done","page":null}})
}
#[test]
fn requests_use_absolute_bounds_structured_tags_and_cursor() {
    let q = query();
    let body = q.span_request(Some("next-page"));
    let attrs = &body["data"]["attributes"];
    assert_eq!(body["data"]["type"], "spans");
    assert_eq!(
        attrs["filter"],
        json!({"from":"2025-10-09T08:53:20Z","to":"2025-10-09T09:04:20Z","tags":{"verify":"fresh"}})
    );
    assert_eq!(attrs["page"], json!({"limit":100,"cursor":"next-page"}));
    assert_eq!(
        q.metric_request(METRICS[1])[2].1,
        "avg:crabber.run.elapsed_ms{verify:fresh}"
    );
    assert_eq!(q.metric_request(METRICS[0])[0].1, "1760000000");
    assert_eq!(q.log_request()["filter"]["from"], attrs["filter"]["from"]);
}
#[test]
fn graph_requires_matching_tags_kinds_trace_and_actual_parent_ids() {
    let q = query();
    let body = graph();
    let spans = contract::span_page(&body).unwrap();
    assert!(
        matches!(q.graph(&spans), Some(Evidence::Graph { agent, workflow, llm, .. }) if agent == "1" && workflow == "2" && llm == "3")
    );
    for (field, value) in [
        ("span_kind", "tool"),
        ("trace_id", "other"),
        ("parent_id", "resource-id-not-span-id"),
        ("span_id", "2"),
    ] {
        let mut bad = body.clone();
        bad["data"][2]["attributes"][field] = json!(value);
        assert!(
            q.graph(&contract::span_page(&bad).unwrap()).is_none(),
            "{field}"
        );
    }
    for tags in [
        json!(["service:crabber", "ml_app:demo"]),
        json!(["verify:fresh", "service:wrong", "ml_app:demo"]),
        json!(["verify:fresh", "service:crabber"]),
        json!(["verify:fresh", "service:crabber", "ml_app:wrong"]),
    ] {
        let mut bad = body.clone();
        bad["data"][2]["attributes"]["tags"] = tags;
        bad["data"][2]["attributes"]["ml_app"] = json!("demo");
        assert!(q.graph(&contract::span_page(&bad).unwrap()).is_none());
    }
    let mut additional = body;
    additional["data"]
        .as_array_mut()
        .unwrap()
        .push(span("tool", "4", "3"));
    additional["data"].as_array_mut().unwrap().push(json!({"type":"span","attributes":{"span_kind":"llm","span_id":"5","parent_id":"none","trace_id":"other","tags":["verify:old"]}}));
    assert!(
        q.graph(&contract::span_page(&additional).unwrap())
            .is_some()
    );
}
#[test]
fn malformed_and_empty_span_results_cannot_pass() {
    let q = query();
    for body in [
        json!({}),
        json!({"data":{}}),
        json!({"data":[{"type":"span","attributes":{}}]}),
    ] {
        assert!(matches!(contract::span_page(&body), Err(Failure::Schema)));
    }
    assert!(
        q.graph(&contract::span_page(&json!({"data":[]})).unwrap())
            .is_none()
    );
    let mut body = graph();
    body["data"].as_array_mut().unwrap().pop();
    assert!(q.graph(&contract::span_page(&body).unwrap()).is_none());
    body["data"][0]["attributes"]["span_id"] = json!("unsafe\noutput");
    assert!(contract::span_page(&body).is_err());
}
fn metric(timestamp: i64, interval: i64, value: &Value) -> Value {
    json!({"status":"ok","series":[{"metric":METRICS[1],"interval":interval,"pointlist":[[timestamp,value]]}]})
}
#[test]
fn metric_gate_checks_finite_values_names_and_overlapping_millisecond_buckets() {
    let q = query();
    // A 20-second bucket begins before the padded window and overlaps it.
    let body = metric(1_759_999_990_000, 20_000, &json!(4.5));
    assert!(q.metric(&body, METRICS[1]).unwrap().is_some());
    let mut floating = body.clone();
    floating["series"][0]["pointlist"][0][0] = json!(1_759_999_990_000.0);
    floating["series"][0]["interval"] = json!(20_000.0);
    assert!(q.metric(&floating, METRICS[1]).unwrap().is_some());
    assert!(q.metric(&body, METRICS[0]).unwrap().is_none());
    for body in [
        metric(1_759_999_970_000, 20_000, &json!(1)),
        metric(1_760_000_661_000, 20_000, &json!(1)),
        metric(1_760_000_000, 20_000, &json!(1)),
        metric(1_759_999_990_000, 20, &json!(1)),
        metric(1_760_000_010_000, 20_000, &Value::Null),
        json!({"status":"ok","series":[]}),
    ] {
        assert!(q.metric(&body, METRICS[1]).unwrap().is_none());
    }
    for body in [
        json!({"status":"error"}),
        json!({"status":"ok"}),
        metric(1_760_000_000_000, 0, &json!(1)),
    ] {
        assert!(q.metric(&body, METRICS[1]).is_err());
    }
    let mut count = body;
    count["series"][0]["metric"] = json!(METRICS[0]);
    assert!(q.metric(&count, METRICS[0]).unwrap().is_some());
    count["series"][0]["pointlist"][0][0] = json!(1_760_000_000);
    assert!(q.metric(&count, METRICS[0]).unwrap().is_none());
}
#[test]
fn logs_require_marker_service_and_safe_runtime_message() {
    let q = query();
    let mut body = json!({"data":[{"attributes":{"service":"crabber","message":"run settled","attributes":{"verify_marker":"fresh"}}}]});
    assert!(q.logs(&body).unwrap().is_some());
    body["data"][0]["attributes"]["attributes"]["verify_marker"] = json!("old");
    assert!(q.logs(&body).unwrap().is_none());
    assert!(q.logs(&json!({"data":[]})).unwrap().is_none());
    assert!(q.logs(&json!({})).is_err());
}
struct FakeClock(std::rc::Rc<Cell<Duration>>);
impl Clock for FakeClock {
    fn elapsed(&self) -> Duration {
        self.0.get()
    }
    fn sleep(&mut self, duration: Duration) {
        self.0.set(self.0.get() + duration);
    }
}
#[test]
fn poll_retains_success_and_fails_if_one_signal_never_arrives() {
    let mut clock = FakeClock(std::rc::Rc::new(Cell::new(Duration::ZERO)));
    let calls = Cell::new(0);
    let evidence = poll(&mut clock, |signal, _| {
        calls.set(calls.get() + 1);
        if matches!(signal, Signal::Logs) && calls.get() == 6 {
            Ok(None)
        } else {
            Ok(Some(Evidence::Logs(1)))
        }
    })
    .unwrap();
    assert_eq!(evidence.len(), 6);
    assert_eq!(calls.get(), 7);
    assert_eq!(clock.elapsed(), Duration::from_secs(10));
    clock.0.set(Duration::ZERO);
    assert!(matches!(
        poll(&mut clock, |signal, _| {
            if matches!(signal, Signal::Spans) {
                Ok(None)
            } else {
                Ok(Some(Evidence::Logs(1)))
            }
        }),
        Err(Failure::Deadline)
    ));
    assert_eq!(clock.elapsed(), BUDGET);
}
#[test]
fn poll_counts_request_time_and_rejects_permissions_immediately() {
    let elapsed = std::rc::Rc::new(Cell::new(Duration::ZERO));
    let mut clock = FakeClock(elapsed.clone());
    assert!(matches!(
        poll(&mut clock, |_, _| Err(Failure::Permissions)),
        Err(Failure::Permissions)
    ));
    assert_eq!(clock.elapsed(), Duration::ZERO);
    assert!(matches!(
        poll(&mut clock, |_, remaining| {
            assert_eq!(remaining, BUDGET);
            elapsed.set(BUDGET);
            Ok(Some(Evidence::Logs(1)))
        }),
        Err(Failure::Deadline)
    ));
}
fn serve(responses: Vec<(u16, String)>) -> (String, std::thread::JoinHandle<Vec<Value>>) {
    use std::{
        io::{Read, Write},
        net::TcpListener,
    };
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let handle = std::thread::spawn(move || {
        let mut requests = Vec::new();
        for (status, body) in responses {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut bytes = Vec::new();
            let mut buf = [0; 4096];
            loop {
                let n = stream.read(&mut buf).unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buf[..n]);
                if let Some(boundary) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&bytes[..boundary]).to_lowercase();
                    let length = headers
                        .lines()
                        .find_map(|l| l.strip_prefix("content-length: "))
                        .map_or(0, |l| l.parse::<usize>().unwrap());
                    if bytes.len() >= boundary + 4 + length {
                        assert!(headers.contains("dd-api-key: offline"));
                        requests.push(if length == 0 {
                            Value::Null
                        } else {
                            serde_json::from_slice(&bytes[boundary + 4..boundary + 4 + length])
                                .unwrap()
                        });
                        break;
                    }
                }
            }
            write!(
                stream,
                "HTTP/1.1 {status} Result\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
        }
        requests
    });
    (origin, handle)
}
fn reader(origin: String) -> Reader {
    Reader {
        client: Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap(),
        origin,
        api_key: "offline".into(),
        application_key: "offline".into(),
        query: query(),
    }
}
#[test]
fn paginated_http_search_uses_cursor_and_validates_combined_graph() {
    let mut first = graph();
    let llm = first["data"].as_array_mut().unwrap().pop().unwrap();
    first["meta"]["page"] = json!({"after":"cursor-2"});
    let second = json!({"data":[llm],"meta":{"page":null,"status":"done"}});
    let (origin, server) = serve(vec![(200, first.to_string()), (200, second.to_string())]);
    assert!(
        reader(origin)
            .probe(Signal::Spans, Duration::from_secs(2))
            .unwrap()
            .is_some()
    );
    let requests = server.join().unwrap();
    assert_eq!(
        requests[1]["data"]["attributes"]["page"]["cursor"],
        "cursor-2"
    );
    assert_eq!(
        requests[0]["data"]["attributes"]["filter"],
        requests[1]["data"]["attributes"]["filter"]
    );
}
#[test]
fn http_failures_malformed_json_and_oversized_responses_fail_safely() {
    for (status, body, expected) in [
        (
            403,
            "credential content never printed".into(),
            Failure::Permissions,
        ),
        (400, "bad request".into(), Failure::Request),
        (200, "not json".into(), Failure::Schema),
        (200, "x".repeat(1_048_577), Failure::ResponseLimit),
        (503, "unavailable".into(), Failure::Transport),
    ] {
        let (origin, server) = serve(vec![(status, body)]);
        assert!(
            matches!(reader(origin).probe(Signal::Logs,Duration::from_secs(2)),Err(e) if e == expected)
        );
        server.join().unwrap();
    }
}
