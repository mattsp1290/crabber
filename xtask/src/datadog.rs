//! Explicit live read gate; workspace checks exercise only offline fixtures.
mod contract;

use contract::{Evidence, Query, Window};
use reqwest::blocking::{Client, RequestBuilder};
use std::{
    env,
    io::Read,
    path::Path,
    process::Command,
    time::{Duration, Instant},
};
use time::OffsetDateTime;

const BUDGET: Duration = Duration::from_secs(120);
const MAX_BODY: u64 = 1_048_576;
const METRICS: [&str; 4] = [
    "crabber.run.count",
    "crabber.run.elapsed_ms",
    "crabber.model.elapsed_ms",
    "crabber.model.first_token_ms",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Failure {
    Permissions,
    Request,
    Transport,
    Schema,
    ResponseLimit,
    Deadline,
}
impl Failure {
    fn category(self) -> &'static str {
        match self {
            Self::Permissions => "permissions",
            Self::Request => "request",
            Self::Transport => "transport",
            Self::Schema => "schema",
            Self::ResponseLimit => "response_limit",
            Self::Deadline => "deadline",
        }
    }
    fn retryable(self) -> bool {
        matches!(self, Self::Transport)
    }
}

pub(super) fn verify() {
    if let Err(category) = live() {
        eprintln!("visibility=failed category={category}");
        std::process::exit(1);
    }
}

fn required(name: &str) -> Result<String, &'static str> {
    env::var(name)
        .ok()
        .filter(|v| !v.is_empty())
        .ok_or("missing_environment")
}

fn live() -> Result<(), &'static str> {
    let site = required("DD_SITE")?;
    // Never send credentials to an arbitrary URL assembled from operator input.
    if !matches!(
        site.as_str(),
        "datadoghq.com"
            | "us3.datadoghq.com"
            | "us5.datadoghq.com"
            | "datadoghq.eu"
            | "ap1.datadoghq.com"
            | "ap2.datadoghq.com"
            | "uk1.datadoghq.com"
            | "ddog-gov.com"
            | "us2.ddog-gov.com"
    ) {
        return Err("invalid_site");
    }
    let api_key = required("DD_API_KEY")?;
    let application_key = required("DD_APP_KEY")?;
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let sha = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(workspace)
        .output()
        .map_err(|_| "source_identity")?;
    let sha = String::from_utf8(sha.stdout).map_err(|_| "source_identity")?;
    if sha.trim().len() != 40 || !sha.trim().bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("source_identity");
    }
    let state = Command::new("git")
        .args(["status", "--porcelain"])
        .current_dir(workspace)
        .output()
        .map_err(|_| "source_identity")?;
    if !state.status.success() || !state.stdout.is_empty() {
        return Err("dirty_source");
    }
    let start = OffsetDateTime::now_utc();
    let marker = format!(
        "crabber-{}-{}",
        start.unix_timestamp_nanos(),
        std::process::id()
    );
    let status = Command::new("cargo")
        .args(["run", "--quiet", "-p", "datadog-export"])
        .env("CRABBER_OBS_VERIFY_MARKER", &marker)
        .current_dir(workspace)
        .status()
        .map_err(|_| "fixture_launch")?;
    let submitted = Instant::now();
    let end = OffsetDateTime::now_utc();
    if !status.success() {
        return Err("fixture_export");
    }
    let query = Query {
        marker,
        service: env::var("DD_SERVICE").unwrap_or_else(|_| "crabber".into()),
        ml_app: env::var("DD_LLMOBS_ML_APP")
            .unwrap_or_else(|_| env::var("DD_SERVICE").unwrap_or_else(|_| "crabber".into())),
        window: Window::new(start.unix_timestamp() - 600, end.unix_timestamp() + 60),
    };
    println!(
        "fixture_flush_shutdown=accepted site={site} marker={} source_sha={} from={} to={}",
        query.marker,
        sha.trim(),
        query.window.start_rfc3339(),
        query.window.end_rfc3339()
    );
    let reader = Reader {
        client: Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| "client")?,
        origin: format!("https://api.{site}"),
        api_key,
        application_key,
        query,
    };
    let mut clock = WallClock(submitted);
    poll(&mut clock, |signal, remaining| {
        reader.probe(signal, remaining)
    })
    .map_err(Failure::category)?;
    println!("visibility=passed");
    Ok(())
}

#[derive(Clone, Copy, Debug)]
enum Signal {
    Spans,
    Metric(&'static str),
    Logs,
}
impl Signal {
    fn name(self) -> &'static str {
        match self {
            Self::Spans => "spans",
            Self::Metric(name) => name,
            Self::Logs => "logs",
        }
    }
}
trait Clock {
    fn elapsed(&self) -> Duration;
    fn sleep(&mut self, duration: Duration);
}
struct WallClock(Instant);
impl Clock for WallClock {
    fn elapsed(&self) -> Duration {
        self.0.elapsed()
    }
    fn sleep(&mut self, duration: Duration) {
        std::thread::sleep(duration);
    }
}

fn poll(
    clock: &mut impl Clock,
    mut probe: impl FnMut(Signal, Duration) -> Result<Option<Evidence>, Failure>,
) -> Result<Vec<Evidence>, Failure> {
    let signals = [
        Signal::Spans,
        Signal::Metric(METRICS[0]),
        Signal::Metric(METRICS[1]),
        Signal::Metric(METRICS[2]),
        Signal::Metric(METRICS[3]),
        Signal::Logs,
    ];
    let mut found = vec![None; signals.len()];
    loop {
        for (signal, result) in signals.iter().zip(&mut found) {
            if result.is_some() {
                continue;
            }
            let remaining = BUDGET
                .checked_sub(clock.elapsed())
                .filter(|v| !v.is_zero())
                .ok_or(Failure::Deadline)?;
            match probe(*signal, remaining) {
                Ok(evidence) => {
                    if let Some(item) = &evidence {
                        item.print();
                    }
                    *result = evidence;
                    println!(
                        "signal={} status={}",
                        signal.name(),
                        if result.is_some() {
                            "visible"
                        } else {
                            "pending"
                        }
                    );
                }
                Err(error) => {
                    println!("signal={} category={}", signal.name(), error.category());
                    if !error.retryable() {
                        return Err(error);
                    }
                }
            }
            if clock.elapsed() >= BUDGET {
                return Err(Failure::Deadline);
            }
        }
        if found.iter().all(Option::is_some) {
            return Ok(found.into_iter().flatten().collect());
        }
        let remaining = BUDGET.saturating_sub(clock.elapsed());
        if remaining.is_zero() {
            return Err(Failure::Deadline);
        }
        clock.sleep(remaining.min(Duration::from_secs(10)));
    }
}

struct Reader {
    client: Client,
    origin: String,
    api_key: String,
    application_key: String,
    query: Query,
}
impl Reader {
    fn read(
        &self,
        request: RequestBuilder,
        remaining: Duration,
    ) -> Result<serde_json::Value, Failure> {
        let response = request
            .header("DD-API-KEY", &self.api_key)
            .header("DD-APPLICATION-KEY", &self.application_key)
            .timeout(remaining.min(Duration::from_secs(10)))
            .send()
            .map_err(|_| Failure::Transport)?;
        let status = response.status().as_u16();
        println!("read_http_status={status}");
        match status {
            200 => (),
            401 | 403 => return Err(Failure::Permissions),
            429 | 500..=599 => return Err(Failure::Transport),
            _ => return Err(Failure::Request),
        }
        let mut bytes = Vec::new();
        response
            .take(MAX_BODY + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| Failure::Transport)?;
        if bytes.len() as u64 > MAX_BODY {
            return Err(Failure::ResponseLimit);
        }
        serde_json::from_slice(&bytes).map_err(|_| Failure::Schema)
    }
    fn probe(&self, signal: Signal, remaining: Duration) -> Result<Option<Evidence>, Failure> {
        match signal {
            Signal::Spans => self.spans(remaining),
            Signal::Metric(metric) => {
                let body = self.read(
                    self.client
                        .get(format!("{}/api/v1/query", self.origin))
                        .query(&self.query.metric_request(metric)),
                    remaining,
                )?;
                self.query.metric(&body, metric)
            }
            Signal::Logs => {
                let body = self.read(
                    self.client
                        .post(format!("{}/api/v2/logs/events/search", self.origin))
                        .json(&self.query.log_request()),
                    remaining,
                )?;
                self.query.logs(&body)
            }
        }
    }
    fn spans(&self, remaining: Duration) -> Result<Option<Evidence>, Failure> {
        let start = Instant::now();
        let mut cursor = None;
        let mut seen = std::collections::HashSet::new();
        let mut spans = Vec::new();
        loop {
            let remaining = remaining
                .checked_sub(start.elapsed())
                .filter(|v| !v.is_zero())
                .ok_or(Failure::Deadline)?;
            let body = self.read(
                self.client
                    .post(format!(
                        "{}/api/v2/llm-obs/v1/spans/events/search",
                        self.origin
                    ))
                    .header("Content-Type", "application/vnd.api+json")
                    .json(&self.query.span_request(cursor.as_deref())),
                remaining,
            )?;
            spans.extend(contract::span_page(&body)?);
            if spans.len() > 1000 {
                return Err(Failure::ResponseLimit);
            }
            if let Some(graph) = self.query.graph(&spans) {
                return Ok(Some(graph));
            }
            match contract::cursor(&body)? {
                Some(next) if seen.insert(next.clone()) => cursor = Some(next),
                Some(_) => return Err(Failure::Schema),
                None => return Ok(None),
            }
        }
    }
}

#[cfg(test)]
mod tests;
