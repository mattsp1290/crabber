//! Bounded web search through a trusted host callback.
//!
//! This native extension bundles no network provider or credentials. Hosts own
//! network access, credentials, rate limits, caching and freshness.
use crabber::JsonToolExecutor;
mod bounds;
mod input;

use crate::safe_future::SafeFuture;

use async_trait::async_trait;
use crabber::{
    core::{RunId, SessionId, ToolCallId, ToolInfo},
    extension::{
        Extension, ExtensionError, Registrar, ToolContext, ToolDefinition, WorkspaceContext,
    },
};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::Arc,
    time::Duration,
};
use tokio::sync::Semaphore;

/// Model-visible search tool name.
pub const TOOL_NAME: &str = "web_search";
/// Permission metadata; the host decides access.
pub const PERMISSION_SEARCH: &str = "network.web.search";
/// Smallest URL byte cap, sufficient for `http://a`.
pub const MIN_URL_BYTES: usize = 8;
const MAX_QUERY_BYTES: usize = 16 * 1024;
const MAX_RESULTS: usize = 12;
const MAX_TITLE_BYTES: usize = 1024;
const MAX_URL_BYTES: usize = 8 * 1024;
const MAX_SNIPPET_BYTES: usize = 16 * 1024;
const MAX_IN_FLIGHT: usize = 256;
const MAX_WAIT: Duration = Duration::from_secs(600);

/// Per-call persisted-result limits, not bounds on host allocations.
/// Parallel execution can accumulate `max_in_flight` such results in a turn.
#[derive(Clone, Serialize)]
pub struct Limits {
    /// Query bytes after trimming.
    pub max_query_bytes: usize,
    /// Maximum host records inspected, without refilling discarded records.
    pub max_results: usize,
    /// Maximum title bytes.
    pub max_title_bytes: usize,
    /// Maximum URL bytes.
    pub max_url_bytes: usize,
    /// Maximum snippet bytes.
    pub max_snippet_bytes: usize,
    /// Maximum concurrently awaited host futures.
    pub max_in_flight: usize,
    /// Maximum wait for a reply.
    pub max_wait: Duration,
}
impl Limits {
    /// Upper bound on serialized result bytes, including worst-case escaping.
    /// Call with limits accepted by [`WebSearch::new`].
    pub fn worst_case_result_bytes(&self) -> usize {
        let empty = serde_json::to_string(&Results { results: vec![] })
            .unwrap()
            .len();
        let record = serde_json::to_string(&Source {
            title: String::new(),
            url: String::new(),
            snippet: String::new(),
        })
        .unwrap()
        .len();
        empty
            + self.max_results
                * (record
                    + 6 * (self.max_title_bytes + self.max_url_bytes + self.max_snippet_bytes))
            + self.max_results.saturating_sub(1)
    }
}
/// Host-supplied source. Valid bounded fields become durable model-visible data.
#[derive(Clone, Debug, Serialize)]
pub struct Source {
    /// Source title, truncated at a UTF-8 boundary.
    pub title: String,
    /// Absolute HTTP(S) URL, validated and stored verbatim.
    pub url: String,
    /// Source snippet, truncated at a UTF-8 boundary.
    pub snippet: String,
}
#[derive(Serialize)]
struct Results {
    results: Vec<Source>,
}

/// Authoritative routing data and trimmed query; durable arguments stay raw.
#[derive(Clone)]
pub struct Request {
    session_id: SessionId,
    run_id: RunId,
    call_id: ToolCallId,
    workspace: WorkspaceContext,
    query: String,
    max_wait: Duration,
}
impl Request {
    /// Authoritative session identity.
    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }
    /// Authoritative run identity.
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
    /// Authoritative call identity.
    pub fn call_id(&self) -> &ToolCallId {
        &self.call_id
    }
    /// Host routing context, not authorization.
    pub fn workspace(&self) -> &WorkspaceContext {
        &self.workspace
    }
    /// Trimmed, bounded, NUL-free query.
    pub fn query(&self) -> &str {
        &self.query
    }
    /// Maximum wait for this call.
    pub fn max_wait(&self) -> Duration {
        self.max_wait
    }
}
/// Trusted backend callback, invoked concurrently up to `max_in_flight`.
///
/// The future is awaited inline and dropped after reply, interruption or the
/// deadline. Drop is its only cancellation signal; it must be drop-safe and
/// must not block while polled. Spawned work remains host-owned and unbounded
/// by this extension. Hosts own network access, credentials and search policy.
/// Error payloads are discarded. Returned strings become durable model-visible
/// content and must contain no secrets; ownership transfers to the extension.
pub type Searcher = Arc<
    dyn Fn(Request) -> Pin<Box<dyn Future<Output = Result<Vec<Source>, ExtensionError>> + Send>>
        + Send
        + Sync,
>;
/// Frozen construction options.
pub struct Options {
    /// Backend behavior identity. Change when routing or behavior changes:
    /// hashes cover this identity and limits, never the closure itself.
    pub searcher_identity: String,
    /// Trusted backend callback.
    pub searcher: Searcher,
    /// Required finite limits.
    pub limits: Limits,
}
/// Native search extension. Use one instance per tenant for isolated capacity.
/// Shared instances share capacity. Close adds no work beyond Crabber's drain.
pub struct WebSearch {
    policy: Arc<Policy>,
    hash: String,
}
struct Policy {
    searcher: Searcher,
    limits: Limits,
    capacity: Arc<Semaphore>,
}
impl WebSearch {
    /// Validate finite limits and freeze backend configuration.
    pub fn new(options: Options) -> Result<Self, ExtensionError> {
        let l = &options.limits;
        if !(1..=MAX_QUERY_BYTES).contains(&l.max_query_bytes)
            || !(1..=MAX_RESULTS).contains(&l.max_results)
            || !(1..=MAX_TITLE_BYTES).contains(&l.max_title_bytes)
            || !(MIN_URL_BYTES..=MAX_URL_BYTES).contains(&l.max_url_bytes)
            || !(1..=MAX_SNIPPET_BYTES).contains(&l.max_snippet_bytes)
            || !(1..=MAX_IN_FLIGHT).contains(&l.max_in_flight)
            || l.max_wait.is_zero()
            || l.max_wait > MAX_WAIT
        {
            return Err(crate::config_error("web-search-limits"));
        }
        if !crate::valid_identity(&options.searcher_identity) {
            return Err(crate::config_error("web-search-policy"));
        }
        let hash = crate::config_hash(&(
            "web-search-v2",
            TOOL_NAME,
            PERMISSION_SEARCH,
            &options.searcher_identity,
            l,
        ));
        let capacity = Arc::new(Semaphore::new(l.max_in_flight));
        Ok(Self {
            hash,
            policy: Arc::new(Policy {
                searcher: options.searcher,
                limits: options.limits,
                capacity,
            }),
        })
    }
}
fn failure(code: &'static str) -> ExtensionError {
    ExtensionError::Tool(format!("web search failed: {code}"))
}
fn invalid(code: &'static str) -> ExtensionError {
    ExtensionError::Tool(format!("web search input invalid: {code}"))
}
impl Policy {
    async fn execute(
        &self,
        context: ToolContext,
        arguments: Value,
    ) -> Result<Value, ExtensionError> {
        if context.cancel.is_cancelled() {
            return Err(failure("cancelled"));
        }
        let query = input::normalize(arguments, &self.limits)?;
        let permit = self
            .capacity
            .clone()
            .try_acquire_owned()
            .map_err(|_| failure("capacity"))?;
        let request = Request {
            session_id: context.session_id.clone(),
            run_id: context.run_id.clone(),
            call_id: context.call_id.clone(),
            workspace: context.workspace().clone(),
            query,
            max_wait: self.limits.max_wait,
        };
        let deadline = tokio::time::sleep(self.limits.max_wait);
        let expires_at = deadline.deadline();
        tokio::pin!(deadline);
        let future = catch_unwind(AssertUnwindSafe(|| (self.searcher)(request)))
            .map_err(|_| failure("searcher"))?;
        let mut future = SafeFuture::new(future, || failure("searcher"));
        let result = tokio::select! {
            biased;
            _ = context.cancel.cancelled() => Err(failure("cancelled")),
            _ = &mut deadline => Err(failure("timed_out")),
            reply = future.wait() => {
                if context.cancel.is_cancelled() {
                    Err(failure("cancelled"))
                } else if tokio::time::Instant::now() >= expires_at {
                    Err(failure("timed_out"))
                } else {
                    reply.map_err(|_| failure("searcher")).and_then(|records| {
                        serde_json::to_value(Results {
                            results: bounds::bound_sources(records, &self.limits),
                        }).map_err(|_| failure("searcher"))
                    })
                }
            },
        };
        drop(future);
        drop(permit);
        result
    }
}
#[async_trait]
impl JsonToolExecutor for Policy {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        Err(failure("context"))
    }
    async fn execute_with_context(
        &self,
        context: ToolContext,
        arguments: Value,
    ) -> Result<Value, ExtensionError> {
        self.execute(context, arguments).await
    }
}
#[async_trait]
impl Extension for WebSearch {
    fn id(&self) -> &str {
        "crabber-extensions/web-search"
    }
    fn version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }
    fn config_hash(&self) -> String {
        self.hash.clone()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        registrar.tool(Arc::new(ToolDefinition {
            info: ToolInfo { name: TOOL_NAME.into(), description: "Search one query and return bounded title, URL, and snippet source records.".into(), parameters: json!({"type":"object","additionalProperties":false,"required":["query"],"properties":{"query":{"type":"string"}}}), retry_safe: false, required_permissions: vec![PERMISSION_SEARCH.into()] }, executor: self.policy.clone()
        }));
        Ok(())
    }
}
