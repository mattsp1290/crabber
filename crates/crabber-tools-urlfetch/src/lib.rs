//! Policy-constrained HTTPS and workspace-confined file text fetching.
#![warn(missing_docs)]
use async_trait::async_trait;
use crabber::{ExtensionError, ToolDefinition, ToolExecutor, extension::ToolContext};
use crabber_tools_core::{
    AcquireError, Capacity, Limits, ToolError, WorkspaceRoot, acquire_read, check_cancelled,
    failed, run_blocking_owned,
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use url::Url;
mod fetch;
mod file;
mod info;
mod policy;
mod transport;
pub use info::info;
pub use policy::{HostPattern, UrlFetchPolicy};
/// Total invocation deadline, including redirects and body reads.
pub const TIMEOUT: Duration = Duration::from_secs(30);
/// Maximum UTF-8 resource body in bytes.
pub const MAX_BODY_BYTES: usize = 1024 * 1024;
/// Maximum input or redirect URL in bytes.
pub const MAX_URL_BYTES: usize = 8192;
/// Maximum manually validated redirect hops.
pub const MAX_REDIRECTS: usize = 5;
/// Host-owned workspace, network policy and shared capacity.
pub struct Options {
    /// Host path exclusions, shared across read and write tools.
    pub deny: crabber_tools_core::PathDenyPolicy,
    /// Admitted workspace for file URLs and session routing.
    pub root: Arc<WorkspaceRoot>,
    /// Explicit network policy.
    pub policy: UrlFetchPolicy,
    /// Shared acquisition bounds.
    pub limits: Limits,
    /// Mount-wide concurrent work capacity.
    pub capacity: Capacity,
}
/// Validate policy and build a context-required executor.
///
/// # Errors
/// Returns a plan error if URL policy or shared execution limits are invalid.
pub fn definition(options: Arc<Options>) -> Result<Arc<ToolDefinition>, ExtensionError> {
    options.policy.validate()?;
    options.limits.validate()?;
    Ok(Arc::new(ToolDefinition {
        info: info(),
        executor: Arc::new(FetchTool {
            options,
            transport: Arc::new(transport::Https),
        }),
    }))
}
struct FetchTool {
    options: Arc<Options>,
    transport: Arc<dyn transport::Transport>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Args {
    url: String,
}
fn envelope(result: Result<String, ToolError>) -> Value {
    match result {
        Ok(content) => json!({"outcome":"succeeded","content":content}),
        Err(error) => error.value(),
    }
}
#[async_trait]
impl ToolExecutor for FetchTool {
    async fn execute(&self, _: Value) -> Result<crabber::core::ToolOutput, ExtensionError> {
        crabber_tools_core::context_required()
    }
    async fn execute_with_context(
        &self,
        ctx: ToolContext,
        value: Value,
    ) -> Result<crabber::core::ToolOutput, ExtensionError> {
        self.invoke(ctx, value)
            .await
            .map(|value| crabber_tools_core::to_output(value, &["content"]))
    }
}
impl FetchTool {
    async fn invoke(&self, ctx: ToolContext, value: Value) -> Result<Value, ExtensionError> {
        check_cancelled(&ctx.cancel)?;
        let args = match serde_json::from_value::<Args>(value) {
            Ok(args)
                if !args.url.trim().is_empty()
                    && args.url.len() <= MAX_URL_BYTES
                    && !args.url.contains('\0') =>
            {
                args
            }
            _ => return Ok(failed("validation", "invalid URL arguments")),
        };
        let url = match Url::parse(args.url.trim()) {
            Ok(url) if matches!(url.scheme(), "https" | "file") => url,
            _ => return Ok(failed("validation", "use https or a workspace file URL")),
        };
        if let Err(e) = self
            .options
            .root
            .matches_session_directory(ctx.workspace().directory())
        {
            return Ok(e.value());
        }
        let start = std::time::Instant::now();
        let work = async {
            let permit =
                match acquire_read(&self.options.capacity, &self.options.limits, &ctx.cancel).await
                {
                    Ok(p) => p,
                    Err(AcquireError::Cancelled) => {
                        return Err(ExtensionError::Tool("cancelled".into()));
                    }
                    Err(AcquireError::Unavailable) => {
                        return Ok(failed("unavailable", "workspace busy"));
                    }
                };
            if url.scheme() == "file" {
                let options = self.options.clone();
                run_blocking_owned(&ctx, move |cancel| {
                    let _permit = permit;
                    envelope(file::read(
                        &options.root,
                        &options.deny,
                        &url,
                        cancel,
                        start + TIMEOUT,
                    ))
                })
                .await
            } else {
                let _permit = permit;
                Ok(envelope(
                    fetch::https(self.transport.as_ref(), &self.options.policy, url).await,
                ))
            }
        };
        tokio::select! { biased;
            () = ctx.cancel.cancelled() => Err(ExtensionError::Tool("cancelled".into())),
            result = tokio::time::timeout(TIMEOUT, work) => result.unwrap_or_else(|_| Ok(failed("timeout", "URL fetch timed out"))),
        }
    }
}

#[cfg(test)]
mod tests;
