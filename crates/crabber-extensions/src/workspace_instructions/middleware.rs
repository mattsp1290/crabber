use super::{
    Policy, ReaderRoot, chain, failure,
    files::{Entry, Frame},
};
use crabber::extension::{
    ModelAttemptContext, SystemPromptMiddleware, WorkspaceReadErrorKind as Kind,
};
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
pub(super) struct Middleware(pub Arc<Policy>);
#[async_trait::async_trait]
impl SystemPromptMiddleware for Middleware {
    async fn contribute(&self, context: ModelAttemptContext) -> Result<Option<String>, String> {
        if context.cancellation().is_cancelled() {
            return Err(failure("cancelled"));
        }
        let directory = match (
            context.workspace().workspace_id(),
            context.workspace().directory(),
        ) {
            (None, None) => return Ok(None),
            (Some(_), Some(directory)) => directory,
            _ => return Err(failure("workspace")),
        };
        let chain = match self.0.reader_root {
            ReaderRoot::Workspace => vec![String::new()],
            ReaderRoot::Boundary => {
                match chain::derive(&self.0.boundaries, directory, self.0.limits.max_chain_depth)? {
                    Some(chain) => chain,
                    None => return Ok(None),
                }
            }
        };
        let permit = self
            .0
            .capacity
            .clone()
            .try_acquire_owned()
            .map_err(|_| failure("capacity"))?;
        let cancelled = context.cancellation().child_token();
        let worker_cancel = cancelled.clone();
        let (send, recv) = oneshot::channel();
        let policy = self.0.clone();
        let tracker = context.cleanup().clone();
        // The tracked worker keeps its permit and reader future across a caller
        // deadline. Dropping a blocking host read cannot prove it has stopped.
        tracker.spawn(async move {
            let _permit = permit;
            let result = render(policy, context, chain, worker_cancel).await;
            let _ = send.send(result);
        });
        let result = tokio::select! {
            biased;
            _ = cancelled.cancelled() => Err(failure("cancelled")),
            reply = tokio::time::timeout(self.0.limits.max_wait, recv) => match reply {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => Err(failure("worker")),
                Err(_) => Err(failure("deadline")),
            },
        };
        cancelled.cancel();
        result
    }
}
async fn render(
    policy: Arc<Policy>,
    context: ModelAttemptContext,
    chain: Vec<String>,
    cancelled: CancellationToken,
) -> Result<Option<String>, String> {
    let resolver = context
        .workspace_reader_resolver()
        .ok_or_else(|| failure("resolver"))?;
    let reader = match resolver.resolve(context.workspace()).await {
        Ok(reader) => reader,
        Err(e) if e.kind() == Kind::NotFound => return Ok(None),
        Err(_) => return Err(failure("reader")),
    };
    let mut frame = Frame::new(policy.limits.max_section_bytes);
    for (index, directory) in chain.iter().enumerate() {
        for name in &policy.names {
            if cancelled.is_cancelled() {
                return Err(failure("cancelled"));
            }
            let path = if directory.is_empty() {
                name.clone()
            } else {
                format!("{directory}/{name}")
            };
            let entry = match reader
                .read_limited(&path, policy.limits.max_file_bytes)
                .await
            {
                Ok(bytes) => {
                    if bytes.len() > policy.limits.max_file_bytes {
                        return Err(failure("reader"));
                    }
                    let Ok(content) = String::from_utf8(bytes) else {
                        continue;
                    };
                    let content = content.trim_end();
                    if content.is_empty() || content.contains('\0') {
                        continue;
                    }
                    Entry::Content(content.into())
                }
                Err(e) if e.kind() == Kind::NotFound => continue,
                Err(e) if e.kind() == Kind::TooLarge => Entry::TooLarge,
                Err(_) => return Err(failure("reader")),
            };
            let prefix = if index == chain.len() - 1 {
                "./".into()
            } else {
                "../".repeat(chain.len() - 1 - index)
            };
            if !frame.push(&format!("{prefix}{name}"), entry) {
                return Ok(frame.finish());
            }
        }
    }
    if cancelled.is_cancelled() {
        return Err(failure("cancelled"));
    }
    Ok(frame.finish())
}
