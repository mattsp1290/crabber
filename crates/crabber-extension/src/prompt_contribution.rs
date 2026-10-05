//! Named, bounded native prompt contributions collected anew for each model attempt.
use crate::dispatch::{InFlight, discard_panic_payload, with_mount};
use crate::{CleanupTracker, ExtensionError, WorkspaceContext};
use crabber_core::{RunId, SessionId, TurnId};
use futures::{FutureExt, future::BoxFuture};
use std::{panic::AssertUnwindSafe, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

pub const PROMPT_CONTRIBUTION_CONTRACT_VERSION: u32 = 1;
pub const PROMPT_CONTRIBUTION_DEADLINE: Duration = Duration::from_secs(5);
pub const PROMPT_CONTRIBUTIONS_TOTAL_DEADLINE: Duration = Duration::from_secs(15);
pub const MAX_PROMPT_CONTRIBUTION_BYTES: usize = 32 * 1024;
pub const MAX_PROMPT_CONTRIBUTIONS_TOTAL_BYTES: usize = 128 * 1024;
pub const MAX_PROMPT_CONTRIBUTOR_NAME_BYTES: usize = 128;

#[must_use]
pub fn prompt_contribution_failed_message(name: &str) -> String {
    format!("prompt contribution failed: {name}")
}

/// Read-only attempt identity. Only the context constructed by the runtime is
/// authoritative. The driver replaces the cancellation token and cleanup tracker
/// for each invocation; neither is serialized.
#[derive(Debug, Clone)]
pub struct PromptAttemptContext {
    session_id: SessionId,
    run_id: RunId,
    turn_id: TurnId,
    workspace: WorkspaceContext,
    provider_id: String,
    model_id: String,
    attempt: u32,
    after_compaction: bool,
    cancellation: CancellationToken,
    cleanup: CleanupTracker,
}
impl PromptAttemptContext {
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        session_id: SessionId,
        run_id: RunId,
        turn_id: TurnId,
        workspace: WorkspaceContext,
        provider_id: String,
        model_id: String,
        attempt: u32,
        after_compaction: bool,
    ) -> Self {
        Self {
            session_id,
            run_id,
            turn_id,
            workspace,
            provider_id,
            model_id,
            attempt,
            after_compaction,
            cancellation: CancellationToken::new(),
            cleanup: CleanupTracker::detached(),
        }
    }
    #[must_use]
    pub fn with_cancellation(mut self, cancellation: CancellationToken) -> Self {
        self.cancellation = cancellation;
        self
    }
    #[must_use]
    pub fn with_cleanup(mut self, cleanup: CleanupTracker) -> Self {
        self.cleanup = cleanup;
        self
    }
    #[must_use]
    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }
    #[must_use]
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
    #[must_use]
    pub fn turn_id(&self) -> &TurnId {
        &self.turn_id
    }
    #[must_use]
    pub fn workspace(&self) -> &WorkspaceContext {
        &self.workspace
    }
    #[must_use]
    pub fn provider_id(&self) -> &str {
        &self.provider_id
    }
    #[must_use]
    pub fn model_id(&self) -> &str {
        &self.model_id
    }
    /// One-based attempt within this execution of the current turn.
    #[must_use]
    pub fn attempt(&self) -> u32 {
        self.attempt
    }
    #[must_use]
    pub fn after_compaction(&self) -> bool {
        self.after_compaction
    }
    #[must_use]
    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }
    #[must_use]
    pub fn cleanup(&self) -> &CleanupTracker {
        &self.cleanup
    }
}

pub type PromptContributor = Arc<
    dyn Fn(PromptAttemptContext) -> BoxFuture<'static, Result<Option<String>, ExtensionError>>
        + Send
        + Sync,
>;

#[derive(Clone)]
pub struct MountedPromptContributor {
    pub name: String,
    pub order: i32,
    pub(crate) mount_id: u64,
    pub(crate) mount_seq: u64,
    pub(crate) cleanup: CleanupTracker,
    pub(crate) callback: PromptContributor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromptContribution {
    pub name: String,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromptContributionOutcome {
    Completed { sections: Vec<PromptContribution> },
    Failed { contributor: String },
    Interrupted,
}

/// Collects in frozen slice order. Empty texts are skipped without trimming.
/// Errors and panics expose only the registration name. Parent cancellation
/// takes precedence; deadline or mount close cancels the invocation child.
/// Successful callbacks may leave work running on their mount's tracker.
pub async fn collect_prompt_contributions(
    contributors: &[MountedPromptContributor],
    context: PromptAttemptContext,
) -> PromptContributionOutcome {
    let parent = context.cancellation();
    let attempt_deadline = tokio::time::Instant::now() + PROMPT_CONTRIBUTIONS_TOTAL_DEADLINE;
    let mut sections = Vec::new();
    let mut total_bytes = 0;
    for contributor in contributors {
        let failed = || PromptContributionOutcome::Failed {
            contributor: contributor.name.clone(),
        };
        if parent.is_cancelled() {
            return PromptContributionOutcome::Interrupted;
        }
        if contributor.cleanup.is_closing() {
            return failed();
        }
        let child = parent.child_token();
        let invocation = context
            .clone()
            .with_cancellation(child.clone())
            .with_cleanup(contributor.cleanup.clone());
        let deadline =
            attempt_deadline.min(tokio::time::Instant::now() + PROMPT_CONTRIBUTION_DEADLINE);
        let mut future = InFlight::new(
            AssertUnwindSafe(with_mount(contributor.mount_id, async move {
                (contributor.callback)(invocation).await
            }))
            .catch_unwind(),
        );
        let output = tokio::select! {
            biased;
            () = parent.cancelled() => None,
            () = contributor.cleanup.closing() => None,
            () = tokio::time::sleep_until(deadline) => None,
            output = &mut future => Some(output),
        };
        // Normalize arbitrary panic payloads before any early return can drop
        // them outside containment, including when completion races cancellation.
        let output = output.map(|result| result.map_err(discard_panic_payload));
        if parent.is_cancelled() {
            child.cancel();
            return PromptContributionOutcome::Interrupted;
        }
        if tokio::time::Instant::now() >= deadline {
            child.cancel();
            return failed();
        }
        let Some(output) = output else {
            child.cancel();
            return failed();
        };
        let Ok(Ok(text)) = output else {
            return failed();
        };
        if let Some(text) = text {
            if text.len() > MAX_PROMPT_CONTRIBUTION_BYTES {
                return failed();
            }
            total_bytes += text.len();
            if total_bytes > MAX_PROMPT_CONTRIBUTIONS_TOTAL_BYTES {
                return failed();
            }
            if !text.is_empty() {
                sections.push(PromptContribution {
                    name: contributor.name.clone(),
                    text,
                });
            }
        }
    }
    if parent.is_cancelled() {
        PromptContributionOutcome::Interrupted
    } else {
        PromptContributionOutcome::Completed { sections }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::is_active_mount;
    use std::{
        future::Future,
        pin::Pin,
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
        task::{Context, Poll},
    };
    use tokio_util::task::TaskTracker;

    fn context() -> PromptAttemptContext {
        PromptAttemptContext::new(
            "session".into(),
            "run".into(),
            "turn".into(),
            WorkspaceContext::from_persisted("workspace", "/workspace"),
            "provider".into(),
            "model".into(),
            2,
            true,
        )
    }
    fn contributor(name: &str, callback: PromptContributor) -> MountedPromptContributor {
        MountedPromptContributor {
            name: name.into(),
            order: 0,
            mount_id: 42,
            mount_seq: 42,
            cleanup: CleanupTracker::detached(),
            callback,
        }
    }
    fn text(name: &str, value: Option<String>) -> MountedPromptContributor {
        contributor(
            name,
            Arc::new(move |_| {
                let value = value.clone();
                Box::pin(async move { Ok(value) })
            }),
        )
    }
    fn failed(name: &str) -> PromptContributionOutcome {
        PromptContributionOutcome::Failed {
            contributor: name.into(),
        }
    }
    #[test]
    fn accessors_builders_and_constants() {
        let context = context();
        assert_eq!(context.session_id(), &SessionId::from("session"));
        assert_eq!(context.run_id(), &RunId::from("run"));
        assert_eq!(context.turn_id(), &TurnId::from("turn"));
        assert_eq!(context.workspace().workspace_id(), Some("workspace"));
        assert_eq!(context.workspace().directory(), Some("/workspace"));
        assert_eq!(context.provider_id(), "provider");
        assert_eq!(context.model_id(), "model");
        assert_eq!(context.attempt(), 2);
        assert!(context.after_compaction());
        let token = CancellationToken::new();
        let close = CancellationToken::new();
        let context =
            context
                .with_cancellation(token.clone())
                .with_cleanup(CleanupTracker::from_parts(
                    TaskTracker::new(),
                    close.clone(),
                ));
        token.cancel();
        close.cancel();
        assert!(context.cancellation().is_cancelled());
        assert!(context.cleanup().is_closing());
        assert_eq!(PROMPT_CONTRIBUTION_CONTRACT_VERSION, 1);
        assert_eq!(PROMPT_CONTRIBUTION_DEADLINE, Duration::from_secs(5));
        assert_eq!(PROMPT_CONTRIBUTIONS_TOTAL_DEADLINE, Duration::from_secs(15));
        assert_eq!(MAX_PROMPT_CONTRIBUTION_BYTES, 32_768);
        assert_eq!(MAX_PROMPT_CONTRIBUTIONS_TOTAL_BYTES, 131_072);
        assert_eq!(MAX_PROMPT_CONTRIBUTOR_NAME_BYTES, 128);
        assert_eq!(
            prompt_contribution_failed_message("name"),
            "prompt contribution failed: name"
        );
    }
    #[tokio::test]
    async fn frozen_slice_order_skips_empty_without_trimming() {
        let contributors = [
            text("z", Some(" z ".into())),
            text("none", None),
            text("empty", Some(String::new())),
            text("a", Some("a".into())),
        ];
        assert_eq!(
            collect_prompt_contributions(&contributors, context()).await,
            PromptContributionOutcome::Completed {
                sections: vec![
                    PromptContribution {
                        name: "z".into(),
                        text: " z ".into()
                    },
                    PromptContribution {
                        name: "a".into(),
                        text: "a".into()
                    },
                ]
            }
        );
    }
    struct PendingDrop {
        dropped: Arc<AtomicBool>,
        panic: bool,
    }
    impl Future for PendingDrop {
        type Output = Result<Option<String>, ExtensionError>;
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }
    impl Drop for PendingDrop {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
            assert!(!self.panic, "secret destructor");
        }
    }
    #[tokio::test(start_paused = true)]
    async fn errors_panics_and_panicking_destructors_stop_the_chain() {
        for mode in 0..3 {
            let dropped = Arc::new(AtomicBool::new(false));
            let later = Arc::new(AtomicUsize::new(0));
            let cb: PromptContributor = match mode {
                0 => Arc::new(|_| Box::pin(async { Err(ExtensionError::Tool("secret".into())) })),
                1 => Arc::new(|_| panic!("secret callback")),
                _ => {
                    let dropped = dropped.clone();
                    Arc::new(move |_| {
                        Box::pin(PendingDrop {
                            dropped: dropped.clone(),
                            panic: true,
                        })
                    })
                }
            };
            let after = later.clone();
            let contributors = [
                contributor("bad", cb),
                contributor(
                    "later",
                    Arc::new(move |_| {
                        after.fetch_add(1, Ordering::SeqCst);
                        Box::pin(async { Ok(None) })
                    }),
                ),
            ];
            assert_eq!(
                collect_prompt_contributions(&contributors, context()).await,
                failed("bad")
            );
            assert_eq!(later.load(Ordering::SeqCst), 0);
            if mode == 2 {
                assert!(dropped.load(Ordering::SeqCst));
            }
        }
    }
    #[tokio::test(start_paused = true)]
    async fn deadline_cancels_child_drops_future_and_preserves_tracked_work() {
        let captured = Arc::new(std::sync::Mutex::new(None));
        let dropped = Arc::new(AtomicBool::new(false));
        let release = CancellationToken::new();
        let tracker = CleanupTracker::detached();
        let cb = {
            let captured = captured.clone();
            let dropped = dropped.clone();
            let release = release.clone();
            Arc::new(move |context: PromptAttemptContext| {
                *captured.lock().unwrap() = Some(context.cancellation().clone());
                context.cleanup().spawn(release.clone().cancelled_owned());
                Box::pin(PendingDrop {
                    dropped: dropped.clone(),
                    panic: false,
                }) as BoxFuture<'static, _>
            })
        };
        let mut c = contributor("slow", cb);
        c.cleanup = tracker.clone();
        let start = tokio::time::Instant::now();
        assert_eq!(
            collect_prompt_contributions(&[c], context()).await,
            failed("slow")
        );
        assert_eq!(start.elapsed(), PROMPT_CONTRIBUTION_DEADLINE);
        assert!(captured.lock().unwrap().as_ref().unwrap().is_cancelled());
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(tracker.pending(), 1);
        release.cancel();
        tokio::task::yield_now().await;
        assert_eq!(tracker.pending(), 0);
    }
    #[tokio::test(start_paused = true)]
    async fn total_deadline_stops_fourth_contributor_at_fifteen_seconds() {
        let calls = Arc::new(AtomicUsize::new(0));
        let contributors: Vec<_> = (0..5)
            .map(|i| {
                let calls = calls.clone();
                contributor(
                    &i.to_string(),
                    Arc::new(move |_| {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Box::pin(async {
                            tokio::time::sleep(Duration::from_secs(4)).await;
                            Ok(None)
                        })
                    }),
                )
            })
            .collect();
        let start = tokio::time::Instant::now();
        assert_eq!(
            collect_prompt_contributions(&contributors, context()).await,
            failed("3")
        );
        assert_eq!(start.elapsed(), PROMPT_CONTRIBUTIONS_TOTAL_DEADLINE);
        assert_eq!(calls.load(Ordering::SeqCst), 4);
    }
    #[tokio::test]
    async fn cancellation_before_during_and_completion_race_wins() {
        for mode in 0..3 {
            let parent = CancellationToken::new();
            let captured = Arc::new(std::sync::Mutex::new(None));
            if mode == 0 {
                parent.cancel();
            }
            let cb = {
                let parent = parent.clone();
                let captured = captured.clone();
                Arc::new(move |context: PromptAttemptContext| {
                    *captured.lock().unwrap() = Some(context.cancellation().clone());
                    let parent = parent.clone();
                    Box::pin(async move {
                        parent.cancel();
                        if mode == 1 {
                            futures::future::pending::<()>().await;
                        }
                        Ok(Some("discard".into()))
                    }) as BoxFuture<'static, _>
                })
            };
            assert_eq!(
                collect_prompt_contributions(
                    &[contributor("cancel", cb)],
                    context().with_cancellation(parent)
                )
                .await,
                PromptContributionOutcome::Interrupted
            );
            if mode == 0 {
                assert!(captured.lock().unwrap().is_none());
            } else {
                assert!(captured.lock().unwrap().as_ref().unwrap().is_cancelled());
            }
        }
    }
    #[tokio::test]
    async fn individual_and_total_byte_boundaries() {
        for (size, expected) in [
            (MAX_PROMPT_CONTRIBUTION_BYTES, true),
            (MAX_PROMPT_CONTRIBUTION_BYTES + 1, false),
        ] {
            let result =
                collect_prompt_contributions(&[text("size", Some("x".repeat(size)))], context())
                    .await;
            assert_eq!(
                matches!(result, PromptContributionOutcome::Completed { .. }),
                expected
            );
            if !expected {
                assert_eq!(result, failed("size"));
            }
        }
        let mut contributors: Vec<_> = (0..4)
            .map(|i| {
                text(
                    &i.to_string(),
                    Some("x".repeat(MAX_PROMPT_CONTRIBUTION_BYTES)),
                )
            })
            .collect();
        assert!(matches!(
            collect_prompt_contributions(&contributors, context()).await,
            PromptContributionOutcome::Completed { .. }
        ));
        contributors.push(text("overflow", Some("x".into())));
        assert_eq!(
            collect_prompt_contributions(&contributors, context()).await,
            failed("overflow")
        );
    }
    #[tokio::test]
    async fn mount_close_before_and_during_callback_and_mount_scope() {
        for before in [true, false] {
            let close = CancellationToken::new();
            let cb = {
                let close = close.clone();
                Arc::new(move |_| {
                    assert!(is_active_mount(42));
                    close.cancel();
                    Box::pin(futures::future::pending()) as BoxFuture<'static, _>
                })
            };
            let mut c = contributor("close", cb);
            c.cleanup = CleanupTracker::from_parts(TaskTracker::new(), close.clone());
            if before {
                close.cancel();
            }
            assert_eq!(
                collect_prompt_contributions(&[c], context()).await,
                failed("close")
            );
        }
    }
    struct PanicPayload;
    struct SecondaryPayload;
    impl Drop for PanicPayload {
        fn drop(&mut self) {
            std::panic::panic_any(SecondaryPayload);
        }
    }
    impl Drop for SecondaryPayload {
        fn drop(&mut self) {
            panic!("secondary payload must never be dropped");
        }
    }
    fn panic_with_payload() -> Result<Option<String>, ExtensionError> {
        std::panic::panic_any(PanicPayload)
    }
    #[tokio::test]
    async fn panic_payload_destructors_cannot_escape_failure_or_cancellation() {
        for cancelled in [false, true] {
            let parent = CancellationToken::new();
            let callback = {
                let parent = parent.clone();
                Arc::new(move |_| {
                    let parent = parent.clone();
                    Box::pin(async move {
                        if cancelled {
                            parent.cancel();
                        }
                        panic_with_payload()
                    }) as BoxFuture<'static, _>
                })
            };
            let result = AssertUnwindSafe(collect_prompt_contributions(
                &[contributor("payload", callback)],
                context().with_cancellation(parent),
            ))
            .catch_unwind()
            .await
            .expect("panic payload escaped collection");
            assert_eq!(
                result,
                if cancelled {
                    PromptContributionOutcome::Interrupted
                } else {
                    failed("payload")
                }
            );
        }
    }
    struct PayloadDropFuture;
    impl Future for PayloadDropFuture {
        type Output = Result<Option<String>, ExtensionError>;
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }
    impl Drop for PayloadDropFuture {
        fn drop(&mut self) {
            std::panic::panic_any(PanicPayload);
        }
    }
    #[tokio::test(start_paused = true)]
    async fn panic_payload_from_future_drop_is_contained() {
        let c = contributor("drop", Arc::new(|_| Box::pin(PayloadDropFuture)));
        let result = AssertUnwindSafe(collect_prompt_contributions(&[c], context()))
            .catch_unwind()
            .await;
        assert_eq!(result.unwrap(), failed("drop"));
    }
    /// The timer arm was polled before advance, but the callback returns Ready
    /// in the same poll that moves Tokio's paused clock beyond its deadline.
    struct LateReady {
        advance: BoxFuture<'static, ()>,
    }
    impl Future for LateReady {
        type Output = Result<Option<String>, ExtensionError>;
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let _ = self.advance.as_mut().poll(cx);
            Poll::Ready(Ok(Some("expired".into())))
        }
    }
    #[tokio::test(start_paused = true)]
    async fn late_ready_result_is_rejected_and_child_cancelled() {
        let captured = Arc::new(std::sync::Mutex::new(None));
        let callback = {
            let captured = captured.clone();
            Arc::new(move |context: PromptAttemptContext| {
                *captured.lock().unwrap() = Some(context.cancellation().clone());
                Box::pin(LateReady {
                    advance: Box::pin(tokio::time::advance(PROMPT_CONTRIBUTION_DEADLINE)),
                }) as BoxFuture<'static, _>
            })
        };
        assert_eq!(
            collect_prompt_contributions(&[contributor("late", callback)], context()).await,
            failed("late")
        );
        assert!(captured.lock().unwrap().as_ref().unwrap().is_cancelled());
    }
}
