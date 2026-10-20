//! Public orderly close observes original work; it never certifies native policy.
mod orderly_host_close {
    pub mod support;
}
use crabber::session::{DeleteSession, DeletionBlocker, DeletionOutcome, Store};
use crabber::{
    Admission, AdmissionKey, AdmissionOptions, CloseObservation, HostExtensionClose,
    InputFingerprint, RuntimeError, SessionId,
};
use orderly_host_close::support::{BOUND, Factory, Provider, Release, builder};
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio::time::timeout;

fn options(agent: &crabber::Agent, text: &str) -> AdmissionOptions {
    AdmissionOptions {
        key: AdmissionKey::new("original-close-input").unwrap(),
        fingerprint: InputFingerprint::new(crabber::session::admission_config_hash(
            &serde_json::json!(["orderly-public-close.input.v1", text]),
        ))
        .unwrap(),
        behavior_fingerprint: agent.behavior_fingerprint().clone(),
    }
}
#[tokio::test]
async fn idle_zero_and_repeated_close_cut_fresh_public_intake() {
    let agent = builder(Arc::new(Provider::new(false, 1))).build().unwrap();
    let zero = agent.close(Duration::ZERO).await;
    let closed = agent.close(BOUND).await;
    let repeated = agent.close(BOUND).await;
    assert!(!zero.observer_failed);
    assert_eq!(closed.observation, CloseObservation::Complete);
    assert!(closed.local_drained());
    assert!(repeated.local_drained());
    assert_eq!(closed.extensions, HostExtensionClose::NotConfigured);
    assert!(matches!(agent.ready().await, Err(RuntimeError::HostClosed)));
    assert!(matches!(
        agent.prompt(None, "fresh").await,
        Err(RuntimeError::HostClosed)
    ));
    assert!(matches!(
        agent
            .prompt_keyed(SessionId::new(), "fresh", options(&agent, "fresh"))
            .await,
        Err(RuntimeError::HostClosed)
    ));
}

#[tokio::test]
async fn unread_events_do_not_block_close_and_exact_keyed_replay_stays_metadata() {
    let provider = Arc::new(Provider::new(false, 300));
    let store = Arc::new(crabber::session::MemoryStore::new());
    let agent = builder(provider.clone())
        .store(store.clone())
        .build()
        .unwrap();
    let session = SessionId::new();
    let original_options = options(&agent, "original");
    let Admission::Started { receipt, handle } = agent
        .prompt_keyed(session.clone(), "original", original_options.clone())
        .await
        .unwrap()
    else {
        panic!("first genuine input must start");
    };
    let run = handle.run_id().clone();
    // Leave its real event receiver unread, including while the worker finishes.
    let result = timeout(BOUND, handle.done()).await.unwrap();
    let closed = agent.close(BOUND).await;
    let revision = store.session_revision(&session).await.unwrap();
    let replay = agent
        .prompt_keyed(session.clone(), "original", original_options)
        .await
        .unwrap();
    let deletion = store
        .delete_session(DeleteSession {
            id: crabber::core::ControlId::new(),
            session_id: session.clone(),
            expected_revision: revision,
        })
        .await
        .unwrap();
    assert!(result.is_ok());
    assert!(closed.local_drained());
    assert!(matches!(replay, Admission::Replayed(actual) if actual == receipt));
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(store.session_revision(&session).await.unwrap(), revision);
    assert_eq!(
        store.get_run(&run).await.unwrap().unwrap().status,
        crabber::core::RunStatus::Completed
    );
    assert_eq!(
        deletion,
        DeletionOutcome::Blocked(DeletionBlocker::OwnershipUnknown)
    );
}

#[tokio::test]
async fn dropped_admission_keeps_actual_prerun_factory_until_original_release() {
    let provider = Arc::new(Provider::new(false, 1));
    let factory = Factory::new("held-prerun", true, false);
    let release = Release(factory.release.clone());
    let store = Arc::new(crabber::session::MemoryStore::new());
    let agent = Arc::new(
        builder(provider.clone())
            .store(store.clone())
            .extension_factory(factory.clone())
            .build()
            .unwrap(),
    );
    let current = agent.clone();
    let admission = tokio::spawn(async move { current.prompt(None, "never dispatched").await });
    timeout(BOUND, factory.entered.notified()).await.unwrap();
    admission.abort();
    let cancelled = admission.await;
    let pending = agent.close(Duration::ZERO).await;
    drop(release);
    let closed = agent.close(BOUND).await;
    let unfinished = store.list_unfinished_runs().await.unwrap();
    assert!(matches!(cancelled, Err(error) if error.is_cancelled()));
    assert!(!pending.local_drained());
    assert!(!closed.observer_failed);
    assert_eq!(closed.runtime.pending_enrollments, []);
    assert_eq!(closed.runtime.executions.pending_preparations, []);
    assert!(closed.local_drained());
    assert_eq!(factory.returned.load(Ordering::SeqCst), 1);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    assert_eq!(unfinished, []);
}

#[tokio::test]
async fn all_factory_resources_are_signalled_before_wait_and_cancelled_close_is_retained() {
    let first = Factory::new("first-owned-resource", false, true);
    let second = Factory::new("second-owned-resource", false, true);
    let release_first = Release(first.release.clone());
    let release_second = Release(second.release.clone());
    let agent = builder(Arc::new(Provider::new(false, 1)))
        .extension_factory(first.clone())
        .extension_factory(second.clone())
        .build()
        .unwrap();
    let handle = agent
        .prompt(None, "establish both original owners")
        .await
        .unwrap();
    timeout(BOUND, handle.done()).await.unwrap().unwrap();
    let signals = async {
        first.closed.acquire().await.unwrap().forget();
        second.closed.acquire().await.unwrap().forget();
    };
    let signalled = {
        let first_close = agent.close(BOUND);
        tokio::pin!(first_close);
        tokio::select! {
            () = signals => true,
            _ = &mut first_close => false,
        }
    }; // The actual caller close future drops; original cleanup remains retained.
    let (one, two) = tokio::join!(agent.close(Duration::ZERO), agent.close(Duration::ZERO));
    let before_release = (
        first.returned.load(Ordering::SeqCst),
        second.returned.load(Ordering::SeqCst),
    );
    drop(release_first);
    drop(release_second);
    let closed = agent.close(BOUND).await;
    assert!(
        signalled,
        "both original resources must receive close before either wait completes"
    );
    assert!(!one.local_drained() && !two.local_drained());
    assert_eq!(before_release, (1, 1));
    assert!(closed.local_drained());
    assert_eq!(first.returned.load(Ordering::SeqCst), 1);
    assert_eq!(second.returned.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn close_extensions_remains_non_cancelling_for_a_genuine_active_provider() {
    let provider = Arc::new(Provider::new(true, 1));
    let release = Release(provider.release.clone());
    let agent = builder(provider.clone()).build().unwrap();
    let handle = agent.prompt(None, "already running").await.unwrap();
    timeout(BOUND, provider.entered.notified()).await.unwrap();
    let closed_extensions = agent.close_extensions().await;
    let sealed = agent.prompt(None, "new work").await;
    drop(release);
    let original = timeout(BOUND, handle.done()).await.unwrap();
    let closed = agent.close(BOUND).await;
    assert!(closed_extensions.is_ok());
    assert!(matches!(sealed, Err(RuntimeError::HostClosed)));
    assert!(
        original.is_ok(),
        "extension-only close must not cancel the original execution"
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert!(closed.local_drained());
}

#[tokio::test]
async fn actual_tool_callback_self_close_is_distinct_from_deadline_and_external_drain() {
    let tool = Arc::new(orderly_host_close::support::SelfCloseTool::default());
    let call = crabber::core::ToolCallId::new();
    let script = vec![
        crabber::StreamDelta::ToolCallStart {
            call_id: call.clone(),
            name: "self_close".into(),
        },
        crabber::StreamDelta::ToolCallArgsDelta {
            call_id: call.clone(),
            text: "{}".into(),
        },
        crabber::StreamDelta::ToolCallDone { call_id: call },
        crabber::StreamDelta::Completed,
    ];
    let agent = Arc::new(
        builder(Arc::new(crabber::FakeProvider::scripted(vec![script])))
            .policy(Arc::new(crabber::StaticPolicy::new(
                crabber::PermissionDecision::Allow,
            )))
            .shared_tool(Arc::new(crabber::ToolDefinition {
                info: crabber::core::ToolInfo {
                    name: "self_close".into(),
                    description: "close from the actual callback".into(),
                    parameters: serde_json::json!({"type":"object"}),
                    retry_safe: false,
                    required_permissions: vec![],
                },
                executor: tool.clone(),
            }))
            .build()
            .unwrap(),
    );
    *tool.agent.lock().unwrap() = Arc::downgrade(&agent);
    let handle = agent
        .prompt(None, "invoke the actual callback")
        .await
        .unwrap();
    let callback = timeout(BOUND, tool.finished.notified()).await;
    let original = timeout(BOUND, handle.done()).await;
    let closed = agent.close(BOUND).await;
    assert!(callback.is_ok());
    assert_eq!(
        *tool.observed.lock().unwrap(),
        Some(CloseObservation::SelfClose)
    );
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    assert!(
        original.is_ok(),
        "the same actual worker must finish after callback return"
    );
    assert!(closed.local_drained());
    assert!(matches!(agent.ready().await, Err(RuntimeError::HostClosed)));
}

#[tokio::test]
async fn unrepresentable_close_deadline_still_cuts_intake_and_preserves_later_observation() {
    let agent = builder(Arc::new(Provider::new(false, 1))).build().unwrap();
    let overflow = agent.close(Duration::MAX).await;
    let rejected = agent.prompt(None, "fresh after overflow").await;
    let closed = agent.close(BOUND).await;
    assert_eq!(
        overflow.observation,
        CloseObservation::UnrepresentableDeadline
    );
    assert!(!overflow.local_drained());
    assert!(matches!(rejected, Err(RuntimeError::HostClosed)));
    assert!(closed.local_drained());
}

#[tokio::test]
async fn actual_shared_initializer_refuses_same_agent_prompt_without_closing_provider() {
    let provider = Arc::new(Provider::new(false, 1));
    let initializer = Arc::new(orderly_host_close::support::ReentrantInitializer::default());
    let agent = Arc::new(
        builder(provider.clone())
            .shared_extension(initializer.clone(), crabber::Scope::Global)
            .build()
            .unwrap(),
    );
    *initializer.agent.lock().unwrap() = Arc::downgrade(&agent);
    let ready = timeout(BOUND, agent.ready()).await;
    let before_prompt = provider.calls.load(Ordering::SeqCst);
    let next = agent
        .prompt(None, "ordinary prompt after refused initializer reentry")
        .await;
    let completed = match next {
        Ok(handle) => timeout(BOUND, handle.done())
            .await
            .is_ok_and(|result| result.is_ok()),
        Err(_) => false,
    };
    let closed = agent.close(BOUND).await;
    assert!(ready.is_ok_and(|result| result.is_ok()));
    assert!(!initializer.timed_out.load(Ordering::SeqCst));
    assert!(matches!(
        initializer.error.lock().unwrap().as_ref(),
        Some(RuntimeError::ExtensionPlan(
            crabber::ExtensionError::SelfClose
        ))
    ));
    assert_eq!(before_prompt, 0);
    assert_eq!(initializer.installs.load(Ordering::SeqCst), 1);
    assert!(
        completed,
        "refused reentry must leave the same provider open for normal admission"
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert!(closed.local_drained());
}

#[tokio::test]
async fn held_shared_shutdown_cannot_report_complete_and_later_close_refreshes_once() {
    let extension = Arc::new(orderly_host_close::support::HeldShutdown::default());
    let release = Release(extension.release.clone());
    let agent = builder(Arc::new(Provider::new(false, 1)))
        .extension_close_timeout(Duration::from_millis(10))
        .shared_extension(extension.clone(), crabber::Scope::Global)
        .build()
        .unwrap();
    agent.ready().await.unwrap();
    let (entered, pending) = {
        let closing = agent.close(Duration::from_millis(50));
        tokio::pin!(closing);
        tokio::select! {
            () = extension.entered.notified() => (true, closing.await),
            report = &mut closing => (false, report),
        }
    };
    // Legacy Shared mounts bound leases/cleanup, then retain shutdown beyond
    // that mount bound. The caller deadline must leave this original pending.
    let original_calls = extension.calls.load(Ordering::SeqCst);
    drop(release);
    let closed = agent.close(BOUND).await;
    assert!(entered);
    assert!(!pending.local_drained());
    assert_ne!(pending.observation, CloseObservation::Complete);
    assert!(!pending.observer_failed);
    assert_eq!(pending.extensions, HostExtensionClose::Pending);
    assert_eq!(original_calls, 1);
    assert_eq!(closed.observation, CloseObservation::Complete);
    assert_eq!(closed.extensions, HostExtensionClose::Closed);
    assert!(closed.local_drained());
    assert_eq!(extension.calls.load(Ordering::SeqCst), 1);
}
