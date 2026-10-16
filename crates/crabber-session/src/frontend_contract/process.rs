//! Actual killed writers and fresh readers retain only original request metadata.
use super::{Store, fixture};
use crate::*;
use crabber_core::{Clock, ControlId, ManualClock};
use serde::{Deserialize, Serialize};
use std::path::Path;
#[derive(Serialize, Deserialize)]
enum Saved {
    Stage(Box<FrontendCallStaging>, FrontendStagingReceipt),
    Suspend(Box<ToolContinuationSuspension>, ToolContinuationReceipt),
    Claim(Box<FrontendContinuationClaim>, ToolContinuationReceipt),
    Recover(Box<FrontendContinuationRecovery>, ToolContinuationReceipt),
}
pub(crate) async fn write(store: &impl Store, clock: &ManualClock, directory: &Path) {
    let step: u8 = std::env::var("CRABBER_TOOL_SQL_STEP")
        .unwrap()
        .parse()
        .unwrap();
    let f = fixture::fixture(store).await;
    let receipt = f
        .execution
        .stage_frontend_call(f.request.clone(), f.authority.clone())
        .await
        .unwrap();
    let saved = if step == 0 {
        Saved::Stage(Box::new(f.request), receipt)
    } else {
        let batch = fixture::suspension(store, &f.request).await;
        let paused = f
            .execution
            .suspend_tool_continuation(
                batch.clone(),
                ToolSuspensionAuthority {
                    attempt: f.authority,
                    phases: Vec::new(),
                },
            )
            .await
            .unwrap();
        if step == 1 {
            Saved::Suspend(Box::new(batch), paused)
        } else {
            let claim = fixture::external_claim(&f.request, &batch, paused.revision);
            let ToolContinuationClaimOutcome::Applied { receipt, .. } = store
                .claim_frontend_continuation(claim.clone())
                .await
                .unwrap()
            else {
                panic!("fresh claim")
            };
            if step == 2 {
                Saved::Claim(Box::new(claim), receipt)
            } else {
                clock.set(clock.now() + time::Duration::milliseconds(2));
                let recovery = FrontendContinuationRecovery {
                    id: ControlId::new(),
                    expected_revision: receipt.revision,
                    original: claim,
                    attempt: batch.next_attempt,
                    owner: "cold-frontend-recovery".into(),
                    lease_ms: 30000,
                };
                let ToolContinuationClaimOutcome::Applied { receipt, .. } = store
                    .recover_frontend_continuation(recovery.clone())
                    .await
                    .unwrap()
                else {
                    panic!("fresh recovery")
                };
                Saved::Recover(Box::new(recovery), receipt)
            }
        }
    };
    std::fs::write(
        directory.join("request"),
        serde_json::to_vec(&saved).unwrap(),
    )
    .unwrap();
    std::fs::write(directory.join("committed"), b"committed").unwrap();
    loop {
        std::thread::park();
    }
}
pub(crate) async fn verify(store: &impl Store, directory: &Path) {
    let saved: Saved =
        serde_json::from_slice(&std::fs::read(directory.join("request")).unwrap()).unwrap();
    match saved {
        Saved::Stage(request, receipt) => {
            let revision = store.session_revision(&request.session_id).await.unwrap();
            assert_eq!(
                store
                    .lookup_frontend_staging(&request.session_id, &request.id)
                    .await
                    .unwrap(),
                Some(receipt)
            );
            assert_eq!(
                store
                    .frontend_call_origin(&request.session_id, &request.run_id, &request.call.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .descriptor_fingerprint,
                request.descriptor_fingerprint
            );
            assert_eq!(
                store.session_revision(&request.session_id).await.unwrap(),
                revision
            );
        }
        Saved::Suspend(batch, receipt) => {
            assert_eq!(
                store
                    .lookup_tool_continuation(&receipt.session_id, &batch.id)
                    .await
                    .unwrap(),
                Some(receipt.clone())
            );
            let snapshot = store
                .tool_continuation_snapshot(&receipt.session_id, &receipt.run_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(snapshot.state, ToolContinuationState::Waiting);
            assert_eq!(snapshot.frontend_calls.len(), 1);
        }
        Saved::Claim(request, receipt) => {
            let revision = store.session_revision(&receipt.session_id).await.unwrap();
            assert!(
                matches!(store.claim_frontend_continuation(*request).await.unwrap(),ToolContinuationClaimOutcome::Replayed(saved) if saved==receipt)
            );
            assert_eq!(
                store.session_revision(&receipt.session_id).await.unwrap(),
                revision
            );
        }
        Saved::Recover(request, receipt) => {
            let revision = store.session_revision(&receipt.session_id).await.unwrap();
            assert!(
                matches!(store.recover_frontend_continuation(*request).await.unwrap(),ToolContinuationClaimOutcome::Replayed(saved) if saved==receipt)
            );
            assert_eq!(
                store.session_revision(&receipt.session_id).await.unwrap(),
                revision
            );
        }
    }
}
