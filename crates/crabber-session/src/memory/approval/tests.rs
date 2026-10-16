#[tokio::test]
async fn control_faults_keep_attempt_barrier_answers_and_receipts_atomic() {
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = super::MemoryStore::with_clock(clock.clone());
    crate::approval_contract::run_fault_contract(&store, &clock, |point| {
        store
            .approval_fault
            .store(point as u8, std::sync::atomic::Ordering::SeqCst);
    })
    .await;
}

#[tokio::test]
async fn recovery_rejects_coherently_changed_consumed_answer() {
    use crate::Store;
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = super::MemoryStore::with_clock(clock.clone());
    let p = crate::approval_contract::recovery::prepare(&store, &clock).await;
    clock.set(time::OffsetDateTime::UNIX_EPOCH + time::Duration::milliseconds(31));
    let original = store.state.lock().unwrap().approvals[&p.original.continuation.id].clone();
    for change in 0..2 {
        {
            let mut state = store.state.lock().unwrap();
            let record = state
                .approvals
                .get_mut(&p.original.continuation.id)
                .unwrap();
            *record = original.clone();
            if change == 0 {
                record.questions[0].answer.as_mut().unwrap().reply = crate::ApprovalReply::Deny;
                record.questions[0].view.state = crabber_core::InteractionState::Denied;
            } else {
                record.batch.calls[2].decision = crate::CallApproval::Deny;
            }
            // Changed values agree internally, so record-local validation succeeds.
            record
                .validate(&p.original.session_id, &p.original.run_id)
                .unwrap();
        }
        let before = crate::approval_contract::origin::evidence(&store, &p).await;
        assert_eq!(
            store
                .reclaim_approval_continuation(p.request.clone())
                .await
                .unwrap_err(),
            crate::ApprovalError::UnknownStoreOutcome
        );
        assert_eq!(
            crate::approval_contract::origin::evidence(&store, &p).await,
            before
        );
    }
}

#[tokio::test]
async fn approval_origin_rejects_coherent_deadline_and_source_changes() {
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = super::MemoryStore::with_clock(clock.clone());
    crate::approval_contract::origin::run(&store, &clock, |id, case| {
        let mut state = store.state.lock().unwrap();
        let record = state.approvals.get_mut(&id).unwrap();
        let fingerprint = record
            .batch
            .fingerprint(&record.session_id, &record.run_id)
            .unwrap();
        crate::approval_contract::origin::corrupt(record, case);
        record.validate(&record.session_id, &record.run_id).unwrap();
        assert_eq!(
            record
                .batch
                .fingerprint(&record.session_id, &record.run_id)
                .unwrap(),
            fingerprint
        );
        std::future::ready(())
    })
    .await;
}

#[tokio::test]
async fn immutable_current_approval_sources_reject_coherent_private_changes() {
    use crate::approval_contract::current::{self, Case};
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = super::MemoryStore::with_clock(clock.clone());
    for case in current::CASES {
        let saved = current::run(&store, &clock, case, |target| {
            let mut state = store.state.lock().unwrap();
            let saved = state.clone();
            let record = state.approvals.get_mut(&target.id).unwrap();
            current::corrupt(record, target.case);
            let session = record.session_id.clone();
            let run = record.run_id.clone();
            if matches!(
                target.case,
                Case::ReleasedNoBarrier | Case::FakeTerminalCleanup
            ) {
                state.approval_barriers.remove(&session);
            }
            if target.case == Case::FakeTerminalCleanup {
                state.runs.get_mut(&run).unwrap().status = crabber_core::RunStatus::Interrupted;
            }
            if target.case == Case::ChangedClaimReceipt {
                let Some(super::OperationReceipt::Approval(receipt)) = state
                    .operation_receipts
                    .get_mut(&(session, target.claim_id.unwrap()))
                else {
                    panic!("claim receipt")
                };
                receipt.semantic_digest =
                    crabber_core::InputFingerprint::new("c".repeat(64)).unwrap();
            }
            std::future::ready(saved)
        })
        .await;
        *store.state.lock().unwrap() = saved;
    }
}

#[tokio::test]
async fn current_approval_accepts_authentic_successor_and_parent_retirement() {
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = super::MemoryStore::with_clock(clock.clone());
    crate::public_session::rounds::checked(&store, &clock, |prior, run| {
        let state = store.state.lock().unwrap();
        let record = state.approvals.get(&prior.id).unwrap();
        assert_eq!(record.state, crate::ApprovalContinuationState::Released);
        assert_eq!(run.status, crabber_core::RunStatus::Paused);
        super::verify_record(&state, &run, record).unwrap();
        std::future::ready(())
    })
    .await;
    crate::approval_contract::current::retired(&store, &clock).await;
}

#[tokio::test]
async fn original_approval_terminal_source_binds_request_and_actual_settlement() {
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = super::MemoryStore::with_clock(clock.clone());
    crate::approval::terminal::contract::run(&store, &clock).await;
}
