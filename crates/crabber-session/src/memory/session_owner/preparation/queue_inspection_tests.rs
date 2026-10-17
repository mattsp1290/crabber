use super::*;
use crate::{Store, session_owner::preparation_contract::resources};
async fn fresh(store: &MemoryStore) -> (JoinedPreparation, QueueReceipt) {
    let (_, original, _) = resources::queue::prepared(store).await;
    let crate::session_owner::preparation::PreparationCompletion::Queue(outcome) =
        Store::complete_preparation(store, original.clone())
            .await
            .unwrap()
    else {
        panic!("Queue completion")
    };
    (original, outcome.receipt().clone())
}
#[tokio::test]
async fn pending_queue_original_inspection_uses_one_exact_budget_and_ignores_later_private_run_and_heads()
 {
    let store = MemoryStore::new();
    let (original, receipt) = fresh(&store).await;
    let mut proof = Proof::bounded(inspection::MAX_PROOF);
    assert_eq!(
        store
            .inspect_queue_admission_with_proof(&original, &mut proof)
            .unwrap(),
        Some(receipt.clone())
    );
    let used = proof.used();
    assert_eq!(
        store
            .inspect_queue_admission_with_proof(&original, &mut Proof::bounded(used))
            .unwrap(),
        Some(receipt.clone())
    );
    assert!(
        store
            .inspect_queue_admission_with_proof(&original, &mut Proof::bounded(used - 1))
            .is_err()
    );
    let run = &receipt.summary.admission.as_ref().unwrap().run_id;
    {
        let mut state = store.state.lock().unwrap();
        state.runs.get_mut(run).unwrap().error = Some("private-current".repeat(3_000_000));
        state
            .native_run_source_heads
            .remove(&(receipt.session_id.clone(), run.clone()));
        state
            .queue
            .get_mut(&(receipt.session_id.clone(), receipt.summary.queue_id.clone()))
            .unwrap()
            .summary
            .claim_generation += 1;
    }
    assert_eq!(
        store
            .inspect_queue_preparation_admission(&original)
            .await
            .unwrap(),
        Some(receipt)
    );
}
#[tokio::test]
async fn pending_queue_original_inspection_rejects_missing_sources_and_coherent_mutable_prepare_replacement()
 {
    for fault in [
        "index",
        "witness",
        "native19",
        "init",
        "prepare",
        "coherent",
        "legacy",
        "leaves",
        "all-queue-leaves",
    ] {
        let store = MemoryStore::new();
        let (original, receipt) = fresh(&store).await;
        let run = &receipt.summary.admission.as_ref().unwrap().run_id;
        {
            let mut state = store.state.lock().unwrap();
            match fault {
                "all-queue-leaves" => {
                    state
                        .queue_audits
                        .remove(&(receipt.session_id.clone(), receipt.operation_id.clone()));
                    state
                        .operation_receipts
                        .remove(&(receipt.session_id.clone(), receipt.operation_id.clone()));
                    state.queue_publications = crate::queue::source::Ledger::default();
                }
                "leaves" => {
                    state
                        .queue_audits
                        .remove(&(receipt.session_id.clone(), receipt.operation_id.clone()));
                    state
                        .operation_receipts
                        .remove(&(receipt.session_id.clone(), receipt.operation_id.clone()));
                }
                "index" => state.preparation_admission_joins.clear(),
                "witness" => {
                    state.preparation_admissions.remove(run);
                }
                "native19" => {
                    state
                        .native_run_sources
                        .remove(&(receipt.session_id.clone(), run.clone(), 1));
                }
                "init" => {
                    state
                        .admission_execution_sources
                        .remove(&(run.clone(), crate::admission_journal::Kind::Init));
                }
                "legacy" => state.queue_publications = crate::queue::source::Ledger::default(),
                "prepare" => {
                    state.queue_audits.retain(|_, audit| {
                        !matches!(audit.request.action, crate::QueueAction::Prepare { .. })
                    });
                }
                "coherent" => replace_prepare(&mut state, &receipt),
                _ => unreachable!(),
            }
        }
        assert!(
            store
                .inspect_queue_preparation_admission(&original)
                .await
                .is_err(),
            "{fault}"
        );
    }
}

fn replace_prepare(state: &mut crate::memory::State, receipt: &QueueReceipt) {
    let id = state
        .queue
        .get(&(receipt.session_id.clone(), receipt.summary.queue_id.clone()))
        .unwrap()
        .prepare_operation
        .clone()
        .unwrap();
    let key = (receipt.session_id.clone(), id);
    let audit = state.queue_audits.get_mut(&key).unwrap();
    let crate::QueueAction::Prepare { prepared, .. } = &mut audit.request.action else {
        panic!()
    };
    let replacement = crabber_core::MessageId::new();
    prepared
        .keyed
        .request
        .user_message
        .id
        .clone_from(&replacement);
    for part in &mut prepared.keyed.request.user_message.parts {
        part.message_id.clone_from(&replacement);
    }
    let changed = *prepared.clone();
    audit.receipt.semantic_digest = audit.request.fingerprint().unwrap();
    let saved = audit.receipt.clone();
    state
        .operation_receipts
        .insert(key, crate::memory::OperationReceipt::Queue(saved));
    state
        .queue
        .get_mut(&(receipt.session_id.clone(), receipt.summary.queue_id.clone()))
        .unwrap()
        .prepared = Some(changed);
}
