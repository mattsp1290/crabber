use crate::phase_contract::{applied, fixture, mutation, output, resolver};
use crate::{
    PhaseCommand, PhaseError, PhaseResolution, PhaseWriteAuthority, Store, UncertainPhaseChoice,
};
use crabber_core::{ManualClock, MessageId, PartId};
use std::sync::Arc;
use time::OffsetDateTime;

#[tokio::test]
async fn host_resolution_rejects_changed_authoritative_call_binding_without_writes() {
    let (store, _path, _directory) = crate::sqlite::tests::temp_store().await;
    let store = store.with_clock(Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH)));
    for field in ["name", "arguments", "retry_safe"] {
        let (admitted, execution, owner, claim) = fixture(&store).await;
        let (claimed, worker) = applied(
            execution
                .mutate_phase(claim, Some(PhaseWriteAuthority::Attempt(owner)))
                .await
                .unwrap(),
        );
        let (started, _) = applied(
            execution
                .mutate_phase(
                    mutation(
                        claimed.revision,
                        PhaseCommand::Begin {
                            target: claimed.phase.target,
                        },
                    ),
                    Some(PhaseWriteAuthority::Phase(worker.unwrap())),
                )
                .await
                .unwrap(),
        );
        let (uncertain, _) = applied(
            execution
                .mutate_phase(
                    mutation(
                        started.revision,
                        PhaseCommand::MarkStoppedUncertain {
                            target: started.phase.target,
                            claim: resolver(),
                        },
                    ),
                    None,
                )
                .await
                .unwrap(),
        );
        let mut call = store
            .list_unfinished_tool_calls(&admitted.run.id)
            .await
            .unwrap()
            .remove(0);
        match field {
            "name" => call.name = "a different authoritative tool".into(),
            "arguments" => {
                call.arguments = serde_json::json!({"changed":"authoritative normalized input"});
            }
            "retry_safe" => call.retry_safe = !call.retry_safe,
            _ => unreachable!(),
        }
        // Preserve all codec/accounting invariants: this must fail because the
        // immutable call binding changed, rather than a malformed storage row.
        let accounting = crate::sql_snapshot::call_record(&call).unwrap();
        sqlx::query("UPDATE tool_calls SET data=$2,snapshot_parts=$3,snapshot_text=$4,snapshot_bytes=$5 WHERE id=$1")
            .bind(&call.id.0).bind(&accounting.record)
            .bind(accounting.parts).bind(accounting.text).bind(accounting.bytes)
            .execute(&store.writer).await.unwrap();
        assert_eq!(
            store
                .list_unfinished_tool_calls(&admitted.run.id)
                .await
                .unwrap(),
            vec![call]
        );
        let revision = store.session_revision(&admitted.session.id).await.unwrap();
        let request = PhaseResolution {
            session_id: admitted.session.id,
            run_id: admitted.run.id,
            resolution: mutation(
                revision,
                PhaseCommand::ResolveUncertain {
                    target: uncertain.phase.target,
                    claim: resolver(),
                    choice: UncertainPhaseChoice::Success { output: output() },
                },
            ),
            message_id: MessageId::new(),
            part_id: PartId::new(),
            turn_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            duration_ms: 1,
        };
        let before = evidence(&store, &request).await;
        assert_eq!(
            store
                .resolve_uncertain_phase(request.clone())
                .await
                .unwrap_err(),
            PhaseError::SemanticConflict
        );
        assert_eq!(evidence(&store, &request).await, before);
        assert!(
            store
                .lookup_phase_mutation(&request.session_id, &request.resolution.id)
                .await
                .unwrap()
                .is_none()
        );
    }
}

async fn evidence(store: &super::SqliteStore, request: &PhaseResolution) -> serde_json::Value {
    let snapshot_revision: i64 =
        sqlx::query_scalar("SELECT snapshot_revision FROM sessions WHERE id=$1")
            .bind(&request.session_id.0)
            .fetch_one(&store.writer)
            .await
            .unwrap();
    serde_json::json!({
        "run": store.get_run(&request.run_id).await.unwrap(),
        "attempt": store.latest_execution_attempt(&request.run_id).await.unwrap(),
        "phase": store.tool_phase(&request.run_id, &request.resolution.command.target().id).await.unwrap(),
        "calls": store.list_unfinished_tool_calls(&request.run_id).await.unwrap(),
        "messages": store.list_messages(&request.session_id, None).await.unwrap(),
        "events": store.list_events(&request.session_id, None, 1000).await.unwrap(),
        "revision": store.session_revision(&request.session_id).await.unwrap(),
        "snapshot_revision": snapshot_revision,
        "approval": store.approval_snapshot(&request.session_id, &request.run_id).await.unwrap(),
    })
}
