//! Parent prepares a fully quiescent suspension; child receives no old authority.
use super::{Fixture, Mode, applied, reserve};
use crate::{
    AttemptCommand, AttemptConclusion, AttemptFinish, AttemptMutation, RunBoundary, Store,
};
use crabber_core::{ControlId, Usage};
use serde_json::json;

pub(super) async fn pending(store: &dyn Store) -> Fixture {
    let admitted =
        crate::public_state_contract::admit(store, time::OffsetDateTime::now_utc()).await;
    let execution = store.execution(admitted.fence).await.unwrap();
    let (reserved, owner) = applied(
        execution
            .mutate_attempt(
                reserve(store.session_revision(&admitted.session.id).await.unwrap()),
                None,
            )
            .await
            .unwrap(),
    );
    let (started, _) = applied(
        execution
            .mutate_attempt(
                AttemptMutation {
                    id: ControlId::new(),
                    expected_revision: reserved.revision,
                    command: AttemptCommand::Begin {
                        target: reserved.attempt.target,
                    },
                },
                owner.clone(),
            )
            .await
            .unwrap(),
    );
    let target = started.attempt.target.clone();
    execution
        .conclude_attempt(
            AttemptConclusion {
                finish: AttemptMutation {
                    id: ControlId::new(),
                    expected_revision: started.revision,
                    command: AttemptCommand::Finish {
                        target: started.attempt.target,
                        outcome: AttemptFinish::Suspended,
                    },
                },
                boundary: RunBoundary::Suspended {
                    checkpoint: json!({"private":"before-activation"}),
                    usage: Usage::default(),
                },
                created_at: time::OffsetDateTime::now_utc(),
                turn_id: None,
                elapsed: std::time::Duration::ZERO,
            },
            owner,
        )
        .await
        .unwrap();
    let fence = store
        .claim_expired_run(&admitted.run.id, "activation-process")
        .await
        .unwrap();
    let mut request = reserve(store.session_revision(&admitted.session.id).await.unwrap());
    if let AttemptCommand::Reserve { target: next } = &mut request.command {
        next.generation = target.generation.next().unwrap();
    }
    Fixture {
        fence,
        session: admitted.session.id,
        request,
        mode: Mode::Resume,
    }
}
