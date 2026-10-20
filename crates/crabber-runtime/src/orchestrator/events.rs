use super::{
    EventKind, EventPublished, EventRecord, ExecutionStore, MessageId, Orchestrator, Role,
    RunAdmitted, RunId, RunPlan, RunSettled, RunStarted, RuntimeError, SessionId, TurnCompleted,
    TurnId, TurnSnapshot, TurnStarted, Value, event_payload,
};

impl Orchestrator {
    pub(super) fn event(
        &self,
        session_id: &SessionId,
        run_id: &RunId,
        kind: EventKind,
    ) -> EventRecord {
        EventRecord {
            cursor: None,
            session_id: session_id.clone(),
            run_id: run_id.clone(),
            turn_id: None,
            kind,
            payload: Value::Null,
            correlation: None,
            live_only: false,
            created_at: self.clock.now(),
        }
    }

    pub(super) fn turn_event(
        &self,
        session_id: &SessionId,
        run_id: &RunId,
        turn_id: &TurnId,
        kind: EventKind,
    ) -> EventRecord {
        let mut event = self.event(session_id, run_id, kind);
        event.turn_id = Some(turn_id.clone());
        event
    }

    pub(crate) fn live_payload(
        &self,
        snapshot: &TurnSnapshot,
        kind: EventKind,
        payload: impl serde::Serialize,
    ) {
        let mut event = self.turn_event(
            &snapshot.identity.session_id,
            &snapshot.identity.run_id,
            &snapshot.identity.turn_id,
            kind,
        );
        event.live_only = true;
        event.payload = serde_json::to_value(payload).expect("event payload serializes");
        self.observer.emit(&event);
    }

    pub(super) async fn committed_message(
        &self,
        execution: &dyn ExecutionStore,
        plan: &RunPlan,
        snapshot: &TurnSnapshot,
        id: &MessageId,
    ) -> Result<(), RuntimeError> {
        let mut event = self.turn_event(
            &snapshot.identity.session_id,
            &snapshot.identity.run_id,
            &snapshot.identity.turn_id,
            EventKind::MessageCommitted,
        );
        event.payload = serde_json::to_value(event_payload::MessageCommitted {
            message_id: id.clone(),
            role: Role::Assistant,
        })
        .expect("event payload serializes");
        self.publish_event(execution, plan, event).await
    }

    pub(super) async fn emit_durable(
        &self,
        execution: &dyn ExecutionStore,
        session_id: &SessionId,
        run_id: &RunId,
        plan: &RunPlan,
        kind: EventKind,
    ) -> Result<(), RuntimeError> {
        self.publish_event(execution, plan, self.event(session_id, run_id, kind))
            .await
    }

    pub(super) async fn publish_event(
        &self,
        execution: &dyn ExecutionStore,
        plan: &RunPlan,
        event: EventRecord,
    ) -> Result<(), RuntimeError> {
        execution.append_event(event.clone()).await?;
        self.observer.emit(&event);
        let projection = serde_json::to_value(&event).unwrap_or(Value::Null);
        plan.dispatcher
            .notify::<EventPublished>(projection.clone())
            .await;
        match event.kind {
            EventKind::RunAdmitted => plan.dispatcher.notify::<RunAdmitted>(projection).await,
            EventKind::RunStarted => plan.dispatcher.notify::<RunStarted>(projection).await,
            EventKind::TurnStarted => plan.dispatcher.notify::<TurnStarted>(projection).await,
            EventKind::TurnCompleted => plan.dispatcher.notify::<TurnCompleted>(projection).await,
            EventKind::RunSettled => plan.dispatcher.notify::<RunSettled>(projection).await,
            _ => {}
        }
        Ok(())
    }
}
