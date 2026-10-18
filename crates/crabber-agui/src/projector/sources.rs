//! Exact typed public-state projection. Native writers establish authority;
//! this adapter validates scope and serializes the actual selected public data.
use ag_ui_core::event::{
    ActivityDeltaEvent, ActivitySnapshotEvent, Event, StateDeltaEvent, StateSnapshotEvent,
};
use crabber_core::{
    EventKind, EventRecord, PublicPatchOperation, PublicSourceChange, PublicSourceEvent,
    PublicSourceIdentity, PublicStateContents,
};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;

use super::{ProjectionError, Projector, base, json_bytes};

pub(super) struct PublicUndo {
    scope: Option<PublicSourceIdentity>,
    revision: Option<u64>,
    cursor: Option<crabber_core::EventCursor>,
    snapshot: bool,
    contents: Option<Arc<PublicStateContents>>,
    state_seen: bool,
    activities: Vec<ActivityBefore>,
}

struct ActivityBefore {
    id: String,
    activity_type: Option<String>,
    metadata: Option<serde_json::Map<String, Value>>,
}

fn is_public(record: &EventRecord) -> bool {
    matches!(
        record.kind,
        EventKind::PublicStateSnapshot
            | EventKind::PublicStateDelta
            | EventKind::ActivitySnapshot
            | EventKind::ActivityDelta
            | EventKind::PublicPresentation
            | EventKind::InvocationStarted
            | EventKind::InvocationFinished
            | EventKind::InvocationError
            | EventKind::RunResumed
            | EventKind::ContinuationWaiting
    )
}

impl Projector {
    pub(super) fn public_undo(
        &mut self,
        record: &EventRecord,
    ) -> Result<Option<PublicUndo>, ProjectionError> {
        if !is_public(record) {
            return Ok(None);
        }
        if let Err(error) = json_bytes(&record.payload, self.config.max_event_bytes) {
            self.fail(error);
            return Err(error);
        }
        let mut activities = Vec::new();
        let change = record.payload.get("change");
        let id = change
            .and_then(|change| change.get("activity"))
            .and_then(|activity| activity.get("id"))
            .or_else(|| change.and_then(|change| change.get("activity_id")))
            .and_then(Value::as_str);
        if let Some(id) = id.filter(|id| id.len() <= 128) {
            activities.push(ActivityBefore {
                id: id.into(),
                activity_type: self.public_state.activities.get(id).cloned(),
                metadata: self.public_state.activity_metadata.get(id).cloned(),
            });
        }
        if matches!(
            record.kind,
            EventKind::PublicStateSnapshot | EventKind::RunResumed
        ) && let Some(items) = record
            .payload
            .pointer("/contents/activities")
            .and_then(Value::as_array)
        {
            if items.len() > 128 {
                self.fail(ProjectionError::Limit);
                return Err(ProjectionError::Limit);
            }
            for item in items {
                if let Some(id) = item
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|id| id.len() <= 128)
                {
                    activities.push(ActivityBefore {
                        id: id.into(),
                        activity_type: self.public_state.activities.get(id).cloned(),
                        metadata: self.public_state.activity_metadata.get(id).cloned(),
                    });
                }
            }
        }
        Ok(Some(PublicUndo {
            scope: self.binding.source.clone(),
            revision: self.public_state.revision,
            cursor: self.public_state.cursor,
            snapshot: self.public_state.snapshot,
            contents: self.public_state.contents.clone(),
            state_seen: self.public_state.seen,
            activities,
        }))
    }

    pub(super) fn restore_public(&mut self, undo: Option<PublicUndo>) {
        let Some(undo) = undo else {
            return;
        };
        self.binding.source = undo.scope;
        self.public_state.revision = undo.revision;
        self.public_state.cursor = undo.cursor;
        self.public_state.snapshot = undo.snapshot;
        self.public_state.contents = undo.contents;
        self.public_state.seen = undo.state_seen;
        for ActivityBefore {
            id,
            activity_type: old,
            metadata,
        } in undo.activities
        {
            if let Some(old) = old {
                self.public_state.activities.insert(id.clone(), old);
            } else {
                self.public_state.activities.remove(&id);
            }
            if let Some(metadata) = metadata {
                self.public_state.activity_metadata.insert(id, metadata);
            } else {
                self.public_state.activity_metadata.remove(&id);
            }
        }
    }

    pub(super) fn check_public_source(
        &self,
        source: &PublicSourceIdentity,
    ) -> Result<(), ProjectionError> {
        if source.session_id != self.session
            || source.run_id != self.run
            || source.workspace_id.is_empty()
            || source.workspace_id.len() > 512
            || source.workspace_id.chars().any(char::is_control)
            || source.attempt.id.0.is_empty()
            || source.attempt.id.0.len() > 128
            || source.attempt.id.0.chars().any(char::is_control)
        {
            return Err(ProjectionError::Identity);
        }
        if let Some(expected) = &self.binding.source
            && (source.workspace_id != expected.workspace_id || source.attempt != expected.attempt)
        {
            return Err(ProjectionError::Identity);
        }
        if self.resume.attempt.as_ref().is_some_and(|attempt| {
            attempt.id != source.attempt.id || attempt.generation != source.attempt.generation
        }) {
            return Err(ProjectionError::Identity);
        }
        if source
            .call_id
            .as_ref()
            .is_some_and(|id| !opaque(&id.0, 512))
        {
            return Err(ProjectionError::Identity);
        }
        if let Some(invocation) = &source.invocation
            && (!opaque(&invocation.id.0, 128)
                || invocation
                    .parent_invocation
                    .as_ref()
                    .is_some_and(|id| !opaque(&id.0, 128) || *id == invocation.id)
                || invocation.parent_invocation.is_some() != invocation.parent_generation.is_some()
                || invocation
                    .parent_call_id
                    .as_ref()
                    .is_some_and(|id| !opaque(&id.0, 512)))
        {
            return Err(ProjectionError::Identity);
        }
        Ok(())
    }

    pub(super) fn project_public(
        &mut self,
        record: &EventRecord,
    ) -> Result<Option<Vec<Event>>, ProjectionError> {
        if !matches!(
            record.kind,
            EventKind::PublicStateSnapshot
                | EventKind::PublicStateDelta
                | EventKind::ActivitySnapshot
                | EventKind::ActivityDelta
                | EventKind::PublicPresentation
        ) {
            return Ok(None);
        }
        if record.session_id != self.session || record.run_id != self.run {
            return Err(ProjectionError::Identity);
        }
        let mut events = Vec::new();
        let event_base = base(Some(record));
        self.open_exchange(&event_base, &mut events);
        if record.payload.get("version").and_then(Value::as_u64) == Some(1) {
            self.legacy_snapshot(record, &mut events)?;
            return Ok(Some(events));
        }
        let payload: PublicSourceEvent = self.payload(record)?;
        self.check_public_envelope(&payload)?;
        let event_base = if matches!(payload.change, PublicSourceChange::Presentation { .. }) {
            self.attributed_base(&payload.source, event_base)?
        } else {
            self.global_public_base(&payload.source, event_base)?
        };
        self.validate_public_transition(record, &payload)?;
        self.project_public_change(record, &payload, event_base, &mut events)?;
        self.binding.source.get_or_insert(payload.source);
        self.public_state.cursor = record.cursor;
        self.public_state.snapshot = record.kind == EventKind::PublicStateSnapshot;
        self.public_state.contents = Some(Arc::new(payload.contents));
        Ok(Some(events))
    }

    fn check_public_envelope(&self, payload: &PublicSourceEvent) -> Result<(), ProjectionError> {
        if payload.version != 2 {
            return Err(ProjectionError::Malformed);
        }
        self.check_public_source(&payload.source)?;
        if self.child_exchange()
            && (!self.binding.child_started
                || self.binding.child_terminal.is_some()
                || payload.source.invocation.is_none())
        {
            return Err(ProjectionError::Order);
        }
        Ok(())
    }

    fn validate_public_transition(
        &mut self,
        record: &EventRecord,
        payload: &PublicSourceEvent,
    ) -> Result<(), ProjectionError> {
        self.check_public_revision(record, payload.revision.get(), &payload.contents)?;
        payload
            .change
            .validate()
            .map_err(|_| ProjectionError::Malformed)?;
        let delta = matches!(
            payload.change,
            PublicSourceChange::StateDelta { .. } | PublicSourceChange::ActivityDelta { .. }
        );
        if delta {
            let before = payload.before.as_ref().ok_or(ProjectionError::Malformed)?;
            if !self.public_state.snapshot
                || payload.before_event_cursor.is_none()
                || payload.before_event_cursor != self.public_state.cursor
                || self.public_state.contents.as_deref() != Some(before)
            {
                return Err(ProjectionError::Order);
            }
            if payload
                .change
                .apply(before)
                .map_err(|_| ProjectionError::Malformed)?
                != payload.contents
            {
                return Err(ProjectionError::Malformed);
            }
        } else if payload.before.is_some() || payload.before_event_cursor.is_some() {
            return Err(ProjectionError::Malformed);
        }
        Ok(())
    }

    fn project_public_change(
        &mut self,
        record: &EventRecord,
        payload: &PublicSourceEvent,
        event_base: ag_ui_core::event::BaseEvent,
        events: &mut Vec<Event>,
    ) -> Result<(), ProjectionError> {
        match (&record.kind, &payload.change) {
            (EventKind::PublicStateSnapshot, PublicSourceChange::StateSnapshot { state }) => {
                if state != payload.contents.state() {
                    return Err(ProjectionError::Malformed);
                }
                events.push(Event::StateSnapshot(StateSnapshotEvent {
                    base: event_base.clone(),
                    snapshot: state.clone(),
                }));
                self.complete_activities(&event_base, &payload.contents, events)?;
                self.public_state.seen = true;
            }
            (EventKind::PublicStateDelta, PublicSourceChange::StateDelta { delta }) => {
                if !self.public_state.seen {
                    return Err(ProjectionError::Order);
                }
                events.push(Event::StateDelta(StateDeltaEvent {
                    base: event_base,
                    delta: patch(delta)?,
                }));
            }
            (EventKind::ActivitySnapshot, PublicSourceChange::ActivitySnapshot { activity }) => {
                if !payload
                    .contents
                    .activities()
                    .iter()
                    .any(|actual| actual == activity)
                {
                    return Err(ProjectionError::Malformed);
                }
                events.push(self.activity_snapshot(event_base, activity)?);
            }
            (
                EventKind::ActivityDelta,
                PublicSourceChange::ActivityDelta {
                    activity_id,
                    activity_type,
                    delta,
                },
            ) => {
                if self.public_state.activities.get(activity_id) != Some(activity_type) {
                    return Err(ProjectionError::Order);
                }
                if !payload.contents.activities().iter().any(|actual| {
                    actual.id() == activity_id
                        && actual.activity_type() == activity_type
                        && actual.content().is_object()
                }) {
                    return Err(ProjectionError::Malformed);
                }
                if let Some(metadata) = &event_base.metadata {
                    self.public_state
                        .activity_metadata
                        .insert(activity_id.clone(), metadata.clone());
                }
                events.push(Event::ActivityDelta(ActivityDeltaEvent {
                    base: event_base,
                    message_id: activity_id.clone().into(),
                    activity_type: activity_type.clone(),
                    patch: patch(delta)?,
                }));
            }
            (EventKind::PublicPresentation, PublicSourceChange::Presentation { presentation }) => {
                if self
                    .public_state
                    .contents
                    .as_deref()
                    .is_some_and(|before| before != &payload.contents)
                {
                    return Err(ProjectionError::Malformed);
                }
                events.extend(self.project_presentation(
                    &payload.source,
                    presentation,
                    event_base,
                )?);
            }
            _ => return Err(ProjectionError::Malformed),
        }
        Ok(())
    }

    fn check_public_revision(
        &mut self,
        record: &EventRecord,
        revision: u64,
        contents: &PublicStateContents,
    ) -> Result<(), ProjectionError> {
        if self
            .public_state
            .revision
            .is_some_and(|previous| revision < previous)
        {
            return Err(ProjectionError::Order);
        }
        if self.public_state.revision == Some(revision)
            && (record.kind != EventKind::PublicStateSnapshot
                || self
                    .public_state
                    .contents
                    .as_deref()
                    .is_some_and(|current| current != contents)
                || record
                    .cursor
                    .zip(self.public_state.cursor)
                    .is_none_or(|(next, old)| next <= old))
        {
            return Err(ProjectionError::Order);
        }
        if record
            .cursor
            .zip(self.public_state.cursor)
            .is_some_and(|(next, old)| next <= old)
        {
            return Err(ProjectionError::Order);
        }
        self.public_state.revision = Some(revision);
        Ok(())
    }

    fn activity_snapshot(
        &mut self,
        event_base: ag_ui_core::event::BaseEvent,
        activity: &crabber_core::PublicActivity,
    ) -> Result<Event, ProjectionError> {
        let content = activity
            .content()
            .as_object()
            .ok_or(ProjectionError::Malformed)?;
        if self.activities.contains(activity.id())
            || self
                .messages
                .contains_key(&crabber_core::MessageId::from(activity.id()))
            || self
                .result_ids
                .contains(&crabber_core::MessageId::from(activity.id()))
            || self
                .ui_messages
                .contains_key(&crabber_core::MessageId::from(activity.id()))
        {
            return Err(ProjectionError::Identity);
        }
        if !self.public_state.activities.contains_key(activity.id())
            && self.public_state.activities.len() + self.activities.len()
                >= self.config.max_messages
        {
            return Err(ProjectionError::Limit);
        }
        self.public_state
            .activities
            .insert(activity.id().into(), activity.activity_type().into());
        if let Some(metadata) = &event_base.metadata {
            self.public_state
                .activity_metadata
                .insert(activity.id().into(), metadata.clone());
        } else {
            self.public_state.activity_metadata.remove(activity.id());
        }
        Ok(Event::ActivitySnapshot(ActivitySnapshotEvent {
            base: event_base,
            message_id: activity.id().into(),
            activity_type: activity.activity_type().into(),
            content: content.clone(),
            replace: Some(true),
        }))
    }

    fn legacy_snapshot(
        &mut self,
        record: &EventRecord,
        events: &mut Vec<Event>,
    ) -> Result<(), ProjectionError> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Snapshot {
            version: u32,
            revision: crabber_core::SessionRevision,
            contents: PublicStateContents,
        }
        if self.child_exchange() {
            return Err(ProjectionError::Identity);
        }
        if record.kind != EventKind::PublicStateSnapshot {
            return Err(ProjectionError::Malformed);
        }
        let snapshot: Snapshot = self.payload(record)?;
        if snapshot.version != 1 {
            return Err(ProjectionError::Malformed);
        }
        self.check_public_revision(record, snapshot.revision.get(), &snapshot.contents)?;
        events.push(Event::StateSnapshot(StateSnapshotEvent {
            base: base(Some(record)),
            snapshot: snapshot.contents.state().clone(),
        }));
        self.complete_activities(&base(Some(record)), &snapshot.contents, events)?;
        self.public_state.cursor = record.cursor;
        self.public_state.snapshot = true;
        self.public_state.contents = Some(Arc::new(snapshot.contents));
        self.public_state.seen = true;
        Ok(())
    }

    pub(super) fn complete_activities(
        &mut self,
        event_base: &ag_ui_core::event::BaseEvent,
        contents: &PublicStateContents,
        events: &mut Vec<Event>,
    ) -> Result<(), ProjectionError> {
        if self.public_state.activities.keys().any(|id| {
            !contents
                .activities()
                .iter()
                .any(|activity| activity.id() == id)
        }) {
            return Err(ProjectionError::Malformed);
        }
        for activity in contents.activities() {
            events.push(self.activity_snapshot(event_base.clone(), activity)?);
        }
        Ok(())
    }

    fn global_public_base(
        &self,
        source: &PublicSourceIdentity,
        base: ag_ui_core::event::BaseEvent,
    ) -> Result<ag_ui_core::event::BaseEvent, ProjectionError> {
        let mut base = self.attributed_base(source, base)?;
        base.subagent_run_id = None;
        base.metadata = Some(serde_json::Map::from_iter([(
            "crabber".into(),
            serde_json::json!({"source":{
                "runId":source.run_id,"attempt":{"id":source.attempt.id,"generation":source.attempt.generation},
                "callId":source.call_id,"invocation":source.invocation
            }}),
        )]));
        Ok(base)
    }
}

fn patch(
    operations: &[PublicPatchOperation],
) -> Result<Vec<ag_ui_core::types::JsonPatchOperation>, ProjectionError> {
    operations
        .iter()
        .map(|operation| {
            serde_json::from_value(Value::Object(operation.members().clone()))
                .map_err(|_| ProjectionError::Malformed)
        })
        .collect()
}
fn opaque(id: &str, max: usize) -> bool {
    !id.is_empty() && id.len() <= max && !id.chars().any(char::is_control)
}
