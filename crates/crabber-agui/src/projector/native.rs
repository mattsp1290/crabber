//! Allowlisted native attempt presentation; this is not native authentication.
use crabber_core::{EventKind, EventRecord};
use serde_json::{Map, Value, json};

use super::ProjectionError;

pub(super) struct AttemptActivity {
    pub message_id: String,
    pub activity_type: &'static str,
    pub content: Map<String, Value>,
}

pub(super) fn activity_id(record: &EventRecord) -> Option<String> {
    if record.kind == EventKind::ToolPhaseChanged {
        let target = record.payload.pointer("/phase/target")?;
        return target_id("phase", target);
    }
    if matches!(
        record.kind,
        EventKind::RunControlChanged
            | EventKind::InteractionChanged
            | EventKind::ContinuationChanged
    ) {
        let id = if record.kind == EventKind::RunControlChanged {
            record.payload.pointer("/receipt/operation_id")
        } else {
            record.payload.get("operation_id")
        }?
        .as_str()?;
        let family = match record.kind {
            EventKind::RunControlChanged => "run-control",
            EventKind::InteractionChanged => "interaction",
            _ if record
                .payload
                .get("kind")
                .is_some_and(|kind| kind == "tool_continuation") =>
            {
                "tool-continuation"
            }
            _ => "approval-continuation",
        };
        return valid_id(id).then(|| format!("crabber:{family}:{id}"));
    }
    if record.kind != EventKind::ExecutionAttemptChanged {
        return None;
    }
    let attempt = record.payload.get("attempt")?;
    let target = attempt.get("target")?;
    target_id("attempt", target)
}

fn target_id(kind: &str, target: &Value) -> Option<String> {
    let id = target.get("id")?.as_str()?;
    let generation = target.get("generation")?.as_u64()?;
    if !valid_id(id) || generation == 0 {
        return None;
    }
    Some(format!("crabber:{kind}:{id}:{generation}"))
}

fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
}

pub(super) fn attempt(record: &EventRecord) -> Result<AttemptActivity, ProjectionError> {
    let version = record
        .payload
        .get("version")
        .and_then(Value::as_u64)
        .filter(|version| (1..=5).contains(version))
        .ok_or(ProjectionError::Malformed)?;
    let snapshot = record
        .payload
        .get("attempt")
        .and_then(Value::as_object)
        .ok_or(ProjectionError::Malformed)?;
    if snapshot.len() != 5
        || snapshot.get("session_id").and_then(Value::as_str) != Some(&record.session_id.0)
        || snapshot.get("run_id").and_then(Value::as_str) != Some(&record.run_id.0)
    {
        return Err(ProjectionError::Identity);
    }
    let target = snapshot
        .get("target")
        .and_then(Value::as_object)
        .filter(|target| target.len() == 2)
        .ok_or(ProjectionError::Malformed)?;
    let message_id = activity_id(record).ok_or(ProjectionError::Malformed)?;
    let state = snapshot
        .get("state")
        .and_then(Value::as_str)
        .filter(|state| {
            matches!(
                *state,
                "Unstarted"
                    | "Started"
                    | "Suspended"
                    | "Completed"
                    | "Failed"
                    | "Interrupted"
                    | "OutcomeUnknown"
            )
        })
        .ok_or(ProjectionError::Malformed)?;
    let cleanup = snapshot
        .get("cleanup")
        .and_then(Value::as_str)
        .filter(|cleanup| matches!(*cleanup, "Pending" | "Complete" | "Incomplete"))
        .ok_or(ProjectionError::Malformed)?;
    // Version 1 includes a public command target. Compound terminal versions
    // contain private audits/settlement: only their common snapshot is emitted.
    if version == 1
        && record
            .payload
            .pointer("/command/target")
            .and_then(Value::as_object)
            != Some(target)
    {
        return Err(ProjectionError::Identity);
    }
    let content = json!({"target":target, "state":state, "cleanup":cleanup});
    let Value::Object(content) = content else {
        unreachable!()
    };
    Ok(AttemptActivity {
        message_id,
        activity_type: "crabber.execution_attempt",
        content,
    })
}

pub(super) fn project(record: &EventRecord) -> Result<AttemptActivity, ProjectionError> {
    match record.kind {
        EventKind::ExecutionAttemptChanged => attempt(record),
        EventKind::ToolPhaseChanged => phase(record),
        EventKind::RunControlChanged => control(record),
        EventKind::InteractionChanged | EventKind::ContinuationChanged => continuation(record),
        _ => Err(ProjectionError::Malformed),
    }
}

fn phase(record: &EventRecord) -> Result<AttemptActivity, ProjectionError> {
    if record.payload.get("version").and_then(Value::as_u64) != Some(1) {
        return Err(ProjectionError::Malformed);
    }
    let snapshot = record
        .payload
        .get("phase")
        .and_then(Value::as_object)
        .ok_or(ProjectionError::Malformed)?;
    if snapshot.get("session_id").and_then(Value::as_str) != Some(&record.session_id.0)
        || snapshot.get("run_id").and_then(Value::as_str) != Some(&record.run_id.0)
    {
        return Err(ProjectionError::Identity);
    }
    let target = snapshot
        .get("target")
        .and_then(Value::as_object)
        .filter(|target| target.len() == 4)
        .ok_or(ProjectionError::Malformed)?;
    let message_id = activity_id(record).ok_or(ProjectionError::Malformed)?;
    let call = target
        .get("call_id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty() && id.len() <= 512 && !id.chars().any(char::is_control))
        .ok_or(ProjectionError::Malformed)?;
    if target
        .get("attempt")
        .and_then(|target| target_id("attempt", target))
        .is_none()
    {
        return Err(ProjectionError::Malformed);
    }
    let state = snapshot
        .get("state")
        .and_then(Value::as_str)
        .filter(|state| {
            matches!(
                *state,
                "Claimed" | "Started" | "Suspended" | "Completed" | "Failed" | "Uncertain"
            )
        })
        .ok_or(ProjectionError::Malformed)?;
    let quiescence = snapshot
        .get("quiescence")
        .and_then(Value::as_str)
        .filter(|state| matches!(*state, "Pending" | "Complete" | "Incomplete"))
        .ok_or(ProjectionError::Malformed)?;
    let Value::Object(content) = json!({"target":{"id":target["id"],"generation":target["generation"],"call_id":call},"state":state,"quiescence":quiescence})
    else {
        unreachable!()
    };
    Ok(AttemptActivity {
        message_id,
        activity_type: "crabber.tool_phase",
        content,
    })
}

fn control(record: &EventRecord) -> Result<AttemptActivity, ProjectionError> {
    let target = record
        .payload
        .pointer("/receipt/target")
        .ok_or(ProjectionError::Malformed)?;
    if target.get("session_id").and_then(Value::as_str) != Some(&record.session_id.0)
        || target.get("run_id").and_then(Value::as_str) != Some(&record.run_id.0)
    {
        return Err(ProjectionError::Identity);
    }
    let kind = record
        .payload
        .pointer("/receipt/kind")
        .and_then(Value::as_str)
        .filter(|kind| matches!(*kind, "Cancel" | "Steer"))
        .ok_or(ProjectionError::Malformed)?;
    let status = record
        .payload
        .get("status")
        .ok_or(ProjectionError::Malformed)?;
    let name = if let Some(status) = status.as_str() {
        if !matches!(status, "Accepted" | "Observed") {
            return Err(ProjectionError::Malformed);
        }
        status
    } else {
        let object = status
            .as_object()
            .filter(|object| object.len() == 1)
            .ok_or(ProjectionError::Malformed)?;
        let name = object.keys().next().ok_or(ProjectionError::Malformed)?;
        if !matches!(
            name.as_str(),
            "Incorporated" | "UnappliedTerminal" | "TerminalCommitted"
        ) {
            return Err(ProjectionError::Malformed);
        }
        name
    };
    let Value::Object(content) = json!({"kind":kind,"status":name}) else {
        unreachable!()
    };
    Ok(AttemptActivity {
        message_id: activity_id(record).ok_or(ProjectionError::Malformed)?,
        activity_type: "crabber.run_control",
        content,
    })
}

// These operational audits do not carry a public prompt, wait permission or a
// resume authorization. Publish only the closed operation status; the separate
// native continuation summary supplies declared public presentation.
fn continuation(record: &EventRecord) -> Result<AttemptActivity, ProjectionError> {
    let version = record
        .payload
        .get("version")
        .and_then(Value::as_u64)
        .ok_or(ProjectionError::Malformed)?;
    let tool = record.payload.get("kind");
    if !(1..=2).contains(&version)
        || tool.is_some_and(|kind| kind != "tool_continuation")
        || (tool.is_some() && version != 1)
    {
        return Err(ProjectionError::Malformed);
    }
    let status = record
        .payload
        .pointer("/outcome/operation")
        .and_then(Value::as_str)
        .ok_or(ProjectionError::Malformed)?;
    if !matches!(
        status,
        "suspended"
            | "answered"
            | "claimed"
            | "reclaimed_unstarted"
            | "terminated"
            | "frontend_claimed"
            | "frontend_reclaimed_unstarted"
    ) || (record.kind == EventKind::InteractionChanged && status != "answered")
    {
        return Err(ProjectionError::Malformed);
    }
    let Value::Object(content) = json!({"status":status}) else {
        unreachable!()
    };
    Ok(AttemptActivity {
        message_id: activity_id(record).ok_or(ProjectionError::Malformed)?,
        activity_type: "crabber.continuation_control",
        content,
    })
}
