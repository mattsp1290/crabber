use super::{EventCursor, MemoryStore, State, StoreError};
use crate::snapshot_profile::{Profile, profile_boundary, profile_json};
use crate::{
    SnapshotContinuation, SnapshotLimit, SnapshotOutcome, SnapshotPage, SnapshotRequest,
    SnapshotUsage,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[path = "memory_snapshot/record.rs"]
mod record;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Boundary {
    session_hash: String,
    messages: usize,
    calls: usize,
    imported: usize,
    provenance: usize,
    revision: u64,
    public_revision: crabber_core::SessionRevision,
    high_water: EventCursor,
    message_offset: usize,
    call_offset: usize,
    imported_offset: usize,
    provenance_offset: usize,
}

fn digest(parts: &[&str]) -> String {
    let mut hash = Sha256::new();
    for part in parts {
        hash.update(part.len().to_le_bytes());
        hash.update(part.as_bytes());
    }
    format!("{:x}", hash.finalize())
}

impl MemoryStore {
    fn snapshot_token(
        &self,
        boundary: &Boundary,
        profile: Profile,
    ) -> Result<SnapshotContinuation, StoreError> {
        let json = profile_json(boundary, profile)?;
        let signature = digest(&[&self.snapshot_key, &json]);
        Ok(SnapshotContinuation(format!("v2:{signature}:{json}")))
    }

    fn snapshot_boundary(
        &self,
        state: &State,
        request: &SnapshotRequest,
        profile: Profile,
    ) -> Result<Boundary, StoreError> {
        let session_hash = digest(&[&request.session_id.0]);
        let revision = state
            .snapshot_revisions
            .get(&request.session_id)
            .copied()
            .unwrap_or(0);
        let boundary = if let Some(token) = &request.continuation {
            if token.0.len() > profile.token_cap() {
                return Err(StoreError::Validation(
                    "invalid snapshot continuation".into(),
                ));
            }
            let (signature, json) = token
                .0
                .strip_prefix("v2:")
                .ok_or_else(|| StoreError::Validation("unsupported snapshot continuation".into()))?
                .split_once(':')
                .ok_or_else(|| StoreError::Validation("invalid snapshot continuation".into()))?;
            if signature != digest(&[&self.snapshot_key, json]) {
                return Err(StoreError::Validation(
                    "invalid snapshot continuation".into(),
                ));
            }
            let boundary: Boundary = profile_boundary(json, profile)?;
            if boundary.session_hash != session_hash {
                return Err(StoreError::Validation(
                    "snapshot belongs to another session".into(),
                ));
            }
            boundary
        } else {
            Boundary {
                session_hash,
                messages: state.messages.for_session(&request.session_id).len(),
                calls: state.call_order.for_session(&request.session_id).len(),
                imported: state.imported_calls.for_session(&request.session_id).len(),
                provenance: state
                    .imported_provenance
                    .for_session(&request.session_id)
                    .len(),
                revision,
                public_revision: state
                    .public_revisions
                    .get(&request.session_id)
                    .copied()
                    .unwrap_or(crabber_core::SessionRevision::INITIAL),
                high_water: state
                    .events
                    .iter()
                    .rev()
                    .find(|e| e.session_id == request.session_id)
                    .and_then(|e| e.cursor)
                    .unwrap_or(EventCursor(0)),
                message_offset: 0,
                call_offset: 0,
                imported_offset: 0,
                provenance_offset: 0,
            }
        };
        Ok(boundary)
    }

    pub(super) fn read_snapshot(
        &self,
        state: &State,
        request: &SnapshotRequest,
    ) -> Result<SnapshotOutcome, StoreError> {
        self.read_snapshot_profile(state, request, Profile::Raw)
    }
    pub(super) fn read_snapshot_profile(
        &self,
        state: &State,
        request: &SnapshotRequest,
        profile: Profile,
    ) -> Result<SnapshotOutcome, StoreError> {
        if !state.sessions.contains_key(&request.session_id) {
            return Err(StoreError::NotFound);
        }
        let mut boundary = self.snapshot_boundary(state, request, profile)?;
        if boundary.revision
            != state
                .snapshot_revisions
                .get(&request.session_id)
                .copied()
                .unwrap_or(0)
        {
            return Ok(SnapshotOutcome::Invalidated {
                high_water: boundary.high_water,
            });
        }
        let mut page = SnapshotPage {
            captured_revision: boundary.public_revision,
            high_water: boundary.high_water,
            messages: Vec::new(),
            tool_calls: Vec::new(),
            imported_calls: Vec::new(),
            imported_provenance: Vec::new(),
            usage: SnapshotUsage::default(),
            continuation: None,
        };
        loop {
            let Some(next) = record::next(state, &request.session_id, &boundary)? else {
                return Ok(SnapshotOutcome::Page(page));
            };
            match next.measure(page.usage, request.limits) {
                Ok(usage) => {
                    next.append(&mut page, &mut boundary);
                    page.usage = usage;
                }
                Err(limit) => {
                    let continuation = self.snapshot_token(&boundary, profile)?;
                    if page.usage.encoded_bytes == 0 {
                        return Ok(SnapshotOutcome::Limited {
                            high_water: boundary.high_water,
                            limit,
                            continuation,
                        });
                    }
                    page.continuation = Some(continuation);
                    return Ok(SnapshotOutcome::Page(page));
                }
            }
        }
    }
}

#[cfg(test)]
mod snapshot_tests {
    use super::*;
    use crate::{SnapshotLimits, SnapshotOutcome};
    use crabber_core::{
        ContentBlock, Message, MessageId, Part, PartId, PartKind, Role, Session, SessionId,
    };
    use time::OffsetDateTime;

    fn message(session: &SessionId, id: &str, text: &str) -> Message {
        let id = MessageId::from(id);
        Message {
            id: id.clone(),
            session_id: session.clone(),
            run_id: None,
            role: Role::Assistant,
            parent_id: None,
            parts: vec![Part {
                id: PartId::new(),
                message_id: id,
                ordinal: 0,
                kind: PartKind::AssistantText,
                content: ContentBlock::Text { text: text.into() },
            }],
            created_at: OffsetDateTime::UNIX_EPOCH,
        }
    }

    fn seeded() -> (MemoryStore, SessionId) {
        let store = MemoryStore::new();
        let session = SessionId::from("snapshot-session");
        store.state.lock().unwrap().sessions.insert(
            session.clone(),
            Session {
                id: session.clone(),
                workspace_id: "fixture".into(),
                directory: "fixture".into(),
                title: "fixture".into(),
                created_at: OffsetDateTime::UNIX_EPOCH,
                updated_at: OffsetDateTime::UNIX_EPOCH,
            },
        );
        (store, session)
    }

    fn request(session: &SessionId) -> SnapshotRequest {
        SnapshotRequest {
            session_id: session.clone(),
            limits: SnapshotLimits {
                messages: 1,
                tool_calls: 1,
                parts: 1,
                text_bytes: 100,
                encoded_bytes: 4096,
            },
            continuation: None,
        }
    }

    #[test]
    fn large_history_allocation_is_bounded_before_cloning() {
        let (store, session) = seeded();
        let mut state = store.state.lock().unwrap();
        state
            .messages
            .push(session.clone(), message(&session, "small", "small"));
        let text = "x".repeat(32_768);
        for i in 0..2048 {
            state.messages.push(
                session.clone(),
                message(&session, &format!("large-{i}"), &text),
            );
        }
        let mut continuation = None;
        let allocation = allocation_counter::measure(|| {
            let SnapshotOutcome::Page(page) =
                store.read_snapshot(&state, &request(&session)).unwrap()
            else {
                panic!("page")
            };
            assert_eq!(page.messages.len(), 1);
            continuation = page.continuation;
        });
        assert!(allocation.bytes_total < 16_384, "{allocation:?}");
        let mut next = request(&session);
        next.continuation = continuation;
        next.limits.text_bytes = usize::MAX;
        let allocation = allocation_counter::measure(|| {
            assert!(matches!(
                store.read_snapshot(&state, &next).unwrap(),
                SnapshotOutcome::Limited {
                    limit: SnapshotLimit::EncodedBytes,
                    ..
                }
            ));
        });
        assert!(
            allocation.bytes_total < 16_384,
            "oversized record cloned: {allocation:?}"
        );
    }

    #[test]
    fn imported_carriers_are_bounded_before_cloning() {
        use crabber_core::{
            ImportedCallSource, ImportedMessageProvenance, ImportedToolCall, ToolResult,
            ToolResultStatus,
        };
        for provenance in [false, true] {
            let (store, session) = seeded();
            let mut state = store.state.lock().unwrap();
            if provenance {
                state.imported_provenance.push(
                    session.clone(),
                    ImportedMessageProvenance {
                        session_id: session.clone(),
                        message_id: "target-message".into(),
                        source_session_id: "source".into(),
                        source_revision: crabber_core::SessionRevision::INITIAL,
                        source_message_id: "source-message".into(),
                        source_run_id: None,
                        parts: (0..8192)
                            .map(|n| (PartId(format!("target-{n}")), PartId("s".repeat(512))))
                            .collect(),
                    },
                );
            } else {
                state.imported_calls.push(
                    session.clone(),
                    ImportedToolCall {
                        session_id: session.clone(),
                        id: "imported".into(),
                        assistant_message_id: "assistant".into(),
                        result_message_id: "result".into(),
                        name: "tool".into(),
                        arguments: serde_json::json!({"large":"x".repeat(1_048_576)}),
                        result: ToolResult {
                            status: ToolResultStatus::Completed,
                            content: vec![],
                        },
                        source_session_id: "source".into(),
                        source_revision: crabber_core::SessionRevision::INITIAL,
                        source_call_id: "source-call".into(),
                        source_kind: ImportedCallSource::NativeExecution,
                        source_run_id: Some("source-run".into()),
                    },
                );
            }
            let allocation = allocation_counter::measure(|| {
                assert!(matches!(
                    store.read_snapshot(&state, &request(&session)).unwrap(),
                    SnapshotOutcome::Limited {
                        limit: SnapshotLimit::EncodedBytes,
                        ..
                    }
                ));
            });
            assert!(
                allocation.bytes_total < 16_384,
                "imported carrier cloned: {allocation:?}"
            );
        }
    }

    #[test]
    fn exact_caps_and_retry_do_not_skip_record() {
        let (store, session) = seeded();
        let mut state = store.state.lock().unwrap();
        let first = message(&session, "one", "é");
        let bytes = serde_json::to_vec(&first).unwrap().len();
        state.messages.push(session.clone(), first.clone());
        for (limit, expected) in [
            (
                SnapshotLimits {
                    messages: 0,
                    ..request(&session).limits
                },
                SnapshotLimit::Messages,
            ),
            (
                SnapshotLimits {
                    parts: 0,
                    ..request(&session).limits
                },
                SnapshotLimit::Parts,
            ),
            (
                SnapshotLimits {
                    text_bytes: 1,
                    ..request(&session).limits
                },
                SnapshotLimit::TextBytes,
            ),
            (
                SnapshotLimits {
                    encoded_bytes: bytes - 1,
                    ..request(&session).limits
                },
                SnapshotLimit::EncodedBytes,
            ),
        ] {
            let mut request = request(&session);
            request.limits = limit;
            let SnapshotOutcome::Limited {
                limit,
                continuation,
                ..
            } = store.read_snapshot(&state, &request).unwrap()
            else {
                panic!("limit")
            };
            assert_eq!(limit, expected);
            let mut retry = self::request(&session);
            retry.limits.text_bytes = 2;
            retry.limits.encoded_bytes = bytes;
            retry.continuation = Some(continuation);
            let SnapshotOutcome::Page(page) = store.read_snapshot(&state, &retry).unwrap() else {
                panic!("retry page")
            };
            assert_eq!(page.messages, vec![first.clone()]);
            assert_eq!(page.usage.encoded_bytes, bytes);
            assert!(page.continuation.is_none());
        }
    }

    #[test]
    fn mutable_record_invalidates_original_boundary_and_tokens_are_authenticated() {
        let (store, session) = seeded();
        let mut state = store.state.lock().unwrap();
        state
            .messages
            .push(session.clone(), message(&session, "one", "one"));
        state
            .messages
            .push(session.clone(), message(&session, "two", "two"));
        let SnapshotOutcome::Page(page) = store.read_snapshot(&state, &request(&session)).unwrap()
        else {
            panic!("page")
        };
        let mut next = request(&session);
        next.continuation = page.continuation;
        let mut tampered = next.clone();
        tampered.continuation.as_mut().unwrap().0.push(' ');
        assert!(matches!(
            store.read_snapshot(&state, &tampered),
            Err(StoreError::Validation(_))
        ));
        state.snapshot_revisions.insert(session.clone(), 1);
        state.messages.iter_mut().next().unwrap().parts[0].content = ContentBlock::Text {
            text: "changed".into(),
        };
        assert!(
            matches!(store.read_snapshot(&state, &next).unwrap(), SnapshotOutcome::Invalidated { high_water } if high_water == page.high_water)
        );
        let SnapshotOutcome::Page(restarted) =
            store.read_snapshot(&state, &request(&session)).unwrap()
        else {
            panic!("restart")
        };
        assert_eq!(
            restarted.messages[0],
            state.messages.for_session(&session)[0]
        );
    }
    #[test]
    fn aggregate_history_profile_cannot_cross_raw_boundaries() {
        let (store, session) = seeded();
        let mut state = store.state.lock().unwrap();
        state
            .messages
            .push(session.clone(), message(&session, "one", "one"));
        state
            .messages
            .push(session.clone(), message(&session, "two", "two"));
        let req = request(&session);
        let SnapshotOutcome::Page(first) = store
            .read_snapshot_profile(&state, &req, Profile::AggregateV2)
            .unwrap()
        else {
            panic!("page")
        };
        let mut next = req.clone();
        next.continuation = first.continuation.clone();
        assert!(store.read_snapshot(&state, &next).is_err());
        let SnapshotOutcome::Page(second) = store
            .read_snapshot_profile(&state, &next, Profile::AggregateV2)
            .unwrap()
        else {
            panic!("page")
        };
        assert_eq!(second.messages[0].id.0, "two");
        let SnapshotOutcome::Page(raw) = store.read_snapshot(&state, &req).unwrap() else {
            panic!("page")
        };
        next.continuation = raw.continuation;
        assert!(
            store
                .read_snapshot_profile(&state, &next, Profile::AggregateV2)
                .is_err()
        );
    }
}

#[cfg(test)]
#[tokio::test]
async fn captured_public_revision_survives_metadata_changes_and_rejects_tampering() {
    crate::management_contract::captured_history_revision(&MemoryStore::new()).await;
}
