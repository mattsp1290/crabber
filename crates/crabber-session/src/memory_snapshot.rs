use super::{EventCursor, MemoryStore, State, StoreError};
use crate::{
    SnapshotContinuation, SnapshotLimit, SnapshotOutcome, SnapshotPage, SnapshotRequest,
    SnapshotUsage,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Serialize, Deserialize)]
struct Boundary {
    session_hash: String,
    messages: usize,
    calls: usize,
    revision: u64,
    high_water: EventCursor,
    message_offset: usize,
    call_offset: usize,
}

impl Boundary {
    fn skip_unrelated(&mut self, state: &State, session: &crabber_core::SessionId) {
        // Skip unrelated sessions without allocating an index or history vector.
        while self.message_offset < self.messages
            && state.messages[self.message_offset].session_id != *session
        {
            self.message_offset += 1;
        }
        while self.call_offset < self.calls {
            let call = &state.calls[&state.call_order[self.call_offset]];
            if state.runs[&call.run_id].session_id == *session {
                break;
            }
            self.call_offset += 1;
        }
    }
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
    fn snapshot_token(&self, boundary: &Boundary) -> Result<SnapshotContinuation, StoreError> {
        let json = serde_json::to_string(boundary)
            .map_err(|_| StoreError::Validation("invalid snapshot boundary".into()))?;
        let signature = digest(&[&self.snapshot_key, &json]);
        Ok(SnapshotContinuation(format!("{signature}:{json}")))
    }

    fn snapshot_boundary(
        &self,
        state: &State,
        request: &SnapshotRequest,
    ) -> Result<Boundary, StoreError> {
        let session_hash = digest(&[&request.session_id.0]);
        let revision = state
            .snapshot_revisions
            .get(&request.session_id)
            .copied()
            .unwrap_or(0);
        let boundary = if let Some(token) = &request.continuation {
            if token.0.len() > 2048 {
                return Err(StoreError::Validation(
                    "invalid snapshot continuation".into(),
                ));
            }
            let (signature, json) = token
                .0
                .split_once(':')
                .ok_or_else(|| StoreError::Validation("invalid snapshot continuation".into()))?;
            if signature != digest(&[&self.snapshot_key, json]) {
                return Err(StoreError::Validation(
                    "invalid snapshot continuation".into(),
                ));
            }
            let boundary: Boundary = serde_json::from_str(json)
                .map_err(|_| StoreError::Validation("invalid snapshot continuation".into()))?;
            if boundary.session_hash != session_hash {
                return Err(StoreError::Validation(
                    "snapshot belongs to another session".into(),
                ));
            }
            boundary
        } else {
            Boundary {
                session_hash,
                messages: state.messages.len(),
                calls: state.call_order.len(),
                revision,
                high_water: state
                    .events
                    .iter()
                    .rev()
                    .find(|e| e.session_id == request.session_id)
                    .and_then(|e| e.cursor)
                    .unwrap_or(EventCursor(0)),
                message_offset: 0,
                call_offset: 0,
            }
        };
        Ok(boundary)
    }

    pub(super) fn read_snapshot(
        &self,
        state: &State,
        request: &SnapshotRequest,
    ) -> Result<SnapshotOutcome, StoreError> {
        if !state.sessions.contains_key(&request.session_id) {
            return Err(StoreError::NotFound);
        }
        let mut boundary = self.snapshot_boundary(state, request)?;
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
            high_water: boundary.high_water,
            messages: Vec::new(),
            tool_calls: Vec::new(),
            usage: SnapshotUsage::default(),
            continuation: None,
        };
        loop {
            boundary.skip_unrelated(state, &request.session_id);
            let mut usage = page.usage;
            let encoded = if boundary.message_offset < boundary.messages {
                let message = &state.messages[boundary.message_offset];
                usage.messages = usage.messages.saturating_add(1);
                usage.parts = usage.parts.saturating_add(message.parts.len());
                usage.text_bytes = message.parts.iter().fold(usage.text_bytes, |n, p| {
                    n.saturating_add(crate::snapshot::text_bytes(&p.content))
                });
                if usage.exceeded(request.limits).is_none() {
                    crate::snapshot::encoded_bytes(
                        message,
                        request
                            .limits
                            .encoded_bytes
                            .saturating_sub(page.usage.encoded_bytes),
                    )
                } else {
                    None
                }
            } else if boundary.call_offset < boundary.calls {
                let call = &state.calls[&state.call_order[boundary.call_offset]];
                usage.tool_calls = usage.tool_calls.saturating_add(1);
                if let Some(result) = &call.result {
                    usage.text_bytes = result.content.iter().fold(usage.text_bytes, |n, b| {
                        n.saturating_add(crate::snapshot::text_bytes(b))
                    });
                }
                if usage.exceeded(request.limits).is_none() {
                    crate::snapshot::encoded_bytes(
                        call,
                        request
                            .limits
                            .encoded_bytes
                            .saturating_sub(page.usage.encoded_bytes),
                    )
                } else {
                    None
                }
            } else {
                return Ok(SnapshotOutcome::Page(page));
            };
            let limit = usage.exceeded(request.limits).or_else(|| {
                if let Some(bytes) = encoded {
                    usage.encoded_bytes = usage.encoded_bytes.saturating_add(bytes);
                    None
                } else {
                    Some(SnapshotLimit::EncodedBytes)
                }
            });
            if let Some(limit) = limit {
                let continuation = self.snapshot_token(&boundary)?;
                if page.usage.messages == 0 && page.usage.tool_calls == 0 {
                    return Ok(SnapshotOutcome::Limited {
                        high_water: boundary.high_water,
                        limit,
                        continuation,
                    });
                }
                page.continuation = Some(continuation);
                return Ok(SnapshotOutcome::Page(page));
            }
            // Clone only a complete record that has passed every cap.
            if boundary.message_offset < boundary.messages {
                page.messages
                    .push(state.messages[boundary.message_offset].clone());
                boundary.message_offset += 1;
            } else {
                page.tool_calls
                    .push(state.calls[&state.call_order[boundary.call_offset]].clone());
                boundary.call_offset += 1;
            }
            page.usage = usage;
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
        state.messages.push(message(&session, "small", "small"));
        let text = "x".repeat(32_768);
        for i in 0..2048 {
            state
                .messages
                .push(message(&session, &format!("large-{i}"), &text));
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
    fn exact_caps_and_retry_do_not_skip_record() {
        let (store, session) = seeded();
        let mut state = store.state.lock().unwrap();
        let first = message(&session, "one", "é");
        let bytes = serde_json::to_vec(&first).unwrap().len();
        state.messages.push(first.clone());
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
        state.messages.push(message(&session, "one", "one"));
        state.messages.push(message(&session, "two", "two"));
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
        state.messages[0].parts[0].content = ContentBlock::Text {
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
        assert_eq!(restarted.messages[0], state.messages[0]);
    }
}
