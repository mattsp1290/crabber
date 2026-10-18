//! Finite-fixture regression for the accepted O(state) test-store transaction.
//! This measures public writes, not a production allocation or proof allowance.
use super::MemoryStore;
use crate::{
    ManageSession, ManagementError, ManagementReceipt, SessionChange, SessionDefaults, Store,
};
use crabber_core::{
    ContentBlock, ControlId, Message, MessageId, Part, PartId, PartKind, Role, SessionId,
    SessionRevision,
};
use futures::executor::block_on;
use time::OffsetDateTime;

const TEXT_BYTES: usize = 4096;

fn create(store: &MemoryStore) -> ManagementReceipt {
    block_on(store.manage_session(ManageSession {
        id: ControlId::new(),
        session_id: SessionId::new(),
        expected_revision: SessionRevision::INITIAL,
        change: SessionChange::Create {
            workspace_id: "transaction-cost".into(),
            directory: "/transaction-cost".into(),
            title: "original".into(),
            defaults: SessionDefaults::default(),
        },
    }))
    .unwrap()
}

fn history_message(session: &SessionId, ordinal: usize) -> Message {
    let id = MessageId::from(format!("cost-message-{ordinal:03}"));
    Message {
        id: id.clone(),
        session_id: session.clone(),
        run_id: None,
        role: Role::Assistant,
        parent_id: None,
        parts: vec![Part {
            id: PartId::from(format!("cost-part-{ordinal:03}")),
            message_id: id,
            ordinal: 0,
            kind: PartKind::AssistantText,
            content: ContentBlock::Text {
                text: "x".repeat(TEXT_BYTES),
            },
        }],
        created_at: OffsetDateTime::UNIX_EPOCH,
    }
}

fn rename(created: &ManagementReceipt, title: &str) -> ManageSession {
    ManageSession {
        id: ControlId::new(),
        session_id: created.session_id.clone(),
        expected_revision: created.revision,
        change: SessionChange::Rename {
            title: title.into(),
        },
    }
}

fn measure_transactions(message_count: usize) -> [u64; 2] {
    let store = MemoryStore::new();
    let target = create(&store);
    let unrelated = create(&store);
    let messages: Vec<_> = (0..message_count)
        .map(|ordinal| history_message(&unrelated.session_id, ordinal))
        .collect();
    // Seed only presentation history, as in the snapshot allocation fixtures;
    // avoid O(n²) setup writes. Both measured operations use the public Store ABI.
    {
        let mut state = store.state.lock().unwrap();
        for message in &messages {
            state
                .messages
                .push(unrelated.session_id.clone(), message.clone());
        }
    }
    let applied = rename(&target, "committed");
    let stale = rename(&target, "must-not-commit");
    let stale_id = stale.id.clone();
    let mut committed = None;
    let success = allocation_counter::measure(|| {
        committed = Some(block_on(store.manage_session(applied)).unwrap());
    });
    let committed = committed.unwrap();
    let mut rejected = None;
    let failure = allocation_counter::measure(|| {
        rejected = Some(block_on(store.manage_session(stale)));
    });
    assert_eq!(rejected.unwrap(), Err(ManagementError::RevisionConflict));
    assert_eq!(committed.revision.get(), target.revision.get() + 1);
    assert_eq!(
        block_on(store.get_session(&target.session_id))
            .unwrap()
            .unwrap()
            .title,
        "committed"
    );
    assert_eq!(
        block_on(store.lookup_session_management(&target.session_id, &stale_id)).unwrap(),
        None
    );
    assert_eq!(
        block_on(store.list_all_messages(&unrelated.session_id)).unwrap(),
        messages
    );
    assert_eq!(
        store.state.lock().unwrap().public_revisions[&target.session_id],
        committed.revision
    );
    [success.bytes_total, failure.bytes_total]
}

#[test]
fn public_commit_and_rollback_have_finite_linear_clone_cost() {
    let mut previous = None;
    for message_count in [8, 32, 128] {
        let measured = measure_transactions(message_count);
        // Each fixture has one retained 4-KiB string per message. Allow another
        // payload-sized amount for message/part/index carriers and 64 KiB for
        // the two managed sessions and the constant-size management mutation.
        // This is a regression envelope for these fixtures, not a store limit.
        let ceiling = u64::try_from(2 * message_count * TEXT_BYTES + 64 * 1024).unwrap();
        for bytes in measured {
            assert!(
                bytes <= ceiling,
                "messages={message_count} bytes={bytes} ceiling={ceiling}"
            );
        }
        if let Some(prior) = previous {
            // Fourfold retained history may use up to fivefold allocations;
            // leave room for B-tree node boundaries without permitting quadratic
            // copying. There is no lower bound: cheaper valid clones may pass.
            for (bytes, earlier) in measured.into_iter().zip(prior) {
                assert!(
                    bytes <= earlier * 5 + 64 * 1024,
                    "messages={message_count} bytes={bytes} earlier={earlier}"
                );
            }
        }
        previous = Some(measured);
    }
}
