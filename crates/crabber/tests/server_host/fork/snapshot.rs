//! Imported presentation records participate in ordinary bounded public history.
use super::*;
use crabber::core::ImportedCallSource;
use crabber::session::{PublicHistoryPage, SnapshotOutcome, SnapshotRequest};

pub(super) async fn check(
    store: &dyn Store,
    target: &SessionId,
    expected: ImportedCallSource,
    count: usize,
) {
    let mut continuation = None;
    let mut calls = vec![];
    let mut provenance = vec![];
    let mut messages = vec![];
    loop {
        let SnapshotOutcome::Page(page) = store
            .snapshot(SnapshotRequest {
                session_id: target.clone(),
                limits: SnapshotLimits {
                    messages: 1,
                    tool_calls: 1,
                    parts: 256,
                    text_bytes: 4096,
                    encoded_bytes: 4096,
                },
                continuation,
            })
            .await
            .unwrap()
        else {
            panic!("bounded history page")
        };
        assert!(
            page.usage.messages <= 1
                && page.usage.tool_calls <= 1
                && page.usage.encoded_bytes <= 4096
        );
        assert_eq!(page.tool_calls.len(), 0);
        assert_eq!(page.usage.tool_calls, page.imported_calls.len());
        let public = PublicHistoryPage::from(page);
        calls.extend(public.imported_calls);
        provenance.extend(public.imported_provenance);
        messages.extend(public.messages);
        continuation = public.continuation;
        if continuation.is_none() {
            break;
        }
    }
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].source_kind, expected);
    assert_eq!(provenance.len(), count);
    assert_eq!(messages.len(), count);
    for origin in provenance {
        assert_eq!(origin.session_id, *target);
        let message = messages
            .iter()
            .find(|message| message.id == origin.message_id)
            .unwrap();
        assert_eq!(message.parts.len(), origin.parts.len());
        for (part, (target_id, source_id)) in message.parts.iter().zip(origin.parts) {
            assert_eq!(part.id, target_id);
            assert_ne!(target_id, source_id);
        }
    }
}
