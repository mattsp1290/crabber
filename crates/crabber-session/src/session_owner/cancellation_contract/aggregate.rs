//! The aggregate retains authenticated cancellation metadata until actual Join.
use super::*;
pub(super) async fn check(store: &impl Store, owner: &SessionOwnerSnapshot) {
    use crate::{
        PublicHeaderOutcomeV2, PublicHeaderRequestV2, PublicSessionLimitsV2, SnapshotLimits,
    };
    let session = &owner.binding.session_id;
    let current = store
        .get_session_owner(session, &owner.target.id)
        .await
        .unwrap();
    let revision = store.session_revision(session).await.unwrap();
    let request = PublicHeaderRequestV2 {
        session_id: session.clone(),
        run_id: None,
        expected_revision: revision,
        limits: PublicSessionLimitsV2 {
            header_bytes: 65536,
            proof_bytes: 32 << 20,
            approval_interactions: 64,
            tool_requirements: 64,
            queue_entries: 64,
            registered_owners: 1024,
            run_controls: 64,
            history: SnapshotLimits {
                messages: 1,
                tool_calls: 1,
                parts: 256,
                text_bytes: 65536,
                encoded_bytes: 65536,
            },
        },
    };
    let PublicHeaderOutcomeV2::Header(header) = store
        .public_control_header_v2(request.clone())
        .await
        .unwrap()
    else {
        panic!("complete owner aggregate")
    };
    if current.state == SessionOwnerState::Joined {
        assert_eq!(header.owners, [] as [crate::PublicRegisteredOwner; 0]);
    } else {
        assert_eq!(header.owners.len(), 1);
        let public = &header.owners[0];
        assert_eq!(public.target, current.target);
        assert_eq!(public.state, current.state);
        assert_eq!(public.cancellation, current.cancellation);
        assert!(
            !serde_json::to_string(public)
                .unwrap()
                .contains("authenticated cancellation assertion")
        );
    }
    let mut limited = request;
    limited.limits.proof_bytes = header.read_usage.proof_bytes - 1;
    assert!(matches!(
        store.public_control_header_v2(limited).await.unwrap(),
        PublicHeaderOutcomeV2::Limited(crate::PublicSessionLimitV2::ProofBytes)
    ));
    assert_eq!(store.session_revision(session).await.unwrap(), revision);
}
