//! Public credential transport is always Raw, including inside an owned task.
//! This gate never adopts a Run or issues an enrolled member.
use super::Status;
use crate::StoreError;
use crate::public_session::v2::proof::Failure;

pub(crate) fn allow(status: Status) {
    // Keep exhaustive: adding a Pending status requires an explicit refusal
    // here before any public credential transport can support that protocol.
    let (Status::Legacy | Status::Unclassified) = status;
}

pub(crate) fn failure(_: Failure) -> StoreError {
    StoreError::Validation("raw execution credential evidence unavailable".into())
}

#[cfg(test)]
pub(crate) mod contract {
    use crate::{AdmitOutcome, KeyedAdmitOutcome, Store};
    use crabber_core::{Run, RunId, SessionId};

    pub(crate) async fn admit(store: &dyn Store) -> Box<AdmitOutcome> {
        let request = crate::admission_execution_contract::keyed(
            &SessionId::new(),
            time::OffsetDateTime::now_utc(),
        );
        let KeyedAdmitOutcome::Started { admitted, .. } =
            store.admit_keyed_run(request).await.unwrap()
        else {
            panic!("fresh native admission")
        };
        admitted
    }

    pub(crate) async fn succeeds(store: &dyn Store, admitted: &AdmitOutcome) {
        let actual = store.get_run(&admitted.run.id).await.unwrap().unwrap();
        assert_eq!(
            serde_json::to_value(actual).unwrap(),
            serde_json::to_value(&admitted.run).unwrap()
        );
        assert!(store.get_run(&RunId::new()).await.unwrap().is_none());
        let runs = store.list_unfinished_runs().await.unwrap();
        assert!(runs.iter().any(|r: &Run| r.id == admitted.run.id && r.claim_token == admitted.run.claim_token));
        assert!(store.execution(admitted.fence.clone()).await.is_ok());
    }
}
