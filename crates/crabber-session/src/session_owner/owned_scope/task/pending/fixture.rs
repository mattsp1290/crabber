use crate::session_owner::{MutateSessionOwner, SessionOwnerCommand, SessionOwnerOutcome};
use crate::{Store, session_owner::preparation::*};
use crabber_core::{ManualClock, SessionContext};
pub(in super::super) async fn prepared(
    store: &impl Store,
    clock: &ManualClock,
    admission: bool,
) -> (SessionContext, JoinedPreparation) {
    Box::pin(async {
        let (root, bound) =
            crate::session_owner::preparation_contract::resources::declaration::setup(store).await;
        let original = bound.clone();
        let PreparationCompletion::Keyed(crate::KeyedAdmitOutcome::Started { admitted, .. }) =
            crate::session_owner::preparation_contract::resources::initial_driver_completion(
                store,
                root,
                PreparedOperation::AdmitKeyed(Box::new(bound)),
            )
            .await
        else {
            panic!("fresh initial");
        };
        let context = crabber_core::SessionContext::new(
            admitted.session.id.clone(),
            admitted.session.workspace_id.clone(),
            admitted.session.directory.clone(),
        );
        if !admission {
            store
                .execution(admitted.fence.clone())
                .await
                .unwrap()
                .begin_admission_execution()
                .await
                .unwrap();
        }
        clock.set(admitted.run.lease_until);
        let joined = if admission {
            recovery(store, &admitted, original).await
        } else {
            let source = store
                .inspect_expired_run_preparation(InspectExpiredRunPreparation {
                    session_id: admitted.session.id.clone(),
                    expected_fence: admitted.fence.clone(),
                    owner: "successor-host".into(),
                    lease: std::time::Duration::from_secs(30),
                })
                .await
                .unwrap();
            crate::run_control_contract::expired::prepare(store, &source, original.binding).await
        };
        (context, joined)
    })
    .await
}

async fn recovery(
    store: &impl Store,
    admitted: &crate::AdmitOutcome,
    mut bound: crate::BoundKeyedAdmitRequest,
) -> JoinedPreparation {
    let request = crate::ClaimUnstartedAdmissionRequest {
        session_id: admitted.session.id.clone(),
        key: bound.keyed.options.key.clone(),
        expected_fence: admitted.fence.clone(),
        expected_owner: admitted.run.owner.clone(),
        owner: "successor-host".into(),
        lease: std::time::Duration::from_secs(30),
        fingerprint: bound.keyed.options.fingerprint.clone(),
        behavior_fingerprint: bound.keyed.options.behavior_fingerprint.clone(),
        semantic_digest: bound.keyed.semantic_digest().unwrap(),
        capsule_digest: bound.keyed.execution.as_ref().unwrap().digest().unwrap(),
    };
    let source = store.inspect_admission_recovery(request).await.unwrap();
    bound.keyed.request.owner = "successor-host".into();
    let mut owner = crate::session_owner_contract::reservation(admitted.session.id.clone());
    owner.binding.workspace_id = admitted.session.workspace_id.clone();
    owner.binding.directory = admitted.session.directory.clone();
    owner.binding.behavior_fingerprint = bound.binding.behavior_fingerprint.clone();
    owner.binding.component_id = OWNED_PREPARATION_COMPONENT.into();
    owner.lease_ms = 30000;
    let key = PreparationKey::AdmissionRecovery {
        key: bound.keyed.options.key.clone(),
        issuance: source.expected_issuance(),
    };
    let request = ReservePreparation {
        owner,
        key: key.clone(),
        resource_contract: crate::session_owner::preparation::resource_contract(
            &bound.binding.runtime_semantics,
        )
        .unwrap(),
        expected_revision: source.expected_revision(),
        purpose: PreparationPurpose::ExistingRun {
            run_id: admitted.run.id.clone(),
            work: ExistingWorkTarget::AdmissionRecovery {
                key: bound.keyed.options.key.clone(),
                issuance: source.expected_issuance(),
            },
        },
        source: Some(PreparationSource::AdmissionRecovery(source.clone())),
        intent: source.fingerprint().unwrap(),
    }
    .bind_key(key)
    .unwrap();
    let SessionOwnerOutcome::Applied {
        authority: Some(authority),
        ..
    } = store.reserve_preparation(request.clone()).await.unwrap()
    else {
        panic!("fresh reserve");
    };
    store
        .mutate_session_owner(
            MutateSessionOwner {
                id: request.reconciliation().unwrap().begin_id,
                session_id: admitted.session.id.clone(),
                target: authority.target().clone(),
                command: SessionOwnerCommand::Begin,
            },
            Some(authority.clone()),
        )
        .await
        .unwrap();
    crate::run_control_contract::expired::owned::observe(
        store,
        PreparationGrant::from_fresh_begin(request, authority).unwrap(),
        PreparedOperation::AdmissionRecovery(Box::new(PreparedAdmissionRecovery {
            source,
            binding: Box::new(bound),
        })),
    )
    .await
}
