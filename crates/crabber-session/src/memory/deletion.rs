//! Permanent identity checks under the ordinary Memory session gate.
use super::{MemoryStore, State};
use crate::deletion::coverage::{self, CoverageRead};
use crate::deletion::origin::OriginKind;
use crate::{DeletionError, DeletionReceipt, SessionDisposition, StoreError};
use crabber_core::{ControlId, SessionId, SessionRevision};
#[path = "deletion/coordinator.rs"]
mod coordinator;
#[cfg(test)]
#[path = "deletion/fork_fixture.rs"]
mod fork_fixture;
#[path = "deletion/imported.rs"]
mod imported;
#[cfg(test)]
#[path = "deletion/journey_purge_tests.rs"]
mod journey_purge_tests;
#[cfg(test)]
#[path = "deletion/lookup_tests.rs"]
mod lookup_tests;
#[path = "deletion/native.rs"]
mod native;
#[path = "deletion/owned.rs"]
mod owned;
#[cfg(test)]
#[path = "deletion/owned_tests.rs"]
mod owned_tests;
#[path = "deletion/owners.rs"]
pub(in crate::memory) mod owners;
#[path = "deletion/purge.rs"]
mod purge;
#[cfg(test)]
#[path = "deletion/purge_tests.rs"]
mod purge_tests;
#[path = "deletion/readiness.rs"]
mod readiness;
#[path = "deletion/run_history.rs"]
mod run_history;
#[cfg(test)]
#[path = "deletion/snapshot_tests.rs"]
mod snapshot_tests;

pub(super) fn coverage(state: &State, session: &SessionId) -> Result<CoverageRead, DeletionError> {
    let Some(origin) = state.ownership_origins.get(session) else {
        return Ok(CoverageRead::Unknown);
    };
    if !state.ownership_protocol_sessions.contains(session) {
        return Err(DeletionError::UnknownStoreOutcome);
    }
    let session = state
        .sessions
        .get(session)
        .ok_or(DeletionError::UnknownStoreOutcome)?;
    let revision = state
        .public_revisions
        .get(&session.id)
        .copied()
        .unwrap_or(SessionRevision::INITIAL);
    let mut bytes = 0;
    if !charge(origin, 8192, &mut bytes) || !charge(session, 16384, &mut bytes) {
        return Ok(CoverageRead::Limited);
    }
    origin.validate(&session.id, revision)?;
    let key = (session.id.clone(), origin.operation_id.clone());
    let proof = match (origin.kind, state.operation_receipts.get(&key)) {
        (OriginKind::ManagedCreate, Some(super::OperationReceipt::Management(receipt))) => {
            let audit = state
                .management_audits
                .get(&key)
                .ok_or(DeletionError::UnknownStoreOutcome)?;
            if !charge(audit, 32768, &mut bytes) || !charge(receipt, 32768, &mut bytes) {
                return Ok(CoverageRead::Limited);
            }
            coverage::managed(origin, session, revision, audit, receipt, bytes)?
        }
        (OriginKind::Fork, Some(super::OperationReceipt::Fork(receipt))) => {
            let audit = state
                .fork_audits
                .get(&key)
                .ok_or(DeletionError::UnknownStoreOutcome)?;
            if !charge(audit, 32768, &mut bytes) || !charge(receipt, 32768, &mut bytes) {
                return Ok(CoverageRead::Limited);
            }
            coverage::fork(origin, session, revision, audit, receipt, bytes)?
        }
        (OriginKind::PreparedAdmission, _) => {
            let run = crabber_core::RunId(origin.operation_id.0.clone());
            let witness = state
                .preparation_admissions
                .get(&run)
                .ok_or(DeletionError::UnknownStoreOutcome)?;
            let initial = state
                .native_run_issuances
                .get(&(session.id.clone(), run, 1))
                .ok_or(DeletionError::UnknownStoreOutcome)?;
            if !charge(witness, 8192, &mut bytes) || !charge(initial, 2048, &mut bytes) {
                return Ok(CoverageRead::Limited);
            }
            let join = witness
                .join_id()
                .map_err(|_| DeletionError::UnknownStoreOutcome)?;
            if state
                .preparation_admission_joins
                .get(&(session.id.clone(), join))
                != Some(witness.run_id())
            {
                return Err(DeletionError::UnknownStoreOutcome);
            }
            let ids = witness
                .audit_ids()
                .map_err(|_| DeletionError::UnknownStoreOutcome)?;
            let mut audits = Vec::with_capacity(5);
            for id in &ids {
                let audit = state
                    .session_owners
                    .source_audit(&session.id, id)
                    .ok_or(DeletionError::UnknownStoreOutcome)?;
                if !charge(audit, 32768, &mut bytes) {
                    return Ok(CoverageRead::Limited);
                }
                audits.push(audit);
            }
            let sources = crate::session_owner::preparation::admission::SourceAudits {
                reserve: audits[0],
                begin: audits[1],
                join: audits[2],
                child_reserve: audits[3],
                child_begin: audits[4],
            };
            coverage::prepared(origin, session, revision, witness, initial, &sources, bytes)?
        }
        _ => return Err(DeletionError::UnknownStoreOutcome),
    };
    Ok(CoverageRead::Verified(proof))
}
fn charge(value: &impl serde::Serialize, cap: usize, used: &mut usize) -> bool {
    let remaining = coverage::MAX_PROOF_BYTES.saturating_sub(*used);
    let Some(bytes) = crate::snapshot::encoded_bytes(value, remaining.min(cap)) else {
        return false;
    };
    *used += bytes;
    true
}

pub(super) fn ensure_live(state: &State, session: &SessionId) -> Result<(), StoreError> {
    if let Some(tombstone) = state.tombstones.get(session) {
        tombstone
            .verify(session)
            .map_err(|_| StoreError::Validation("session deletion evidence unavailable".into()))?;
        return Err(StoreError::SessionDeleted);
    }
    Ok(())
}

impl MemoryStore {
    pub(super) fn deletion_receipt(
        &self,
        session: &SessionId,
        id: &ControlId,
    ) -> Result<Option<DeletionReceipt>, DeletionError> {
        crate::management::validate_scope(session, id)
            .map_err(|_| DeletionError::InvalidRequest)?;
        let state = self.state.lock().expect("memory store poisoned");
        let Some(tombstone) = state.tombstones.get(session) else {
            return Ok(None);
        };
        tombstone.verify(session)?;
        if state.sessions.contains_key(session) {
            return Err(DeletionError::UnknownStoreOutcome);
        }
        if tombstone.receipt.operation_id != *id {
            return Err(DeletionError::Deleted);
        }
        Ok(Some(tombstone.receipt.clone()))
    }

    pub(super) fn disposition(
        &self,
        session: &SessionId,
    ) -> Result<SessionDisposition, DeletionError> {
        crate::deletion::validate_session(session)?;
        let state = self.state.lock().expect("memory store poisoned");
        if let Some(tombstone) = state.tombstones.get(session) {
            tombstone.verify(session)?;
            if state.sessions.contains_key(session) {
                return Err(DeletionError::UnknownStoreOutcome);
            }
            return Ok(SessionDisposition::Deleted {
                final_revision: tombstone.receipt.final_revision,
                deleted_at: tombstone.receipt.deleted_at,
            });
        }
        Ok(if state.sessions.contains_key(session) {
            SessionDisposition::Present {
                revision: state
                    .public_revisions
                    .get(session)
                    .copied()
                    .unwrap_or(SessionRevision::INITIAL),
            }
        } else {
            // Absence says nothing about pre-session preparation ownership.
            SessionDisposition::Missing
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::Store;
    #[tokio::test]
    async fn bare_admission_and_rename_never_install_coverage() {
        let store = super::MemoryStore::new();
        let session = crabber_core::SessionId::new();
        store
            .admit_run(crate::deletion::contract::admission(&session))
            .await
            .unwrap();
        let revision = store.session_revision(&session).await.unwrap();
        store
            .manage_session(crate::ManageSession {
                id: crabber_core::ControlId::new(),
                session_id: session.clone(),
                expected_revision: revision,
                change: crate::SessionChange::Rename {
                    title: "renamed legacy session".into(),
                },
            })
            .await
            .unwrap();
        let state = store.state.lock().unwrap();
        assert!(!state.ownership_protocol_sessions.contains(&session));
        assert!(matches!(
            super::coverage(&state, &session).unwrap(),
            super::CoverageRead::Unknown
        ));
    }
    #[tokio::test]
    async fn creation_coverage_requires_exact_original_evidence() {
        let store = super::MemoryStore::new();
        let created = store
            .manage_session(crate::management_contract::create())
            .await
            .unwrap();
        let id = &created.session_id;
        let original = store.state.lock().unwrap().ownership_origins[id].clone();
        assert_coverage(&store, id, created.revision);
        let renamed = store
            .manage_session(crate::ManageSession {
                id: crabber_core::ControlId::new(),
                session_id: id.clone(),
                expected_revision: created.revision,
                change: crate::SessionChange::Rename {
                    title: "renamed".into(),
                },
            })
            .await
            .unwrap();
        assert_coverage(&store, id, renamed.revision);
        store
            .state
            .lock()
            .unwrap()
            .ownership_protocol_sessions
            .remove(id);
        assert!(matches!(
            super::coverage(&store.state.lock().unwrap(), id),
            Err(crate::DeletionError::UnknownStoreOutcome)
        ));
        store
            .state
            .lock()
            .unwrap()
            .ownership_protocol_sessions
            .insert(id.clone());
        store.state.lock().unwrap().ownership_origins.remove(id);
        assert!(matches!(
            super::coverage(&store.state.lock().unwrap(), id).unwrap(),
            super::CoverageRead::Unknown
        ));
        store
            .state
            .lock()
            .unwrap()
            .ownership_origins
            .insert(id.clone(), original);
        store
            .state
            .lock()
            .unwrap()
            .management_audits
            .remove(&(id.clone(), created.operation_id));
        assert!(matches!(
            super::coverage(&store.state.lock().unwrap(), id),
            Err(crate::DeletionError::UnknownStoreOutcome)
        ));
    }
    fn assert_coverage(
        store: &super::MemoryStore,
        id: &crabber_core::SessionId,
        revision: crabber_core::SessionRevision,
    ) {
        let super::CoverageRead::Verified(proof) =
            super::coverage(&store.state.lock().unwrap(), id).unwrap()
        else {
            panic!("positive original coverage")
        };
        assert_eq!(proof.session_id(), id);
        assert_eq!(proof.revision(), revision);
        assert!(proof.proof_bytes() > 0);
    }
    #[tokio::test]
    async fn dangling_creation_origin_cannot_be_overwritten() {
        let store = super::MemoryStore::new();
        let mut create = crate::management_contract::create();
        store.manage_session(create.clone()).await.unwrap();
        let original = store.state.lock().unwrap().ownership_origins[&create.session_id].clone();
        store
            .state
            .lock()
            .unwrap()
            .sessions
            .remove(&create.session_id);
        create.id = crabber_core::ControlId::new();
        assert_eq!(
            store.manage_session(create.clone()).await.unwrap_err(),
            crate::ManagementError::UnknownStoreOutcome
        );
        assert_eq!(
            store.state.lock().unwrap().ownership_origins[&create.session_id],
            original
        );
    }
    #[tokio::test]
    async fn permanent_identity_guards() {
        let store = super::MemoryStore::new();
        let tombstone = crate::deletion::contract::deleted();
        store
            .state
            .lock()
            .unwrap()
            .tombstones
            .insert(tombstone.receipt.session_id.clone(), tombstone.clone());
        crate::deletion::contract::metadata(&store, &tombstone).await;
        crate::deletion::contract::creation_guards(&store, &tombstone).await;
        store
            .state
            .lock()
            .unwrap()
            .tombstones
            .get_mut(&tombstone.receipt.session_id)
            .unwrap()
            .expected_revision = tombstone.receipt.final_revision;
        assert_eq!(
            store
                .deletion_receipt(
                    &tombstone.receipt.session_id,
                    &tombstone.receipt.operation_id
                )
                .unwrap_err(),
            crate::DeletionError::UnknownStoreOutcome
        );
    }
}

#[cfg(test)]
#[tokio::test]
async fn prepared_admission_origin_authenticates_independent_sources() {
    let store = MemoryStore::new();
    let receipt =
        crate::session_owner::preparation_contract::resources::successful(&store, false).await;
    let mut state = store.state.lock().unwrap();
    let CoverageRead::Verified(proof) = coverage(&state, &receipt.session_id).unwrap() else {
        panic!("original owned admission");
    };
    assert_eq!(proof.session_id(), &receipt.session_id);
    assert!(proof.proof_bytes() > 0);
    let original = state.preparation_admissions[&receipt.run_id].clone();
    let mut witness = serde_json::to_value(&state.preparation_admissions[&receipt.run_id]).unwrap();
    witness["session"]["directory"] = serde_json::json!("other-directory");
    state.preparation_admissions.insert(
        receipt.run_id.clone(),
        serde_json::from_value(witness).unwrap(),
    );
    assert!(matches!(
        coverage(&state, &receipt.session_id),
        Err(DeletionError::UnknownStoreOutcome)
    ));
    state
        .preparation_admissions
        .insert(receipt.run_id.clone(), original);
    state
        .native_run_issuances
        .remove(&(receipt.session_id.clone(), receipt.run_id, 1));
    assert!(matches!(
        coverage(&state, &receipt.session_id),
        Err(DeletionError::UnknownStoreOutcome)
    ));
}
