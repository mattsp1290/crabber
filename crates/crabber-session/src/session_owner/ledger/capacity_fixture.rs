//! Authentic fixture interiors; the public Store checks the actual limit.
use super::*;

impl Ledger {
    pub(crate) fn seed_capacity_interior_for_test(
        &mut self,
        first: &ReserveSessionOwner,
        clock: &dyn crabber_core::Clock,
    ) -> Vec<ReserveSessionOwner> {
        let mut expected = vec![first.clone()];
        self.verify_capacity_fixture_for_test(&expected);
        for _ in 0..1022 {
            let mut request = first.clone();
            request.id = SessionOwnerOperationId::new();
            request.binding.id = SessionOwnerId::new();
            let digest = request.fingerprint().unwrap();
            let record = Record::reserve(&request, None, clock.now()).unwrap();
            drop(record.grant().unwrap());
            let receipt =
                record.receipt(request.id.clone(), digest, SessionOwnerOperation::Reserve);
            let revision = RevisionState::Absent(u64::try_from(expected.len()).unwrap());
            self.publish(record, receipt, Publication::new(revision, true).unwrap())
                .unwrap();
            expected.push(request);
        }
        expected
    }

    pub(crate) fn verify_capacity_fixture_for_test(&self, expected: &[ReserveSessionOwner]) {
        assert!(matches!(expected.len(), 1 | 1023 | 1024));
        let first = &expected[0];
        let session = &first.binding.session_id;
        let ids: BTreeMap<_, _> = expected
            .iter()
            .map(|request| (request.binding.id.clone(), request.id.clone()))
            .collect();
        assert_eq!(ids.len(), expected.len());
        assert_eq!(self.records.len(), expected.len());
        assert_eq!(self.audits.len(), expected.len());
        assert_eq!(self.publications.len(), expected.len());
        assert_eq!(
            self.next_publication,
            u64::try_from(expected.len()).unwrap()
        );
        assert_eq!(self.publication_count(session), self.next_publication);
        assert!(self.native_sessions.is_empty());
        assert_eq!(self.contexts.len(), 1);
        let context = self.contexts.get(session).unwrap();
        assert!(context.native_run_protocol);
        assert_eq!(context.workspace, first.binding.workspace_id);
        assert_eq!(context.directory, first.binding.directory);
        assert_eq!(context.origin, first.id);
        self.validate_scope(session).unwrap();
        for (index, request) in expected.iter().enumerate() {
            assert_eq!(&request.binding.session_id, session);
            assert!(request.binding.parent.is_none());
            assert!(request.binding.resource_contract.is_none());
            let record = self.checked(session, &request.binding.id).unwrap();
            assert_eq!(record.snapshot.binding, request.binding);
            assert_eq!(record.origin, request.id);
            assert_eq!(record.snapshot.state, SessionOwnerState::ReservedUnstarted);
            assert_eq!(record.snapshot.termination, None);
            assert_eq!(
                record.snapshot.target.generation,
                crabber_core::SessionOwnerGeneration::INITIAL
            );
            assert_eq!(record.sequence, 1);
            assert_eq!(record.owner, request.owner);
            let audit = self
                .audits
                .get(&(session.clone(), request.id.clone()))
                .unwrap();
            audit.validate().unwrap();
            assert_eq!(audit.version, 2);
            assert_eq!(audit.receipt.operation, SessionOwnerOperation::Reserve);
            assert_eq!(
                audit.receipt.semantic_digest,
                request.fingerprint().unwrap()
            );
            assert!(audit.record == *record);
            let ordinal = u64::try_from(index).unwrap();
            assert_eq!(
                audit.publication,
                Some(Publication {
                    before: RevisionState::Absent(ordinal),
                    after: RevisionState::Absent(ordinal + 1),
                    public_change: true,
                })
            );
            assert_eq!(
                self.publications
                    .get(&(session.clone(), request.id.clone())),
                Some(&(ordinal + 1, request.binding.id.clone()))
            );
        }
    }

    pub(crate) fn forge_capacity_current_joined_for_test(
        &mut self,
        first: &ReserveSessionOwner,
    ) -> Record {
        let record = self
            .records
            .get_mut(&(first.binding.session_id.clone(), first.binding.id.clone()))
            .unwrap();
        let original = record.clone();
        record.snapshot.state = SessionOwnerState::Joined;
        record.snapshot.termination = Some(super::super::SessionOwnerTermination::Returned);
        record.token.clear();
        record.validate().unwrap();
        original
    }

    pub(crate) fn restore_capacity_current_for_test(&mut self, record: Record) {
        let session = record.snapshot.binding.session_id.clone();
        let id = record.snapshot.target.id.clone();
        let audit = self
            .audits
            .get(&(session.clone(), record.origin.clone()))
            .unwrap();
        assert!(audit.record == record);
        self.records.insert((session, id), record);
    }
}
