//! Compatibility of the persisted identity recipe using genuine registrations.
use super::super::SourceKey;
use crate::public_session::v2::proof::{Failure, Proof};
use crate::session_owner::owned_scope::initial_consumer_fixture;
use crate::session_owner::preparation::PreparedOperation;
use crabber_extension::LibraryScriptedSource;

#[tokio::test]
async fn shared_identity_recipe_preserves_original_wire_and_exact_bound() {
    for keyed in [false, true] {
        let store = crate::MemoryStore::new();
        let source = LibraryScriptedSource::new(
            crabber_providers::Selection {
                provider_id: "p".into(),
                model_id: "m".into(),
            },
            vec!["actual original source".into()],
        )
        .unwrap();
        let (_, original) = initial_consumer_fixture::prepared(&store, keyed, &source).await;
        let grant = original.observed_grant();
        let registration = original.observed_registration().unwrap();
        let reservation = grant.original_reservation();
        let root = reservation.reconciliation().unwrap();
        let session = &reservation.owner.binding.session_id;
        let target = grant.owner_authority().target();
        // Pre-extraction persisted v1 tuple: this independent wire oracle keeps
        // its order/domain fixed when shared live/historical code is refactored.
        let old_identity = crate::native_enrollment::pending_initial::digest(&(
            "crabber.pending-initial.source-key.v1",
            (
                session,
                &root.reserve_id,
                &root.begin_id,
                target,
                &registration.source.reserve,
                &registration.source.begin,
                &registration.source.target,
                &registration.id,
                &registration.nonce,
            ),
        ))
        .unwrap();
        let mut live = Proof::bounded(32 << 20);
        let expected = SourceKey::expected(&original, &mut live).unwrap();
        assert!(expected.identity == old_identity && expected.session_id == *session);
        let read = |proof: &mut Proof| {
            SourceKey::from_original_identities(
                session,
                &root.reserve_id,
                &root.begin_id,
                target,
                registration,
                proof,
            )
        };
        let mut proof = Proof::bounded(32 << 20);
        assert!(read(&mut proof).unwrap() == expected);
        let used = proof.used();
        assert!(used > 0);
        assert!(read(&mut Proof::bounded(used)).unwrap() == expected);
        assert!(matches!(
            read(&mut Proof::bounded(used - 1)),
            Err(Failure::Limited(_))
        ));
        let (binding, options) = match original.observed_operation() {
            PreparedOperation::Admit(bound) => (&bound.binding, None),
            PreparedOperation::AdmitKeyed(bound) => (&bound.binding, Some(&bound.keyed.options)),
            _ => panic!("actual original Initial operation"),
        };
        let semantic = crate::session_owner::preparation::admission_intent(
            &binding.request,
            binding.input_envelope.as_ref().unwrap(),
            options,
        )
        .unwrap();
        assert_eq!(semantic, reservation.intent);
    }
}
