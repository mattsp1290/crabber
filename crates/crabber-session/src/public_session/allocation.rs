use crate::{PublicSessionLimit, Store};
use crabber_core::SessionId;
pub(crate) fn proof_allocation(
    runtime: &tokio::runtime::Runtime,
    store: &impl Store,
    session: SessionId,
    run: crabber_core::RunId,
) {
    let revision = runtime.block_on(store.session_revision(&session)).unwrap();
    let request = crate::PublicHeaderRequest {
        session_id: session,
        run_id: Some(run),
        expected_revision: revision,
        max_bytes: 65536,
        max_proof_bytes: 1,
        max_interactions: 64,
    };
    let measured = allocation_counter::measure(|| {
        assert!(matches!(
            runtime
                .block_on(store.public_control_header(request))
                .unwrap(),
            crate::PublicHeaderOutcome::Limited(PublicSessionLimit::ProofBytes)
        ));
    });
    assert!(
        measured.bytes_max < 131_072,
        "oversized private proof materialized: {measured:?}"
    );
}
