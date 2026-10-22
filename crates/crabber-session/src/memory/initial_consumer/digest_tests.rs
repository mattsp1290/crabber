//! Actual native originals pin the existing serde bytes and local index budget.
use super::tests::{MAX, execute, store};
use super::{Proof, selected};
use crate::native_enrollment::initial_consumer::{self as protocol, ResultIndex};
use crate::session_owner::owned_scope::{PendingInitialProducerStatus, initial_consumer_fixture};
use crabber_extension::LibraryScriptedSource;
use sha2::{Digest, Sha256};

#[tokio::test]
async fn streamed_original_digests_and_borrowed_index_preserve_native_records() {
    for keyed in [false, true] {
        let store = store();
        let answer = "Unicode λ / slash \" quote \\ newline\n literal escape \\0".repeat(1024);
        let source = LibraryScriptedSource::new(
            crabber_providers::Selection {
                provider_id: "p".into(),
                model_id: "m".into(),
            },
            vec![answer],
        )
        .unwrap();
        let (context, original) = initial_consumer_fixture::prepared(&*store, keyed, &source).await;
        assert_eq!(
            execute(store.clone(), context, original, source, false).await,
            PendingInitialProducerStatus::ClosedAssignment
        );
        let state = store.state.lock().unwrap();
        let origin = state.initial_consumer_cohorts.values().next().unwrap();
        let view = selected(&state, origin.correlation(), &mut Proof::bounded(MAX)).unwrap();
        let mut largest = 0;
        for result in &view.results {
            // Independent old byte-producing oracle; production streams exactly
            // this domain tuple without sorting fields or JSON object keys.
            let bytes =
                serde_json::to_vec(&("crabber.initial-consumer.result.v1", result)).unwrap();
            largest = largest.max(bytes.len());
            assert_eq!(
                result.digest().unwrap().as_str(),
                format!("{:x}", Sha256::digest(bytes))
            );
        }
        let mut proof = Proof::bounded(MAX);
        let allocation = allocation_counter::measure(|| {
            let index = ResultIndex::new(&view.results, &mut proof).unwrap();
            for result in &view.results {
                assert!(std::ptr::eq(
                    index.get(&result.digest().unwrap()).unwrap().1,
                    *result
                ));
            }
        });
        assert!(
            allocation.bytes_total < u64::try_from(largest).unwrap(),
            "index recopied a full original: {allocation:?}"
        );
        let used = proof.used();
        ResultIndex::new(&view.results, &mut Proof::bounded(used)).unwrap();
        let limited = allocation_counter::measure(|| {
            assert!(ResultIndex::new(&view.results, &mut Proof::bounded(used - 1)).is_err());
        });
        assert!(
            limited.bytes_total < 256,
            "index allocated before limit: {limited:?}"
        );
        let duplicate = [view.results[0], view.results[0]];
        assert!(ResultIndex::new(&duplicate, &mut Proof::bounded(MAX)).is_err());
        view.verify(origin.correlation(), &mut Proof::bounded(MAX))
            .unwrap();
        let message = view
            .results
            .iter()
            .find_map(|result| match &result.original {
                protocol::Original::AssistantMessage(value) => Some(&value.message),
                _ => None,
            })
            .unwrap();
        assert!(message.parts.iter().any(|part| matches!(&part.content,
            crabber_core::ContentBlock::Text { text } if text.contains('λ') && text.contains(r"\0") && !text.contains('\0'))));
    }
}

#[test]
fn streaming_hash_preserves_raw_json_escaping_without_admitting_nul() {
    let value = serde_json::json!({"raw":"a\0b", "literal":r"\0", "nested":["λ\n\"\\"]});
    let carrier = ("serializer-oracle.v1", &value);
    let bytes = serde_json::to_vec(&carrier).unwrap();
    assert_eq!(
        protocol::digest(&carrier).unwrap().as_str(),
        format!("{:x}", Sha256::digest(bytes))
    );
    // Hash equivalence is not native admission: the storable boundary still
    // rejects a raw NUL, whereas a literal backslash escape is ordinary text.
    assert!(crate::ensure_storable(&value).is_err());
    assert!(crate::ensure_storable(&serde_json::json!(r"\0")).is_ok());
}
