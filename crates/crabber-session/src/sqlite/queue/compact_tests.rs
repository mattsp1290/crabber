//! Real SQL compact storage, original-source rejection and lossless capacity.
use crate::public_session::v2::proof::Proof;
use crate::queue::{Audit, Record, decode, encode};
use crate::{QueueAction, SqliteStore, Store};
use crabber_core::{AdmissionKind, InputContext, InputEnvelope, InputState};
use std::time::Duration;
use time::OffsetDateTime;

async fn read_record(store: &SqliteStore, fixture: &crate::queue_contract::Fixture) -> Record {
    let data: String = sqlx::query_scalar(
        "SELECT data_json FROM future_queue WHERE session_id=$1 AND queue_id=$2",
    )
    .bind(&fixture.session.0)
    .bind(&fixture.queue.0)
    .fetch_one(&store.writer)
    .await
    .unwrap();
    decode(&data).unwrap()
}
async fn write_record(store: &SqliteStore, fixture: &crate::queue_contract::Fixture, data: String) {
    sqlx::query("UPDATE future_queue SET data_json=$3 WHERE session_id=$1 AND queue_id=$2")
        .bind(&fixture.session.0)
        .bind(&fixture.queue.0)
        .bind(data)
        .execute(&store.writer)
        .await
        .unwrap();
}
async fn original(
    store: &SqliteStore,
    session: &crabber_core::SessionId,
    operation: &crabber_core::ControlId,
) -> (String, String) {
    let audit: String = sqlx::query_scalar(
        "SELECT data_json FROM future_queue_audit WHERE session_id=$1 AND operation_id=$2",
    )
    .bind(&session.0)
    .bind(&operation.0)
    .fetch_one(&store.writer)
    .await
    .unwrap();
    let source: String = sqlx::query_scalar(
        "SELECT data_json FROM queue_publication_sources WHERE session_id=$1 AND operation_id=$2",
    )
    .bind(&session.0)
    .bind(&operation.0)
    .fetch_one(&store.writer)
    .await
    .unwrap();
    (audit, source)
}
async fn prepared_values(
    store: &SqliteStore,
) -> (
    crate::queue_contract::Fixture,
    crate::QueueMutation,
    crate::QueueAuthority,
) {
    let mut fixture = crate::queue_contract::fixture(store).await;
    assert!(InputEnvelope::text("invalid\u{0}input", fixture.enqueue.expected_revision).is_err());
    let text = "unicode:λ🙂 quotes:\" slash:\\ control:\n\t";
    let simple = InputEnvelope::text(text, fixture.enqueue.expected_revision).unwrap();
    let value = serde_json::json!({"large":1.25e30,"small":1.25e-30,"nested":["λ🙂\"\\",null,-7]});
    let input = InputEnvelope::new(
        simple.messages().to_vec(),
        vec![],
        vec![InputContext {
            description: text.into(),
            value: value.clone(),
        }],
        InputState::Present { value },
        fixture.enqueue.expected_revision,
        AdmissionKind::Ordinary,
    )
    .unwrap();
    fixture.prepared = crate::typed_admission_contract::prepare(
        fixture.prepared,
        input.clone(),
        OffsetDateTime::UNIX_EPOCH,
    );
    let QueueAction::Enqueue { input: intent, .. } = &mut fixture.enqueue.action else {
        panic!("actual enqueue");
    };
    intent.input = input;
    store.mutate_queue(fixture.enqueue.clone()).await.unwrap();
    let authority = crate::queue_contract::claim(store, &fixture).await;
    let prepare = crate::queue_contract::mutation(
        store,
        &fixture,
        QueueAction::Prepare {
            authority: authority.clone(),
            prepared: Box::new(fixture.prepared.clone()),
        },
    )
    .await;
    store.mutate_queue(prepare.clone()).await.unwrap();
    (fixture, prepare, authority)
}

#[tokio::test]
async fn compact_projection_retains_actual_prepare_and_exact_former_capacity() {
    let (store, _, _directory) = crate::sqlite::tests::temp_store().await;
    let (fixture, prepare, authority) = prepared_values(&store).await;
    let before = original(&store, &fixture.session, &prepare.id).await;
    let mut tx = crate::transaction::begin(&store.writer).await.unwrap();
    let mut proof = Proof::bounded(32 << 20);
    let selected =
        super::reads::record_with_proof(&mut tx, &fixture.session, &fixture.queue, &mut proof)
            .await
            .unwrap()
            .unwrap();
    let expected = serde_json::to_vec(&selected.projection.view(selected.prepared().unwrap()))
        .unwrap()
        .len();
    let (count, actual) = super::capacity::pending(&mut tx, &fixture.session, &mut proof)
        .await
        .unwrap();
    let projected = serde_json::to_value(&selected.projection).unwrap();
    assert_eq!(projected["prepared"].as_object().unwrap().len(), 2);
    assert!(projected["prepared"].get("keyed").is_none());
    assert_eq!((count, actual), (1, expected));
    tx.rollback().await.unwrap();
    let admit = crate::queue_contract::mutation(
        &store,
        &fixture,
        QueueAction::Admit {
            authority,
            owner: "actual-capacity-owner".into(),
            lease: Duration::from_secs(30),
        },
    )
    .await;
    assert!(matches!(
        store.mutate_queue(admit).await.unwrap(),
        crate::QueueOutcome::Applied {
            admitted: Some(_),
            ..
        }
    ));
    assert_eq!(
        original(&store, &fixture.session, &prepare.id).await,
        before
    );
    let retained = store
        .retained_queue(&fixture.session, &fixture.queue)
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(retained.prepared.unwrap()).unwrap(),
        serde_json::to_value(fixture.prepared).unwrap()
    );
}

#[tokio::test]
async fn compact_projection_rejects_foreign_prepare_reference_and_old_codec() {
    let (store, _, _directory) = crate::sqlite::tests::temp_store().await;
    let (fixture, prepare, authority) = crate::queue_contract::corruption::prepared(&store).await;
    let (foreign, _, _) = crate::queue_contract::corruption::prepared(&store).await;
    let mut current = read_record(&store, &fixture).await;
    let original = encode(&current).unwrap();
    let other = read_record(&store, &foreign).await;
    current.prepare_operation = other.prepare_operation;
    current.prepared = other.prepared;
    current
        .validate(&fixture.session, &fixture.queue, current.summary.sequence)
        .unwrap();
    write_record(&store, &fixture, encode(&current).unwrap()).await;
    crate::queue_contract::corruption::refused(
        &store,
        &fixture,
        prepare.clone(),
        authority.clone(),
    )
    .await;
    write_record(&store, &fixture, original.clone()).await;
    let (audit, _) = self::original(&store, &fixture.session, &prepare.id).await;
    let audit: Audit = decode(&audit).unwrap();
    let QueueAction::Prepare { prepared, .. } = audit.request.action else {
        panic!("actual prepare");
    };
    let mut old: serde_json::Value = serde_json::from_str(&original).unwrap();
    old.as_object_mut().unwrap().remove("version");
    old["prepared"] = serde_json::to_value(prepared).unwrap();
    assert!(decode::<Record>(&old.to_string()).is_err());
    write_record(&store, &fixture, old.to_string()).await;
    assert!(
        store
            .retained_queue(&fixture.session, &fixture.queue)
            .await
            .is_err()
    );
    write_record(&store, &fixture, original).await;
    assert!(
        store
            .retained_queue(&fixture.session, &fixture.queue)
            .await
            .is_ok()
    );
}

#[test]
fn scalar_capacity_row_bound_covers_its_complete_representation() {
    #[derive(serde::Serialize)]
    struct Row {
        stored_bytes: i64,
        reference_bytes: Option<i64>,
        original_bytes: Option<i64>,
        valid: bool,
    }
    let actual = serde_json::to_vec(&Row {
        stored_bytes: i64::MIN,
        reference_bytes: Some(i64::MIN),
        original_bytes: Some(i64::MIN),
        valid: false,
    })
    .unwrap()
    .len();
    assert_eq!(actual, 128);
}
