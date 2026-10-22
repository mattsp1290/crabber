//! Actual fixed task + concrete source; independent native storage corroboration.
use crate::Store;
use std::sync::Arc;
const RESPONSE: &str = "actual finite response\u{1} literal \\u0000 slashes \\\\\\u0000";
mod erased;
mod retirement;
use crate::sql_initial_consumer_fixture as forwarding;
mod guards;
mod stopped;
mod worker;
use crate::native_enrollment::pending_initial::SourceKey;
use crate::public_session::v2::proof::Proof;
use crate::session_owner::owned_scope::{
    PendingDriverTasks, PendingInitialProducer, PendingInitialProducerStatus,
    initial_consumer_fixture,
};
#[tokio::test]
async fn sqlite_closed_initial_scripted_consumer_preserves_all_actual_originals() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("consumer.sqlite");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let store = Arc::new(crate::SqliteStore::connect(&path).await.unwrap());

    for keyed in [false, true] {
        let source = crabber_extension::LibraryScriptedSource::new(
            crabber_providers::Selection {
                provider_id: "library".into(),
                model_id: "text".into(),
            },
            vec![RESPONSE.into()],
        )
        .unwrap();
        let (context, original) =
            initial_consumer_fixture::prepared_with(store.as_ref(), keyed, &source, |bound| {
                let old = bound.keyed.request.input_envelope.as_ref().unwrap();
                let input = crabber_core::InputEnvelope::new(
                    old.messages().to_vec(),
                    old.frontend_tools().to_vec(),
                    vec![crabber_core::InputContext {
                        description: "private".into(),
                        value: serde_json::json!({"nul":"\0","one":"\u{1}","literal":"\\u0000"}),
                    }],
                    old.state().clone(),
                    old.expected_revision(),
                    old.admission_kind().clone(),
                )
                .unwrap();
                *bound = crate::typed_admission_contract::prepare(
                    bound.clone(),
                    input,
                    time::OffsetDateTime::UNIX_EPOCH,
                );
            })
            .await;
        let session = context.session_id().clone();
        let key = SourceKey::expected(&original, &mut Proof::bounded(32 << 20)).unwrap();
        let owner = PendingDriverTasks::default();
        let mut producer = PendingInitialProducer::register_scripted(
            &owner,
            store.clone(),
            context,
            original,
            source,
        )
        .unwrap();
        producer.start().unwrap();
        let observed = producer.join().await.unwrap();
        let status = observed.status().clone();
        let failure = format!("{:?}", observed.error());
        let failed = observed.observation_failed();
        owner.wait_for_supervision().await;
        // Genuine original task and resources are observed before diagnostics assert.
        assert!(
            matches!(status, PendingInitialProducerStatus::ClosedAssignment),
            "{status:?}: {failure}"
        );
        assert!(!failed, "{failure}");
        verify(store.as_ref(), &session, &key, keyed).await;
        Box::pin(crate::sqlite::deletion::verify_initial_classifier_fixture(
            store.as_ref(),
            &session,
        ))
        .await;
        guards::verify(store.as_ref(), &session, &key).await;
        Box::pin(erased::verify(store.as_ref(), &session, &key)).await;
    }
}
async fn verify(
    store: &crate::SqliteStore,
    session: &crabber_core::SessionId,
    key: &SourceKey,
    keyed: bool,
) {
    let pool = &store.readers;
    let run: String = sqlx::query_scalar(
        "SELECT run_id FROM initial_consumer_assignments WHERE session_id=$1 AND source_key=$2",
    )
    .bind(&session.0)
    .bind(key.identity.as_str())
    .fetch_one(pool)
    .await
    .unwrap();
    verify_raw_register(store, session).await;
    let message:String=sqlx::query_scalar("SELECT data_json FROM initial_consumer_results WHERE session_id=$1 AND call_id=(SELECT call_id FROM initial_consumer_calls WHERE session_id=$1 AND kind='assistant_message')").bind(&session.0).fetch_one(pool).await.unwrap();
    let message: crate::native_enrollment::initial_consumer::CallResult =
        serde_json::from_str(&message).unwrap();
    let crate::native_enrollment::initial_consumer::Original::AssistantMessage(message) =
        message.original
    else {
        panic!("actual assistant original");
    };
    let text: String = message
        .message
        .parts
        .into_iter()
        .filter_map(|part| match part.content {
            crabber_core::ContentBlock::Text { text } => Some(text),
            _ => None,
        })
        .collect();
    assert_eq!(
        text, RESPONSE,
        "full original preserves U1 and literal backslashes"
    );
    let expected_calls = if keyed { 12_i64 } else { 11_i64 };
    for table in [
        "initial_consumer_calls",
        "initial_consumer_results",
        "initial_consumer_roster",
        "initial_consumer_reverse",
    ] {
        let count: i64 = sqlx::query_scalar(&format!(
            "SELECT count(*) FROM {table} WHERE session_id=$1 AND source_key=$2"
        ))
        .bind(&session.0)
        .bind(key.identity.as_str())
        .fetch_one(pool)
        .await
        .unwrap();
        assert_eq!(count, expected_calls, "{table}");
    }
    let closes:(i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM initial_consumer_closes WHERE session_id=$1),(SELECT count(*) FROM pending_initial_closes WHERE session_id=$1)").bind(&session.0).fetch_one(pool).await.unwrap();
    assert_eq!(closes, (1, 1));
    let issuer: i64 = sqlx::query_scalar(
        "SELECT MAX(sequence) FROM native_run_issuances WHERE session_id=$1 AND run_id=$2",
    )
    .bind(&session.0)
    .bind(&run)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(issuer, 2); // Actual birth plus canonical terminal transition, no invented member issuers.
    let mut tx = store.begin_write().await.unwrap();
    let expected = sqlx::query_scalar::<_, String>(
        "SELECT data_json FROM initial_consumer_origins WHERE session_id=$1 AND source_key=$2",
    )
    .bind(&session.0)
    .bind(key.identity.as_str())
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    let origin: crate::native_enrollment::initial_consumer::Origin =
        serde_json::from_str(&expected).unwrap();
    let mut proof = Proof::bounded(32 << 20);
    let selected = super::storage::load(&mut tx, origin.correlation(), &mut proof)
        .await
        .unwrap();
    let view = selected.selection(&mut proof).unwrap();
    view.verify(origin.correlation(), &mut proof).unwrap();
    let birth = super::super::pending_initial::storage::load(
        &mut tx,
        &origin.correlation().key,
        &origin.correlation().identity.epoch,
        &mut proof,
    )
    .await
    .unwrap();
    view.verify_birth(birth.view(), &mut proof).unwrap();
    tx.rollback().await.unwrap();
}

async fn verify_raw_register(store: &crate::SqliteStore, session: &crabber_core::SessionId) {
    let pool = &store.readers;
    let register:String=sqlx::query_scalar("SELECT data_json FROM initial_consumer_results WHERE session_id=$1 AND call_id=(SELECT call_id FROM initial_consumer_calls WHERE session_id=$1 AND kind='register')").bind(&session.0).fetch_one(pool).await.unwrap();
    let register: crate::native_enrollment::initial_consumer::CallResult =
        serde_json::from_str(&register).unwrap();
    let crate::native_enrollment::initial_consumer::Original::Register(register) =
        register.original
    else {
        panic!("actual register original");
    };
    let request = crate::session_owner::driver::RegisterExecutionDriver {
        context: crabber_core::SessionContext::new(
            register.request.session,
            register.request.workspace,
            register.request.directory,
        ),
        fence: register.request.fence,
    };
    // The original public request remains valid metadata. It cannot replay a
    // private consumer's Driver receipt through a Raw wrapper, even after close.
    let replay = store.register_execution_driver(request).await;
    assert!(matches!(
        replay,
        Err(crate::session_owner::SessionOwnerError::Unsupported)
    ));
}
