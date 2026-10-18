use crate::queue_contract::corruption;
#[tokio::test]
async fn queue_rejects_replaced_prepared_and_coherent_input() {
    let store = crate::MemoryStore::new();
    for change_input in [false, true] {
        let (fixture, prepare, authority) = corruption::prepared(&store).await;
        {
            let mut state = store.state.lock().unwrap();
            let audit = state.queue_audits[&(fixture.session.clone(), prepare.id.clone())].clone();
            let source = state
                .queue_publications
                .source(&fixture.session, &prepare.id)
                .cloned();
            let record = state
                .queue
                .get_mut(&(fixture.session.clone(), fixture.queue.clone()))
                .unwrap();
            corruption::replace(record, &audit, source.as_ref(), change_input).unwrap();
        }
        corruption::refused(&store, &fixture, prepare, authority).await;
    }
}
#[tokio::test]
async fn queue_rejects_replaced_claim_authority() {
    let store = crate::MemoryStore::new();
    let (fixture, prepare, authority) = corruption::prepared(&store).await;
    {
        let mut state = store.state.lock().unwrap();
        let record = state
            .queue
            .get_mut(&(fixture.session.clone(), fixture.queue.clone()))
            .unwrap();
        record.claim.as_mut().unwrap().authority.token = uuid::Uuid::new_v4().to_string();
    }
    corruption::refused(&store, &fixture, prepare, authority).await;
}
#[tokio::test]
async fn queue_rejects_cancelled_state_and_old_claim_resurrection() {
    use crate::{QueueAction, Store};
    for claimed in [false, true] {
        let store = crate::MemoryStore::new();
        let fixture = crate::queue_contract::fixture(&store).await;
        store.mutate_queue(fixture.enqueue.clone()).await.unwrap();
        if claimed {
            crate::queue_contract::claim(&store, &fixture).await;
        }
        let key = (fixture.session.clone(), fixture.queue.clone());
        let old = store.state.lock().unwrap().queue[&key].clone();
        let cancel = crate::queue_contract::mutation(
            &store,
            &fixture,
            QueueAction::Cancel {
                queue_id: fixture.queue.clone(),
            },
        )
        .await;
        store.mutate_queue(cancel.clone()).await.unwrap();
        store.state.lock().unwrap().queue.insert(key, old);
        assert!(
            store
                .retained_queue(&fixture.session, &fixture.queue)
                .await
                .is_err()
        );
        assert!(
            store
                .get_queue(&fixture.session, &fixture.queue)
                .await
                .is_err()
        );
        let claim = crate::queue_contract::mutation(
            &store,
            &fixture,
            QueueAction::Claim {
                owner: "resurrection".into(),
                lease: std::time::Duration::from_secs(30),
            },
        )
        .await;
        assert!(store.mutate_queue(claim).await.is_err());
        assert!(matches!(
            store.mutate_queue(cancel).await.unwrap(),
            crate::QueueOutcome::Replayed(_)
        ));
    }
}
#[tokio::test]
async fn queue_rejects_replaced_admission_mapping() {
    use crate::{QueueAction, Store};
    let store = crate::MemoryStore::new();
    let (fixture, _, authority) = corruption::prepared(&store).await;
    let admit = crate::queue_contract::mutation(
        &store,
        &fixture,
        QueueAction::Admit {
            authority,
            owner: "mapping".into(),
            lease: std::time::Duration::from_secs(30),
        },
    )
    .await;
    store.mutate_queue(admit.clone()).await.unwrap();
    {
        let mut state = store.state.lock().unwrap();
        state
            .queue
            .get_mut(&(fixture.session.clone(), fixture.queue.clone()))
            .unwrap()
            .summary
            .admission
            .as_mut()
            .unwrap()
            .run_id = crabber_core::RunId::new();
    }
    assert!(
        store
            .retained_queue(&fixture.session, &fixture.queue)
            .await
            .is_err()
    );
    assert!(
        store
            .get_queue(&fixture.session, &fixture.queue)
            .await
            .is_err()
    );
    let cancel = crate::queue_contract::mutation(
        &store,
        &fixture,
        QueueAction::Cancel {
            queue_id: fixture.queue.clone(),
        },
    )
    .await;
    assert!(store.mutate_queue(cancel).await.is_err());
    assert!(matches!(
        store.mutate_queue(admit).await.unwrap(),
        crate::QueueOutcome::Replayed(_)
    ));
}
#[tokio::test]
async fn queue_cannot_hide_corrupt_head_behind_changed_session_scope() {
    use crate::{QueueAction, QueuePageRequest, Store};
    let store = crate::MemoryStore::new();
    let fixture = crate::queue_contract::fixture(&store).await;
    store.mutate_queue(fixture.enqueue.clone()).await.unwrap();
    let second = crate::queue_contract::entry(&store, fixture.session.clone()).await;
    store.mutate_queue(second.enqueue).await.unwrap();
    {
        let mut state = store.state.lock().unwrap();
        state
            .queue
            .get_mut(&(fixture.session.clone(), fixture.queue.clone()))
            .unwrap()
            .summary
            .session_id = crabber_core::SessionId::new();
    }
    assert!(
        store
            .list_queue(QueuePageRequest {
                session_id: fixture.session.clone(),
                limit: 1,
                after_sequence: None,
                expected_revision: None
            })
            .await
            .is_err()
    );
    let claim = crate::queue_contract::mutation(
        &store,
        &fixture,
        QueueAction::Claim {
            owner: "skip-head".into(),
            lease: std::time::Duration::from_secs(30),
        },
    )
    .await;
    assert!(store.mutate_queue(claim).await.is_err());
}

#[tokio::test]
async fn current_projection_is_compact_and_former_capacity_is_lossless() {
    let store = crate::MemoryStore::new();
    let (fixture, prepare, _) = corruption::prepared(&store).await;
    let state = store.state.lock().unwrap();
    let record = &state.queue[&(fixture.session.clone(), fixture.queue.clone())];
    let audit = &state.queue_audits[&(fixture.session.clone(), prepare.id)];
    let crate::QueueAction::Prepare { prepared, .. } = &audit.request.action else {
        panic!()
    };
    let compact = crate::queue::encode(record).unwrap();
    let value: serde_json::Value = serde_json::from_str(&compact).unwrap();
    assert_eq!(value["version"], 2);
    assert!(value["prepared"].get("operation").is_some());
    assert!(value["prepared"].get("keyed").is_none());
    assert!(value["prepared"].get("binding").is_none());
    assert!(
        compact.len()
            < crate::queue::encode(&record.view(Some(prepared)))
                .unwrap()
                .len()
    );
    assert_capacity_parity(record, prepared);
    // Codec counting must preserve escaping, Unicode and numeric JSON syntax;
    // these borrowed views exercise serialization, not a new admission authority.
    for number in [
        serde_json::json!(1.0),
        serde_json::json!(1e-8),
        serde_json::json!(9_007_199_254_740_991_u64),
    ] {
        let mut projection = record.clone();
        projection.input.request.text = "é雪\\\"\n".into();
        let mut original = prepared.as_ref().clone();
        original.binding.runtime_semantics = serde_json::json!([number, "é雪\\\"\n"]);
        assert_capacity_parity(&projection, &original);
    }
    // A small current projection must not expand the former full Record cap.
    let mut oversized = prepared.as_ref().clone();
    oversized.binding.runtime_semantics =
        serde_json::Value::String("x".repeat(crate::queue::MAX_RECORD));
    assert!(crate::queue::encode(record).is_ok());
    assert!(record.former_encoded_len(Some(&oversized)).is_err());
    let mut wrong_version = record.clone();
    wrong_version.version = 1;
    assert!(
        wrong_version
            .validate(&fixture.session, &fixture.queue, record.summary.sequence)
            .is_err()
    );
    let mut old = serde_json::to_value(record).unwrap();
    old.as_object_mut().unwrap().remove("version");
    assert!(serde_json::from_value::<crate::queue::Record>(old).is_err());
    let mut old_body = serde_json::to_value(record).unwrap();
    old_body["prepared"] = serde_json::to_value(prepared).unwrap();
    assert!(serde_json::from_value::<crate::queue::Record>(old_body).is_err());
}
fn assert_capacity_parity(record: &crate::queue::Record, prepared: &crate::BoundKeyedAdmitRequest) {
    let compact = crate::queue::encode(record).unwrap();
    let reference = crate::queue::encode(record.prepared.as_ref().unwrap()).unwrap();
    let body = crate::queue::encode(prepared).unwrap();
    let full = crate::queue::encode(&record.view(Some(prepared))).unwrap();
    assert_eq!(
        record.former_encoded_len(Some(prepared)).unwrap(),
        full.len()
    );
    assert_eq!(
        crate::queue::former_encoded_len(compact.len(), Some(reference.len()), Some(body.len()))
            .unwrap(),
        full.len()
    );
    assert!(crate::queue::former_encoded_len(compact.len(), Some(reference.len()), None).is_err());
    assert!(
        crate::queue::former_encoded_len(crate::queue::MAX_RECORD, Some(1), Some(100)).is_err()
    );
}

#[tokio::test]
async fn retained_output_uses_exact_named_carrier_budget() {
    use crate::public_session::v2::proof::Proof;
    let store = crate::MemoryStore::new();
    let (fixture, _, _) = corruption::prepared(&store).await;
    let state = store.state.lock().unwrap();
    let scope = crate::queue::source::Scope {
        session: fixture.session,
        queue: fixture.queue,
    };
    let read = |proof: &mut Proof| super::protocol::record(&state, &scope, proof)?.retained(proof);
    let mut measured = Proof::bounded(32 << 20);
    let output = read(&mut measured).unwrap();
    let exact = measured.used();
    let mut proof = Proof::bounded(exact);
    let repeated = read(&mut proof).unwrap();
    assert_eq!(proof.used(), exact);
    assert_eq!(
        serde_json::to_value(output_view(&output)).unwrap(),
        serde_json::to_value(output_view(&repeated)).unwrap()
    );
    assert!(read(&mut Proof::bounded(exact - 1)).is_err());
    // The whole output named-field shape is charged independently, before clone.
    let mut proof = Proof::bounded(32 << 20);
    let loaded = super::protocol::record(&state, &scope, &mut proof).unwrap();
    let before = proof.used();
    let output = loaded.retained(&mut proof).unwrap();
    assert_eq!(
        proof.used() - before,
        serde_json::to_vec(&output_view(&output)).unwrap().len()
    );
}

fn output_view(output: &crate::RetainedQueue) -> impl serde::Serialize + '_ {
    #[derive(serde::Serialize)]
    struct View<'a> {
        summary: &'a crate::QueueSummary,
        input: &'a crate::QueueInput,
        prepared: &'a Option<crate::BoundKeyedAdmitRequest>,
    }
    View {
        summary: &output.summary,
        input: &output.input,
        prepared: &output.prepared,
    }
}
