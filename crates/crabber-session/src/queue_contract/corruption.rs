use super::*;
pub(crate) async fn prepared(store: &dyn Store) -> (Fixture, QueueMutation, QueueAuthority) {
    let fixture = fixture(store).await;
    store.mutate_queue(fixture.enqueue.clone()).await.unwrap();
    let authority = claim(store, &fixture).await;
    let prepare = mutation(
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
pub(crate) fn replace(
    record: &mut Record,
    audit: &Audit,
    source: Option<&crate::queue::source::Source>,
    change_input: bool,
) -> Result<(), StoreError> {
    if change_input {
        record.input.input = InputEnvelope::text(
            "COHERENTLY_REPLACED_INPUT",
            record.input.input.expected_revision(),
        )
        .unwrap();
    }
    let mut replacement = crate::typed_admission_contract::prepare(
        crate::execution_binding_contract::fixture(
            &record.summary.session_id,
            OffsetDateTime::UNIX_EPOCH,
            "changed-backend",
            'b',
        ),
        record.input.input.clone(),
        OffsetDateTime::UNIX_EPOCH,
    );
    replacement.keyed.options = record.input.admission.clone();
    let mut counterfeit = audit.clone();
    let QueueAction::Prepare { prepared, .. } = &mut counterfeit.request.action else {
        return Err(crate::queue::corrupt());
    };
    **prepared = replacement;
    counterfeit.receipt.semantic_digest = counterfeit.request.fingerprint()?;
    let counterfeit_source = source
        .map(|original| {
            crate::queue::source::Source::new(
                &counterfeit,
                original.sequence,
                original.previous.clone(),
            )
        })
        .transpose()?;
    record.prepared = Some(crate::queue::PreparedReference::published(
        &counterfeit,
        counterfeit_source.as_ref(),
        &mut crate::public_session::v2::proof::Proof::bounded(32 << 20),
    )?);
    // These are internally consistent but differ from the authoritative original audits.
    record.validate(
        &record.summary.session_id,
        &record.summary.queue_id,
        record.summary.sequence,
    )?;
    Ok(())
}
pub(crate) async fn refused(
    store: &dyn Store,
    fixture: &Fixture,
    original: QueueMutation,
    authority: QueueAuthority,
) {
    assert!(
        store
            .retained_queue(&fixture.session, &fixture.queue)
            .await
            .is_err()
    );
    assert!(matches!(
        store.mutate_queue(original).await.unwrap(),
        QueueOutcome::Replayed(_)
    ));
    let admit = mutation(
        store,
        fixture,
        QueueAction::Admit {
            authority,
            owner: "corrupt-winner".into(),
            lease: Duration::from_secs(30),
        },
    )
    .await;
    assert!(store.mutate_queue(admit).await.is_err());
    assert!(
        store
            .lookup_admission(&fixture.session, &fixture.prepared.keyed.options.key)
            .await
            .unwrap()
            .is_none()
    );
}
