use super::*;
pub(crate) async fn run(store: &impl Store, arm: impl Fn(u8)) {
    let (admitted, _, _, planned) = crate::phase_contract::fixture(store).await;
    let request = uncertainty(
        &admitted,
        planned.command.target().attempt.clone(),
        store.session_revision(&admitted.session.id).await.unwrap(),
    );
    let before = store.get_run(&request.run_id).await.unwrap();
    arm(1);
    assert_eq!(
        store
            .mark_attempt_uncertain(request.clone())
            .await
            .unwrap_err(),
        AttemptError::UnknownStoreOutcome
    );
    arm(0);
    assert_eq!(store.get_run(&request.run_id).await.unwrap(), before);
    assert_eq!(
        store
            .lookup_attempt_mutation(&request.session_id, &request.id)
            .await
            .unwrap(),
        None
    );
    arm(2);
    assert_eq!(
        store
            .mark_attempt_uncertain(request.clone())
            .await
            .unwrap_err(),
        AttemptError::UnknownStoreOutcome
    );
    let AttemptControlOutcome::Replayed(marked) =
        store.mark_attempt_uncertain(request.clone()).await.unwrap()
    else {
        panic!("replay with lost ACK still armed");
    };
    arm(0);
    let retire = retirement(&request, marked.revision);
    let before = store.get_run(&request.run_id).await.unwrap();
    let messages = store.list_all_messages(&request.session_id).await.unwrap();
    arm(1);
    assert_eq!(
        store.retire_attempt(retire.clone()).await.unwrap_err(),
        AttemptError::UnknownStoreOutcome
    );
    arm(0);
    assert_eq!(store.get_run(&request.run_id).await.unwrap(), before);
    assert_eq!(
        store.list_all_messages(&request.session_id).await.unwrap(),
        messages
    );
    assert_eq!(
        store
            .lookup_attempt_mutation(&request.session_id, &retire.id)
            .await
            .unwrap(),
        None
    );
    arm(2);
    assert_eq!(
        store.retire_attempt(retire.clone()).await.unwrap_err(),
        AttemptError::UnknownStoreOutcome
    );
    let AttemptControlOutcome::Replayed(saved) = store.retire_attempt(retire).await.unwrap() else {
        panic!("retirement replay while fault armed");
    };
    arm(0);
    assert_eq!(saved.attempt.cleanup, AttemptCleanup::Complete);
}
