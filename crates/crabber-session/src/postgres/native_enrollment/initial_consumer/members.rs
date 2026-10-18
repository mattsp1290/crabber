//! Independently corroborate exact captured native members under SAME gate/Proof.
mod attempt;
mod keyed;
mod parts;
mod records;
use crate::native_enrollment::initial_consumer::{self as domain, Original, Selection};
use crate::postgres::transaction::Transaction;
use crate::public_session::v2::proof::{Failure, Proof, ReadError, unknown};
use sqlx::Postgres;
fn read<E: std::fmt::Debug>(error: ReadError<E>) -> Failure {
    match error {
        ReadError::Budget(failure) => failure,
        ReadError::Domain(error) => unknown(error),
    }
}
fn same<T: serde::Serialize>(actual: &T, expected: &T) -> Result<(), Failure> {
    // Both actual selected native carrier and original consumer carrier were
    // charged before their separate transfers/decodes. Hash these exact borrows.
    if domain::digest(actual)? != domain::digest(expected)? {
        return Err(unknown("changed native consumer member"));
    }
    Ok(())
}
pub(super) async fn verify(
    tx: &mut Transaction<'_, Postgres>,
    view: &Selection<'_>,
    proof: &mut Proof,
) -> Result<(), Failure> {
    for result in &view.results {
        match &result.original {
            Original::Register(value) => {
                audit(tx, &value.reserve, proof).await?;
                audit(tx, &value.begin, proof).await?;
            }
            Original::Activate(value) => audit(tx, &value.audit, proof).await?,
            Original::KeyedBegin(value) => keyed::verify(tx, value, proof).await?,
            Original::ReserveInitial(value) | Original::BeginInitial(value) => {
                attempt::verify(tx, value, proof).await?;
            }
            Original::AssistantMessage(value) => {
                records::message(tx, &value.message, proof).await?;
            }
            Original::Lifecycle(value) => records::event(tx, &value.event, proof).await?,
            Original::Conclude(value) => {
                attempt::verify(tx, &value.attempt, proof).await?;
                records::event(tx, &value.settlement, proof).await?;
                records::run(tx, &value.after_run, proof).await?;
            }
        }
    }
    let mut latest = None;
    for publication in view.publications {
        for result in &view.results {
            if result.digest()?.as_str() == publication.original_digest.as_str() {
                match &result.original {
                    Original::ReserveInitial(value) | Original::BeginInitial(value) => {
                        latest = Some(&value.after);
                    }
                    Original::Conclude(value) => latest = Some(&value.attempt.after),
                    _ => (),
                }
            }
        }
    }
    if let Some(latest) = latest {
        attempt::current(tx, latest, proof).await?;
    }
    Ok(())
}
async fn audit(
    tx: &mut Transaction<'_, Postgres>,
    expected: &crate::session_owner::audit::Audit,
    proof: &mut Proof,
) -> Result<(), Failure> {
    let actual = crate::postgres::session_owner::drivers::selected_consumer_audit(
        tx,
        &expected.receipt.session_id,
        &expected.receipt.operation_id,
        proof,
    )
    .await
    .map_err(read)?
    .ok_or_else(|| unknown("missing native consumer owner audit"))?;
    same(&actual, expected)
}
