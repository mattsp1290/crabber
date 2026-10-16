//! Materialization accounting shared by every nested aggregate proof reader.
use crate::public_session::v2::proof::{Failure, Proof, unknown};
use sqlx::{Sqlite, Transaction};
pub(super) async fn charge<'q>(
    tx: &mut Transaction<'_, Sqlite>,
    query: sqlx::query::QueryScalar<'q, Sqlite, Option<i64>, sqlx::sqlite::SqliteArguments<'q>>,
    proof: &mut Proof,
) -> Result<(), Failure> {
    if !proof.active() {
        return Ok(());
    }
    let bytes = query
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?
        .flatten()
        .unwrap_or(0);
    proof.charge(usize::try_from(bytes).map_err(unknown)?)
}
