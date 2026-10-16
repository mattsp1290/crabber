//! Setwise physical erasure; the caller commits the tombstone in this transaction.
use crate::{DeletionError as Error, sql_deletion::VerifiedScope};
use sqlx::{Postgres, Transaction};

pub(super) async fn purge(
    tx: &mut Transaction<'_, Postgres>,
    scope: &VerifiedScope,
) -> Result<(), Error> {
    super::super::lock_session_scope(tx, scope.session())
        .await
        .map_err(super::unknown)?;
    let current: Option<i64> =
        sqlx::query_scalar("SELECT public_revision FROM sessions WHERE id=$1")
            .bind(&scope.session().0)
            .fetch_optional(&mut **tx)
            .await
            .map_err(super::unknown)?;
    let current = current.ok_or(Error::NotFound)?;
    if u64::try_from(current).map_err(super::unknown)? != scope.revision().get() {
        return Err(Error::RevisionConflict);
    }
    let corrupt: bool = sqlx::query_scalar(crate::sql_deletion::CROSS_SCOPE)
        .bind(&scope.session().0)
        .fetch_one(&mut **tx)
        .await
        .map_err(super::unknown)?;
    if corrupt {
        return Err(Error::UnknownStoreOutcome);
    }
    for statement in crate::sql_deletion::PURGE {
        sqlx::query(statement)
            .bind(&scope.session().0)
            .execute(&mut **tx)
            .await
            .map_err(super::unknown)?;
    }
    let erased = sqlx::query("DELETE FROM sessions WHERE id=$1 AND public_revision=$2")
        .bind(&scope.session().0)
        .bind(current)
        .execute(&mut **tx)
        .await
        .map_err(super::unknown)?;
    if erased.rows_affected() != 1 {
        return Err(Error::RevisionConflict);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
