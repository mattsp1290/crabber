//! Scope markers are chosen only in the actual first creation transaction.
use super::{Audit, Error, Origin, Postgres, Proof, SessionId, Transaction, failure, unknown};
pub(in super::super) struct Birth {
    pub protocol: i64,
    pub absent: bool,
}
pub(in super::super) async fn context_protocol(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    proof: &mut Proof,
) -> Result<Birth, Error> {
    proof.charge(8).map_err(|error| failure(&error))?;
    let context: Option<i64> = sqlx::query_scalar(
        "SELECT CAST(scope_protocol AS BIGINT) FROM session_owner_contexts WHERE session_id=$1",
    )
    .bind(&session.0)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unknown)?;
    if let Some(protocol) = context {
        return Ok(Birth {
            protocol,
            absent: false,
        });
    }
    proof.charge(8).map_err(|error| failure(&error))?;
    let session_marker: Option<i64> =
        sqlx::query_scalar("SELECT CAST(owner_scope_protocol AS BIGINT) FROM sessions WHERE id=$1")
            .bind(&session.0)
            .fetch_optional(&mut **tx)
            .await
            .map_err(unknown)?;
    if let Some(protocol) = session_marker {
        return Ok(Birth {
            protocol,
            absent: false,
        });
    }
    proof.charge(1).map_err(|error| failure(&error))?;
    let prior: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM session_owner_audits WHERE session_id=$1 UNION ALL SELECT 1 FROM session_owner_current WHERE session_id=$1 UNION ALL SELECT 1 FROM session_owner_scope_origins WHERE session_id=$1 UNION ALL SELECT 1 FROM session_owner_publication_cohorts WHERE session_id=$1 UNION ALL SELECT 1 FROM session_owner_publications WHERE session_id=$1)")
        .bind(&session.0).fetch_one(&mut **tx).await.map_err(unknown)?;
    if prior {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(Birth {
        protocol: 1,
        absent: true,
    })
}
pub(in super::super) async fn record_absent(
    tx: &mut Transaction<'_, Postgres>,
    audit: &Audit,
    birth: &Birth,
) -> Result<(), Error> {
    if birth.absent {
        insert(tx, &Origin::absent(audit)?).await?;
    }
    Ok(())
}
pub(in crate::postgres) async fn created(
    tx: &mut Transaction<'_, Postgres>,
    session: &crabber_core::Session,
    original: &crate::deletion::origin::OwnershipOrigin,
) -> Result<(), crate::StoreError> {
    let marker: i64 =
        sqlx::query_scalar("SELECT CAST(owner_scope_protocol AS BIGINT) FROM sessions WHERE id=$1")
            .bind(&session.id.0)
            .fetch_one(&mut **tx)
            .await
            .map_err(super::super::super::db)?;
    if marker == 0 {
        return Ok(());
    }
    if marker != 1 {
        return Err(crate::StoreError::Validation(
            "invalid owner scope protocol".into(),
        ));
    }
    let present: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM session_owner_scope_origins WHERE session_id=$1)",
    )
    .bind(&session.id.0)
    .fetch_one(&mut **tx)
    .await
    .map_err(super::super::super::db)?;
    if !present {
        let source = super::source::created(
            tx,
            &session.id,
            &original.operation_id,
            &mut Proof::bounded(32 * 1024 * 1024),
        )
        .await
        .map_err(|_| crate::StoreError::Validation("creation source unavailable".into()))?;
        insert(
            tx,
            &Origin::created(session, original, &source.receipt, &source.audit)
                .map_err(|_| crate::StoreError::Validation("invalid scope source".into()))?,
        )
        .await
        .map_err(|_| crate::StoreError::Validation("scope source persistence failed".into()))?;
    }
    Ok(())
}
async fn insert(tx: &mut Transaction<'_, Postgres>, origin: &Origin) -> Result<(), Error> {
    sqlx::query("INSERT INTO session_owner_scope_origins(session_id,data_json) VALUES($1,$2)")
        .bind(&origin.session.0)
        .bind(origin.encoded()?)
        .execute(&mut **tx)
        .await
        .map_err(unknown)?;
    Ok(())
}
