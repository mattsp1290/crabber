//! Selected native Run bytes are charged before transfer or decoding.
use super::super::{db, read_proof};
use crate::postgres::transaction::Transaction;
use crate::public_session::v2::proof::Proof;
use crate::{AttemptError as Error, attempt::sql};
use crabber_core::{Run, RunId};
use sqlx::{Postgres, Row};

pub(in crate::postgres) async fn load(
    tx: &mut Transaction<'_, Postgres>,
    id: &RunId,
    proof: &mut Proof,
) -> Result<Run, Error> {
    read_proof::charge(tx, sqlx::query_scalar("SELECT CAST(octet_length(data::text)+octet_length(session_id) AS BIGINT) FROM runs WHERE id=$1").bind(&id.0), proof)
        .await.map_err(|_| Error::UnknownStoreOutcome)?;
    let row = sqlx::query("SELECT CASE WHEN octet_length(session_id)<=512 THEN session_id END AS session_id,CASE WHEN octet_length(data::text)<=33554432 THEN data::text END AS data FROM runs WHERE id=$1 FOR UPDATE")
        .bind(&id.0).fetch_optional(&mut **tx).await.map_err(db).map_err(sql::unknown)?.ok_or(Error::NotFound)?;
    let text = row
        .try_get::<Option<String>, _>("data")
        .map_err(|_| Error::UnknownStoreOutcome)?
        .ok_or(Error::UnknownStoreOutcome)?;
    let run: Run = serde_json::from_str(&text).map_err(|_| Error::UnknownStoreOutcome)?;
    if run.id != *id
        || row
            .try_get::<Option<String>, _>("session_id")
            .map_err(|_| Error::UnknownStoreOutcome)?
            .as_deref()
            != Some(&run.session_id.0)
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(run)
}

pub(in crate::postgres) async fn begin<'a>(
    store: &'a super::super::PostgresStore,
    id: &RunId,
    proof: &mut Proof,
) -> Result<(Transaction<'a, Postgres>, Run), Error> {
    let mut tx = crate::postgres::transaction::begin(&store.pool)
        .await
        .map_err(db)
        .map_err(sql::unknown)?;
    read_proof::charge(
        &mut tx,
        sqlx::query_scalar("SELECT CAST(octet_length(session_id) AS BIGINT) FROM runs WHERE id=$1")
            .bind(&id.0),
        proof,
    )
    .await
    .map_err(|_| Error::UnknownStoreOutcome)?;
    let session: Option<String> = sqlx::query_scalar(
        "SELECT CASE WHEN octet_length(session_id)<=512 THEN session_id END FROM runs WHERE id=$1",
    )
    .bind(&id.0)
    .fetch_optional(&mut *tx)
    .await
    .map_err(db)
    .map_err(sql::unknown)?
    .ok_or(Error::NotFound)?;
    let session = crabber_core::SessionId(session.ok_or(Error::UnknownStoreOutcome)?);
    super::super::lock_session(&mut tx, &session)
        .await
        .map_err(sql::unknown)?;
    let run = load(&mut tx, id, proof).await?;
    if run.session_id != session {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok((tx, run))
}

pub(super) async fn request(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    request: &crate::AttemptMutation,
    authority: Option<&crate::AttemptAuthority>,
    conclusion: Option<&crate::AttemptConclusion>,
    read_proof: &mut Proof,
) -> Result<
    (
        crabber_core::InputFingerprint,
        Option<crate::AttemptMutationOutcome>,
    ),
    Error,
> {
    use super::records;
    use crate::AttemptMutationOutcome as Outcome;
    use crate::native_enrollment::caller;
    read_proof
        .measure(&(run, request, conclusion))
        .map_err(|_| Error::UnknownStoreOutcome)?;
    caller::measure_authority(authority, read_proof).map_err(|_| Error::UnknownStoreOutcome)?;
    let enrollment =
        super::super::native_enrollment::original(tx, &run.session_id, &run.id, read_proof)
            .await
            .map_err(|_| Error::UnknownStoreOutcome)?;
    crate::native_enrollment::access::allow(enrollment).map_err(|_| Error::Unsupported)?;
    let digest = if let Some(conclusion) = conclusion {
        conclusion.fingerprint(&run.session_id, &run.id)?
    } else {
        request.fingerprint(&run.session_id, &run.id)?
    };
    if let Some(receipt) = records::receipt_with_proof(tx, &run.session_id, &request.id, read_proof)
        .await
        .map_err(|error| error.into_domain_or(Error::UnknownStoreOutcome))?
    {
        if receipt.attempt.run_id != run.id || receipt.semantic_digest != digest {
            return Err(Error::SemanticConflict);
        }
        if let Some(conclusion) = conclusion {
            conclusion.verify_replay(&receipt)?;
        }
        return Ok((digest, Some(Outcome::Replayed(receipt))));
    }

    Ok((digest, None))
}
