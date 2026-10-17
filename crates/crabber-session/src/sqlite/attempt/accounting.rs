//! Selected native Run bytes are charged before transfer or decoding.
use super::super::{db, read_proof};
use crate::public_session::v2::proof::Proof;
use crate::{AttemptError as Error, attempt::sql};
use crabber_core::{Run, RunId};
use sqlx::{Row, Sqlite, Transaction};

pub(super) async fn load(
    tx: &mut Transaction<'_, Sqlite>,
    id: &RunId,
    proof: &mut Proof,
) -> Result<Run, Error> {
    read_proof::charge(tx, sqlx::query_scalar("SELECT CAST(length(CAST(data AS BLOB))+length(CAST(session_id AS BLOB)) AS BIGINT) FROM runs WHERE id=$1").bind(&id.0), proof)
        .await.map_err(|_| Error::UnknownStoreOutcome)?;
    let row = sqlx::query("SELECT CASE WHEN length(CAST(session_id AS BLOB))<=512 THEN session_id END AS session_id,CASE WHEN length(CAST(data AS BLOB))<=33554432 THEN data END AS data FROM runs WHERE id=$1")
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

pub(super) async fn request(
    tx: &mut Transaction<'_, Sqlite>,
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
