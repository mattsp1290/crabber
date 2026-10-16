//! Protected issuer slots and a monotonic source head, never execution authority.
use crate::native_parent::source_protocol::{JournalHead, JournalSource};
use crate::native_parent::{Error, RunIssuance, RunState};
use crate::public_session::v2::proof::{Proof, ReadError};
use crabber_core::{InputFingerprint, RunId, SessionId};
use sqlx::{Row, Sqlite, Transaction};
type Result<T> = std::result::Result<T, ReadError<Error>>;
fn bad(_: impl std::fmt::Debug) -> ReadError<Error> {
    ReadError::Domain(Error)
}

pub(super) async fn protocol(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    run: &RunId,
    proof: &mut Proof,
) -> Result<bool> {
    crate::native_parent::source_protocol::validate_scope(session, run).map_err(bad)?;
    proof
        .charge(session.0.len() + run.0.len() + 8)
        .map_err(ReadError::Budget)?;
    let value: i32 = sqlx::query_scalar(
        "SELECT CAST(native_source_protocol AS INTEGER) FROM runs WHERE session_id=$1 AND id=$2",
    )
    .bind(&session.0)
    .bind(&run.0)
    .fetch_one(&mut **tx)
    .await
    .map_err(bad)?;
    match value {
        1 => Ok(true),
        0 => {
            let session_protocol: i32 = sqlx::query_scalar(
                "SELECT CAST(native_run_protocol AS INTEGER) FROM sessions WHERE id=$1",
            )
            .bind(&session.0)
            .fetch_one(&mut **tx)
            .await
            .map_err(bad)?;
            if session_protocol != 0 {
                return Err(bad("missing fresh Run protocol"));
            }

            let orphan:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM native_run_sources WHERE run_id=$1) OR EXISTS(SELECT 1 FROM native_run_source_heads WHERE run_id=$1)")
            .bind(&run.0).fetch_one(&mut **tx).await.map_err(bad)?;
            if orphan {
                Err(bad("legacy protected source"))
            } else {
                Ok(false)
            }
        }
        _ => Err(bad("source protocol")),
    }
}

pub(super) async fn source(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    run: &RunId,
    sequence: u64,
    proof: &mut Proof,
) -> Result<Option<JournalSource>> {
    let seq = i64::try_from(sequence).map_err(bad)?;
    super::super::read_proof::charge(tx,sqlx::query_scalar("SELECT CAST(length(CAST(session_id AS BLOB))+length(CAST(run_id AS BLOB))+length(CAST(issuance_digest AS BLOB))+COALESCE(length(CAST(previous_digest AS BLOB)),0)+length(CAST(chain_digest AS BLOB))+16 AS BIGINT) FROM native_run_sources WHERE session_id=$1 AND run_id=$2 AND sequence=$3")
        .bind(&session.0).bind(&run.0).bind(seq),proof).await.map_err(ReadError::Budget)?;
    let row=sqlx::query("SELECT sequence,CAST(protocol_version AS INTEGER) AS protocol_version,issuance_digest,previous_digest,chain_digest FROM native_run_sources WHERE session_id=$1 AND run_id=$2 AND sequence=$3")
        .bind(&session.0).bind(&run.0).bind(seq).fetch_optional(&mut **tx).await.map_err(bad)?;
    row.map(|r| {
        let value = JournalSource {
            version: u32::try_from(r.try_get::<i32, _>("protocol_version").map_err(bad)?)
                .map_err(bad)?,
            session_id: session.clone(),
            run_id: run.clone(),
            sequence: u64::try_from(r.try_get::<i64, _>("sequence").map_err(bad)?).map_err(bad)?,
            issuance_digest: InputFingerprint::new(
                r.try_get::<String, _>("issuance_digest").map_err(bad)?,
            )
            .map_err(bad)?,
            previous_digest: r
                .try_get::<Option<String>, _>("previous_digest")
                .map_err(bad)?
                .map(InputFingerprint::new)
                .transpose()
                .map_err(bad)?,
            chain_digest: InputFingerprint::new(
                r.try_get::<String, _>("chain_digest").map_err(bad)?,
            )
            .map_err(bad)?,
        };
        value.validate().map_err(bad)?;
        Ok(value)
    })
    .transpose()
}

pub(super) async fn head(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    run: &RunId,
    proof: &mut Proof,
) -> Result<Option<JournalHead>> {
    super::super::read_proof::charge(tx,sqlx::query_scalar("SELECT CAST(length(CAST(session_id AS BLOB))+length(CAST(run_id AS BLOB))+length(CAST(origin_digest AS BLOB))+length(CAST(chain_digest AS BLOB))+16 AS BIGINT) FROM native_run_source_heads WHERE session_id=$1 AND run_id=$2")
        .bind(&session.0).bind(&run.0),proof).await.map_err(ReadError::Budget)?;
    let row=sqlx::query("SELECT sequence,CAST(protocol_version AS INTEGER) AS protocol_version,origin_digest,chain_digest FROM native_run_source_heads WHERE session_id=$1 AND run_id=$2")
        .bind(&session.0).bind(&run.0).fetch_optional(&mut **tx).await.map_err(bad)?;
    row.map(|r| {
        let value = JournalHead {
            version: u32::try_from(r.try_get::<i32, _>("protocol_version").map_err(bad)?)
                .map_err(bad)?,
            session_id: session.clone(),
            run_id: run.clone(),
            sequence: u64::try_from(r.try_get::<i64, _>("sequence").map_err(bad)?).map_err(bad)?,
            origin_digest: InputFingerprint::new(
                r.try_get::<String, _>("origin_digest").map_err(bad)?,
            )
            .map_err(bad)?,
            chain_digest: InputFingerprint::new(
                r.try_get::<String, _>("chain_digest").map_err(bad)?,
            )
            .map_err(bad)?,
        };
        value.validate().map_err(bad)?;
        Ok(value)
    })
    .transpose()
}

pub(in super::super) async fn append(
    tx: &mut Transaction<'_, Sqlite>,
    issuer: &RunIssuance,
    creation: bool,
    proof: &mut Proof,
) -> Result<()> {
    let session = &issuer.state.session_id;
    let run = &issuer.state.run_id;
    proof.measure(issuer).map_err(ReadError::Budget)?;
    issuer.validate().map_err(bad)?;
    if !protocol(tx, session, run, proof).await? {
        return Ok(());
    }
    let previous = head(tx, session, run, proof).await?;
    if creation != previous.is_none() {
        return Err(bad("source birth"));
    }
    let value = JournalSource::new(previous.as_ref(), issuer).map_err(bad)?;
    sqlx::query("INSERT INTO native_run_sources(session_id,run_id,sequence,issuance_digest,previous_digest,chain_digest) VALUES($1,$2,$3,$4,$5,$6)")
        .bind(&session.0).bind(&run.0).bind(i64::try_from(value.sequence).map_err(bad)?).bind(value.issuance_digest.as_str()).bind(value.previous_digest.as_ref().map(InputFingerprint::as_str)).bind(value.chain_digest.as_str())
        .execute(&mut **tx).await.map_err(bad)?;
    Ok(())
}

pub(super) async fn verify_previous(
    tx: &mut Transaction<'_, Sqlite>,
    previous: &RunState,
    latest: Option<&RunIssuance>,
    proof: &mut Proof,
) -> Result<()> {
    let session = &previous.session_id;
    let run = &previous.run_id;
    if !history(tx, session, run, proof).await? {
        return Ok(());
    }
    let head = head(tx, session, run, proof)
        .await?
        .ok_or_else(|| bad("missing source head"))?;
    let latest = latest.ok_or_else(|| bad("missing latest issuer"))?;
    proof.measure(latest).map_err(ReadError::Budget)?;
    if latest.sequence != head.sequence || latest.state != *previous {
        return Err(bad("current source rollback"));
    }
    let current = source(tx, session, run, head.sequence, proof)
        .await?
        .ok_or_else(|| bad("missing current slot"))?;
    current.verify(latest).map_err(bad)?;
    let original = source(tx, session, run, 1, proof)
        .await?
        .ok_or_else(|| bad("missing original slot"))?;
    if original.chain_digest != head.origin_digest || current.chain_digest != head.chain_digest {
        return Err(bad("head source binding"));
    }
    Ok(())
}

/// Historical metadata validates only the selected protected slot and its own
/// chain links. Later projection/head corruption cannot revoke an original ACK.
pub(in super::super) async fn original(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    run: &RunId,
    sequence: u64,
    proof: &mut Proof,
) -> Result<Option<RunIssuance>> {
    let protected = protocol(tx, session, run, proof).await?;
    let issuer = issuer(tx, session, run, sequence, proof).await?;
    if !protected {
        return Ok(issuer);
    }
    let selected = source(tx, session, run, sequence, proof).await?;
    if issuer.is_none() && selected.is_none() && sequence > 1 {
        source(tx, session, run, 1, proof)
            .await?
            .ok_or_else(|| bad("missing source origin"))?;
        return Ok(None);
    }
    let (Some(issuer), Some(selected)) = (issuer, selected) else {
        return Err(bad("missing selected source/projection"));
    };
    selected.verify(&issuer).map_err(bad)?;
    let first = source(tx, session, run, 1, proof)
        .await?
        .ok_or_else(|| bad("missing source origin"))?;
    if sequence > 1 {
        let previous = source(tx, session, run, sequence - 1, proof)
            .await?
            .ok_or_else(|| bad("missing preceding source"))?;
        if selected.previous_digest.as_ref() != Some(&previous.chain_digest) {
            return Err(bad("selected source chain"));
        }
    } else if selected != first {
        return Err(bad("original source slot"));
    }
    Ok(Some(issuer))
}
async fn issuer(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    run: &RunId,
    sequence: u64,
    proof: &mut Proof,
) -> Result<Option<RunIssuance>> {
    let sequence = i64::try_from(sequence).map_err(bad)?;
    super::super::read_proof::charge(tx,sqlx::query_scalar("SELECT CAST(length(CAST(data_json AS BLOB))+8 AS BIGINT) FROM native_run_issuances WHERE session_id=$1 AND run_id=$2 AND sequence=$3")
        .bind(&session.0).bind(&run.0).bind(sequence),proof).await.map_err(ReadError::Budget)?;
    let row=sqlx::query("SELECT CASE WHEN length(CAST(data_json AS BLOB))<=2048 THEN data_json END AS record FROM native_run_issuances WHERE session_id=$1 AND run_id=$2 AND sequence=$3")
        .bind(&session.0).bind(&run.0).bind(sequence).fetch_optional(&mut **tx).await.map_err(bad)?;
    row.map(|row| {
        let text = row
            .try_get::<Option<String>, _>("record")
            .map_err(bad)?
            .ok_or_else(|| bad("issuer cap"))?;
        let issuer: RunIssuance = serde_json::from_str(&text).map_err(bad)?;
        issuer.validate().map_err(bad)?;
        if issuer.state.session_id != *session
            || issuer.state.run_id != *run
            || i64::try_from(issuer.sequence).map_err(bad)? != sequence
        {
            return Err(bad("issuer routing"));
        }
        Ok(issuer)
    })
    .transpose()
}
/// Bounded O(1)-memory fold authenticates every retained slot and independently
/// rejects an erased tail, missing middle, extra projection, or current rollback.
pub(in super::super) async fn history(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    run: &RunId,
    proof: &mut Proof,
) -> Result<bool> {
    if !protocol(tx, session, run, proof).await? {
        return Ok(false);
    }
    let head = head(tx, session, run, proof)
        .await?
        .ok_or_else(|| bad("missing source head"))?;
    let mut stream = crate::native_parent::source_protocol::Stream::default();
    let mut after = 0i64;
    loop {
        proof.charge(8).map_err(ReadError::Budget)?;
        let next:Option<i64>=sqlx::query_scalar("SELECT sequence FROM native_run_sources WHERE run_id=$1 AND sequence>$2 ORDER BY sequence LIMIT 1")
            .bind(&run.0).bind(after).fetch_optional(&mut **tx).await.map_err(bad)?;
        let Some(next) = next else { break };
        let seq = u64::try_from(next).map_err(bad)?;
        let selected = source(tx, session, run, seq, proof)
            .await?
            .ok_or_else(|| bad("foreign source scope"))?;
        let issuer = issuer(tx, session, run, seq, proof)
            .await?
            .ok_or_else(|| bad("missing issuer projection"))?;
        stream.observe(&selected, &issuer).map_err(bad)?;
        after = next;
    }
    stream.finish(&head).map_err(bad)?;
    let extra:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM native_run_issuances i LEFT JOIN native_run_sources s ON s.session_id=i.session_id AND s.run_id=i.run_id AND s.sequence=i.sequence WHERE i.run_id=$1 AND (i.session_id<>$2 OR s.run_id IS NULL))")
        .bind(&run.0).bind(&session.0).fetch_one(&mut **tx).await.map_err(bad)?;
    if extra {
        return Err(bad("unregistered issuer projection"));
    }
    Ok(true)
}

#[cfg(test)]
#[path = "source_protocol/tests.rs"]
mod tests;

/// Called only with `scope_origin`'s independently authenticated birth decision.
pub(in super::super) async fn context_birth(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    absent: bool,
    proof: &mut Proof,
) -> Result<i64> {
    proof.charge(16).map_err(ReadError::Budget)?;
    let marker:Option<i64>=sqlx::query_scalar("SELECT COALESCE((SELECT CAST(native_run_protocol AS BIGINT) FROM session_owner_contexts WHERE session_id=$1),(SELECT CAST(native_run_protocol AS BIGINT) FROM sessions WHERE id=$1))").bind(&session.0).fetch_one(&mut **tx).await.map_err(bad)?;
    match marker {
        Some(0) => Ok(0),
        Some(1) => Ok(1),
        None if absent => Ok(1),
        _ => Err(bad("native context birth")),
    }
}

#[cfg(test)]
#[path = "source_protocol/begin_tests.rs"]
mod begin_tests;

#[path = "source_protocol/current.rs"]
mod current;
pub(in super::super) use current::read as current;

#[cfg(test)]
#[path = "source_protocol/budget_tests.rs"]
mod budget_tests;
