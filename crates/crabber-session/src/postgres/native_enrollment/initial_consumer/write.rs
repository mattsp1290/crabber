//! Actual consumer originals and head CAS remain in the supplied Session transaction.
use crate::native_enrollment::initial_consumer::{Correlation, Origin, RecordDelta};
use crate::postgres::transaction::Transaction;
use crate::public_session::v2::proof::{Failure, Proof, unknown};
use crabber_core::InputFingerprint;
use serde::Serialize;
use sqlx::Postgres;

enum Value<'a> {
    Text(&'a str),
}
fn tag(value: &impl Serialize) -> Result<String, Failure> {
    let raw = serde_json::to_string(value).map_err(unknown)?;
    Ok(raw.trim_matches('"').to_owned())
}
fn encode(value: &impl Serialize, proof: &mut Proof) -> Result<String, Failure> {
    proof.measure(value)?;
    serde_json::to_string(value).map_err(unknown)
}
async fn insert<T: Serialize>(
    tx: &mut Transaction<'_, Postgres>,
    table: &str,
    correlation: &Correlation,
    digest: &InputFingerprint,
    value: &T,
    extras: &[(&str, Value<'_>)],
    proof: &mut Proof,
) -> Result<(), Failure> {
    let data = encode(value, proof)?;
    proof.measure(&(correlation, digest))?;
    let columns = extras
        .iter()
        .map(|(name, _)| *name)
        .collect::<Vec<_>>()
        .join(",");
    let mut q = sqlx::QueryBuilder::<Postgres>::new(format!(
        "INSERT INTO {table}(session_id,source_key,epoch_id,record_digest,data_json{}{columns}) VALUES(",
        if extras.is_empty() { "" } else { "," }
    ));
    {
        let mut b = q.separated(",");
        b.push_bind(&correlation.key.session_id.0)
            .push_bind(correlation.key.identity.as_str())
            .push_bind(&correlation.identity.epoch.0)
            .push_bind(digest.as_str())
            .push_bind(data);
        for (_, value) in extras {
            match value {
                Value::Text(s) => {
                    b.push_bind(*s);
                }
            }
        }
    }
    q.push(")");
    q.build().execute(&mut **tx).await.map_err(unknown)?;
    Ok(())
}
pub(super) async fn persist(
    tx: &mut Transaction<'_, Postgres>,
    delta: &RecordDelta,
    proof: &mut Proof,
) -> Result<(), Failure> {
    bootstrap(tx, delta, proof).await?;
    members(tx, delta, proof).await?;
    let p = &delta.publication;
    let kind = tag(&p.kind)?;
    // Optional predecessor is a real nullable scalar, not a replacement digest.
    let data = encode(p, proof)?;
    sqlx::query("INSERT INTO initial_consumer_publications(session_id,source_key,epoch_id,record_digest,sequence,kind,original_digest,previous_digest,data_json) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)").bind(&p.correlation.key.session_id.0).bind(p.correlation.key.identity.as_str()).bind(&p.correlation.identity.epoch.0).bind(p.digest.as_str()).bind(i64::try_from(p.sequence).map_err(unknown)?).bind(kind).bind(p.original_digest.as_str()).bind(p.previous_digest.as_ref().map(InputFingerprint::as_str)).bind(data).execute(&mut **tx).await.map_err(unknown)?;
    head(tx, delta, proof).await?;
    // Absorbing close is last: canonical publication/head already include it.
    if let Some(a) = &delta.close {
        proof.measure(a)?;
        let d = a.digest()?;
        insert(
            tx,
            "initial_consumer_closes",
            a.correlation(),
            &d,
            a,
            &[
                ("anchor_digest", Value::Text(a.anchor_digest.as_str())),
                (
                    "birth_close_digest",
                    Value::Text(a.birth_close_digest.as_str()),
                ),
            ],
            proof,
        )
        .await?;
    }
    Ok(())
}

async fn head(
    tx: &mut Transaction<'_, Postgres>,
    delta: &RecordDelta,
    proof: &mut Proof,
) -> Result<(), Failure> {
    let h = &delta.head;
    let data = encode(h, proof)?;
    let changed = if h.sequence == 1 {
        sqlx::query("INSERT INTO initial_consumer_heads(session_id,source_key,epoch_id,record_digest,sequence,origin_digest,data_json) VALUES($1,$2,$3,$4,1,$5,$6)").bind(&h.correlation.key.session_id.0).bind(h.correlation.key.identity.as_str()).bind(&h.correlation.identity.epoch.0).bind(h.digest.as_str()).bind(h.origin_digest.as_str()).bind(data).execute(&mut **tx).await.map_err(unknown)?.rows_affected()
    } else {
        let previous = delta
            .publication
            .previous_digest
            .as_ref()
            .ok_or_else(|| unknown("consumer predecessor"))?;
        sqlx::query("UPDATE initial_consumer_heads SET sequence=$4,record_digest=$5,data_json=$6 WHERE session_id=$1 AND source_key=$2 AND epoch_id=$3 AND sequence=$7 AND record_digest=$8 AND origin_digest=$9").bind(&h.correlation.key.session_id.0).bind(h.correlation.key.identity.as_str()).bind(&h.correlation.identity.epoch.0).bind(i64::try_from(h.sequence).map_err(unknown)?).bind(h.digest.as_str()).bind(data).bind(i64::try_from(h.sequence-1).map_err(unknown)?).bind(previous.as_str()).bind(h.origin_digest.as_str()).execute(&mut **tx).await.map_err(unknown)?.rows_affected()
    };
    if changed != 1 {
        return Err(unknown("consumer head CAS"));
    }
    Ok(())
}

async fn bootstrap(
    tx: &mut Transaction<'_, Postgres>,
    delta: &RecordDelta,
    proof: &mut Proof,
) -> Result<(), Failure> {
    if let Some(origin) = &delta.origin {
        proof.measure(origin)?;
        let digest = origin.digest()?;
        let kind = match origin {
            Origin::Opened(_) => "opened",
            Origin::NoOpenedMember(_) => "no_opened_member",
        };
        for table in ["initial_consumer_origins", "initial_consumer_cohorts"] {
            insert(
                tx,
                table,
                origin.correlation(),
                &digest,
                origin,
                &[("origin_kind", Value::Text(kind))],
                proof,
            )
            .await?;
        }
    }
    if let Some(a) = &delta.assignment {
        proof.measure(a)?;
        let d = a.digest()?;
        insert(
            tx,
            "initial_consumer_assignments",
            a.correlation(),
            &d,
            a,
            &[("run_id", Value::Text(&a.assignment.run_id.0))],
            proof,
        )
        .await?;
    }

    Ok(())
}
async fn members(
    tx: &mut Transaction<'_, Postgres>,
    delta: &RecordDelta,
    proof: &mut Proof,
) -> Result<(), Failure> {
    if let Some(a) = &delta.call {
        proof.measure(a)?;
        let d = a.digest()?;
        let kind = tag(&a.kind)?;
        insert(
            tx,
            "initial_consumer_calls",
            a.correlation(),
            &d,
            a,
            &[
                ("call_id", Value::Text(&a.call.0)),
                ("kind", Value::Text(&kind)),
                ("input_digest", Value::Text(a.input_digest.as_str())),
            ],
            proof,
        )
        .await?;
    }
    if let Some(a) = &delta.result {
        proof.measure(a)?;
        let d = a.digest()?;
        insert(
            tx,
            "initial_consumer_results",
            a.correlation(),
            &d,
            a,
            &[
                ("call_id", Value::Text(&a.call.0)),
                (
                    "registration_digest",
                    Value::Text(a.registration_digest.as_str()),
                ),
            ],
            proof,
        )
        .await?;
    }
    if let Some(a) = &delta.roster {
        proof.measure(a)?;
        let d = a.digest()?;
        insert(
            tx,
            "initial_consumer_roster",
            a.correlation(),
            &d,
            a,
            &[
                ("call_id", Value::Text(&a.call.0)),
                (
                    "registration_digest",
                    Value::Text(a.registration_digest.as_str()),
                ),
                ("result_digest", Value::Text(a.result_digest.as_str())),
            ],
            proof,
        )
        .await?;
    }
    for a in &delta.reverse {
        proof.measure(a)?;
        let d = a.digest()?;
        let kind = tag(&a.kind)?;
        insert(
            tx,
            "initial_consumer_reverse",
            a.correlation(),
            &d,
            a,
            &[
                ("call_id", Value::Text(&a.call.0)),
                ("kind", Value::Text(&kind)),
                ("member_key", Value::Text(a.member_key.as_str())),
                ("result_digest", Value::Text(a.result_digest.as_str())),
            ],
            proof,
        )
        .await?;
    }

    Ok(())
}
