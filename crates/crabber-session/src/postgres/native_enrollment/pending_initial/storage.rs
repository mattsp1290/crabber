//! Selected finite protocol records; originals and indexed routing share one Proof.
use crate::native_enrollment::pending_initial::{
    Assignment, Bundle, CloseRecord, Epoch, EpochKind, Head, Publication, PublicationKind,
    RegisteredComplete, SourceKey, SourceSlot,
};
use crate::postgres::transaction::Transaction;
use crate::public_session::v2::proof::{Failure, Proof, unknown};
use crabber_core::ControlId;
use serde::{Serialize, de::DeserializeOwned};
use sqlx::{Postgres, Row};
use std::fmt::Write as _;

#[derive(Default)]
pub(in crate::postgres) struct Selected {
    pub slot: Option<SourceSlot>,
    pub epoch: Option<Epoch>,
    pub registered: Option<RegisteredComplete>,
    pub assignment: Option<Assignment>,
    pub close: Option<CloseRecord>,
    pub publications: Vec<Publication>,
    pub head: Option<Head>,
}
const TABLES: [&str; 7] = [
    "pending_initial_source_slots",
    "pending_initial_epochs",
    "pending_initial_registered_completes",
    "pending_initial_assignments",
    "pending_initial_closes",
    "pending_initial_publications",
    "pending_initial_heads",
];

pub(in crate::postgres) async fn load(
    tx: &mut Transaction<'_, Postgres>,
    key: &SourceKey,
    expected_epoch: &ControlId,
    proof: &mut Proof,
) -> Result<Selected, Failure> {
    proof.measure(&(key, expected_epoch))?;
    let slot: Option<SourceSlot> = one(tx, TABLES[0], key, expected_epoch, None, proof).await?;
    cohort(tx, key, expected_epoch, slot.as_ref(), proof).await?;
    let selected = match &slot {
        Some(slot) => &slot.epoch,
        None => expected_epoch,
    };
    let epoch = one(tx, TABLES[1], key, selected, slot.as_ref(), proof).await?;
    let registered = one(tx, TABLES[2], key, selected, slot.as_ref(), proof).await?;
    let assignment = one(tx, TABLES[3], key, selected, slot.as_ref(), proof).await?;
    let close = one(tx, TABLES[4], key, selected, slot.as_ref(), proof).await?;
    let publications = rows(tx, TABLES[5], key, selected, slot.as_ref(), proof).await?;
    let head = one(tx, TABLES[6], key, selected, slot.as_ref(), proof).await?;
    if slot.is_none()
        && (epoch.is_some()
            || registered.is_some()
            || assignment.is_some()
            || close.is_some()
            || !publications.is_empty()
            || head.is_some())
    {
        return Err(unknown("orphan Pending epoch"));
    }
    Ok(Selected {
        slot,
        epoch,
        registered,
        assignment,
        close,
        publications,
        head,
    })
}
async fn one<T: Stored>(
    tx: &mut Transaction<'_, Postgres>,
    table: &str,
    key: &SourceKey,
    epoch: &ControlId,
    slot: Option<&SourceSlot>,
    proof: &mut Proof,
) -> Result<Option<T>, Failure> {
    let mut selected = rows(tx, table, key, epoch, slot, proof).await?;
    if selected.len() > 1 {
        return Err(unknown("duplicate Pending selected record"));
    }
    Ok(selected.pop())
}
async fn rows<T: Stored>(
    tx: &mut Transaction<'_, Postgres>,
    table: &str,
    key: &SourceKey,
    epoch: &ControlId,
    slot: Option<&SourceSlot>,
    proof: &mut Proof,
) -> Result<Vec<T>, Failure> {
    if !TABLES.contains(&table) {
        return Err(unknown("Pending table"));
    }
    // Only fixed internal identifiers enter SQL. Input values are always bound.
    let scope = "((session_id=$1 AND source_key=$2) OR epoch_id=$3)";
    let (extra_cost, extra_columns) = extras(table);
    let cost = format!(
        "SELECT count(*), COALESCE(sum(octet_length(data_json)+octet_length(session_id)+octet_length(source_key)+octet_length(epoch_id)+octet_length(epoch_digest)+{extra_cost}),0) FROM {table} WHERE {scope}"
    );
    let (count, bytes): (i64, i64) = sqlx::query_as(&cost)
        .bind(&key.session_id.0)
        .bind(key.identity.as_str())
        .bind(&epoch.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    proof.charge(16)?;
    proof.charge(usize::try_from(bytes).map_err(unknown)?)?;
    if count > if table == TABLES[5] { 3 } else { 1 } {
        return Err(unknown("Pending finite protocol membership"));
    }
    let query = format!(
        "SELECT session_id,source_key,epoch_id,epoch_digest,{extra_columns}CASE WHEN octet_length(data_json)<=8192 THEN data_json END AS data FROM {table} WHERE {scope}{}",
        if table == TABLES[5] {
            " ORDER BY sequence"
        } else {
            ""
        }
    );
    let selected = sqlx::query(&query)
        .bind(&key.session_id.0)
        .bind(key.identity.as_str())
        .bind(&epoch.0)
        .fetch_all(&mut **tx)
        .await
        .map_err(unknown)?;
    let mut out = Vec::with_capacity(selected.len());
    for row in selected {
        let session: String = row.try_get("session_id").map_err(unknown)?;
        let source: String = row.try_get("source_key").map_err(unknown)?;
        let actual_epoch: String = row.try_get("epoch_id").map_err(unknown)?;
        let actual_digest: String = row.try_get("epoch_digest").map_err(unknown)?;
        if session != key.session_id.0
            || source != key.identity.as_str()
            || slot.is_some_and(|slot| {
                actual_epoch != slot.epoch.0 || actual_digest != slot.epoch_digest.as_str()
            })
        {
            return Err(unknown("Pending indexed routing"));
        }
        let raw: Option<String> = row.try_get("data").map_err(unknown)?;
        let value: T =
            serde_json::from_str(&raw.ok_or_else(|| unknown("Pending selected body cap"))?)
                .map_err(unknown)?;
        if !value.matches(&row)? {
            return Err(unknown("Pending body/index equality"));
        }
        out.push(value);
    }
    Ok(out)
}
pub(super) fn encode(value: &impl Serialize, proof: &mut Proof) -> Result<String, Failure> {
    proof.measure(value)?;
    if crate::snapshot::encoded_bytes(value, 8192).is_none() {
        return Err(unknown("Pending record cap"));
    }
    serde_json::to_string(value).map_err(unknown)
}

trait Stored: DeserializeOwned {
    fn routing(&self) -> (&SourceKey, &ControlId);
    fn fields(&self, row: &sqlx::postgres::PgRow) -> Result<bool, Failure>;
    fn matches(&self, row: &sqlx::postgres::PgRow) -> Result<bool, Failure> {
        let (key, epoch) = self.routing();
        Ok(text(row, "session_id")? == key.session_id.0
            && text(row, "source_key")? == key.identity.as_str()
            && text(row, "epoch_id")? == epoch.0
            && self.fields(row)?)
    }
}
fn text<'a>(row: &'a sqlx::postgres::PgRow, column: &str) -> Result<&'a str, Failure> {
    row.try_get(column).map_err(unknown)
}
impl Stored for SourceSlot {
    fn routing(&self) -> (&SourceKey, &ControlId) {
        (&self.key, &self.epoch)
    }
    fn fields(&self, row: &sqlx::postgres::PgRow) -> Result<bool, Failure> {
        Ok(text(row, "epoch_digest")? == self.epoch_digest.as_str())
    }
}
impl Stored for Epoch {
    fn routing(&self) -> (&SourceKey, &ControlId) {
        (&self.correlation.key, &self.correlation.identity.epoch)
    }
    fn fields(&self, row: &sqlx::postgres::PgRow) -> Result<bool, Failure> {
        let opened = self.kind == EpochKind::Opened;
        Ok(text(row, "epoch_digest")? == self.digest()?.as_str()
            && text(row, "kind")? == if opened { "Opened" } else { "SealedBeforeOpen" }
            && text(row, "complete_call")? == self.correlation.identity.complete.0
            && row
                .try_get::<Option<&str>, _>("required_complete")
                .map_err(unknown)?
                == opened.then_some(self.correlation.identity.complete.0.as_str())
            && row
                .try_get::<Option<&str>, _>("required_close")
                .map_err(unknown)?
                == (!opened).then_some(self.correlation.identity.seal.0.as_str()))
    }
}
impl Stored for RegisteredComplete {
    fn routing(&self) -> (&SourceKey, &ControlId) {
        (&self.key, &self.epoch)
    }
    fn fields(&self, row: &sqlx::postgres::PgRow) -> Result<bool, Failure> {
        Ok(text(row, "call_id")? == self.call.0
            && text(row, "epoch_digest")? == self.epoch_digest.as_str())
    }
}
impl Stored for Assignment {
    fn routing(&self) -> (&SourceKey, &ControlId) {
        (&self.correlation.key, &self.correlation.identity.epoch)
    }
    fn fields(&self, row: &sqlx::postgres::PgRow) -> Result<bool, Failure> {
        Ok(text(row, "call_id")? == self.call.0
            && text(row, "run_id")? == self.run_id.0
            && text(row, "birth_kind")? == "pending"
            && text(row, "epoch_digest")? == self.epoch_digest.as_str())
    }
}
impl Stored for CloseRecord {
    fn routing(&self) -> (&SourceKey, &ControlId) {
        (&self.correlation.key, &self.correlation.identity.epoch)
    }
    fn fields(&self, row: &sqlx::postgres::PgRow) -> Result<bool, Failure> {
        Ok(text(row, "seal_id")? == self.correlation.identity.seal.0
            && text(row, "epoch_digest")? == self.epoch_digest.as_str())
    }
}
impl Stored for Publication {
    fn routing(&self) -> (&SourceKey, &ControlId) {
        (&self.key, &self.epoch)
    }
    fn fields(&self, row: &sqlx::postgres::PgRow) -> Result<bool, Failure> {
        Ok(row.try_get::<i64, _>("sequence").map_err(unknown)?
            == i64::try_from(self.sequence).map_err(unknown)?
            && text(row, "kind")? == kind(self.kind)
            && text(row, "publication_digest")? == self.digest.as_str())
    }
}
impl Stored for Head {
    fn routing(&self) -> (&SourceKey, &ControlId) {
        (&self.key, &self.epoch)
    }
    fn fields(&self, row: &sqlx::postgres::PgRow) -> Result<bool, Failure> {
        Ok(row.try_get::<i64, _>("sequence").map_err(unknown)?
            == i64::try_from(self.sequence).map_err(unknown)?
            && text(row, "publication_digest")? == self.digest.as_str())
    }
}
fn kind(kind: PublicationKind) -> &'static str {
    match kind {
        PublicationKind::Opened => "Opened",
        PublicationKind::Assigned => "Assigned",
        PublicationKind::Closed => "Closed",
    }
}
fn extras(table: &str) -> (String, &'static str) {
    let (columns, names, scalars) = match table {
        "pending_initial_epochs" => (
            "kind,complete_call,required_complete,required_close,",
            &[
                "kind",
                "complete_call",
                "required_complete",
                "required_close",
            ][..],
            0,
        ),
        "pending_initial_registered_completes" => ("call_id,", &["call_id"][..], 0),
        "pending_initial_assignments" => (
            "call_id,run_id,birth_kind,",
            &["call_id", "run_id", "birth_kind"][..],
            0,
        ),
        "pending_initial_closes" => ("seal_id,", &["seal_id"][..], 0),
        "pending_initial_publications" => (
            "sequence,kind,publication_digest,",
            &["kind", "publication_digest"][..],
            8,
        ),
        "pending_initial_heads" => (
            "sequence,publication_digest,",
            &["publication_digest"][..],
            8,
        ),
        _ => ("", &[][..], 0),
    };
    let mut cost = scalars.to_string();
    for name in names {
        let _ = write!(cost, "+COALESCE(octet_length({name}),0)");
    }
    (cost, columns)
}

impl Selected {
    pub(in crate::postgres) fn view(&self) -> Bundle<'_> {
        Bundle {
            slot: self.slot.as_ref(),
            epoch: self.epoch.as_ref(),
            registered: self.registered.as_ref(),
            assignment: self.assignment.as_ref(),
            close: self.close.as_ref(),
            publications: &self.publications,
            head: self.head.as_ref(),
        }
    }
}

async fn cohort(
    tx: &mut Transaction<'_, Postgres>,
    key: &SourceKey,
    epoch: &ControlId,
    slot: Option<&SourceSlot>,
    proof: &mut Proof,
) -> Result<(), Failure> {
    let (count,bytes):(i64,i64)=sqlx::query_as("SELECT count(*),COALESCE(sum(octet_length(session_id)+octet_length(source_key)+octet_length(epoch_id)+octet_length(epoch_digest)),0) FROM pending_initial_cohorts WHERE ((session_id=$1 AND source_key=$2) OR epoch_id=$3)")
  .bind(&key.session_id.0).bind(key.identity.as_str()).bind(&epoch.0).fetch_one(&mut **tx).await.map_err(unknown)?;
    proof.charge(16)?;
    proof.charge(usize::try_from(bytes).map_err(unknown)?)?;
    if count != i64::from(slot.is_some()) {
        return Err(unknown("Pending cohort/projection membership"));
    }
    if let Some(slot) = slot {
        let (session,source,selected,digest):(String,String,String,String)=sqlx::query_as("SELECT session_id,source_key,epoch_id,epoch_digest FROM pending_initial_cohorts WHERE session_id=$1 AND source_key=$2")
   .bind(&key.session_id.0).bind(key.identity.as_str()).fetch_one(&mut **tx).await.map_err(unknown)?;
        if session != key.session_id.0
            || source != key.identity.as_str()
            || selected != slot.epoch.0
            || digest != slot.epoch_digest.as_str()
        {
            return Err(unknown("Pending cohort original identity"));
        }
    }
    Ok(())
}
