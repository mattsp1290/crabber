//! Complete selected originals, precharged across every table before transfer.
use crate::native_enrollment::initial_consumer::{
    self as domain, AssignmentLink, CallResult, ConsumerClose, Correlation, Head, Origin,
    Publication, RegisteredCall, ReverseMembership, RosterEntry,
};
use crate::public_session::v2::proof::{Failure, Proof, unknown};
use crate::transaction::Transaction;
use serde::{Serialize, de::DeserializeOwned};
use sqlx::{Row, Sqlite};

const TABLES: [&str; 10] = [
    "initial_consumer_origins",
    "initial_consumer_cohorts",
    "initial_consumer_assignments",
    "initial_consumer_calls",
    "initial_consumer_results",
    "initial_consumer_roster",
    "initial_consumer_reverse",
    "initial_consumer_publications",
    "initial_consumer_heads",
    "initial_consumer_closes",
];
#[derive(Default)]
pub(in crate::sqlite) struct Selected {
    pub origin: Option<Origin>,
    pub cohort: Option<Origin>,
    pub assignment: Option<AssignmentLink>,
    pub calls: Vec<RegisteredCall>,
    pub results: Vec<CallResult>,
    pub roster: Vec<RosterEntry>,
    pub reverse: Vec<ReverseMembership>,
    pub publications: Vec<Publication>,
    pub head: Option<Head>,
    pub close: Option<ConsumerClose>,
}
impl Selected {
    pub(super) fn selection(&self, proof: &mut Proof) -> Result<domain::Selection<'_>, Failure> {
        let count = self
            .calls
            .len()
            .checked_add(self.results.len())
            .and_then(|n| n.checked_add(self.roster.len()))
            .and_then(|n| n.checked_add(self.reverse.len()))
            .ok_or_else(|| unknown("consumer selected reference count"))?;
        proof.charge(
            count
                .checked_mul(std::mem::size_of::<&RegisteredCall>())
                .ok_or_else(|| unknown("consumer selected reference allocation"))?,
        )?;
        Ok(domain::Selection {
            origin: self.origin.as_ref(),
            cohort: self.cohort.as_ref(),
            assignment: self.assignment.as_ref(),
            calls: self.calls.iter().collect(),
            results: self.results.iter().collect(),
            roster: self.roster.iter().collect(),
            reverse: self.reverse.iter().collect(),
            publications: &self.publications,
            head: self.head.as_ref(),
            close: self.close.as_ref(),
        })
    }
}
fn scope(table: &str) -> String {
    let embedded = if table == TABLES[0] || table == TABLES[1] {
        "(json_extract(data_json,'$.record.correlation.key.session_id')=$1 AND json_extract(data_json,'$.record.correlation.key.identity')=$2) OR json_extract(data_json,'$.record.correlation.identity.epoch')=$3"
    } else {
        "(json_extract(data_json,'$.correlation.key.session_id')=$1 AND json_extract(data_json,'$.correlation.key.identity')=$2) OR json_extract(data_json,'$.correlation.identity.epoch')=$3"
    };
    format!("((session_id=$1 AND source_key=$2) OR epoch_id=$3 OR {embedded})")
}
/// All scalar sets are measured first; no table may transfer partial bodies
/// before the complete selected set has been admitted under this SAME Proof.
pub(super) async fn load(
    tx: &mut Transaction<'_, Sqlite>,
    expected: &Correlation,
    proof: &mut Proof,
) -> Result<Selected, Failure> {
    expected.validate(proof)?;
    let mut counts = [0usize; 10];
    for (i, table) in TABLES.iter().enumerate() {
        let columns = columns(table);
        let cost = format!(
            "SELECT count(*),COALESCE(sum(length(CAST(data_json AS BLOB))+length(CAST(session_id AS BLOB))+length(CAST(source_key AS BLOB))+length(CAST(epoch_id AS BLOB))+length(CAST(record_digest AS BLOB))+{}),0) FROM {table} WHERE {}",
            columns.0,
            scope(table)
        );
        let (count, bytes): (i64, i64) = sqlx::query_as(&cost)
            .bind(&expected.key.session_id.0)
            .bind(expected.key.identity.as_str())
            .bind(&expected.identity.epoch.0)
            .fetch_one(&mut **tx)
            .await
            .map_err(unknown)?;
        proof.charge(16)?;
        proof.charge(usize::try_from(bytes).map_err(unknown)?)?;
        counts[i] = usize::try_from(count).map_err(unknown)?;
    }
    Ok(Selected {
        origin: one(rows(tx, TABLES[0], expected, counts[0]).await?)?,
        cohort: one(rows(tx, TABLES[1], expected, counts[1]).await?)?,
        assignment: one(rows(tx, TABLES[2], expected, counts[2]).await?)?,
        calls: rows(tx, TABLES[3], expected, counts[3]).await?,
        results: rows(tx, TABLES[4], expected, counts[4]).await?,
        roster: rows(tx, TABLES[5], expected, counts[5]).await?,
        reverse: rows(tx, TABLES[6], expected, counts[6]).await?,
        publications: rows(tx, TABLES[7], expected, counts[7]).await?,
        head: one(rows(tx, TABLES[8], expected, counts[8]).await?)?,
        close: one(rows(tx, TABLES[9], expected, counts[9]).await?)?,
    })
}
fn one<T>(mut rows: Vec<T>) -> Result<Option<T>, Failure> {
    if rows.len() > 1 {
        return Err(unknown("consumer duplicate singleton"));
    }
    Ok(rows.pop())
}
async fn rows<T: Stored>(
    tx: &mut Transaction<'_, Sqlite>,
    table: &str,
    expected: &Correlation,
    count: usize,
) -> Result<Vec<T>, Failure> {
    let query = format!(
        "SELECT session_id,source_key,epoch_id,record_digest,{},data_json FROM {table} WHERE {}{}",
        columns(table).1,
        scope(table),
        if table == TABLES[7] {
            " ORDER BY sequence"
        } else {
            ""
        }
    );
    let rows = sqlx::query(&query)
        .bind(&expected.key.session_id.0)
        .bind(expected.key.identity.as_str())
        .bind(&expected.identity.epoch.0)
        .fetch_all(&mut **tx)
        .await
        .map_err(unknown)?;
    if rows.len() != count {
        return Err(unknown("consumer selected membership changed"));
    }
    rows.into_iter()
        .map(|row| {
            let raw: String = row.try_get("data_json").map_err(unknown)?;
            let value: T = serde_json::from_str(&raw).map_err(unknown)?;
            let correlation = value.correlation();
            if row.try_get::<String, _>("session_id").map_err(unknown)?
                != correlation.key.session_id.0
                || row.try_get::<String, _>("source_key").map_err(unknown)?
                    != correlation.key.identity.as_str()
                || row.try_get::<String, _>("epoch_id").map_err(unknown)?
                    != correlation.identity.epoch.0
                || row.try_get::<String, _>("record_digest").map_err(unknown)?
                    != value.stored_digest()?.as_str()
            {
                return Err(unknown("consumer indexed original routing"));
            }
            value.indexed(&row)?;
            Ok(value)
        })
        .collect()
}
trait Stored: Serialize + DeserializeOwned {
    fn correlation(&self) -> &Correlation;
    fn stored_digest(&self) -> Result<crabber_core::InputFingerprint, Failure>;
    fn indexed(&self, row: &sqlx::sqlite::SqliteRow) -> Result<(), Failure>;
}

fn columns(table: &str) -> (&'static str, &'static str) {
    match table {
        "initial_consumer_origins" | "initial_consumer_cohorts" => {
            ("length(CAST(origin_kind AS BLOB))", "origin_kind")
        }
        "initial_consumer_assignments" => ("length(CAST(run_id AS BLOB))", "run_id"),
        "initial_consumer_calls" => (
            "length(CAST(call_id AS BLOB))+length(CAST(kind AS BLOB))+length(CAST(input_digest AS BLOB))",
            "call_id,kind,input_digest",
        ),
        "initial_consumer_results" => (
            "length(CAST(call_id AS BLOB))+length(CAST(registration_digest AS BLOB))",
            "call_id,registration_digest",
        ),
        "initial_consumer_roster" => (
            "length(CAST(call_id AS BLOB))+length(CAST(registration_digest AS BLOB))+length(CAST(result_digest AS BLOB))",
            "call_id,registration_digest,result_digest",
        ),
        "initial_consumer_reverse" => (
            "length(CAST(call_id AS BLOB))+length(CAST(kind AS BLOB))+length(CAST(member_key AS BLOB))+length(CAST(result_digest AS BLOB))",
            "call_id,kind,member_key,result_digest",
        ),
        "initial_consumer_publications" => (
            "8+length(CAST(kind AS BLOB))+length(CAST(original_digest AS BLOB))+COALESCE(length(CAST(previous_digest AS BLOB)),0)",
            "sequence,kind,original_digest,previous_digest",
        ),
        "initial_consumer_heads" => (
            "8+length(CAST(origin_digest AS BLOB))",
            "sequence,origin_digest",
        ),
        "initial_consumer_closes" => (
            "length(CAST(anchor_digest AS BLOB))+length(CAST(birth_close_digest AS BLOB))",
            "anchor_digest,birth_close_digest",
        ),
        _ => unreachable!("closed consumer table"),
    }
}
fn tag(value: &impl Serialize) -> Result<String, Failure> {
    let raw = serde_json::to_string(value).map_err(unknown)?;
    Ok(raw.trim_matches('"').to_owned())
}
impl Stored for Origin {
    fn correlation(&self) -> &Correlation {
        self.correlation()
    }
    fn stored_digest(&self) -> Result<crabber_core::InputFingerprint, Failure> {
        self.digest()
    }
    fn indexed(&self, row: &sqlx::sqlite::SqliteRow) -> Result<(), Failure> {
        same(
            row,
            "origin_kind",
            match self {
                Origin::Opened(_) => "opened",
                Origin::NoOpenedMember(_) => "no_opened_member",
            },
        )?;
        Ok(())
    }
}
impl Stored for AssignmentLink {
    fn correlation(&self) -> &Correlation {
        self.correlation()
    }
    fn stored_digest(&self) -> Result<crabber_core::InputFingerprint, Failure> {
        self.digest()
    }
    fn indexed(&self, row: &sqlx::sqlite::SqliteRow) -> Result<(), Failure> {
        same(row, "run_id", &self.assignment.run_id.0)?;
        Ok(())
    }
}
impl Stored for RegisteredCall {
    fn correlation(&self) -> &Correlation {
        self.correlation()
    }
    fn stored_digest(&self) -> Result<crabber_core::InputFingerprint, Failure> {
        self.digest()
    }
    fn indexed(&self, row: &sqlx::sqlite::SqliteRow) -> Result<(), Failure> {
        same(row, "call_id", &self.call.0)?;
        same(row, "kind", &tag(&self.kind)?)?;
        same(row, "input_digest", self.input_digest.as_str())?;
        Ok(())
    }
}
impl Stored for CallResult {
    fn correlation(&self) -> &Correlation {
        self.correlation()
    }
    fn stored_digest(&self) -> Result<crabber_core::InputFingerprint, Failure> {
        self.digest()
    }
    fn indexed(&self, row: &sqlx::sqlite::SqliteRow) -> Result<(), Failure> {
        same(row, "call_id", &self.call.0)?;
        same(
            row,
            "registration_digest",
            self.registration_digest.as_str(),
        )?;
        Ok(())
    }
}
impl Stored for RosterEntry {
    fn correlation(&self) -> &Correlation {
        self.correlation()
    }
    fn stored_digest(&self) -> Result<crabber_core::InputFingerprint, Failure> {
        self.digest()
    }
    fn indexed(&self, row: &sqlx::sqlite::SqliteRow) -> Result<(), Failure> {
        same(row, "call_id", &self.call.0)?;
        same(
            row,
            "registration_digest",
            self.registration_digest.as_str(),
        )?;
        same(row, "result_digest", self.result_digest.as_str())?;
        Ok(())
    }
}
impl Stored for ReverseMembership {
    fn correlation(&self) -> &Correlation {
        self.correlation()
    }
    fn stored_digest(&self) -> Result<crabber_core::InputFingerprint, Failure> {
        self.digest()
    }
    fn indexed(&self, row: &sqlx::sqlite::SqliteRow) -> Result<(), Failure> {
        same(row, "call_id", &self.call.0)?;
        same(row, "kind", &tag(&self.kind)?)?;
        same(row, "member_key", self.member_key.as_str())?;
        same(row, "result_digest", self.result_digest.as_str())?;
        Ok(())
    }
}
impl Stored for ConsumerClose {
    fn correlation(&self) -> &Correlation {
        self.correlation()
    }
    fn stored_digest(&self) -> Result<crabber_core::InputFingerprint, Failure> {
        self.digest()
    }
    fn indexed(&self, row: &sqlx::sqlite::SqliteRow) -> Result<(), Failure> {
        same(row, "anchor_digest", self.anchor_digest.as_str())?;
        same(row, "birth_close_digest", self.birth_close_digest.as_str())?;
        Ok(())
    }
}
impl Stored for Publication {
    fn correlation(&self) -> &Correlation {
        self.correlation()
    }
    fn stored_digest(&self) -> Result<crabber_core::InputFingerprint, Failure> {
        self.canonical_digest()
    }
    fn indexed(&self, row: &sqlx::sqlite::SqliteRow) -> Result<(), Failure> {
        number(row, self.sequence)?;
        same(row, "kind", &tag(&self.kind)?)?;
        same(row, "original_digest", self.original_digest.as_str())?;
        if row
            .try_get::<Option<String>, _>("previous_digest")
            .map_err(unknown)?
            .as_deref()
            != self
                .previous_digest
                .as_ref()
                .map(crabber_core::InputFingerprint::as_str)
        {
            return Err(unknown("consumer original previous digest"));
        }
        Ok(())
    }
}
impl Stored for Head {
    fn correlation(&self) -> &Correlation {
        self.correlation()
    }
    fn stored_digest(&self) -> Result<crabber_core::InputFingerprint, Failure> {
        Ok(self.digest.clone())
    }
    fn indexed(&self, row: &sqlx::sqlite::SqliteRow) -> Result<(), Failure> {
        number(row, self.sequence)?;
        same(row, "origin_digest", self.origin_digest.as_str())?;
        Ok(())
    }
}
fn same(row: &sqlx::sqlite::SqliteRow, column: &str, expected: &str) -> Result<(), Failure> {
    if row.try_get::<String, _>(column).map_err(unknown)? != expected {
        return Err(unknown("consumer indexed original scalar"));
    }
    Ok(())
}
fn number(row: &sqlx::sqlite::SqliteRow, expected: u64) -> Result<(), Failure> {
    if u64::try_from(row.try_get::<i64, _>("sequence").map_err(unknown)?).map_err(unknown)?
        != expected
    {
        return Err(unknown("consumer indexed sequence"));
    }
    Ok(())
}
