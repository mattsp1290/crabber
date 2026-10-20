use super::{SqliteStore, db};
use crate::{PublicRunOutcome, StoreError, public_run};
use crabber_core::SessionId;
use sqlx::Row;

const CURRENT: &str = "WITH latest AS (SELECT id,session_id,status,data FROM runs WHERE session_id=$1 ORDER BY seq DESC LIMIT 1), projected AS (SELECT (r.id IS NULL OR (json_extract(r.data,'$.id')=r.id AND json_extract(r.data,'$.session_id')=r.session_id AND json_extract(r.data,'$.status')=r.status)) AS valid, json_object('session_id',s.id,'revision',s.public_revision,'run',CASE WHEN r.id IS NULL THEN NULL ELSE json_object('run_id',r.id,'session_id',r.session_id,'status',json_extract(r.data,'$.status'),'usage',json_object('input_tokens',json_extract(r.data,'$.usage.input_tokens'),'output_tokens',json_extract(r.data,'$.usage.output_tokens')),'created_at',json_extract(r.data,'$.created_at')) END) AS value FROM sessions s LEFT JOIN latest r ON true WHERE s.id=$1) SELECT valid,CASE WHEN length(CAST(value AS BLOB))<=$2 THEN value END AS value FROM projected";
impl SqliteStore {
    pub(super) async fn read_public_run(
        &self,
        session: &SessionId,
        max_bytes: usize,
    ) -> Result<PublicRunOutcome, StoreError> {
        public_run::validate_session(session)?;
        let row = sqlx::query(CURRENT)
            .bind(&session.0)
            .bind(i64::try_from(max_bytes.min(public_run::MAX_BYTES)).unwrap())
            .fetch_optional(&self.readers)
            .await
            .map_err(db)?
            .ok_or(StoreError::NotFound)?;
        if row.get::<Option<bool>, _>("valid") != Some(true) {
            return Err(StoreError::Validation("invalid public run metadata".into()));
        }
        public_run::decode(session, row.get("value"))
    }
}
