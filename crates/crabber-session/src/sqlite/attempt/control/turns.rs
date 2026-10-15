use crate::{
    AttemptError as Error,
    attempt_control::{TurnContext, TurnState},
};
use crabber_core::{EventRecord, Run};
use sqlx::{Row, Sqlite, Transaction};
pub(super) async fn read(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    source: Option<crabber_core::TurnId>,
) -> Result<TurnContext, Error> {
    let mut state = TurnState::new(run, source)?;
    let mut after = 0i64;
    loop {
        let rows=sqlx::query("SELECT seq,CASE WHEN length(CAST(data AS BLOB))<=16384 THEN data END AS event FROM events WHERE session_id=$1 AND run_id=$2 AND seq>$3 AND json_extract(data,'$.kind') IN ('turn_started','turn_completed') ORDER BY seq LIMIT 16").bind(&run.session_id.0).bind(&run.id.0).bind(after).fetch_all(&mut **tx).await.map_err(|_|Error::UnknownStoreOutcome)?;
        if rows.is_empty() {
            break;
        }
        for row in rows {
            let text: String = row
                .get::<Option<String>, _>("event")
                .ok_or(Error::UnknownStoreOutcome)?;
            let event: EventRecord =
                serde_json::from_str(&text).map_err(|_| Error::UnknownStoreOutcome)?;
            state.observe(run, &event)?;
            after = row.get("seq");
        }
    }
    Ok(state.finish())
}
