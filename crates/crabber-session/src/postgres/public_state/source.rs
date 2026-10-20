//! Native source attribution is selected under the same transaction as publication.
use super::super::{PostgresStore, db};
use super::{
    Error, UPSERT_STATE, bump_public_revision, finish, public_state, record_receipt,
    saved_with_proof, snapshot,
};
use crate::postgres::transaction::Transaction;
use crate::public_session::v2::proof::Proof;
use crate::{PublicSourceAuthority as Authority, PublicSourceMutation, PublicStateReceipt};
use crabber_core::{EventCursor, EventRecord, PublicSourceChange, Run, RunFence, SessionId};
use sqlx::{Postgres, Row};

impl PostgresStore {
    pub(in crate::postgres) async fn write_public_source(
        &self,
        mutation: PublicSourceMutation,
        authority: Authority,
        fence: Option<&RunFence>,
    ) -> Result<PublicStateReceipt, Error> {
        mutation.validate()?;
        let mut tx = self
            .session_transaction(authority.session())
            .await
            .map_err(|_| Error::UnknownStoreOutcome)?;
        let result = Box::pin(write(self, &mut tx, &mutation, &authority, fence)).await;
        let result = finish(tx, result).await;
        #[cfg(test)]
        if self
            .public_state_fault
            .load(std::sync::atomic::Ordering::SeqCst)
            == 2
            && result.is_ok()
        {
            return Err(Error::UnknownStoreOutcome);
        }
        result
    }
}
async fn write(
    store: &PostgresStore,
    tx: &mut Transaction<'_, Postgres>,
    mutation: &PublicSourceMutation,
    authority: &Authority,
    fence: Option<&RunFence>,
) -> Result<PublicStateReceipt, Error> {
    let mut proof = Proof::bounded(32 << 20);
    let SelectedSource { run, source, owner } = select_source(tx, authority, &mut proof).await?;
    let session = authority.session();
    if fence.is_some_and(|fence| fence.run_id != run.id) {
        return Err(Error::StaleOwner);
    }
    if let Some(saved) = saved_with_proof(tx, session, &mutation.id, &mut proof).await? {
        public_state::source_matches(&saved.event, &source, mutation, &mut proof)?;
        return Ok(saved.receipt);
    }
    Box::pin(verify_fresh(
        tx,
        authority,
        fence,
        &run,
        owner.as_ref(),
        store.clock.now(),
        &mut proof,
    ))
    .await?;
    charge_state(tx, session, &mut proof).await?;
    let current = snapshot(tx, session).await?;
    if current.revision != mutation.expected_revision {
        return Err(Error::RevisionConflict);
    }
    let revision = current
        .revision
        .next()
        .map_err(|_| Error::RevisionExhausted)?;
    proof
        .measure(&current.contents)
        .map_err(|_| Error::InvalidRequest)?;
    proof
        .measure(&mutation.change)
        .map_err(|_| Error::InvalidRequest)?;
    let changed = mutation
        .change
        .apply(&current.contents)
        .map_err(|_| Error::InvalidRequest)?;
    proof.measure(&changed).map_err(|_| Error::InvalidRequest)?;
    proof.measure(&source).map_err(|_| Error::InvalidRequest)?;
    let (mut event, mut digest) = public_state::source_event(
        source.clone(),
        mutation,
        revision,
        changed.clone(),
        store.clock.now(),
        &mut proof,
    )?;
    if matches!(
        &mutation.change,
        PublicSourceChange::StateDelta { .. } | PublicSourceChange::ActivityDelta { .. }
    ) {
        digest =
            append_before(store, tx, mutation, source, current, &mut event, &mut proof).await?;
    }
    let cursor = append(tx, &event, &mut proof).await?;
    persist_state(tx, &run, changed, &mut proof).await?;
    bump_public_revision(tx, session)
        .await
        .map_err(public_state::unknown)?;
    let saved = PublicStateReceipt {
        session_id: session.clone(),
        run_id: run.id,
        operation_id: mutation.id.clone(),
        semantic_digest: digest,
        revision,
        event_cursor: cursor,
    };
    #[cfg(test)]
    if store
        .public_state_fault
        .load(std::sync::atomic::Ordering::SeqCst)
        == 1
    {
        return Err(Error::UnknownStoreOutcome);
    }
    record_receipt(tx, &saved).await?;
    Ok(saved)
}

async fn append_before(
    store: &PostgresStore,
    tx: &mut Transaction<'_, Postgres>,
    mutation: &PublicSourceMutation,
    source: crabber_core::PublicSourceIdentity,
    current: crate::PublicStateSnapshot,
    event: &mut EventRecord,
    proof: &mut Proof,
) -> Result<crabber_core::InputFingerprint, Error> {
    proof
        .measure(&current.contents)
        .map_err(|_| Error::InvalidRequest)?;
    proof
        .measure(current.contents.state())
        .map_err(|_| Error::InvalidRequest)?;
    let initial = PublicSourceMutation {
        id: mutation.id.clone(),
        expected_revision: current.revision,
        change: PublicSourceChange::StateSnapshot {
            state: current.contents.state().clone(),
        },
    };
    proof
        .measure(&current.contents)
        .map_err(|_| Error::InvalidRequest)?;
    let (before, _) = public_state::source_event(
        source,
        &initial,
        current.revision,
        current.contents.clone(),
        store.clock.now(),
        proof,
    )?;
    let cursor = append(tx, &before, proof).await?;
    public_state::attach_before(event, current.contents, cursor, proof)
}

struct SelectedSource {
    run: Run,
    source: crabber_core::PublicSourceIdentity,
    owner: Option<crate::session_owner::record::Record>,
}
async fn select_source(
    tx: &mut Transaction<'_, Postgres>,
    authority: &Authority,
    proof: &mut Proof,
) -> Result<SelectedSource, Error> {
    let session = authority.session();
    super::super::deletion::ensure_live(tx, session)
        .await
        .map_err(|error| match error {
            crate::StoreError::NotFound | crate::StoreError::SessionDeleted => Error::NotFound,
            _ => Error::UnknownStoreOutcome,
        })?;
    let owner = match authority {
        Authority::Invocation(owner) => Some(
            super::super::session_owner::current_owner_with_proof(
                tx,
                session,
                &owner.target().id,
                proof,
            )
            .await
            .map_err(|_| Error::UnknownStoreOutcome)?,
        ),
        Authority::Attempt(_) | Authority::Phase(_) => None,
    };
    if let Some(record) = &owner {
        super::super::session_owner::invocation::verify(tx, record, proof)
            .await
            .map_err(|_| Error::UnknownStoreOutcome)?;
    }
    let run_id = if let Some(record) = &owner {
        &record
            .snapshot
            .binding
            .parent
            .as_ref()
            .ok_or(Error::UnknownStoreOutcome)?
            .run_id
    } else {
        authority.run().ok_or(Error::StaleOwner)?
    };
    let run = super::super::session_owner::run::load(tx, session, run_id)
        .await
        .map_err(|error| {
            if error == crate::session_owner::SessionOwnerError::NotFound {
                Error::NotFound
            } else {
                Error::UnknownStoreOutcome
            }
        })?;
    super::super::credential_access::check(tx, &run, proof)
        .await
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let workspace:Option<String>=sqlx::query_scalar("SELECT CASE WHEN octet_length(data->>'workspace_id')<=512 THEN data->>'workspace_id' END FROM sessions WHERE id=$1").bind(&session.0).fetch_optional(&mut **tx).await.map_err(db).map_err(public_state::unknown)?.flatten();
    let source = authority.identity(
        &workspace.ok_or(Error::NotFound)?,
        &run,
        owner.as_ref(),
        proof,
    )?;
    Ok(SelectedSource { run, source, owner })
}
async fn verify_fresh(
    tx: &mut Transaction<'_, Postgres>,
    authority: &Authority,
    fence: Option<&RunFence>,
    run: &Run,
    owner: Option<&crate::session_owner::record::Record>,
    now: time::OffsetDateTime,
    proof: &mut Proof,
) -> Result<(), Error> {
    if !matches!(authority, Authority::Invocation(_)) {
        super::super::credential_access::check_current(tx, run, proof)
            .await
            .map_err(|_| Error::StaleOwner)?;
    }
    if let Some(fence) = fence {
        public_state::verify_owner(run, fence, now)?;
    }
    let attempt = if authority.attempt().is_some() {
        super::super::attempt::records::load_with_proof(tx, &run.id, None, proof)
            .await
            .map_err(|_| Error::UnknownStoreOutcome)?
            .map(|pair| pair.0)
    } else {
        None
    };
    let phase = if let Some(target) = authority.phase() {
        super::super::phase::load_with_proof(tx, &run.id, &target.id, proof)
            .await
            .map_err(|_| Error::UnknownStoreOutcome)?
    } else {
        None
    };
    authority.verify_current(run, attempt.as_ref(), phase.as_ref(), owner, now, proof)?;
    if let Some(attempt) = &attempt {
        super::super::native_parent::verify_public_source(tx, run, attempt, phase.as_ref(), proof)
            .await
            .map_err(|_| Error::StaleOwner)?;
    }
    Ok(())
}
async fn persist_state(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    contents: crabber_core::PublicStateContents,
    proof: &mut Proof,
) -> Result<(), Error> {
    proof
        .measure(contents.state())
        .and_then(|()| proof.measure(&contents.activities()))
        .map_err(|_| Error::UnknownStoreOutcome)?;
    sqlx::query(UPSERT_STATE)
        .bind(&run.session_id.0)
        .bind(serde_json::to_string(contents.state()).map_err(|_| Error::InvalidRequest)?)
        .bind(serde_json::to_string(contents.activities()).map_err(|_| Error::InvalidRequest)?)
        .execute(&mut **tx)
        .await
        .map_err(db)
        .map_err(public_state::unknown)?;
    Ok(())
}
async fn append(
    tx: &mut Transaction<'_, Postgres>,
    event: &EventRecord,
    proof: &mut Proof,
) -> Result<EventCursor, Error> {
    let seq: i64 = sqlx::query_scalar(
        "INSERT INTO events(session_id,run_id,data) VALUES($1,$2,$3) RETURNING seq",
    )
    .bind(&event.session_id.0)
    .bind(&event.run_id.0)
    .bind(public_state::encode_source_event(event, proof)?)
    .fetch_one(&mut **tx)
    .await
    .map_err(db)
    .map_err(public_state::unknown)?;
    Ok(EventCursor(
        u64::try_from(seq).map_err(|_| Error::UnknownStoreOutcome)?,
    ))
}
pub(super) async fn event_at(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    cursor: EventCursor,
    proof: &mut Proof,
) -> Result<EventRecord, Error> {
    charge_event(tx, session, cursor, proof).await?;
    let row=sqlx::query("SELECT CASE WHEN octet_length(data::text)<=4194304 THEN data END AS data,run_id FROM events WHERE session_id=$1 AND seq=$2").bind(&session.0).bind(i64::try_from(cursor.0).map_err(|_|Error::UnknownStoreOutcome)?)
        .fetch_optional(&mut **tx).await.map_err(db).map_err(public_state::unknown)?.ok_or(Error::UnknownStoreOutcome)?;
    let mut event = public_state::decode_source_event(
        row.get::<Option<sqlx::types::Json<serde_json::Value>>, _>("data")
            .ok_or(Error::UnknownStoreOutcome)?,
        proof,
    )?;
    if event.session_id != *session
        || row.get::<String, _>("run_id") != event.run_id.0
        || event.cursor.is_some_and(|saved| saved != cursor)
    {
        return Err(Error::UnknownStoreOutcome);
    }
    event.cursor = Some(cursor);
    Ok(event)
}
pub(super) async fn read_before(
    tx: &mut Transaction<'_, Postgres>,
    event: &EventRecord,
    proof: &mut Proof,
) -> Result<Option<EventRecord>, Error> {
    if let Some(cursor) = public_state::before_event(event)? {
        let before = event_at(tx, &event.session_id, cursor, proof).await?;
        public_state::verify_before_event_with_proof(event, &before, proof)?;
        return Ok(Some(before));
    }
    Ok(None)
}

async fn charge_event(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    cursor: EventCursor,
    proof: &mut Proof,
) -> Result<(), Error> {
    super::super::read_proof::charge(tx,sqlx::query_scalar("SELECT CAST(octet_length(data::text) AS BIGINT) FROM events WHERE session_id=$1 AND seq=$2").bind(&session.0).bind(i64::try_from(cursor.0).map_err(|_|Error::UnknownStoreOutcome)?),proof).await.map_err(|_|Error::UnknownStoreOutcome)
}
async fn charge_state(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    proof: &mut Proof,
) -> Result<(), Error> {
    super::super::read_proof::charge(tx,sqlx::query_scalar("SELECT CAST(octet_length(state_json)+octet_length(activities_json) AS BIGINT) FROM session_public_state WHERE session_id=$1").bind(&session.0),proof).await.map_err(|_|Error::UnknownStoreOutcome)
}

impl super::super::PostgresStore {
    pub(in crate::postgres) async fn read_public_source_publication(
        &self,
        session: &SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<Option<crate::PublicSourcePublication>, Error> {
        public_state::validate_id(id)?;
        let mut tx = self
            .session_transaction(session)
            .await
            .map_err(|_| Error::UnknownStoreOutcome)?;
        let result = publication(&mut tx, session, id).await;
        finish(tx, result).await
    }
}
async fn publication(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    id: &crabber_core::ControlId,
) -> Result<Option<crate::PublicSourcePublication>, Error> {
    let mut proof = Proof::bounded(32 << 20);
    let Some(saved) = saved_with_proof(tx, session, id, &mut proof).await? else {
        return Ok(None);
    };
    if saved
        .event
        .payload
        .get("version")
        .and_then(serde_json::Value::as_u64)
        != Some(2)
    {
        return Err(Error::SemanticConflict);
    }
    proof
        .charge(2 * std::mem::size_of::<EventRecord>())
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let mut events = Vec::with_capacity(2);
    if let Some(before) = saved.before {
        events.push(before);
    }
    events.push(saved.event);
    Ok(Some(crate::PublicSourcePublication {
        receipt: saved.receipt,
        events,
    }))
}
