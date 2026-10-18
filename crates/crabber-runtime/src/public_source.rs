//! Runtime-bound public data and canonical delivery on one actual worker scope.
use async_trait::async_trait;
use crabber_core::{
    ControlId, EventCursor, PublicAttemptIdentity, PublicSourceChange, PublicSourceEvent, RunId,
    SessionId, SessionRevision, ToolCallId,
};
use crabber_extension::{
    PublicSourceError as Error, PublicSourceSink, PublicSourceSnapshot, PublicSourceWriter,
};
use crabber_session::{
    PublicSourceAuthority, PublicSourceMutation, PublicSourcePublication, PublicStateError, Store,
};
use std::sync::Arc;
use tokio::sync::{Mutex, MutexGuard};
const MAX_PENDING: usize = 256;
const DELIVERY_BYTES: usize = 32 << 20;

pub(crate) struct SourceDelivery {
    session: SessionId,
    run: RunId,
    attempt: PublicAttemptIdentity,
    observer: Arc<dyn crate::Observer>,
    state: Mutex<DeliveryState>,
}
struct DeliveryState {
    cursor: EventCursor,
    pending: Vec<ControlId>,
    sealed: bool,
}
pub(crate) struct TerminalDelivery<'a> {
    state: MutexGuard<'a, DeliveryState>,
}
impl TerminalDelivery<'_> {
    pub(crate) fn definite_pending_input(&mut self) {
        self.state.sealed = false;
    }
}
impl SourceDelivery {
    pub(crate) fn new(
        session: SessionId,
        run: RunId,
        attempt: PublicAttemptIdentity,
        begin: EventCursor,
        observer: Arc<dyn crate::Observer>,
    ) -> Arc<Self> {
        Arc::new(Self {
            session,
            run,
            attempt,
            observer,
            state: Mutex::new(DeliveryState {
                cursor: begin,
                pending: Vec::new(),
                sealed: false,
            }),
        })
    }
    pub(crate) async fn terminal(&self, store: &dyn Store) -> Result<TerminalDelivery<'_>, Error> {
        let mut state = self.state.lock().await;
        state.sealed = true;
        self.reconcile(store, &mut state).await?;
        if !state.pending.is_empty() {
            return Err(Error::UnknownOutcome);
        }
        Ok(TerminalDelivery { state })
    }
    fn matches(
        &self,
        publication: &PublicSourcePublication,
        call: Option<&ToolCallId>,
        exact_call: bool,
    ) -> Result<(), Error> {
        if publication.events.is_empty()
            || publication.events.len() > 2
            || publication.receipt.session_id != self.session
            || publication.receipt.run_id != self.run
        {
            return Err(Error::UnknownOutcome);
        }
        for event in &publication.events {
            let source = PublicSourceEvent::deserialize(&event.payload)
                .map_err(|_| Error::UnknownOutcome)?;
            if source.source.session_id != self.session
                || source.source.run_id != self.run
                || source.source.attempt != self.attempt
                || source.source.invocation.is_some()
                || (exact_call && source.source.call_id.as_ref() != call)
                || event.cursor.is_none()
            {
                return Err(Error::Rejected);
            }
        }
        Ok(())
    }
    async fn reconcile(&self, store: &dyn Store, state: &mut DeliveryState) -> Result<(), Error> {
        // Only bounded operation identities/receipts accumulate. Full event pairs
        // cross the native read one at a time after their actual cursor is sorted.
        let mut committed = Vec::with_capacity(state.pending.len());
        for id in &state.pending {
            if let Some(receipt) = store
                .lookup_public_state_mutation(&self.session, id)
                .await
                .map_err(|failure| error(&failure))?
            {
                committed.push(receipt);
            }
        }
        // None is not a no-commit witness. Until every original has a canonical
        // cursor, a worker may still commit ahead of another selected receipt.
        // Do not advance delivery past an unresolved original.
        if committed.len() != state.pending.len() {
            return Err(Error::UnknownOutcome);
        }
        committed.sort_by_key(|receipt| receipt.event_cursor);
        let mut remaining = DELIVERY_BYTES;
        for receipt in committed {
            if receipt.event_cursor <= state.cursor {
                state.pending.retain(|id| *id != receipt.operation_id);
                continue;
            }
            let publication = store
                .lookup_public_source_publication(&self.session, &receipt.operation_id)
                .await
                .map_err(|failure| error(&failure))?
                .ok_or(Error::UnknownOutcome)?;
            if publication.receipt != receipt {
                return Err(Error::UnknownOutcome);
            }
            self.matches(&publication, None, false)?;
            let bytes = encoded_len(&publication.events, remaining)?;
            remaining -= bytes;
            for event in &publication.events {
                let cursor = event.cursor.ok_or(Error::UnknownOutcome)?;
                if cursor > state.cursor {
                    self.observer.emit(event);
                    state.cursor = cursor;
                }
            }
            state.pending.retain(|id| *id != receipt.operation_id);
        }
        Ok(())
    }
}
pub(crate) fn encoded_len(value: &impl serde::Serialize, limit: usize) -> Result<usize, Error> {
    struct Counter {
        written: usize,
        limit: usize,
    }
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let next = self
                .written
                .checked_add(bytes.len())
                .filter(|next| *next <= self.limit)
                .ok_or_else(|| std::io::Error::other("public delivery limit"))?;
            self.written = next;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter { written: 0, limit };
    serde_json::to_writer(&mut counter, value).map_err(|_| Error::UnknownOutcome)?;
    Ok(counter.written)
}
struct NativeSource {
    store: Arc<dyn Store>,
    session: SessionId,
    authority: PublicSourceAuthority,
    delivery: Option<Arc<SourceDelivery>>,
    call: Option<ToolCallId>,
}
pub(crate) fn bind(
    store: Arc<dyn Store>,
    session: SessionId,
    authority: PublicSourceAuthority,
    delivery: Option<Arc<SourceDelivery>>,
    call: Option<ToolCallId>,
) -> PublicSourceWriter {
    PublicSourceWriter::from_host(Arc::new(NativeSource {
        store,
        session,
        authority,
        delivery,
        call,
    }))
}
#[async_trait]
impl PublicSourceSink for NativeSource {
    async fn snapshot(&self) -> Result<PublicSourceSnapshot, Error> {
        let snapshot = self
            .store
            .public_state(&self.session)
            .await
            .map_err(|failure| error(&failure))?;
        Ok(PublicSourceSnapshot {
            revision: snapshot.revision,
            contents: snapshot.contents,
        })
    }
    async fn publish(
        &self,
        id: ControlId,
        expected: SessionRevision,
        change: PublicSourceChange,
    ) -> Result<SessionRevision, Error> {
        if let Some(delivery) = &self.delivery {
            let mut state = delivery.state.lock().await;
            if state.sealed {
                return self.reconcile_only(delivery, &id, expected, &change).await;
            }
            let prior = state.pending.contains(&id);
            if !prior {
                if state.pending.len() == MAX_PENDING {
                    return Err(Error::Rejected);
                }
                state.pending.push(id.clone());
            }
            let result = self
                .store
                .mutate_bound_public_source(
                    PublicSourceMutation {
                        id: id.clone(),
                        expected_revision: expected,
                        change,
                    },
                    self.authority.clone(),
                )
                .await;
            let receipt = match result {
                Ok(receipt) => receipt,
                Err(failure) => {
                    if !prior && failure != PublicStateError::UnknownStoreOutcome {
                        state.pending.retain(|pending| *pending != id);
                    }
                    return Err(error(&failure));
                }
            };
            delivery.reconcile(&*self.store, &mut state).await?;
            Ok(receipt.revision)
        } else {
            self.store
                .mutate_bound_public_source(
                    PublicSourceMutation {
                        id,
                        expected_revision: expected,
                        change,
                    },
                    self.authority.clone(),
                )
                .await
                .map(|receipt| receipt.revision)
                .map_err(|failure| error(&failure))
        }
    }
}
impl NativeSource {
    async fn reconcile_only(
        &self,
        delivery: &SourceDelivery,
        id: &ControlId,
        expected: SessionRevision,
        change: &PublicSourceChange,
    ) -> Result<SessionRevision, Error> {
        let publication = self
            .store
            .lookup_public_source_publication(&self.session, id)
            .await
            .map_err(|failure| error(&failure))?
            .ok_or(Error::StaleOwner)?;
        delivery.matches(&publication, self.call.as_ref(), true)?;
        let saved = PublicSourceEvent::deserialize(
            &publication
                .events
                .last()
                .ok_or(Error::UnknownOutcome)?
                .payload,
        )
        .map_err(|_| Error::UnknownOutcome)?;
        if saved.change != *change || expected.next().ok() != Some(saved.revision) {
            return Err(Error::Rejected);
        }
        Ok(publication.receipt.revision)
    }
}
use serde::Deserialize;
fn error(error: &PublicStateError) -> Error {
    match error {
        PublicStateError::Unsupported => Error::Unsupported,
        PublicStateError::RevisionConflict => Error::RevisionConflict,
        PublicStateError::StaleOwner => Error::StaleOwner,
        PublicStateError::UnknownStoreOutcome => Error::UnknownOutcome,
        PublicStateError::NotFound
        | PublicStateError::InvalidRequest
        | PublicStateError::SemanticConflict
        | PublicStateError::RevisionExhausted => Error::Rejected,
    }
}
