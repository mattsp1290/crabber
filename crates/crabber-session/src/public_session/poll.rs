use super::{
    PublicHeaderRequest, PublicSessionError, PublicSessionLimit, PublicSessionLimits,
    PublicSessionOutcome, PublicSessionPage, PublicSessionRequest,
};
use crate::Store;
use crabber_core::{EventCursor, SessionId, SessionRevision};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PublicResyncReason {
    RevisionChanged,
    Overflow,
    Invalidated,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PublicSessionPoll {
    Page {
        page: Box<PublicSessionPage>,
        reset: bool,
    },
    Unchanged {
        revision: SessionRevision,
        high_water: EventCursor,
    },
    Resync {
        reason: PublicResyncReason,
    },
    Limited {
        limit: PublicSessionLimit,
    },
}
/// Host-owned, pull-driven subscription. It creates no background task or channel.
/// Poll durable revisions even without notifications; hints never establish truth.
#[derive(Debug, Clone)]
pub struct PublicSessionSubscription {
    request: PublicSessionRequest,
    completed: Option<(SessionRevision, EventCursor)>,
    assembling: Option<SessionRevision>,
    overflow: bool,
}
impl PublicSessionSubscription {
    #[must_use]
    pub fn new(session_id: SessionId, limits: PublicSessionLimits) -> Self {
        Self {
            request: PublicSessionRequest {
                session_id,
                limits,
                continuation: None,
            },
            completed: None,
            assembling: None,
            overflow: false,
        }
    }
    /// A notification channel overflow invalidates any partially assembled snapshot.
    pub fn overflow(&mut self) {
        self.overflow = true;
    }
    /// Replace read budgets and restart assembly; the next page has reset=true.
    pub fn set_limits(&mut self, limits: PublicSessionLimits) {
        self.request.limits = limits;
        self.reset();
    }
    /// Read once when the host timer or notification source wakes. No notifications
    /// are trusted or buffered here; cross-process changes are found by revision.
    /// # Errors
    /// Returns bounded read validation, scope or integrity errors without a partial page.
    pub async fn poll(
        &mut self,
        store: &dyn Store,
    ) -> Result<PublicSessionPoll, PublicSessionError> {
        PublicHeaderRequest {
            session_id: self.request.session_id.clone(),
            run_id: None,
            expected_revision: SessionRevision::INITIAL,
            max_bytes: self.request.limits.header_bytes,
            max_proof_bytes: self.request.limits.proof_bytes,
            max_interactions: self.request.limits.interactions,
        }
        .validate()?;
        if self.overflow {
            self.overflow = false;
            self.reset();
            return Ok(PublicSessionPoll::Resync {
                reason: PublicResyncReason::Overflow,
            });
        }
        let revision = store.session_revision(&self.request.session_id).await?;
        if let Some((saved, high_water)) = self.completed {
            if saved == revision {
                return Ok(PublicSessionPoll::Unchanged {
                    revision,
                    high_water,
                });
            }
            self.reset();
            return Ok(PublicSessionPoll::Resync {
                reason: PublicResyncReason::RevisionChanged,
            });
        }
        if self.assembling.is_some_and(|saved| saved != revision) {
            self.reset();
            return Ok(PublicSessionPoll::Resync {
                reason: PublicResyncReason::RevisionChanged,
            });
        }
        let reset = self.request.continuation.is_none();
        match store.public_session_snapshot(self.request.clone()).await? {
            PublicSessionOutcome::Page(page) => {
                self.request
                    .continuation
                    .clone_from(&page.history.continuation);
                if self.request.continuation.is_none() {
                    self.completed = Some((page.captured_revision, page.high_water()));
                    self.assembling = None;
                } else {
                    self.assembling = Some(page.captured_revision);
                }
                Ok(PublicSessionPoll::Page { page, reset })
            }
            PublicSessionOutcome::Limited {
                limit,
                continuation,
            } => {
                self.request.continuation = continuation;
                self.assembling = Some(revision);
                Ok(PublicSessionPoll::Limited { limit })
            }
            PublicSessionOutcome::Invalidated => {
                self.reset();
                Ok(PublicSessionPoll::Resync {
                    reason: PublicResyncReason::Invalidated,
                })
            }
        }
    }
    fn reset(&mut self) {
        self.request.continuation = None;
        self.completed = None;
        self.assembling = None;
    }
}
