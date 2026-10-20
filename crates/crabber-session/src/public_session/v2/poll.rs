use super::{
    PublicSessionError, PublicSessionLimitV2, PublicSessionLimitsV2, PublicSessionOutcomeV2,
    PublicSessionPageV2, PublicSessionRequestV2,
};
use crate::{PublicResyncReason, Store};
use crabber_core::{EventCursor, SessionId, SessionRevision};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PublicSessionPollV2 {
    Page {
        page: Box<PublicSessionPageV2>,
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
        limit: PublicSessionLimitV2,
    },
}
/// Host-owned, pull-driven subscription. It creates no background task or channel.
/// Poll durable revisions even without notifications; hints never establish truth.
#[derive(Debug, Clone)]
pub struct PublicSessionSubscriptionV2 {
    request: PublicSessionRequestV2,
    completed: Option<(SessionRevision, EventCursor)>,
    assembling: Option<SessionRevision>,
    overflow: bool,
}
impl PublicSessionSubscriptionV2 {
    #[must_use]
    pub fn new(session_id: SessionId, limits: PublicSessionLimitsV2) -> Self {
        Self {
            request: PublicSessionRequestV2 {
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
    pub fn set_limits(&mut self, limits: PublicSessionLimitsV2) {
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
    ) -> Result<PublicSessionPollV2, PublicSessionError> {
        self.request.validate()?;
        if self.overflow {
            self.overflow = false;
            self.reset();
            return Ok(PublicSessionPollV2::Resync {
                reason: PublicResyncReason::Overflow,
            });
        }
        let revision = store.session_revision(&self.request.session_id).await?;
        if let Some((saved, high_water)) = self.completed {
            if saved == revision {
                return Ok(PublicSessionPollV2::Unchanged {
                    revision,
                    high_water,
                });
            }
            self.reset();
            return Ok(PublicSessionPollV2::Resync {
                reason: PublicResyncReason::RevisionChanged,
            });
        }
        if self.assembling.is_some_and(|saved| saved != revision) {
            self.reset();
            return Ok(PublicSessionPollV2::Resync {
                reason: PublicResyncReason::RevisionChanged,
            });
        }
        let reset = self.request.continuation.is_none();
        match store
            .public_session_snapshot_v2(self.request.clone())
            .await?
        {
            PublicSessionOutcomeV2::Page(page) => {
                self.request
                    .continuation
                    .clone_from(&page.history.continuation);
                if self.request.continuation.is_none() {
                    self.completed = Some((page.captured_revision, page.high_water()));
                    self.assembling = None;
                } else {
                    self.assembling = Some(page.captured_revision);
                }
                Ok(PublicSessionPollV2::Page { page, reset })
            }
            PublicSessionOutcomeV2::Limited {
                limit,
                continuation,
            } => {
                self.request.continuation = continuation;
                self.assembling = Some(revision);
                Ok(PublicSessionPollV2::Limited { limit })
            }
            PublicSessionOutcomeV2::Invalidated => {
                self.reset();
                Ok(PublicSessionPollV2::Resync {
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
