//! Fixed relational erasure graph. No history or private proof is decoded here.
use crate::deletion::coverage::VerifiedDeletionCoverage;
use crabber_core::{SessionId, SessionRevision};

/// Created only after positive coverage and every readiness proof succeeded.
/// This internal token carries no public force/cleanup assertion.
pub(crate) struct VerifiedScope {
    session: SessionId,
    revision: SessionRevision,
}
impl VerifiedScope {
    pub(crate) fn from_coverage(coverage: &VerifiedDeletionCoverage) -> Self {
        Self {
            session: coverage.session_id().clone(),
            revision: coverage.revision(),
        }
    }
    pub(crate) fn session(&self) -> &SessionId {
        &self.session
    }
    pub(crate) fn revision(&self) -> SessionRevision {
        self.revision
    }
    #[cfg(test)]
    pub(crate) fn fixture(session: SessionId, revision: SessionRevision) -> Self {
        Self::from_coverage(&VerifiedDeletionCoverage::fixture(session, revision))
    }
}

// Independent session/run/message FKs must never make erasing one scope delete
// another scope's row. Reject inconsistent routing before the first mutation.
pub(crate) const CROSS_SCOPE: &str = "SELECT EXISTS(
 SELECT 1 FROM sessions s WHERE s.id=$1 AND s.owner_scope_protocol=1 AND NOT EXISTS(SELECT 1 FROM session_owner_scope_origins o WHERE o.session_id=s.id)
 UNION ALL SELECT 1 FROM session_owner_contexts c WHERE c.session_id=$1 AND c.scope_protocol=1 AND NOT EXISTS(SELECT 1 FROM session_owner_scope_origins o WHERE o.session_id=c.session_id)
 UNION ALL SELECT 1 FROM session_owner_scope_origins o WHERE o.session_id=$1 AND NOT EXISTS(SELECT 1 FROM sessions s WHERE s.id=o.session_id AND s.owner_scope_protocol=1) AND NOT EXISTS(SELECT 1 FROM session_owner_contexts c WHERE c.session_id=o.session_id AND c.scope_protocol=1)
 UNION ALL SELECT 1 FROM messages m JOIN runs r ON r.id=m.run_id WHERE m.session_id<>r.session_id AND (m.session_id=$1 OR r.session_id=$1)
 UNION ALL SELECT 1 FROM epochs e JOIN runs r ON r.id=e.run_id WHERE e.session_id<>r.session_id AND (e.session_id=$1 OR r.session_id=$1)
 UNION ALL SELECT 1 FROM events e JOIN runs r ON r.id=e.run_id WHERE e.session_id<>r.session_id AND (e.session_id=$1 OR r.session_id=$1)
 UNION ALL SELECT 1 FROM admission_receipts a JOIN runs r ON r.id=a.run_id JOIN messages m ON m.id=a.user_message_id WHERE (a.session_id=$1 OR r.session_id=$1 OR m.session_id=$1) AND (a.session_id<>r.session_id OR a.session_id<>m.session_id OR m.run_id IS NULL OR m.run_id<>a.run_id)
 UNION ALL SELECT 1 FROM admission_executions a JOIN runs r ON r.id=a.run_id WHERE a.session_id<>r.session_id AND (a.session_id=$1 OR r.session_id=$1)
 UNION ALL SELECT 1 FROM admission_execution_sources a LEFT JOIN runs r ON r.id=a.run_id LEFT JOIN admission_executions x ON x.run_id=a.run_id WHERE (a.session_id=$1 OR r.session_id=$1 OR x.session_id=$1) AND (r.id IS NULL OR x.run_id IS NULL OR a.session_id<>r.session_id OR a.session_id<>x.session_id OR x.source_protocol<>1)
 UNION ALL SELECT 1 FROM session_owner_current c WHERE c.session_id=$1 AND c.source_protocol=1 AND NOT EXISTS(SELECT 1 FROM session_owner_publication_cohorts h WHERE h.session_id=c.session_id AND h.owner_id=c.owner_id)
 UNION ALL SELECT 1 FROM session_owner_publication_cohorts h LEFT JOIN session_owner_current c ON c.session_id=h.session_id AND c.owner_id=h.owner_id WHERE (h.session_id=$1 OR c.session_id=$1) AND (c.owner_id IS NULL OR c.source_protocol<>h.protocol_version)
 UNION ALL SELECT 1 FROM session_owner_publications p LEFT JOIN session_owner_publication_cohorts h ON h.session_id=p.session_id AND h.owner_id=p.owner_id WHERE (p.session_id=$1 OR h.session_id=$1) AND h.owner_id IS NULL
 UNION ALL SELECT 1 FROM inbox i JOIN runs r ON r.id=i.consumed_by_run WHERE i.session_id<>r.session_id AND (i.session_id=$1 OR r.session_id=$1)
)";

/// Dependents first. Neither global signing state nor allocator sequences appear.
pub(crate) const PURGE: &[&str] = &[
    "DELETE FROM approval_input_sources WHERE session_id=$1",
    "DELETE FROM approval_input_cohorts WHERE session_id=$1",
    "DELETE FROM tool_input_sources WHERE session_id=$1",
    "DELETE FROM tool_input_cohorts WHERE session_id=$1",
    "DELETE FROM run_control_progress WHERE session_id=$1",
    "DELETE FROM run_controls WHERE session_id=$1",
    "DELETE FROM future_queue_audit WHERE session_id=$1",
    "DELETE FROM session_management_audit WHERE session_id=$1",
    "DELETE FROM session_fork_audit WHERE session_id=$1",
    "DELETE FROM native_parent_witnesses WHERE session_id=$1",
    "DELETE FROM tool_continuation_frontend_calls WHERE run_id IN (SELECT id FROM runs WHERE session_id=$1)",
    "DELETE FROM tool_continuation_interactions WHERE run_id IN (SELECT id FROM runs WHERE session_id=$1)",
    "DELETE FROM tool_continuation_phases WHERE run_id IN (SELECT id FROM runs WHERE session_id=$1)",
    "DELETE FROM tool_continuation_barriers WHERE session_id=$1",
    "DELETE FROM tool_continuations WHERE session_id=$1",
    "DELETE FROM continuation_interactions WHERE run_id IN (SELECT id FROM runs WHERE session_id=$1)",
    "DELETE FROM continuation_barriers WHERE session_id=$1",
    "DELETE FROM continuations WHERE session_id=$1",
    "DELETE FROM frontend_tool_origins WHERE session_id=$1",
    "DELETE FROM session_invocations WHERE session_id=$1",
    "DELETE FROM tool_phases WHERE run_id IN (SELECT id FROM runs WHERE session_id=$1)",
    "DELETE FROM execution_attempts WHERE run_id IN (SELECT id FROM runs WHERE session_id=$1)",
    "DELETE FROM operation_receipts WHERE session_id=$1",
    "DELETE FROM future_queue WHERE session_id=$1",
    "DELETE FROM admission_execution_sources WHERE session_id=$1",
    "DELETE FROM admission_executions WHERE session_id=$1",
    "DELETE FROM admission_receipts WHERE session_id=$1",
    "DELETE FROM abandonment_commits WHERE run_id IN (SELECT id FROM runs WHERE session_id=$1)",
    "DELETE FROM imported_message_provenance WHERE session_id=$1",
    "DELETE FROM parts WHERE message_id IN (SELECT id FROM messages WHERE session_id=$1)",
    "DELETE FROM inbox WHERE session_id=$1",
    "DELETE FROM events WHERE session_id=$1",
    "DELETE FROM epochs WHERE session_id=$1",
    "DELETE FROM messages WHERE session_id=$1",
    "DELETE FROM tool_calls WHERE run_id IN (SELECT id FROM runs WHERE session_id=$1)",
    "DELETE FROM imported_tool_calls WHERE session_id=$1",
    "DELETE FROM run_execution_bindings WHERE run_id IN (SELECT id FROM runs WHERE session_id=$1)",
    "DELETE FROM run_suspension_proofs WHERE run_id IN (SELECT id FROM runs WHERE session_id=$1)",
    "DELETE FROM preparation_admission_witnesses WHERE session_id=$1",
    "DELETE FROM native_run_source_heads WHERE session_id=$1",
    "DELETE FROM native_run_sources WHERE session_id=$1",
    "DELETE FROM native_run_issuances WHERE session_id=$1",
    "DELETE FROM runs WHERE session_id=$1",
    "DELETE FROM extension_state WHERE session_id=$1",
    "DELETE FROM session_public_state WHERE session_id=$1",
    "DELETE FROM session_management WHERE session_id=$1",
    "DELETE FROM session_owner_scope_origins WHERE session_id=$1",
    "DELETE FROM session_owner_publications WHERE session_id=$1",
    "DELETE FROM session_owner_publication_cohorts WHERE session_id=$1",
    "DELETE FROM session_owner_audits WHERE session_id=$1",
    "DELETE FROM session_owner_current WHERE session_id=$1",
    "DELETE FROM session_owner_contexts WHERE session_id=$1",
    "DELETE FROM session_ownership_origins WHERE session_id=$1",
];

#[cfg(test)]
pub(crate) mod contract;

pub(crate) mod readiness;
