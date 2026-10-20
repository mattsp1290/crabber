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
 UNION ALL SELECT 1 FROM future_queue q WHERE q.session_id=$1 AND q.source_protocol=1 AND NOT EXISTS(SELECT 1 FROM queue_publication_cohorts c WHERE c.session_id=q.session_id AND c.queue_id=q.queue_id)
 UNION ALL SELECT 1 FROM queue_publication_cohorts c LEFT JOIN future_queue q ON q.session_id=c.session_id AND q.queue_id=c.queue_id WHERE (c.session_id=$1 OR q.session_id=$1) AND (q.queue_id IS NULL OR q.source_protocol<>c.protocol_version)
 UNION ALL SELECT 1 FROM queue_publication_sources s LEFT JOIN queue_publication_cohorts c ON c.session_id=s.session_id AND c.queue_id=s.queue_id WHERE (s.session_id=$1 OR c.session_id=$1) AND c.queue_id IS NULL
 UNION ALL SELECT 1 FROM runs r LEFT JOIN native_execution_origins o ON o.run_id=r.id LEFT JOIN native_execution_unclassified u ON u.run_id=r.id WHERE r.session_id=$1 AND (r.execution_enrollment_protocol NOT IN(0,1) OR (r.execution_enrollment_protocol=0 AND (r.execution_enrollment_origin_id IS NOT NULL OR o.run_id IS NOT NULL OR u.run_id IS NOT NULL)) OR (r.execution_enrollment_protocol=1 AND (r.execution_enrollment_origin_id IS NULL OR r.execution_enrollment_origin_id<>r.id OR o.run_id IS NULL OR (o.birth_kind='raw' AND u.run_id IS NULL))))
 UNION ALL SELECT 1 FROM native_execution_origins o LEFT JOIN runs r ON r.id=o.run_id LEFT JOIN native_execution_unclassified u ON u.run_id=o.unclassified_run_id LEFT JOIN native_run_sources n ON n.run_id=o.run_id AND n.sequence=o.first_sequence WHERE (o.session_id=$1 OR r.session_id=$1 OR u.session_id=$1 OR n.session_id=$1) AND (r.id IS NULL OR r.execution_enrollment_protocol<>1 OR r.execution_enrollment_origin_id IS NULL OR r.execution_enrollment_origin_id<>o.run_id OR r.session_id<>o.session_id OR o.protocol_version<>1 OR o.first_sequence<>1 OR o.birth_kind NOT IN ('raw','pending') OR (o.birth_kind='raw' AND (o.unclassified_run_id IS NULL OR o.unclassified_run_id<>o.run_id OR u.run_id IS NULL OR u.run_id<>o.run_id OR u.session_id<>o.session_id OR u.origin_digest<>o.origin_digest OR o.pending_epoch IS NOT NULL)) OR (o.birth_kind='pending' AND (o.unclassified_run_id IS NOT NULL OR o.pending_epoch IS NULL OR NOT EXISTS(SELECT 1 FROM pending_initial_assignments a WHERE a.session_id=o.session_id AND a.epoch_id=o.pending_epoch AND a.run_id=o.run_id))) OR n.run_id IS NULL OR n.session_id<>o.session_id OR n.issuance_digest<>o.first_issuer_digest)
 UNION ALL SELECT 1 FROM native_execution_unclassified u LEFT JOIN native_execution_origins o ON o.run_id=u.run_id LEFT JOIN runs r ON r.id=u.run_id WHERE (u.session_id=$1 OR o.session_id=$1 OR r.session_id=$1) AND (o.run_id IS NULL OR r.id IS NULL OR o.session_id<>u.session_id OR r.session_id<>u.session_id OR o.origin_digest<>u.origin_digest OR o.birth_kind<>'raw' OR r.execution_enrollment_protocol<>1)
 UNION ALL SELECT 1 FROM pending_initial_cohorts p LEFT JOIN pending_initial_source_slots s ON s.epoch_id=p.epoch_id WHERE (p.session_id=$1 OR s.session_id=$1) AND (s.epoch_id IS NULL OR s.session_id<>p.session_id OR s.source_key<>p.source_key OR s.epoch_digest<>p.epoch_digest)
 UNION ALL SELECT 1 FROM pending_initial_epochs p LEFT JOIN pending_initial_source_slots s ON s.epoch_id=p.epoch_id WHERE (p.session_id=$1 OR s.session_id=$1) AND (s.epoch_id IS NULL OR s.session_id<>p.session_id OR s.source_key<>p.source_key OR s.epoch_digest<>p.epoch_digest)
 UNION ALL SELECT 1 FROM pending_initial_registered_completes p LEFT JOIN pending_initial_source_slots s ON s.epoch_id=p.epoch_id WHERE (p.session_id=$1 OR s.session_id=$1) AND (s.epoch_id IS NULL OR s.session_id<>p.session_id OR s.source_key<>p.source_key OR s.epoch_digest<>p.epoch_digest)
 UNION ALL SELECT 1 FROM pending_initial_assignments p LEFT JOIN pending_initial_source_slots s ON s.epoch_id=p.epoch_id WHERE (p.session_id=$1 OR s.session_id=$1) AND (s.epoch_id IS NULL OR s.session_id<>p.session_id OR s.source_key<>p.source_key OR s.epoch_digest<>p.epoch_digest)
 UNION ALL SELECT 1 FROM pending_initial_closes p LEFT JOIN pending_initial_source_slots s ON s.epoch_id=p.epoch_id WHERE (p.session_id=$1 OR s.session_id=$1) AND (s.epoch_id IS NULL OR s.session_id<>p.session_id OR s.source_key<>p.source_key OR s.epoch_digest<>p.epoch_digest)
 UNION ALL SELECT 1 FROM pending_initial_publications p LEFT JOIN pending_initial_source_slots s ON s.epoch_id=p.epoch_id WHERE (p.session_id=$1 OR s.session_id=$1) AND (s.epoch_id IS NULL OR s.session_id<>p.session_id OR s.source_key<>p.source_key OR s.epoch_digest<>p.epoch_digest)
 UNION ALL SELECT 1 FROM pending_initial_heads p LEFT JOIN pending_initial_source_slots s ON s.epoch_id=p.epoch_id WHERE (p.session_id=$1 OR s.session_id=$1) AND (s.epoch_id IS NULL OR s.session_id<>p.session_id OR s.source_key<>p.source_key OR s.epoch_digest<>p.epoch_digest)
 UNION ALL SELECT 1 FROM pending_initial_source_slots s WHERE s.session_id=$1 AND (NOT EXISTS(SELECT 1 FROM pending_initial_cohorts c WHERE c.session_id=s.session_id AND c.source_key=s.source_key AND c.epoch_id=s.epoch_id AND c.epoch_digest=s.epoch_digest) OR NOT EXISTS(SELECT 1 FROM pending_initial_epochs e WHERE e.session_id=s.session_id AND e.source_key=s.source_key AND e.epoch_id=s.epoch_id AND e.epoch_digest=s.epoch_digest) OR NOT EXISTS(SELECT 1 FROM pending_initial_heads h WHERE h.session_id=s.session_id AND h.source_key=s.source_key AND h.epoch_id=s.epoch_id AND h.epoch_digest=s.epoch_digest))
 UNION ALL SELECT 1 FROM pending_initial_assignments a LEFT JOIN native_execution_origins o ON o.run_id=a.run_id WHERE (a.session_id=$1 OR o.session_id=$1) AND (o.run_id IS NULL OR o.session_id<>a.session_id OR o.birth_kind<>'pending' OR o.pending_epoch<>a.epoch_id)
 UNION ALL SELECT 1 FROM initial_consumer_origins o LEFT JOIN initial_consumer_cohorts c ON c.epoch_id=o.epoch_id WHERE (o.session_id=$1 OR c.session_id=$1) AND (c.epoch_id IS NULL OR c.session_id<>o.session_id OR c.source_key<>o.source_key OR c.record_digest<>o.record_digest OR c.origin_kind<>o.origin_kind)
 UNION ALL SELECT 1 FROM initial_consumer_cohorts c LEFT JOIN initial_consumer_origins o ON o.epoch_id=c.epoch_id WHERE (c.session_id=$1 OR o.session_id=$1) AND (o.epoch_id IS NULL OR c.session_id<>o.session_id OR c.source_key<>o.source_key OR c.record_digest<>o.record_digest OR c.origin_kind<>o.origin_kind)
 UNION ALL SELECT 1 FROM initial_consumer_assignments m LEFT JOIN initial_consumer_origins o ON o.epoch_id=m.epoch_id WHERE (m.session_id=$1 OR o.session_id=$1) AND (o.epoch_id IS NULL OR m.session_id<>o.session_id OR m.source_key<>o.source_key)
 UNION ALL SELECT 1 FROM initial_consumer_calls m LEFT JOIN initial_consumer_origins o ON o.epoch_id=m.epoch_id WHERE (m.session_id=$1 OR o.session_id=$1) AND (o.epoch_id IS NULL OR m.session_id<>o.session_id OR m.source_key<>o.source_key)
 UNION ALL SELECT 1 FROM initial_consumer_results m LEFT JOIN initial_consumer_origins o ON o.epoch_id=m.epoch_id WHERE (m.session_id=$1 OR o.session_id=$1) AND (o.epoch_id IS NULL OR m.session_id<>o.session_id OR m.source_key<>o.source_key)
 UNION ALL SELECT 1 FROM initial_consumer_roster m LEFT JOIN initial_consumer_origins o ON o.epoch_id=m.epoch_id WHERE (m.session_id=$1 OR o.session_id=$1) AND (o.epoch_id IS NULL OR m.session_id<>o.session_id OR m.source_key<>o.source_key)
 UNION ALL SELECT 1 FROM initial_consumer_reverse m LEFT JOIN initial_consumer_origins o ON o.epoch_id=m.epoch_id WHERE (m.session_id=$1 OR o.session_id=$1) AND (o.epoch_id IS NULL OR m.session_id<>o.session_id OR m.source_key<>o.source_key)
 UNION ALL SELECT 1 FROM initial_consumer_publications m LEFT JOIN initial_consumer_origins o ON o.epoch_id=m.epoch_id WHERE (m.session_id=$1 OR o.session_id=$1) AND (o.epoch_id IS NULL OR m.session_id<>o.session_id OR m.source_key<>o.source_key)
 UNION ALL SELECT 1 FROM initial_consumer_heads m LEFT JOIN initial_consumer_origins o ON o.epoch_id=m.epoch_id WHERE (m.session_id=$1 OR o.session_id=$1) AND (o.epoch_id IS NULL OR m.session_id<>o.session_id OR m.source_key<>o.source_key)
 UNION ALL SELECT 1 FROM initial_consumer_closes m LEFT JOIN initial_consumer_origins o ON o.epoch_id=m.epoch_id WHERE (m.session_id=$1 OR o.session_id=$1) AND (o.epoch_id IS NULL OR m.session_id<>o.session_id OR m.source_key<>o.source_key)
 UNION ALL SELECT 1 FROM inbox i JOIN runs r ON r.id=i.consumed_by_run WHERE i.session_id<>r.session_id AND (i.session_id=$1 OR r.session_id=$1)
)";

/// Dependents first. Neither global signing state nor allocator sequences appear.
pub(crate) const PURGE: &[&str] = &[
    "DELETE FROM initial_consumer_closes WHERE session_id=$1",
    "DELETE FROM initial_consumer_reverse WHERE session_id=$1",
    "DELETE FROM initial_consumer_roster WHERE session_id=$1",
    "DELETE FROM initial_consumer_results WHERE session_id=$1",
    "DELETE FROM initial_consumer_calls WHERE session_id=$1",
    "DELETE FROM initial_consumer_assignments WHERE session_id=$1",
    "DELETE FROM initial_consumer_heads WHERE session_id=$1",
    "DELETE FROM initial_consumer_publications WHERE session_id=$1",
    "DELETE FROM initial_consumer_cohorts WHERE session_id=$1",
    "DELETE FROM initial_consumer_origins WHERE session_id=$1",
    "DELETE FROM pending_initial_closes WHERE session_id=$1",
    "DELETE FROM pending_initial_assignments WHERE session_id=$1",
    "DELETE FROM pending_initial_registered_completes WHERE session_id=$1",
    "DELETE FROM pending_initial_heads WHERE session_id=$1",
    "DELETE FROM pending_initial_publications WHERE session_id=$1",
    "DELETE FROM pending_initial_cohorts WHERE session_id=$1",
    "DELETE FROM pending_initial_source_slots WHERE session_id=$1",
    "DELETE FROM pending_initial_epochs WHERE session_id=$1",
    "DELETE FROM approval_input_sources WHERE session_id=$1",
    "DELETE FROM approval_input_cohorts WHERE session_id=$1",
    "DELETE FROM tool_input_sources WHERE session_id=$1",
    "DELETE FROM tool_input_cohorts WHERE session_id=$1",
    "DELETE FROM run_control_progress WHERE session_id=$1",
    "DELETE FROM run_controls WHERE session_id=$1",
    "DELETE FROM queue_publication_sources WHERE session_id=$1",
    "DELETE FROM queue_publication_cohorts WHERE session_id=$1",
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
    "DELETE FROM native_execution_unclassified WHERE session_id=$1",
    "DELETE FROM native_execution_origins WHERE session_id=$1",
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
