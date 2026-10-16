//! Shared fixed-profile limits for actual public lifecycle fixtures.
pub(super) fn limits() -> crabber::PublicSessionLimitsV2 {
    crabber::PublicSessionLimitsV2 {
        header_bytes: 65536,
        proof_bytes: 32 << 20,
        approval_interactions: 64,
        tool_requirements: 64,
        queue_entries: 64,
        registered_owners: 1024,
        run_controls: 64,
        history: crabber::session::SnapshotLimits {
            messages: 1,
            tool_calls: 1,
            parts: 256,
            text_bytes: 4096,
            encoded_bytes: 4096,
        },
    }
}
