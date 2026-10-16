//! Intentional fixture transitions publish authentic Run fencing evidence too.
use crate::native_parent::{RunIssuance, RunState};
use crabber::core::Run;
pub(super) fn advance(previous: &Run, next: &Run, latest: Option<&str>) -> Option<RunIssuance> {
    let latest: Option<RunIssuance> = latest.map(|text| serde_json::from_str(text).unwrap());
    crate::native_parent::advance(
        Some(&RunState::from_run(previous).unwrap()),
        RunState::from_run(next).unwrap(),
        latest.as_ref(),
    )
    .unwrap()
}

/// Intentional fixture writes retain the independently protected source chain.
/// The header comes from the actual database row, never a reconstructed fence.
pub(super) fn source(
    issuance: &RunIssuance,
    header: &str,
) -> crate::native_parent::source_protocol::JournalSource {
    let head: crate::native_parent::source_protocol::JournalHead =
        serde_json::from_str(header).unwrap();
    crate::native_parent::source_protocol::JournalSource::new(Some(&head), issuance).unwrap()
}
