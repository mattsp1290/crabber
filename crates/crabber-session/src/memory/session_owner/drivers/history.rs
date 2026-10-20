//! Selected original Driver joins are independent from whole-Run coverage.
use super::{State, original_read};
use crate::public_session::v2::proof::{Failure, Proof, ReadError, unknown};
use crate::session_owner::record::Record;
pub(in crate::memory) fn classified(
    state: &State,
    record: &Record,
    proof: &mut Proof,
) -> Result<bool, Failure> {
    if !proof.active() {
        return Err(unknown("driver join requires bounded proof"));
    }
    proof.measure(record)?;
    let Some(link) = crate::session_owner::coverage::execution_driver(record).map_err(unknown)?
    else {
        return Ok(false);
    };
    original_read(state, link, proof).map_err(ReadError::public)?;
    let session = state
        .sessions
        .get(&record.snapshot.binding.session_id)
        .ok_or_else(|| unknown("driver original session"))?;
    if session.workspace_id != record.snapshot.binding.workspace_id
        || session.directory != record.snapshot.binding.directory
    {
        return Err(unknown("driver original context"));
    }
    Ok(true)
}

#[cfg(test)]
#[path = "history_tests.rs"]
mod tests;
