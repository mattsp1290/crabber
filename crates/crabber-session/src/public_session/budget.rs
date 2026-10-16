use super::{PublicSessionError, PublicSessionLimit};
use serde::Serialize;
#[derive(Debug)]
pub(crate) enum Failure {
    Limited(PublicSessionLimit),
    Error(PublicSessionError),
}
impl From<PublicSessionError> for Failure {
    fn from(error: PublicSessionError) -> Self {
        Self::Error(error)
    }
}
pub(crate) fn unknown(_: impl std::fmt::Debug) -> Failure {
    Failure::Error(PublicSessionError::UnknownStoreOutcome)
}
pub(crate) struct Budget {
    pub cap: usize,
    pub used: usize,
    limit: PublicSessionLimit,
}
impl Budget {
    pub fn new(cap: usize, limit: PublicSessionLimit) -> Self {
        Self {
            cap,
            used: 0,
            limit,
        }
    }
    pub fn remaining(&self) -> usize {
        self.cap.saturating_sub(self.used)
    }
    pub fn charge(&mut self, bytes: usize) -> Result<(), Failure> {
        self.used = self
            .used
            .checked_add(bytes)
            .ok_or(Failure::Limited(self.limit))?;
        if self.used > self.cap {
            return Err(Failure::Limited(self.limit));
        }
        Ok(())
    }
    pub fn measure<T: Serialize>(&mut self, value: &T) -> Result<(), Failure> {
        let bytes = crate::snapshot::encoded_bytes(value, self.remaining())
            .ok_or(Failure::Limited(self.limit))?;
        self.charge(bytes)
    }
    pub fn finish<T: Serialize>(&mut self, value: &T) -> Result<(), Failure> {
        let bytes =
            crate::snapshot::encoded_bytes(value, self.cap).ok_or(Failure::Limited(self.limit))?;
        self.used = self.used.max(bytes);
        Ok(())
    }
}
pub(crate) fn outcome(
    result: Result<super::PublicControlHeader, Failure>,
) -> Result<super::PublicHeaderOutcome, PublicSessionError> {
    match result {
        Ok(header) => Ok(super::PublicHeaderOutcome::Header(Box::new(header))),
        Err(Failure::Limited(limit)) => Ok(super::PublicHeaderOutcome::Limited(limit)),
        Err(Failure::Error(error)) => Err(error),
    }
}
pub(crate) fn finish_header(
    mut header: super::PublicControlHeader,
    mut safe: Budget,
    proof: &Budget,
) -> Result<super::PublicControlHeader, Failure> {
    header.read_usage.proof_bytes = proof.used;
    for _ in 0..8 {
        safe.finish(&header)?;
        if header.read_usage.encoded_bytes == safe.used {
            return Ok(header);
        }
        header.read_usage.encoded_bytes = safe.used;
    }
    Err(unknown("header accounting"))
}
#[derive(Serialize)]
struct Projection<'a> {
    run_id: &'a crabber_core::RunId,
    continuation: &'a crate::ContinuationTarget,
    state: crate::ApprovalContinuationState,
    termination: Option<&'a crate::ApprovalTerminationKind>,
    interactions: Views<'a>,
}
struct Views<'a>(&'a crate::approval::ApprovalRecord);
impl Serialize for Views<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut sequence = serializer.serialize_seq(Some(self.0.questions.len()))?;
        for question in &self.0.questions {
            sequence.serialize_element(&question.view)?;
        }
        sequence.end()
    }
}
pub(crate) fn approval(
    record: &crate::approval::ApprovalRecord,
    max_interactions: u16,
    safe: &mut Budget,
) -> Result<super::PublicApprovalSummary, Failure> {
    if record.questions.len() > usize::from(max_interactions) {
        return Err(Failure::Limited(PublicSessionLimit::Interactions));
    }
    // Measure the exact borrowed public projection before cloning any views.
    let projection = Projection {
        run_id: &record.run_id,
        continuation: &record.batch.continuation,
        state: record.state,
        termination: record.termination.as_ref().map(|ended| &ended.request.kind),
        interactions: Views(record),
    };
    safe.measure(&projection)?;
    Ok(super::PublicApprovalSummary {
        run_id: record.run_id.clone(),
        continuation: record.batch.continuation.clone(),
        state: record.state,
        termination: record
            .termination
            .as_ref()
            .map(|ended| ended.request.kind.clone()),
        interactions: record
            .questions
            .iter()
            .map(|question| question.view.clone())
            .collect(),
    })
}
pub(crate) fn validate_approval_state(
    record: &crate::approval::ApprovalRecord,
    barrier: bool,
    status: crabber_core::RunStatus,
    attempt: Option<&crate::AttemptSnapshot>,
) -> Result<(), Failure> {
    match record.state {
        crate::ApprovalContinuationState::Waiting => {
            if !barrier
                || status != crabber_core::RunStatus::Paused
                || attempt.is_none_or(|attempt| {
                    attempt.target != record.batch.attempt
                        || attempt.state != crate::AttemptState::Suspended
                        || attempt.cleanup != crate::AttemptCleanup::Complete
                })
            {
                return Err(unknown("waiting approval"));
            }
        }
        crate::ApprovalContinuationState::Claimed => {
            if !barrier
                || status != crabber_core::RunStatus::Running
                || attempt.is_none_or(|attempt| {
                    attempt.target != record.batch.next_attempt
                        || !matches!(
                            attempt.state,
                            crate::AttemptState::Unstarted
                                | crate::AttemptState::Started
                                | crate::AttemptState::OutcomeUnknown
                        )
                })
            {
                return Err(unknown("claimed approval"));
            }
        }
        crate::ApprovalContinuationState::Released => {
            let valid_attempt = attempt.is_some_and(|attempt| {
                attempt.cleanup == crate::AttemptCleanup::Complete
                    && if record.termination.is_some() {
                        attempt.target == record.batch.attempt
                            && attempt.state == crate::AttemptState::Suspended
                    } else {
                        attempt.target == record.batch.next_attempt
                            && matches!(
                                attempt.state,
                                crate::AttemptState::Completed
                                    | crate::AttemptState::Failed
                                    | crate::AttemptState::Interrupted
                            )
                    }
            });
            if barrier || !status.is_terminal() || !valid_attempt {
                return Err(unknown("released approval"));
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_request(
    request: &super::PublicHeaderRequest,
) -> Result<(), PublicSessionError> {
    if request.session_id.0.is_empty()
        || request.session_id.0.len() > 512
        || request.session_id.0.chars().any(char::is_control)
        || request.max_bytes > 2 * 1024 * 1024
        || request.max_proof_bytes > 32 * 1024 * 1024
        || request.max_interactions > 64
        || request.run_id.as_ref().is_some_and(|id| {
            id.0.is_empty() || id.0.len() > 128 || id.0.chars().any(char::is_control)
        })
    {
        return Err(PublicSessionError::InvalidRequest);
    }
    Ok(())
}
