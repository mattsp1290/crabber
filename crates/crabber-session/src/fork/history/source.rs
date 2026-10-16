//! Borrowed source facts keep supplied input separate from observed execution.
use super::{
    ContentBlock, Error, ImportedToolCall, RunId, ToolCallId, ToolCallRecord, ToolCallStatus,
    ToolResult, ToolResultStatus,
};

#[derive(serde::Serialize)]
pub(crate) struct AdmittedCall<'a> {
    pub id: &'a ToolCallId,
    pub name: &'a str,
    pub arguments: &'a serde_json::Value,
    pub run_id: &'a RunId,
    pub result: Option<AdmittedResult<'a>>,
}
#[derive(serde::Serialize)]
pub(crate) struct AdmittedResult<'a> {
    pub content: &'a [ContentBlock],
    pub is_error: bool,
}
#[derive(Clone, Copy)]
pub(super) struct ResultView<'a> {
    pub status: ToolResultStatus,
    pub content: &'a [ContentBlock],
}
impl<'a> From<&'a ToolResult> for ResultView<'a> {
    fn from(result: &'a ToolResult) -> Self {
        Self {
            status: result.status,
            content: &result.content,
        }
    }
}
pub(crate) enum SourceCall<'a> {
    Native(&'a ToolCallRecord),
    /// Terminal content is authenticated but actual cleanup remains retained.
    UnquiescentNative(&'a ToolCallRecord),
    Imported(&'a ImportedToolCall),
    /// Supplied history authenticated against the original admitted envelope.
    /// It never creates or stands in for a native `ToolCallRecord`.
    AdmittedInput(AdmittedCall<'a>),
}
impl SourceCall<'_> {
    pub(super) fn encoded_bytes(&self, cap: usize) -> Option<usize> {
        match self {
            Self::Native(call) | Self::UnquiescentNative(call) => {
                crate::snapshot::encoded_bytes(call, cap)
            }
            Self::Imported(call) => crate::snapshot::encoded_bytes(call, cap),
            Self::AdmittedInput(call) => crate::snapshot::encoded_bytes(call, cap),
        }
    }
    pub(super) fn validate_shape(&self) -> Result<(), Error> {
        let valid = match self {
            Self::Native(call) | Self::UnquiescentNative(call) => matches!(
                (
                    call.status,
                    call.result.as_ref().map(|result| result.status)
                ),
                (
                    ToolCallStatus::Pending | ToolCallStatus::Running | ToolCallStatus::Waiting,
                    None
                ) | (ToolCallStatus::Completed, Some(ToolResultStatus::Completed))
                    | (ToolCallStatus::Failed, Some(ToolResultStatus::Failed))
                    | (
                        ToolCallStatus::Interrupted,
                        Some(ToolResultStatus::Interrupted)
                    )
            ),
            Self::AdmittedInput(_) => true,
            Self::Imported(call) => matches!(
                call.result.status,
                ToolResultStatus::Completed | ToolResultStatus::Failed
            ),
        };
        if valid {
            Ok(())
        } else {
            Err(Error::UnknownStoreOutcome)
        }
    }
    pub(super) fn fields(
        &self,
    ) -> (
        &ToolCallId,
        &str,
        &serde_json::Value,
        Option<ResultView<'_>>,
        Option<&RunId>,
    ) {
        match self {
            Self::Native(call) | Self::UnquiescentNative(call) => (
                &call.id,
                &call.name,
                &call.arguments,
                call.result.as_ref().map(ResultView::from),
                Some(&call.run_id),
            ),
            Self::AdmittedInput(call) => (
                call.id,
                call.name,
                call.arguments,
                call.result.as_ref().map(|result| ResultView {
                    // Presentation outcome only; no native execution is asserted.
                    status: if result.is_error {
                        ToolResultStatus::Failed
                    } else {
                        ToolResultStatus::Completed
                    },
                    content: result.content,
                }),
                Some(call.run_id),
            ),
            Self::Imported(call) => (
                &call.id,
                &call.name,
                &call.arguments,
                Some(ResultView::from(&call.result)),
                None,
            ),
        }
    }
    pub(super) fn source_kind(&self) -> crabber_core::ImportedCallSource {
        match self {
            Self::Native(_) | Self::UnquiescentNative(_) => {
                crabber_core::ImportedCallSource::NativeExecution
            }
            Self::Imported(_) => crabber_core::ImportedCallSource::ImportedHistory,
            Self::AdmittedInput(_) => crabber_core::ImportedCallSource::AdmittedInput,
        }
    }
    pub(super) fn closed(&self) -> bool {
        match self {
            Self::UnquiescentNative(_) => false,
            Self::AdmittedInput(call) => call.result.is_some(),
            Self::Native(call) => matches!(
                (call.status, call.result.as_ref().map(|r| r.status)),
                (ToolCallStatus::Completed, Some(ToolResultStatus::Completed))
                    | (ToolCallStatus::Failed, Some(ToolResultStatus::Failed))
            ),
            Self::Imported(call) => matches!(
                call.result.status,
                ToolResultStatus::Completed | ToolResultStatus::Failed
            ),
        }
    }
}
