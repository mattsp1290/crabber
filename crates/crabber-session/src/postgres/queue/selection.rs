//! Actual original audit owners retained through borrowed Queue admission.
use crate::public_session::v2::proof::Proof;
use crate::queue::{Audit, Record, corrupt, source::Source};
use crate::{BoundKeyedAdmitRequest, RetainedQueue, StoreError};

pub(super) struct Originals {
    pub(super) head: Option<Source>,
    pub(super) enqueue: Option<Audit>,
    pub(super) prepare: Option<Audit>,
    pub(super) claim: Option<Audit>,
    pub(super) prepare_source: Option<Source>,
}
impl Originals {
    pub(super) fn prepared<'a>(
        &'a self,
        record: &Record,
    ) -> Result<Option<&'a BoundKeyedAdmitRequest>, StoreError> {
        crate::queue::verify_sources(
            record,
            self.enqueue.as_ref().ok_or_else(corrupt)?,
            self.prepare.as_ref(),
            self.claim.as_ref(),
            self.prepare_source.as_ref(),
        )
    }
}
pub(super) struct LoadedRecord {
    pub(super) projection: Record,
    pub(super) originals: Originals,
}
impl std::ops::Deref for LoadedRecord {
    type Target = Record;
    fn deref(&self) -> &Record {
        &self.projection
    }
}
impl LoadedRecord {
    pub(super) fn prepared(&self) -> Result<Option<&BoundKeyedAdmitRequest>, StoreError> {
        self.originals.prepared(&self.projection)
    }
    pub(super) fn retained(&self, proof: &mut Proof) -> Result<RetainedQueue, StoreError> {
        #[derive(serde::Serialize)]
        struct View<'a> {
            summary: &'a crate::QueueSummary,
            input: &'a crate::QueueInput,
            prepared: Option<&'a BoundKeyedAdmitRequest>,
        }
        let prepared = self.prepared()?;
        proof
            .measure(&View {
                summary: &self.summary,
                input: &self.input,
                prepared,
            })
            .map_err(|_| corrupt())?;
        Ok(self.projection.retained(prepared))
    }
}
