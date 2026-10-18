//! Borrowed actual originals retained only for one held-State readiness read.
use crate::native_enrollment::{initial_consumer::Selection, pending_initial};
use crate::session_owner::{coverage::FixedInitialOwnerRole, preparation::admission::Witness};
use crabber_core::Run;

pub(in crate::memory) struct FixedClosedSource<'state> {
    pub(super) run: &'state Run,
    pub(super) birth: pending_initial::Bundle<'state>,
    pub(super) consumer: Selection<'state>,
    pub(super) witness: &'state Witness,
    pub(super) roles: [FixedInitialOwnerRole<'state>; 3],
}
impl<'state> FixedClosedSource<'state> {
    pub(in crate::memory) fn run(&self) -> &'state Run {
        self.run
    }
    pub(in crate::memory) fn correlation(&self) -> &'state pending_initial::Correlation {
        &self
            .birth
            .assignment
            .expect("verified fixed source assignment")
            .correlation
    }
    pub(in crate::memory) fn birth(&self) -> pending_initial::Bundle<'state> {
        self.birth
    }
    pub(in crate::memory) fn consumer(&self) -> &Selection<'state> {
        &self.consumer
    }
    pub(in crate::memory) fn owner_roles(&self) -> &[FixedInitialOwnerRole<'state>; 3] {
        &self.roles
    }
    pub(in crate::memory) fn witness(&self) -> &'state Witness {
        self.witness
    }
}
