//! Exact historical owner coverage, separate from existing live classifications.
use super::{Audit, Ledger, SessionId, SessionOwnerOperation, SessionOwnerState};
use crate::public_session::v2::proof::{Failure, Proof, unknown};
use crate::session_owner::{coverage::FixedInitialOwnerRole, record::Record};

impl Ledger {
    /// True means some authenticated original history or current owner is not
    /// covered. Borrowed role metadata never supplies permissions or native joins.
    pub(crate) fn uncovered_fixed_initial_history(
        &self,
        session: &SessionId,
        roles: &[FixedInitialOwnerRole<'_>],
        proof: &mut Proof,
    ) -> Result<bool, Failure> {
        if !proof.active() {
            return Err(unknown("fixed owner coverage requires bounded proof"));
        }
        self.preflight_fixed_history(session, proof)?;
        self.validate_publications(session, proof)?;
        self.validate_owner_membership(session).map_err(unknown)?;
        for (index, role) in roles.iter().enumerate() {
            self.verify_fixed_role(session, role, proof)?;
            if roles[..index].iter().any(|other| {
                other.reserve.record.snapshot.target.id == role.reserve.record.snapshot.target.id
            }) {
                return Err(unknown("duplicate fixed owner role"));
            }
        }
        let mut uncovered = false;
        for ((scope, id), record) in &self.records {
            if scope != session {
                continue;
            }
            let current = self.checked_for_read(session, id, record, proof)?;
            uncovered |= !roles.iter().any(|role| {
                current == &role.joined.record
                    && current.snapshot.target == role.reserve.record.snapshot.target
                    && current.origin == role.reserve.receipt.operation_id
            });
        }
        for ((scope, _), audit) in &self.audits {
            if scope != session {
                continue;
            }
            // Every actual generation and operation is visited, including
            // ReclaimUnstarted and extra controls on an otherwise matched ID.
            // A matching latest owner or origin alone is never sufficient.
            uncovered |= !roles.iter().any(|role| {
                role_audits(role).into_iter().flatten().any(|source| {
                    audit.record.snapshot.target == source.record.snapshot.target
                        && audit.record.origin == source.record.origin
                        && same_audit(audit, source)
                })
            });
        }
        Ok(uncovered)
    }

    fn preflight_fixed_history(
        &self,
        session: &SessionId,
        proof: &mut Proof,
    ) -> Result<(), Failure> {
        for ((scope, id), record) in &self.records {
            if scope == session || &record.snapshot.binding.session_id == session {
                proof.measure(&(scope, id, record))?;
                if scope != session
                    || record.snapshot.binding.session_id != *scope
                    || record.snapshot.target.id != *id
                {
                    return Err(unknown("fixed owner current routing"));
                }
            }
        }
        // Full original carriers precede validators and their hashes/allocations.
        for ((scope, id), audit) in &self.audits {
            if audit_in_scope(scope, audit, session) {
                proof.measure(&(scope, id, audit))?;
            }
        }
        for ((scope, id), audit) in &self.audits {
            if !audit_in_scope(scope, audit, session) {
                continue;
            }
            if scope != session
                || audit.receipt.session_id != *scope
                || audit.record.snapshot.binding.session_id != *scope
                || audit.receipt.owner.binding.session_id != *scope
                || audit.receipt.operation_id != *id
            {
                return Err(unknown("fixed owner historical routing"));
            }
            audit.validate().map_err(unknown)?;
        }
        Ok(())
    }

    fn verify_fixed_role(
        &self,
        session: &SessionId,
        role: &FixedInitialOwnerRole<'_>,
        proof: &mut Proof,
    ) -> Result<(), Failure> {
        for audit in role_audits(role).into_iter().flatten() {
            proof.measure(audit)?;
            audit.validate().map_err(unknown)?;
            if audit.receipt.session_id != *session {
                return Err(unknown("foreign fixed owner role"));
            }
            let actual = self
                .source_audit(session, &audit.receipt.operation_id)
                .ok_or_else(|| unknown("fixed owner original missing"))?;
            proof.measure(actual)?;
            if !same_audit(actual, audit) {
                return Err(unknown("fixed owner original differs"));
            }
        }
        validate_role(role)?;
        let target = &role.reserve.record.snapshot.target;
        let current = self
            .records
            .get(&(session.clone(), target.id.clone()))
            .ok_or_else(|| unknown("fixed owner current missing"))?;
        let current = self.checked_for_read(session, &target.id, current, proof)?;
        if current != &role.joined.record || !current.token.is_empty() {
            return Err(unknown("fixed owner current is not exact retired original"));
        }
        Ok(())
    }
}

fn role_audits<'a>(role: &FixedInitialOwnerRole<'a>) -> [Option<&'a Audit>; 4] {
    [
        Some(role.reserve),
        Some(role.begin),
        role.activation,
        Some(role.joined),
    ]
}
fn same_audit(actual: &Audit, expected: &Audit) -> bool {
    actual.version == expected.version
        && actual.receipt == expected.receipt
        && actual.record == expected.record
        && actual.publication == expected.publication
}
fn audit_in_scope(scope: &SessionId, audit: &Audit, session: &SessionId) -> bool {
    scope == session
        || &audit.receipt.session_id == session
        || &audit.record.snapshot.binding.session_id == session
        || &audit.receipt.owner.binding.session_id == session
}
fn validate_role(role: &FixedInitialOwnerRole<'_>) -> Result<(), Failure> {
    let reserve = role.reserve;
    let begin = role.begin;
    let joined = role.joined;
    if reserve.receipt.operation != SessionOwnerOperation::Reserve
        || begin.receipt.operation != SessionOwnerOperation::Begin
        || joined.receipt.operation != SessionOwnerOperation::Join
        || joined.record.snapshot.state != SessionOwnerState::Joined
        || !joined.record.token.is_empty()
        || role_audits(role).into_iter().flatten().any(|audit| {
            audit.record.snapshot.target != reserve.record.snapshot.target
                || audit.record.origin != reserve.receipt.operation_id
        })
    {
        return Err(unknown("fixed owner role operations/generation"));
    }
    crate::session_owner::audit::verify_current(&begin.record, reserve, begin).map_err(unknown)?;
    crate::session_owner::audit::verify_current(&joined.record, reserve, joined)
        .map_err(unknown)?;
    validate_role_kind(&reserve.record, role.activation)?;
    if let Some(activated) = role.activation {
        crate::session_owner::owned_scope::source::current(
            &joined.record,
            reserve,
            begin,
            Some(activated),
        )
        .map_err(unknown)?;
        if joined.record.owned_join.is_none() {
            return Err(unknown("fixed owner missing original owned Join"));
        }
    } else if joined.record.owned_scope.is_some() || joined.record.owned_join.is_some() {
        return Err(unknown("fixed preparation root has unexpected activation"));
    }
    Ok(())
}
fn validate_role_kind(record: &Record, activation: Option<&Audit>) -> Result<(), Failure> {
    let valid = record.service.is_none()
        && record.successor_driver.is_none()
        && match (
            record.preparation.is_some(),
            record.resource.is_some(),
            record.driver.is_some(),
        ) {
            (true, false, false) => activation.is_none(),
            (false, true, false) | (false, false, true) => activation.is_some(),
            _ => false,
        };
    if !valid {
        return Err(unknown("fixed owner role native kind"));
    }
    Ok(())
}

#[cfg(test)]
#[path = "fixed_initial_coverage/tests.rs"]
mod tests;
