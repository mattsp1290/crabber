//! Scoped reuse of the actual already selected Queue input carrier.
use super::{Caller, Failure, Proof, unknown};
/// Accounting provenance for one actual Queue-selected logical input carrier.
/// This does not authenticate its content: every canonical validation still runs.
/// The full record was charged before selection/cloning; this scoped visitor
/// reuses that same body, rather than charging each subsequent borrowed check.
/// It does not bound internal canonicalization allocations or Memory's inherited
/// whole-State clone. Independent selected reads, outputs and sources still charge.
pub(crate) struct SelectedInput<'a> {
    prepared: &'a crate::BoundKeyedAdmitRequest,
    proof_address: usize,
    scope: usize,
}
impl SelectedInput<'_> {
    pub(super) fn verify(
        &self,
        prepared: &crate::BoundKeyedAdmitRequest,
        proof: &Proof,
    ) -> Result<(), Failure> {
        if !std::ptr::eq(self.prepared, prepared)
            || self.proof_address != std::ptr::from_ref(proof).addr()
            || !proof.has_selected_scope(self.scope)
        {
            return Err(unknown("different Queue selected input or proof"));
        }
        Ok(())
    }
}
pub(crate) fn with_queue_input<R>(
    admission: &crate::queue::VerifiedAdmission<'_>,
    proof: &mut Proof,
    operation: impl for<'a> FnOnce(&'a SelectedInput<'a>, &'a mut Proof) -> Result<R, crate::StoreError>,
) -> Result<R, crate::StoreError> {
    // Cancellation/panic leaves this slot occupied. No carrier can escape this
    // visitor, and a subsequent visitor fails closed rather than reusing it.
    if proof.has_any_selected_scope() {
        return Err(crate::StoreError::Conflict);
    }
    let tag = 0_u8;
    let scope = std::ptr::from_ref(&tag).addr();
    let _ = proof.replace_selected_scope(Some(scope));
    let selected = SelectedInput {
        prepared: admission.bound_ref(),
        proof_address: std::ptr::from_ref(proof).addr(),
        scope,
    };
    let result = operation(&selected, proof);
    let intact = proof.has_selected_scope(scope);
    let _ = proof.replace_selected_scope(None);
    if intact {
        result
    } else {
        Err(crate::StoreError::Conflict)
    }
}
#[cfg(any(feature = "postgres", feature = "sqlite", test))]
pub(crate) async fn with_queue_input_async<'env, R>(
    admission: &'env crate::queue::VerifiedAdmission<'env>,
    proof: &'env mut Proof,
    operation: impl for<'scope> FnOnce(
        &'scope SelectedInput<'env>,
        &'scope mut Proof,
    ) -> std::pin::Pin<
        Box<dyn Future<Output = Result<R, crate::StoreError>> + Send + 'scope>,
    >,
) -> Result<R, crate::StoreError> {
    // Cancellation/panic leaves this slot occupied. No carrier can escape this
    // visitor, and a subsequent visitor fails closed rather than reusing it.
    if proof.has_any_selected_scope() {
        return Err(crate::StoreError::Conflict);
    }
    let tag = 0_u8;
    let scope = std::ptr::from_ref(&tag).addr();
    let _ = proof.replace_selected_scope(Some(scope));
    let selected = SelectedInput {
        prepared: admission.bound_ref(),
        proof_address: std::ptr::from_ref(proof).addr(),
        scope,
    };
    let result = operation(&selected, proof).await;
    let intact = proof.has_selected_scope(scope);
    let _ = proof.replace_selected_scope(None);
    if intact {
        result
    } else {
        Err(crate::StoreError::Conflict)
    }
}
pub(crate) fn input_request(
    request: &crate::AdmitRequest,
    selected: Option<&SelectedInput<'_>>,
    proof: &mut Proof,
) -> Result<(), Failure> {
    match selected {
        Some(selected) => {
            selected.verify(selected.prepared, proof)?;
            if !std::ptr::eq(request, std::ptr::addr_of!(selected.prepared.keyed.request)) {
                return Err(unknown("different Queue selected request"));
            }
            Ok(())
        }
        None => proof.measure(request),
    }
}
pub(crate) fn input_keyed(
    request: &crate::KeyedAdmitRequest,
    selected: Option<&SelectedInput<'_>>,
    proof: &mut Proof,
) -> Result<(), Failure> {
    match selected {
        Some(selected) => {
            selected.verify(selected.prepared, proof)?;
            if !std::ptr::eq(request, std::ptr::addr_of!(selected.prepared.keyed)) {
                return Err(unknown("different Queue selected keyed input"));
            }
            Ok(())
        }
        None => proof.measure(request),
    }
}

/// Closed caller selection for the actual original Initial publisher only.
#[derive(Clone, Copy)]
pub(crate) enum Dispatch<'a> {
    Raw {
        caller: Caller,
        selected: Option<&'a SelectedInput<'a>>,
    },
    PendingInitial(&'a crate::native_enrollment::pending_initial::VerifiedInitialCall<'a>),
}
impl<'a> Dispatch<'a> {
    pub(crate) fn raw(caller: Caller) -> Self {
        Self::Raw {
            caller,
            selected: None,
        }
    }
    pub(crate) fn pending(
        call: &'a crate::native_enrollment::pending_initial::VerifiedInitialCall<'a>,
    ) -> Self {
        Self::PendingInitial(call)
    }
    pub(crate) fn birth(self) -> crate::native_enrollment::pending_initial::BirthDecision<'a> {
        match self {
            Self::Raw {
                caller: Caller::Raw,
                ..
            } => crate::native_enrollment::pending_initial::BirthDecision::Raw,
            Self::PendingInitial(call) => {
                crate::native_enrollment::pending_initial::BirthDecision::PendingInitial(call)
            }
        }
    }
    pub(crate) fn input_request(
        &self,
        request: &crate::AdmitRequest,
        proof: &mut Proof,
    ) -> Result<(), Failure> {
        match self {
            Self::Raw { selected, .. } => input_request(request, *selected, proof),
            Self::PendingInitial(call) => call.selected().request(request, proof),
        }
    }
    pub(crate) fn input_keyed(
        &self,
        request: &crate::KeyedAdmitRequest,
        proof: &mut Proof,
    ) -> Result<(), Failure> {
        match self {
            Self::Raw { selected, .. } => input_keyed(request, *selected, proof),
            Self::PendingInitial(call) => call.selected().keyed(request, proof),
        }
    }
}
impl SelectedInput<'_> {
    pub(crate) fn dispatch(&self, caller: Caller) -> Dispatch<'_> {
        Dispatch::Raw {
            caller,
            selected: Some(self),
        }
    }
}

/// Same-visitor accounting for the one actual retained Initial preparation.
/// Caller establishes this only after measuring that carrier with this Proof.
/// Every canonical check and independently selected native carrier still runs.
pub(crate) struct SelectedOriginal<'a> {
    original: &'a crate::session_owner::preparation::JoinedPreparation,
    proof_address: usize,
    scope: usize,
}
impl SelectedOriginal<'_> {
    pub(crate) fn verify(
        &self,
        original: &crate::session_owner::preparation::JoinedPreparation,
        proof: &Proof,
    ) -> Result<(), Failure> {
        if !std::ptr::eq(self.original, original)
            || self.proof_address != std::ptr::from_ref(proof).addr()
            || !proof.has_selected_scope(self.scope)
        {
            return Err(unknown("different Initial selected original or proof"));
        }
        Ok(())
    }
    pub(crate) fn original(&self) -> &crate::session_owner::preparation::JoinedPreparation {
        self.original
    }
    pub(crate) fn keyed(
        &self,
        request: &crate::KeyedAdmitRequest,
        proof: &Proof,
    ) -> Result<(), Failure> {
        self.verify(self.original, proof)?;
        let crate::session_owner::preparation::PreparedOperation::AdmitKeyed(bound) =
            self.original.observed_operation()
        else {
            return Err(unknown("different selected Initial keyed kind"));
        };
        if !std::ptr::eq(std::ptr::addr_of!(bound.keyed), request) {
            return Err(unknown("different selected Initial keyed body"));
        }
        Ok(())
    }
    pub(crate) fn request(
        &self,
        request: &crate::AdmitRequest,
        proof: &Proof,
    ) -> Result<(), Failure> {
        self.verify(self.original, proof)?;
        let expected = match self.original.observed_operation() {
            crate::session_owner::preparation::PreparedOperation::Admit(bound) => &bound.request,
            crate::session_owner::preparation::PreparedOperation::AdmitKeyed(bound) => {
                &bound.keyed.request
            }
            _ => return Err(unknown("unsupported selected Initial original")),
        };
        if !std::ptr::eq(expected, request) {
            return Err(unknown("different selected Initial request"));
        }
        Ok(())
    }
}
native_pending_initial_items! {
pub(crate) async fn with_initial_original_async<'env, R>(
    original: &'env crate::session_owner::preparation::JoinedPreparation,
    proof: &'env mut Proof,
    operation: impl for<'scope> FnOnce(
        &'scope SelectedOriginal<'env>,
        &'scope mut Proof,
    ) -> std::pin::Pin<
        Box<
            dyn Future<
                    Output = Result<R, crate::session_owner::preparation::PreparationCommitError>,
                > + Send
                + 'scope,
        >,
    >,
) -> Result<R, crate::session_owner::preparation::PreparationCommitError> {
    use crate::session_owner::SessionOwnerError as Error;
    if proof.has_any_selected_scope() {
        return Err(Error::UnknownStoreOutcome.into());
    }
    let tag = 0_u8;
    let scope = std::ptr::from_ref(&tag).addr();
    let _ = proof.replace_selected_scope(Some(scope));
    let selected = SelectedOriginal {
        original,
        proof_address: std::ptr::from_ref(proof).addr(),
        scope,
    };
    let result = operation(&selected, proof).await;
    let intact = proof.has_selected_scope(scope);
    let _ = proof.replace_selected_scope(None);
    if intact {
        result
    } else {
        Err(Error::UnknownStoreOutcome.into())
    }
}

pub(crate) fn input_preparation(
    original: &crate::session_owner::preparation::JoinedPreparation,
    selected: Option<&SelectedOriginal<'_>>,
    proof: &mut Proof,
) -> Result<(), Failure> {
    match selected {
        Some(selected) => selected.verify(original, proof),
        None => super::preparation_input(original, proof),
    }
}
}
