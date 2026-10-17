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
    admission: &crate::queue::VerifiedAdmission,
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
    admission: &'env crate::queue::VerifiedAdmission,
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

/// Keeps the mandatory Raw caller and its optional scoped Queue accounting
/// provenance together through the actual common admission body.
#[derive(Clone, Copy)]
pub(crate) struct Dispatch<'a> {
    pub caller: Caller,
    selected: Option<&'a SelectedInput<'a>>,
}
impl Dispatch<'_> {
    pub(crate) fn raw(caller: Caller) -> Self {
        Self {
            caller,
            selected: None,
        }
    }
    pub(crate) fn selected(&self) -> Option<&SelectedInput<'_>> {
        self.selected
    }
}
impl SelectedInput<'_> {
    pub(crate) fn dispatch(&self, caller: Caller) -> Dispatch<'_> {
        Dispatch {
            caller,
            selected: Some(self),
        }
    }
}
