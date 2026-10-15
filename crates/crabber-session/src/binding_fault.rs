//! Test-only commit seams. No environment-controlled production fault behavior.
use crate::StoreError;
use std::sync::atomic::{AtomicU8, Ordering};

pub(crate) fn trip(fault: &AtomicU8, stage: u8) -> Result<(), StoreError> {
    if fault
        .compare_exchange(stage, 0, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok()
    {
        return Err(StoreError::Validation(
            "execution binding store outcome is unknown".into(),
        ));
    }
    Ok(())
}
