//! Shared pure validation after capturing each concrete factory's metadata.
use crate::{ComponentIdentity, ExtensionError};
use std::collections::BTreeSet;

pub(super) fn validate(
    identity: &ComponentIdentity,
    config: &str,
    names: &mut BTreeSet<String>,
    invalid: &'static str,
) -> Result<(), ExtensionError> {
    if identity.id.is_empty()
        || identity.id.len() > 128
        || identity.id.chars().any(char::is_control)
        || identity.version.is_empty()
        || identity.version.len() > 128
        || identity.version.chars().any(char::is_control)
        || !names.insert(identity.id.clone())
        || config.len() > 16384
    {
        return Err(ExtensionError::Plan(invalid.into()));
    }
    Ok(())
}
