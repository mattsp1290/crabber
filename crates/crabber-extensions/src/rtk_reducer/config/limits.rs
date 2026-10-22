use super::*;
pub(super) fn validate(l: &Limits) -> Result<(), ExtensionError> {
    let Limits {
        max_bindings,
        max_fields_per_binding,
        max_input_matches,
        max_config_bytes,
        max_input_bytes,
        max_field_bytes,
        max_concurrent,
        max_stdout_bytes,
        timeout,
        kill_wait,
        shutdown_grace,
    } = l;
    for (value, cap) in [
        (*max_bindings, 256),
        (*max_fields_per_binding, 256),
        (*max_input_matches, 256),
        (*max_config_bytes, 1024 * 1024),
        (*max_input_bytes, 1024 * 1024),
        (*max_field_bytes, 1024 * 1024),
        (*max_concurrent, 64),
        (*max_stdout_bytes, 1024 * 1024),
    ] {
        if value == 0 || value > cap {
            return Err(crate::config_error("limits"));
        }
    }
    if *max_stdout_bytes > *max_field_bytes {
        return Err(crate::config_error("limit-order"));
    }
    if [*timeout, *kill_wait]
        .iter()
        .any(|d| d.is_zero() || *d > Duration::from_secs(60))
    {
        return Err(crate::config_error("timeouts"));
    }
    if shutdown_grace.is_zero()
        || *shutdown_grace > Duration::from_secs(120)
        || *shutdown_grace < *timeout + *kill_wait
    {
        return Err(crate::config_error("shutdown-grace"));
    }
    Ok(())
}
