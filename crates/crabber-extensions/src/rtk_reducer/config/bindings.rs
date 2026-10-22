use super::*;
pub(super) fn canonicalize(
    source: Vec<Binding>,
    l: &Limits,
) -> Result<BTreeMap<String, CanonicalBinding>, ExtensionError> {
    if source.is_empty() {
        return Err(crate::config_error("bindings-required"));
    }
    if source.len() > l.max_bindings {
        return Err(crate::config_error("binding-count"));
    }
    let mut bytes = 0usize;
    for b in &source {
        bytes = bytes.saturating_add(b.tool_name.len());
        match &b.selection {
            Selection::Text(f) => bytes = bytes.saturating_add(f.cli_name().len()),
            Selection::JsonFields(fields) => {
                for f in fields {
                    bytes = bytes
                        .saturating_add(f.name.len())
                        .saturating_add(f.filter.cli_name().len());
                }
            }
        }
        if let Some(m) = &b.match_input {
            bytes = bytes.saturating_add(m.field.len());
            for v in &m.equals {
                bytes = bytes.saturating_add(v.len());
            }
        }
    }
    if bytes > l.max_config_bytes {
        return Err(crate::config_error("config-bytes"));
    }
    let mut bindings = BTreeMap::new();
    for b in source {
        if !crate::valid_identity(&b.tool_name)
            || b.tool_name.chars().any(char::is_whitespace)
            || b.tool_name.contains('/')
        {
            return Err(crate::config_error("tool-name"));
        }
        if bindings.contains_key(&b.tool_name) {
            return Err(crate::config_error("duplicate-tool"));
        }
        let mut selection = b.selection;
        if let Selection::JsonFields(fields) = &mut selection {
            if fields.is_empty() {
                return Err(crate::config_error("json-fields"));
            }
            if fields.len() > l.max_fields_per_binding {
                return Err(crate::config_error("field-count"));
            }
            let mut names = BTreeSet::new();
            for f in fields.iter() {
                if f.name.is_empty() || f.name.contains('\0') {
                    return Err(crate::config_error("field-name"));
                }
                if !names.insert(&f.name) {
                    return Err(crate::config_error("duplicate-field"));
                }
            }
            fields.sort_by(|a, b| a.name.cmp(&b.name));
        }
        let match_input = if let Some(m) = b.match_input {
            if m.field.is_empty() || m.field.contains('\0') {
                return Err(crate::config_error("match-field"));
            }
            if m.equals.is_empty() || m.equals.len() > l.max_input_matches {
                return Err(crate::config_error("input-match-count"));
            }
            let mut values = BTreeSet::new();
            for v in m.equals {
                if v.contains('\0') || v.len() > l.max_input_bytes {
                    return Err(crate::config_error("input-match-value"));
                }
                if !values.insert(v) {
                    return Err(crate::config_error("duplicate-input-match"));
                }
            }
            Some((m.field, values))
        } else {
            None
        };
        bindings.insert(
            b.tool_name,
            CanonicalBinding {
                match_input,
                selection,
            },
        );
    }
    Ok(bindings)
}
