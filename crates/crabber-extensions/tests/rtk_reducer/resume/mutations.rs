use super::{Field, Filter, Options, Selection};
use sha2::{Digest, Sha256};
use std::{fs, os::unix::fs::PermissionsExt, time::Duration};
pub(super) type Edit = fn(&mut Options);
pub(super) fn mutations() -> Vec<(&'static str, Edit)> {
    vec![
        ("max_bindings", |o| o.limits.max_bindings = 9),
        ("max_fields_per_binding", |o| {
            o.limits.max_fields_per_binding = 9
        }),
        ("max_input_matches", |o| o.limits.max_input_matches = 9),
        ("max_config_bytes", |o| {
            o.limits.max_config_bytes = 32 * 1024
        }),
        ("max_input_bytes", |o| o.limits.max_input_bytes = 8 * 1024),
        ("max_field_bytes", |o| o.limits.max_field_bytes = 512 * 1024),
        ("max_concurrent", |o| o.limits.max_concurrent = 3),
        ("max_stdout_bytes", |o| {
            o.limits.max_stdout_bytes = 128 * 1024
        }),
        ("timeout", |o| o.limits.timeout = Duration::from_secs(5)),
        ("kill_wait", |o| o.limits.kill_wait = Duration::from_secs(2)),
        ("shutdown_grace", |o| {
            o.limits.shutdown_grace = Duration::from_secs(20)
        }),
        ("order", |o| o.order += 1),
        ("digest", |o| {
            let p = o.executable_path.with_file_name("other");
            fs::write(&p, "#!/bin/sh\nexit 1\n").unwrap();
            fs::set_permissions(&p, fs::Permissions::from_mode(0o700)).unwrap();
            o.executable_sha256 = format!("{:x}", Sha256::digest(fs::read(&p).unwrap()));
            o.executable_path = p;
        }),
        ("binding_added", |o| {
            let mut b = o.bindings[0].clone();
            b.tool_name = "extra".into();
            o.bindings.push(b);
        }),
        ("binding_removed", |o| {
            o.bindings.pop();
        }),
        ("field_added", |o| {
            if let Selection::JsonFields(f) = &mut o.bindings[1].selection {
                f.push(Field {
                    name: "extra".into(),
                    filter: Filter::Log,
                });
            }
        }),
        ("filter", |o| {
            o.bindings[0].selection = Selection::Text(Filter::GoTest)
        }),
        ("match_value", |o| {
            o.bindings[1]
                .match_input
                .as_mut()
                .unwrap()
                .equals
                .push("cargo test -q".into())
        }),
        ("match_field", |o| {
            o.bindings[1].match_input.as_mut().unwrap().field = "command".into()
        }),
        ("selection", |o| {
            o.bindings[0].selection = Selection::JsonFields(vec![Field {
                name: "out".into(),
                filter: Filter::Log,
            }])
        }),
    ]
}

pub(super) fn additional_mutations() -> Vec<(&'static str, Edit)> {
    vec![
        ("match_removed", |o| o.bindings[1].match_input = None),
        ("field_filter", |o| {
            if let Selection::JsonFields(fields) = &mut o.bindings[1].selection {
                fields[0].filter = Filter::Log;
            }
        }),
        ("field_renamed", |o| {
            if let Selection::JsonFields(fields) = &mut o.bindings[1].selection {
                fields[0].name = "out".into();
            }
        }),
    ]
}
