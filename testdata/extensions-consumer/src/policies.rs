use crabber::extension::ExtensionError;
use crabber_extensions::{
    command_guard as guard, tool_result_redactor as redactor,
    workspace_instructions as instructions,
};
use std::{path::Path, time::Duration};
pub fn instructions(
    boundary: &Path,
) -> Result<instructions::WorkspaceInstructions, ExtensionError> {
    instructions::WorkspaceInstructions::new(instructions::Options {
        order: 100,
        file_names: vec!["AGENTS.md".into()],
        reader_root: instructions::ReaderRoot::Boundary,
        boundaries: vec![boundary.to_str().unwrap().into()],
        limits: instructions::Limits {
            max_file_names: 4,
            max_chain_depth: 8,
            max_file_bytes: 1024,
            max_section_bytes: 4096,
            max_in_flight: 8,
            max_wait: Duration::from_secs(2),
        },
    })
}
pub fn redactor() -> Result<redactor::ToolResultRedactor, ExtensionError> {
    redactor::ToolResultRedactor::new(redactor::Options {
        order: -100,
        excluded_tools: vec![],
        additional_patterns: vec![],
        limits: redactor::Limits {
            max_field_bytes: 4096,
            max_total_bytes: 16384,
            max_depth: 16,
            max_nodes: 128,
            max_matches_per_field: 16,
            max_patterns: 4,
            max_pattern_bytes: 256,
            max_in_flight: 8,
        },
    })
}
pub fn guard() -> Result<guard::CommandGuard, ExtensionError> {
    let mut bindings = guard::default_bindings();
    bindings.push(guard::Binding {
        tool_name: "guarded_marker".into(),
        command_field: "command".into(),
        dialect: guard::Dialect::Posix,
    });
    guard::CommandGuard::new(guard::Options {
        bindings,
        rules: vec![guard::Rule {
            id: "synthetic-deny".into(),
            executable: "rm".into(),
            arg_prefix: vec![],
        }],
        limits: guard::Limits {
            max_bindings: 8,
            max_rules: 8,
            max_rule_bytes: 1024,
            max_prefix_args: 8,
            max_json_depth: 16,
            max_json_nodes: 128,
            max_command_bytes: 4096,
            max_analysis_bytes: 16384,
            max_ast_nodes: 1024,
            max_ast_depth: 16,
            max_words: 256,
            max_word_bytes: 4096,
            max_wrapper_depth: 8,
            max_in_flight: 8,
        },
    })
}
