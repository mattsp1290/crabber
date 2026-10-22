use crabber::ExtensionError;
use serde::Serialize;
use std::{collections::BTreeMap, fmt};

/// Shell grammar selected by a host binding.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Dialect {
    /// Portable shell grammar with conservative Bash ambiguity denials.
    #[default]
    Posix,
    /// Supported Bash grammar.
    Bash,
}
/// Exact host tool name and literal command-field binding.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Binding {
    /// Case-sensitive host tool name.
    pub tool_name: String,
    /// Literal top-level JSON key; periods have no special meaning.
    pub command_field: String,
    /// Grammar used to analyze this tool's command.
    pub dialect: Dialect,
}
/// Conventional host bindings; hosts must confirm their own tool contracts.
pub fn default_bindings() -> Vec<Binding> {
    [("shell", "cmd"), ("background_job_start", "command")]
        .into_iter()
        .map(|(tool, field)| Binding {
            tool_name: tool.into(),
            command_field: field.into(),
            dialect: Dialect::Posix,
        })
        .collect()
}
/// Deny an executable basename with an exact positional argument prefix.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Rule {
    /// Unique identifier: 1–256 ASCII alphanumerics, `_`, `-`, `.`, or `:`.
    pub id: String,
    /// Executable basename; paths are never resolved.
    pub executable: String,
    /// Exact prefix, including known empty tokens; empty matches every call.
    pub arg_prefix: Vec<String>,
}
/// Hard cap: Binding count.
pub const MAX_BINDINGS: usize = 64;
/// Hard cap: Rule count.
pub const MAX_RULES: usize = 256;
/// Hard cap: Aggregate bytes in each rule.
pub const MAX_RULE_BYTES: usize = 16384;
/// Hard cap: Positional prefix tokens per rule.
pub const MAX_PREFIX_ARGS: usize = 64;
/// Hard cap: Container depth; the root object has depth one.
pub const MAX_JSON_DEPTH: usize = 64;
/// Hard cap: JSON values and object keys visited.
pub const MAX_JSON_NODES: usize = 65536;
/// Hard cap: Bytes in each script admitted to the parser.
pub const MAX_COMMAND_BYTES: usize = 262144;
/// Hard cap: Script bytes shared across nested analyses.
pub const MAX_ANALYSIS_BYTES: usize = 1048576;
/// Hard cap: AST values constructed across the analysis.
pub const MAX_AST_NODES: usize = 32768;
/// Hard cap: syntactic nesting and wrapper delegation depth.
/// Reduced from the proposed 128 after debug analysis overflowed a 512 KiB
/// thread; the full hard-cap stack corpus passes at 32.
pub const MAX_AST_DEPTH: usize = 32;
/// Hard cap: Words constructed across the analysis.
pub const MAX_WORDS: usize = 16384;
/// Hard cap: Decoded bytes in each word and configuration string.
pub const MAX_WORD_BYTES: usize = 262144;
/// Hard cap: Wrapper delegations in a command chain.
pub const MAX_WRAPPER_DEPTH: usize = 64;
/// Hard cap: Concurrent bound-tool checks in `CommandGuard`.
pub const MAX_IN_FLIGHT: usize = 1024;
/// Required positive resource limits. No state or input survives analysis.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Limits {
    /// Binding count.
    pub max_bindings: usize,
    /// Rule count.
    pub max_rules: usize,
    /// Aggregate bytes in each rule.
    pub max_rule_bytes: usize,
    /// Positional prefix tokens per rule.
    pub max_prefix_args: usize,
    /// Container depth; the root object has depth one.
    pub max_json_depth: usize,
    /// JSON values and object keys visited.
    pub max_json_nodes: usize,
    /// Bytes in each script admitted to the parser.
    pub max_command_bytes: usize,
    /// Script bytes shared across nested analyses.
    pub max_analysis_bytes: usize,
    /// AST values constructed across the analysis.
    pub max_ast_nodes: usize,
    /// Syntactic nesting and wrapper delegation depth.
    pub max_ast_depth: usize,
    /// Words constructed across the analysis.
    pub max_words: usize,
    /// Decoded bytes in each word and configuration string.
    pub max_word_bytes: usize,
    /// Wrapper delegations in a command chain.
    pub max_wrapper_depth: usize,
    /// `CommandGuard` takes one permit per bound-tool check; pure analysis takes none.
    pub max_in_flight: usize,
}
impl Limits {
    fn valid(&self) -> bool {
        let ranges = [
            (self.max_bindings, MAX_BINDINGS),
            (self.max_rules, MAX_RULES),
            (self.max_rule_bytes, MAX_RULE_BYTES),
            (self.max_prefix_args, MAX_PREFIX_ARGS),
            (self.max_json_depth, MAX_JSON_DEPTH),
            (self.max_json_nodes, MAX_JSON_NODES),
            (self.max_command_bytes, MAX_COMMAND_BYTES),
            (self.max_analysis_bytes, MAX_ANALYSIS_BYTES),
            (self.max_ast_nodes, MAX_AST_NODES),
            (self.max_ast_depth, MAX_AST_DEPTH),
            (self.max_words, MAX_WORDS),
            (self.max_word_bytes, MAX_WORD_BYTES),
            (self.max_wrapper_depth, MAX_WRAPPER_DEPTH),
            (self.max_in_flight, MAX_IN_FLIGHT),
        ];
        ranges.into_iter().all(|(n, cap)| n > 0 && n <= cap)
            && self.max_analysis_bytes >= self.max_command_bytes
    }
}
/// Owned construction options. Empty bindings or rules are invalid.
#[derive(Clone, Debug)]
pub struct Options {
    /// Explicit tool bindings.
    pub bindings: Vec<Binding>,
    /// Host deny rules.
    pub rules: Vec<Rule>,
    /// Required bounded analysis settings.
    pub limits: Limits,
}
/// Immutable, deny-only syntax policy. This is trusted inspection, not a sandbox.
pub struct Policy {
    pub(super) bindings: Vec<Binding>,
    pub(super) rules: Vec<Rule>,
    pub(super) by_basename: BTreeMap<String, Vec<usize>>,
    pub(super) limits: Limits,
    hash: String,
}
impl fmt::Debug for Policy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Policy")
            .field("binding_count", &self.bindings.len())
            .field("rule_count", &self.rules.len())
            .field("hash", &self.hash)
            .finish()
    }
}
#[derive(Serialize)]
struct Behavior {
    schema: &'static str,
    outcomes: &'static str,
    matching: &'static str,
    wrappers: &'static str,
    builtins: &'static str,
    grammar: &'static str,
    parser: &'static str,
}
const BEHAVIOR: Behavior = Behavior {
    schema: "command-policy-v1",
    outcomes: "fixed-outcomes-v1",
    matching: "posix-basename-positional-prefix-v1",
    wrappers: "bounded-wrappers-special-targets-v1",
    builtins: "builtin-operands-v1",
    grammar: "fail-closed-grammar-v1",
    parser: "crabber-extensions-command-guard-parser-v2",
};
fn identifier(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 256
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-.:".contains(&b))
}
impl Policy {
    /// Validate settings in contract order, then canonicalize and freeze ownership.
    /// Identifiers accept 1–256 ASCII alphanumerics, `_`, `-`, `.`, and `:`.
    pub fn new(mut options: Options) -> Result<Self, ExtensionError> {
        let fail = crate::config_error;
        let l = &options.limits;
        if !l.valid() {
            return Err(fail("command-guard-limits"));
        }
        if options.bindings.is_empty()
            || options.bindings.len() > l.max_bindings
            || options.rules.is_empty()
            || options.rules.len() > l.max_rules
        {
            return Err(fail("command-guard-count"));
        }
        if options.bindings.iter().any(|b| {
            b.tool_name.len() > l.max_word_bytes || b.command_field.len() > l.max_word_bytes
        }) {
            return Err(fail("command-guard-binding-size"));
        }
        if options.bindings.iter().any(|b| {
            !identifier(&b.tool_name)
                || b.command_field.trim().is_empty()
                || b.command_field.contains('\0')
        }) {
            return Err(fail("command-guard-binding"));
        }
        for r in &options.rules {
            if r.arg_prefix.len() > l.max_prefix_args {
                return Err(fail("command-guard-rule-size"));
            }
            let mut left = Some(l.max_rule_bytes);
            for s in std::iter::once(&r.id)
                .chain(std::iter::once(&r.executable))
                .chain(&r.arg_prefix)
            {
                left = left.and_then(|n| n.checked_sub(s.len()));
            }
            if r.arg_prefix.len() > l.max_prefix_args
                || left.is_none()
                || r.arg_prefix.iter().any(|s| s.len() > l.max_word_bytes)
            {
                return Err(fail("command-guard-rule-size"));
            }
        }
        if options
            .rules
            .iter()
            .any(|r| r.arg_prefix.iter().any(|s| s.contains('\0')))
        {
            return Err(fail("command-guard-rule-token"));
        }
        if options.rules.iter().any(|r| {
            !identifier(&r.id)
                || r.executable.is_empty()
                || matches!(r.executable.as_str(), "." | "..")
                || r.executable.contains(['/', '\\', '\0'])
                || r.executable.len() > l.max_word_bytes
        }) {
            return Err(fail("command-guard-rule"));
        }
        options
            .bindings
            .sort_by(|a, b| a.tool_name.cmp(&b.tool_name));
        options.rules.sort_by(|a, b| a.id.cmp(&b.id));
        if options.bindings.windows(2).any(|w| {
            w.first()
                .zip(w.get(1))
                .is_some_and(|(a, b)| a.tool_name == b.tool_name)
        }) {
            return Err(fail("command-guard-duplicate-binding"));
        }
        if options
            .rules
            .windows(2)
            .any(|w| w.first().zip(w.get(1)).is_some_and(|(a, b)| a.id == b.id))
        {
            return Err(fail("command-guard-duplicate-rule"));
        }
        let hash = crate::config_hash(&(
            "command-guard-v1",
            BEHAVIOR,
            &options.bindings,
            &options.rules,
            &options.limits,
        ));
        let mut by_basename: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        for (i, r) in options.rules.iter().enumerate() {
            by_basename.entry(r.executable.clone()).or_default().push(i);
        }
        Ok(Self {
            bindings: options.bindings,
            rules: options.rules,
            limits: options.limits,
            by_basename,
            hash,
        })
    }
    /// Versioned fingerprint of every behavior-bearing configuration value.
    pub fn config_hash(&self) -> &str {
        &self.hash
    }
    /// Bindings sorted by tool name.
    pub fn bindings(&self) -> &[Binding] {
        &self.bindings
    }
    /// Rules sorted by identifier.
    pub fn rules(&self) -> &[Rule] {
        &self.rules
    }
    /// Validated limits; `CommandGuard` enforces the bound-tool capacity limit.
    pub fn limits(&self) -> &Limits {
        &self.limits
    }
    /// Exact, case-sensitive tool-name lookup.
    pub fn binding(&self, tool_name: &str) -> Option<&Binding> {
        self.bindings
            .binary_search_by(|b| b.tool_name.as_str().cmp(tool_name))
            .ok()
            .and_then(|i| self.bindings.get(i))
    }
}
