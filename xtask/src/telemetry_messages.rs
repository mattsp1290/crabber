//! Static library diagnostics are part of the redaction boundary.
use std::{collections::HashSet, fs, path::Path};
use syn::{LitStr, Macro, visit::Visit};

const CRATES: &[&str] = &[
    "crabber-runtime",
    "crabber-session",
    "crabber-providers",
    "crabber-extension",
    "crabber-wasm",
];

pub fn check(workspace: &Path) {
    let catalog = fs::read_to_string(workspace.join("crates/crabber-obs/src/layers/mod.rs"))
        .expect("read telemetry message catalog");
    let parsed = syn::parse_file(&catalog).expect("parse telemetry message catalog");
    let mut messages = None;
    for item in parsed.items {
        if let syn::Item::Const(item) = item
            && item.ident == "MESSAGES"
        {
            let syn::Expr::Reference(reference) = *item.expr else {
                panic!("MESSAGES must be a literal slice");
            };
            let syn::Expr::Array(array) = *reference.expr else {
                panic!("MESSAGES must be a literal slice");
            };
            messages = Some(
                array
                    .elems
                    .into_iter()
                    .map(|expr| {
                        let syn::Expr::Lit(syn::ExprLit {
                            lit: syn::Lit::Str(value),
                            ..
                        }) = expr
                        else {
                            panic!("MESSAGES entries must be string literals");
                        };
                        value.value()
                    })
                    .collect::<HashSet<_>>(),
            );
        }
    }
    let messages = messages.expect("MESSAGES catalog exists");
    let mut failures = Vec::new();
    for name in CRATES {
        walk(
            &workspace.join("crates").join(name).join("src"),
            &messages,
            &mut failures,
        );
    }
    assert!(
        failures.is_empty(),
        "unsafe telemetry messages:\n{}",
        failures.join("\n")
    );
    println!("telemetry static-message check passed");
}

fn walk(path: &Path, messages: &HashSet<String>, failures: &mut Vec<String>) {
    for entry in fs::read_dir(path).expect("read library sources") {
        let path = entry.expect("read library source entry").path();
        if path.is_dir() {
            walk(&path, messages, failures);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            let source = fs::read_to_string(&path).expect("read Rust source");
            let file = syn::parse_file(&source).expect("parse Rust source");
            Check {
                path: &path,
                messages,
                failures,
            }
            .visit_file(&file);
        }
    }
}

struct Check<'a> {
    path: &'a Path,
    messages: &'a HashSet<String>,
    failures: &'a mut Vec<String>,
}
impl Check<'_> {
    fn check_macro(&mut self, node: &Macro) {
        if node.path.segments.last().is_some_and(|part| {
            matches!(
                part.ident.to_string().as_str(),
                "trace" | "debug" | "info" | "warn" | "error" | "event"
            )
        }) {
            // Groups stay opaque: commas in expressions cannot become argument separators.
            // A message is the final argument and must consist of one string literal.
            let mut tail = Vec::new();
            let mut last = Vec::new();
            for token in node.tokens.clone() {
                if matches!(&token, proc_macro2::TokenTree::Punct(p) if p.as_char() == ',') {
                    if !tail.is_empty() {
                        last = std::mem::take(&mut tail);
                    }
                } else {
                    tail.push(token);
                }
            }
            if tail.is_empty() {
                tail = last;
            }
            let tokens: proc_macro2::TokenStream = tail.into_iter().collect();
            let message = syn::parse2::<LitStr>(tokens).ok();
            if !message.as_ref().is_some_and(|value| {
                let text = value.value();
                !text.contains('{') && self.messages.contains(&text)
            }) {
                self.failures.push(format!(
                    "{}:{}: diagnostic must end with an allowlisted static message",
                    self.path.display(),
                    node.path.segments.last().unwrap().ident.span().start().line
                ));
            }
        }
    }

    // syn treats macro bodies as opaque tokens, so Visit alone misses logging
    // hidden inside macro_rules, async helper macros, or generated expressions.
    fn nested(&mut self, tokens: proc_macro2::TokenStream) {
        use proc_macro2::TokenTree;
        let tokens: Vec<_> = tokens.into_iter().collect();
        let mut index = 0;
        while index < tokens.len() {
            if matches!(&tokens[index], TokenTree::Ident(_)) {
                let mut end = index + 1;
                while matches!(tokens.get(end), Some(TokenTree::Punct(p)) if p.as_char() == ':')
                    && matches!(tokens.get(end + 1), Some(TokenTree::Punct(p)) if p.as_char() == ':')
                    && matches!(tokens.get(end + 2), Some(TokenTree::Ident(_)))
                {
                    end += 3;
                }
                if matches!(tokens.get(end), Some(TokenTree::Punct(p)) if p.as_char() == '!')
                    && let Some(TokenTree::Group(group)) = tokens.get(end + 1)
                {
                    let expression = tokens[index..=end + 1].iter().cloned().collect();
                    if let Ok(expression) = syn::parse2::<syn::ExprMacro>(expression) {
                        self.check_macro(&expression.mac);
                    }
                    self.nested(group.stream());
                    index = end + 2;
                    continue;
                }
            }
            if let TokenTree::Group(group) = &tokens[index] {
                self.nested(group.stream());
            }
            index += 1;
        }
    }
}
impl<'ast> Visit<'ast> for Check<'_> {
    fn visit_macro(&mut self, node: &'ast Macro) {
        self.check_macro(node);
        self.nested(node.tokens.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn checks_qualified_event_messages_and_rejects_formatting() {
        let messages = HashSet::from(["safe diagnostic".to_owned()]);
        for (source, valid) in [
            (
                r#"fn f() { tracing::warn!(target: targets::RUNTIME, error.kind = "transport", "safe diagnostic"); }"#,
                true,
            ),
            (
                r#"fn f() { event!(target: T, tracing::Level::WARN, count = 1, "safe diagnostic",); }"#,
                true,
            ),
            (r#"fn f() { warn!("secret {}", payload); }"#, false),
            (r#"fn f() { info!("unknown"); }"#, false),
            (r"fn f() { error!(message); }", false),
            (
                r#"macro_rules! logs { () => { tracing::info!("safe diagnostic"); } }"#,
                true,
            ),
            (
                r#"macro_rules! logs { ($value:expr) => { tracing::warn!("secret {}", $value); } }"#,
                false,
            ),
        ] {
            let mut failures = Vec::new();
            Check {
                path: Path::new("fixture.rs"),
                messages: &messages,
                failures: &mut failures,
            }
            .visit_file(&syn::parse_file(source).unwrap());
            assert_eq!(failures.is_empty(), valid, "{source}");
        }
    }
}
