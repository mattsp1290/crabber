//! Source inventory for coverage audits and fixture adoption. This deliberately
//! does not claim macro expansion, feature resolution or executed test coverage.

use serde_json::{Value, json};
use std::{collections::BTreeSet, fs, path::Path};
use syn::{
    spanned::Spanned,
    visit::{self, Visit},
};

pub(crate) fn tests(workspace: &Path) -> Value {
    let mut records = Vec::new();
    for root in ["crates", "examples", "xtask"] {
        directory(workspace, &workspace.join(root), &mut records);
    }
    json!({"version":1,"kind":"source-only; compare with Cargo/nextest discovery and actual execution","functions":records})
}
fn directory(workspace: &Path, path: &Path, records: &mut Vec<Value>) {
    let mut entries: Vec<_> = fs::read_dir(path)
        .expect("read inventory directory")
        .map(|entry| entry.expect("inventory entry").path())
        .collect();
    entries.sort();
    for path in entries {
        if path.is_dir() {
            if !matches!(
                path.file_name().and_then(|name| name.to_str()),
                Some("target" | ".git" | ".agents" | "reviews")
            ) {
                directory(workspace, &path, records);
            }
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            let source = fs::read_to_string(&path).expect("read inventoried Rust source");
            let file = syn::parse_file(&source)
                .unwrap_or_else(|error| panic!("cannot inventory {}: {error}", path.display()));
            let mut visitor = Inventory {
                source: &source,
                file: path.strip_prefix(workspace).unwrap().to_str().unwrap(),
                modules: Vec::new(),
                cfg: guards(&source, &file.attrs),
                records: Vec::new(),
            };
            visitor.visit_file(&file);
            records.extend(visitor.records);
        }
    }
}
fn guards(source: &str, attrs: &[syn::Attribute]) -> Vec<String> {
    attrs
        .iter()
        .filter(|attr| attr.path().is_ident("cfg") || attr.path().is_ident("cfg_attr"))
        .map(|attr| source[attr.span().byte_range()].to_owned())
        .collect()
}
struct Inventory<'a> {
    source: &'a str,
    file: &'a str,
    modules: Vec<String>,
    cfg: Vec<String>,
    records: Vec<Value>,
}
impl<'ast> Visit<'ast> for Inventory<'_> {
    fn visit_item_mod(&mut self, module: &'ast syn::ItemMod) {
        let previous = self.cfg.len();
        self.cfg.extend(guards(self.source, &module.attrs));
        self.modules.push(module.ident.to_string());
        visit::visit_item_mod(self, module);
        self.modules.pop();
        self.cfg.truncate(previous);
    }
    fn visit_item_fn(&mut self, function: &'ast syn::ItemFn) {
        let test = function.attrs.iter().any(|attr| {
            attr.path()
                .segments
                .last()
                .is_some_and(|segment| segment.ident == "test")
        });
        let mut facts = Facts::default();
        facts.visit_block(&function.block);
        if test
            || facts
                .environment
                .iter()
                .any(|name| name.contains("POSTGRES"))
            || function.sig.ident == "test_url"
        {
            let mut cfg = self.cfg.clone();
            cfg.extend(guards(self.source, &function.attrs));
            self.records.push(json!({
                "file":self.file,"modules":self.modules,"name":function.sig.ident.to_string(),"test":test,
                "ignored":function.attrs.iter().any(|attr|attr.path().is_ident("ignore")),
                "unit_return":matches!(function.sig.output,syn::ReturnType::Default),
                "cfg":cfg,"environment_names":facts.environment,"calls":facts.calls,"locals":facts.locals,
                "body_start":function.block.brace_token.span.open().byte_range().end,
                "body_end":function.block.brace_token.span.close().byte_range().start,
            }));
        }
        visit::visit_item_fn(self, function);
    }
}
#[derive(Default)]
struct Facts {
    environment: BTreeSet<String>,
    calls: BTreeSet<String>,
    locals: Vec<Value>,
}
impl<'ast> Visit<'ast> for Facts {
    fn visit_item_fn(&mut self, _: &'ast syn::ItemFn) {} // nested functions own their facts
    fn visit_expr_call(&mut self, call: &'ast syn::ExprCall) {
        if let syn::Expr::Path(path) = call.func.as_ref() {
            self.calls.insert(
                path.path
                    .segments
                    .iter()
                    .map(|segment| segment.ident.to_string())
                    .collect::<Vec<_>>()
                    .join("::"),
            );
            if path
                .path
                .segments
                .last()
                .is_some_and(|segment| segment.ident == "var" || segment.ident == "var_os")
                && let Some(syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Str(name),
                    ..
                })) = call.args.first()
            {
                self.environment.insert(name.value());
            }
        }
        visit::visit_expr_call(self, call);
    }
    fn visit_local(&mut self, local: &'ast syn::Local) {
        let (variant, binding) = match &local.pat {
            syn::Pat::TupleStruct(pattern) if pattern.elems.len() == 1 => (
                pattern
                    .path
                    .segments
                    .last()
                    .map(|segment| segment.ident.to_string()),
                pattern.elems.first(),
            ),
            pattern => (None, Some(pattern)),
        };
        if let Some(syn::Pat::Ident(binding)) = binding {
            self.locals
                .push(json!({"name":binding.ident.to_string(),"variant":variant,
                "start":local.span().byte_range().start,"end":local.span().byte_range().end,
                "has_else":local.init.as_ref().is_some_and(|init|init.diverge.is_some())}));
        }
        visit::visit_local(self, local);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn records_nested_guards_environment_names_and_exact_utf8_body_ranges() {
        let source = "const MARK: &str = \"🦀\"; mod nested { #[cfg(feature=\"postgres\")] #[tokio::test] async fn parent() { let Ok(url) = std::env::var(\"CRABBER_TEST_POSTGRES_URL\") else { return; }; assert!(!url.is_empty()); } }";
        let file = syn::parse_file(source).unwrap();
        let mut visitor = Inventory {
            source,
            file: "test.rs",
            modules: Vec::new(),
            cfg: Vec::new(),
            records: Vec::new(),
        };
        visitor.visit_file(&file);
        assert_eq!(visitor.records.len(), 1);
        let record = &visitor.records[0];
        assert_eq!(
            record["environment_names"],
            json!(["CRABBER_TEST_POSTGRES_URL"])
        );
        assert_eq!(record["modules"], json!(["nested"]));
        assert_eq!(record["locals"][0]["name"], "url");
        assert!(record["locals"][0]["has_else"].as_bool().unwrap());
        let start = usize::try_from(record["body_start"].as_u64().unwrap()).unwrap();
        let end = usize::try_from(record["body_end"].as_u64().unwrap()).unwrap();
        assert!(source[start..end].trim().starts_with("let Ok(url)"));
        assert!(
            source[start..end]
                .trim()
                .ends_with("assert!(!url.is_empty());")
        );
    }
}
