//! Print root-free tool metadata and schema identities for auditing.
fn main() {
    for (id, info, hash) in crabber_tools_catalog::metadata() {
        println!(
            "{}",
            serde_json::json!({"id":id.id(),"name":info.name,"description":info.description,"parameters":info.parameters,"retry_safe":info.retry_safe,"permissions":info.required_permissions,"schema_hash":hash})
        );
    }
}
