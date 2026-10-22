use super::{Limits, invalid};
use crabber::ExtensionError;
use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    query: String,
}

pub(super) fn normalize(value: Value, limits: &Limits) -> Result<String, ExtensionError> {
    let input: Input = serde_json::from_value(value).map_err(|_| invalid("shape"))?;
    let query = input.query.trim();
    if query.is_empty() || query.contains('\0') || query.len() > limits.max_query_bytes {
        return Err(invalid("query"));
    }
    Ok(query.to_owned())
}
