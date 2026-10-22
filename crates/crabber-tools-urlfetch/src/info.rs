use crabber::core::ToolInfo;
/// Deterministic model-facing URL fetch metadata.
pub fn info() -> ToolInfo {
    ToolInfo { name: "url_fetch".into(), description: "Fetch the raw text content of a file:// or https:// URL and return it as a string. Supported schemes: file:// (local filesystem) and https://. Fails fast with a structured error if the resource does not exist or is not accessible. Does not follow redirects beyond the standard library's defaults. Does not parse HTML, strip CSS, or interpret JavaScript.".into(), parameters: serde_json::json!({"type": "object", "additionalProperties": false, "properties": {"url": {"type": "string", "minLength": 1, "description": "URL to fetch. Supported schemes: file:// (local filesystem) and https://. Returns the raw text content of the resource."}}, "required": ["url"]}), retry_safe: true, required_permissions: vec!["network.http.fetch".into(), crabber_tools_core::permission::FS_READ.into()] }
}
