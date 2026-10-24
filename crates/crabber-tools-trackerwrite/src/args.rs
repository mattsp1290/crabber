use crate::TrackerPolicy;
use crabber_tools_core::{ToolError, category};
use serde::Deserialize;
use std::ffi::OsString;
/// Maximum model-supplied comment or close reason in bytes.
pub const MAX_TEXT_BYTES: usize = 64 * 1024;
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct Args {
    pub(crate) op: String,
    pub(crate) id: String,
    #[serde(default)]
    body: String,
    #[serde(default, rename = "toState")]
    state: String,
    #[serde(default)]
    reason: String,
    #[serde(default, rename = "prURL")]
    pr_url: String,
}
fn invalid() -> ToolError {
    ToolError::new(category::VALIDATION, "invalid tracker arguments")
}
fn id_valid(id: &str) -> bool {
    let Some((prefix, suffix)) = id.rsplit_once('-') else {
        return false;
    };
    !prefix.is_empty()
        && id.len() <= 256
        && prefix
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && suffix.len() == 4
        && suffix
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
}
impl Args {
    pub(crate) fn command(&self, policy: &TrackerPolicy) -> Result<Vec<OsString>, ToolError> {
        if !id_valid(&self.id)
            || [
                &self.op,
                &self.id,
                &self.body,
                &self.state,
                &self.reason,
                &self.pr_url,
            ]
            .iter()
            .any(|s| s.contains('\0'))
            || self.body.len() > MAX_TEXT_BYTES
            || self.reason.len() > MAX_TEXT_BYTES
        {
            return Err(invalid());
        }
        let mut args = vec![OsString::from("--project"), policy.project.clone().into()];
        match self.op.as_str() {
            "close" => {
                let reason = if self.reason.is_empty() {
                    "closed by tracker_write"
                } else {
                    &self.reason
                };
                // bn reason is an option value, so it precedes the positional
                // separator. Reject leading dashes instead of interpreting flags.
                if reason.starts_with('-') {
                    return Err(invalid());
                }
                args.extend([
                    "close".into(),
                    "-r".into(),
                    reason.into(),
                    "--".into(),
                    self.id.clone().into(),
                ]);
            }
            "transition" => {
                if !policy.statuses.contains(&self.state) {
                    return Err(invalid());
                }
                args.extend([
                    "update".into(),
                    "--status".into(),
                    self.state.clone().into(),
                    "--".into(),
                    self.id.clone().into(),
                ]);
            }
            "comment" => {
                if self.body.trim().is_empty() {
                    return Err(invalid());
                }
                args.extend([
                    "note".into(),
                    "--".into(),
                    self.id.clone().into(),
                    self.body.clone().into(),
                ]);
            }
            "link_pr" => return Err(ToolError::new("unsupported_op", "link_pr is not supported")),
            _ => return Err(invalid()),
        }
        Ok(args)
    }
}
