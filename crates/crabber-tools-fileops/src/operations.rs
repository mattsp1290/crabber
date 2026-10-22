//! Argument validation and synchronous capability-only file operations.
use crate::{Kind, MAX_MULTI_EDITS, MAX_OUTPUT_BYTES, MAX_READ_WINDOW_LINES, Options};
use cap_std::fs::{OpenOptions, OpenOptionsExt};
use crabber_tools_core::{RelPath, ToolError, category, category_for_io, failed};
use serde::Deserialize;
use serde_json::{Value, json};
use std::io;
use tokio_util::sync::CancellationToken;
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReadArgs {
    pub(crate) path: String,
    pub(crate) offset: Option<usize>,
    pub(crate) limit: Option<usize>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WriteArgs {
    pub(crate) path: String,
    pub(crate) content: String,
    #[serde(default)]
    pub(crate) create_dirs: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EditArgs {
    pub(crate) path: String,
    pub(crate) anchor: String,
    pub(crate) replacement: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ListArgs {
    #[serde(default)]
    pub(crate) path: String,
    #[serde(default)]
    pub(crate) recursive: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EditStep {
    pub(crate) anchor: String,
    pub(crate) replacement: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MultiEditArgs {
    pub(crate) path: String,
    pub(crate) edits: Vec<EditStep>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DeleteArgs {
    pub(crate) path: String,
}
pub(crate) struct MoveArgs {
    pub(crate) path: String,
    pub(crate) requested_new_path: String,
    pub(crate) new_path: RelPath,
    pub(crate) create_dirs: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawMoveArgs {
    path: String,
    new_path: String,
    #[serde(default)]
    create_dirs: bool,
}
pub(crate) struct Request {
    args: Args,
    pub(crate) path: RelPath,
}
enum Args {
    Read(ReadArgs),
    Write(WriteArgs),
    Edit(EditArgs),
    List(ListArgs),
    MultiEdit(MultiEditArgs),
    Delete(DeleteArgs),
    Move(MoveArgs),
}
fn validation() -> ToolError {
    ToolError::new(category::VALIDATION, "invalid arguments")
}
#[allow(
    clippy::needless_pass_by_value,
    reason = "Owned error callbacks match the Result::map_err interface."
)]
pub(crate) fn io_error(e: io::Error) -> ToolError {
    ToolError::new(category_for_io(&e), "filesystem operation failed")
}
impl Request {
    pub(crate) fn parse(kind: Kind, value: Value) -> Result<Self, ToolError> {
        let args = match kind {
            Kind::Read => {
                let a: ReadArgs = serde_json::from_value(value).map_err(|_| validation())?;
                if a.offset == Some(0)
                    || a.limit.is_some_and(|n| n == 0 || n > MAX_READ_WINDOW_LINES)
                {
                    return Err(validation());
                }
                Args::Read(a)
            }
            Kind::Write => {
                let a: WriteArgs = serde_json::from_value(value).map_err(|_| validation())?;
                if a.content.len() > MAX_OUTPUT_BYTES {
                    return Err(ToolError::new(
                        category::TOO_LARGE,
                        "content exceeds 256 KiB",
                    ));
                }
                Args::Write(a)
            }
            Kind::Edit => {
                let a: EditArgs = serde_json::from_value(value).map_err(|_| validation())?;
                if a.anchor.is_empty() {
                    return Err(validation());
                }
                if a.anchor.len() > MAX_OUTPUT_BYTES || a.replacement.len() > MAX_OUTPUT_BYTES {
                    return Err(ToolError::new(category::TOO_LARGE, "edit exceeds 256 KiB"));
                }
                Args::Edit(a)
            }
            Kind::MultiEdit => {
                let a: MultiEditArgs = serde_json::from_value(value).map_err(|_| validation())?;
                if a.edits.is_empty()
                    || a.edits.len() > MAX_MULTI_EDITS
                    || a.edits.iter().any(|e| e.anchor.is_empty())
                {
                    return Err(validation());
                }
                if a.edits.iter().any(|e| {
                    e.anchor.len() > MAX_OUTPUT_BYTES || e.replacement.len() > MAX_OUTPUT_BYTES
                }) {
                    return Err(ToolError::new(category::TOO_LARGE, "edit exceeds 256 KiB"));
                }
                Args::MultiEdit(a)
            }
            Kind::Delete => Args::Delete(serde_json::from_value(value).map_err(|_| validation())?),
            Kind::Move => {
                let a: RawMoveArgs = serde_json::from_value(value).map_err(|_| validation())?;
                let source = RelPath::parse(&a.path, false)?;
                let new_path = RelPath::parse(&a.new_path, false)?;
                if source.as_path() == new_path.as_path() || new_path.is_root() {
                    return Err(validation());
                }
                Args::Move(MoveArgs {
                    path: a.path,
                    requested_new_path: a.new_path,
                    new_path,
                    create_dirs: a.create_dirs,
                })
            }
            Kind::List => Args::List(serde_json::from_value(value).map_err(|_| validation())?),
        };
        let path = RelPath::parse(args.path(), matches!(args, Args::List(_)))?;
        Ok(Self { args, path })
    }
    pub(crate) fn run(self, options: &Options, cancel: &CancellationToken) -> Value {
        if cancel.is_cancelled() {
            return failed(category::UNKNOWN, "cancelled");
        }
        let mut value = match &self.args {
            Args::Read(a) => self.read(options, a, cancel),
            Args::Write(a) => self.write(options, a),
            Args::Edit(a) => self.edit(options, a),
            Args::List(a) => self.list(options, a, cancel),
            Args::MultiEdit(a) => self.multi_edit(options, a),
            Args::Delete(_) => self.delete(options),
            Args::Move(a) => self.relocate(options, a),
        }
        .unwrap_or_else(|e| e.value());
        value["path"] = json!(if self.path.is_root() {
            "."
        } else {
            self.args.path()
        });
        if matches!(self.args, Args::Edit(_)) && value.get("anchor_occurrences").is_none() {
            value["anchor_occurrences"] = json!(0);
        }
        if let Args::Move(a) = &self.args {
            value["new_path"] = json!(a.requested_new_path);
        }
        value
    }
    pub(crate) fn open(&self, options: &Options) -> Result<cap_std::fs::File, ToolError> {
        let f = options
            .root
            .dir()
            .open_with(
                self.path.as_path(),
                OpenOptions::new().read(true).custom_flags(libc::O_NONBLOCK),
            )
            .map_err(io_error)?;
        let m = f.metadata().map_err(io_error)?;
        if m.is_dir() {
            return Err(ToolError::new(
                category::IS_DIRECTORY,
                "path is a directory",
            ));
        }
        if !m.is_file() {
            return Err(ToolError::new(category::VALIDATION, "not a regular file"));
        }
        Ok(f)
    }
}
impl Args {
    fn path(&self) -> &str {
        match self {
            Self::Read(a) => &a.path,
            Self::Write(a) => &a.path,
            Self::Edit(a) => &a.path,
            Self::List(a) => &a.path,
            Self::MultiEdit(a) => &a.path,
            Self::Delete(a) => &a.path,
            Self::Move(a) => &a.path,
        }
    }
}
