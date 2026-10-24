use sha2::{Digest, Sha256};

pub(crate) const SUPERVISOR_PROTOCOL: &str = "posix-anchor-supervisor-v1";
pub(crate) const SUPERVISOR_SCRIPT: &str = r#"trap '' TERM
if IFS= read -r gate && [ "$gate" = G ]; then
	:
else
	exec </dev/null
	while :; do
		command -p sleep 2147483647 </dev/null >/dev/null 2>&1 || exit 127
	done
fi
exec </dev/null
command -p sleep 2147483647 </dev/null >/dev/null 2>&1 &
shell=$1
command=$2
(
	trap - TERM
	exec "$shell" -c "$command"
)
exit $?
"#;

pub(crate) fn supervisor_digest() -> String {
    format!("{:x}", Sha256::digest(SUPERVISOR_SCRIPT.as_bytes()))
}
