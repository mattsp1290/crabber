#!/bin/sh
set -eu
: "${CRABBER_REV:?supply a pushed 40-character extension commit}"
TEMPLATE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPOSITORY=$(git -C "$TEMPLATE" rev-parse --show-toplevel)
DEST=${CRABBER_EXTENSIONS_CONSUMER_DIR:-$(mktemp -d)}
export DEST REPOSITORY CRABBER_REV
python3 - <<'PY'
import os, re
from pathlib import Path
for name in ["CRABBER_REV"]:
    value = os.environ.get(name)
    if value and not re.fullmatch("[0-9a-f]{40}", value):
        raise SystemExit(f"{name} must be a complete lowercase Git SHA")
dest = Path(os.environ["DEST"]).resolve()
repo = Path(os.environ["REPOSITORY"]).resolve()
if dest.is_relative_to(repo) or (dest.exists() and any(dest.iterdir())):
    raise SystemExit("consumer destination must be empty and outside the repository")
dest.mkdir(parents=True, exist_ok=True)
PY
DEST=$(CDPATH= cd -- "$DEST" && pwd)
export DEST
printf 'Consumer working directory: %s\n' "$DEST"
cp "$TEMPLATE/Cargo.toml" "$TEMPLATE/validate.py" "$DEST/"
cp -R "$TEMPLATE/src" "$DEST/src"
# Export public API journeys from the SAME immutable commit as the Git dependency.
# No test implementation is duplicated in this template, and no path dependency
# or development checkout is made visible to Cargo.
git -C "$REPOSITORY" archive "$CRABBER_REV" -- \
    crates/crabber-extensions/tests/ask_user.rs crates/crabber-extensions/tests/ask_user \
    crates/crabber-extensions/tests/contextual.rs crates/crabber-extensions/tests/contextual \
    crates/crabber-extensions/tests/delegate_task.rs crates/crabber-extensions/tests/delegate_task \
    crates/crabber-extensions/tests/cleanup.rs crates/crabber-extensions/tests/cleanup \
    crates/crabber-extensions/tests/runtime.rs crates/crabber-extensions/tests/runtime \
    crates/crabber-extensions/tests/session_invocation_contract.rs crates/crabber-extensions/tests/support > "$DEST/journeys.tar"
mkdir "$DEST/export"
tar -xf "$DEST/journeys.tar" -C "$DEST/export"
mv "$DEST/export/crates/crabber-extensions/tests" "$DEST/tests"
python3 - <<'PY'
import os
from pathlib import Path
p = Path(os.environ["DEST"]) / "Cargo.toml"
s = p.read_text().replace("CRABBER_REV", os.environ["CRABBER_REV"])
p.write_text(s)
PY
cd "$DEST"
# Keep the source spelling stable while fetching the public repositories via HTTPS.
export CARGO_NET_GIT_FETCH_WITH_CLI=true
export GIT_CONFIG_COUNT=1
export GIT_CONFIG_KEY_0=url.https://github.com/mattsp1290/crabber.git.insteadOf
export GIT_CONFIG_VALUE_0=ssh://git@github.com/mattsp1290/crabber.git
export BACKGROUND_JOBS_REQUIRE_SHELL=1 PYTHON_REPL_REQUIRE_PYTHON=1
FEATURES=
RESTART_FEATURES=sqlite
run_log() {
    log=$1
    shift
    if "$@" > "$log" 2>&1; then cat "$log"; else cat "$log"; return 1; fi
}
run_log lock.log cargo generate-lockfile
run_log fmt.log cargo fmt --all --check
run_log clippy.log cargo clippy --locked --all-targets --features "$FEATURES" -- -D warnings
run_log clippy-restart.log cargo clippy --locked --all-targets --features "$RESTART_FEATURES" -- -D warnings
run_log journeys.log cargo run --locked --features "$FEATURES" -- journeys
run_log public-api-tests.log cargo test --locked --features "$FEATURES" --tests -- --test-threads=2
run_log restart-phase-1.log cargo run --locked --features "$RESTART_FEATURES" -- restart-phase-1 "$DEST/restart.sqlite"
run_log restart-phase-2.log cargo run --locked --features "$RESTART_FEATURES" -- restart-phase-2 "$DEST/restart.sqlite"
cargo metadata --locked --features "$RESTART_FEATURES" --format-version 1 > metadata.json
cargo tree --locked --features "$RESTART_FEATURES" -d > tree-duplicates.txt
cargo tree --locked --features "$RESTART_FEATURES" -d --depth 0 > tree-duplicate-roots.txt
cargo tree --locked --features "$RESTART_FEATURES" -i crabber > tree-crabber.txt
run_log identities.log python3 validate.py
cargo --version > toolchain.txt
rustc --version >> toolchain.txt
python3 - <<'PY'
import hashlib
from pathlib import Path
Path("lock.sha256").write_text(hashlib.sha256(Path("Cargo.lock").read_bytes()).hexdigest() + "  Cargo.lock\n")
PY
printf 'Consumer evidence: %s\n' "$DEST"
if [ -n "${CRABBER_EXTENSIONS_EVIDENCE_DIR:-}" ]; then
    mkdir -p "$CRABBER_EXTENSIONS_EVIDENCE_DIR"
    cp Cargo.lock lock.sha256 metadata.json identities.json toolchain.txt tree-*.txt *.log "$CRABBER_EXTENSIONS_EVIDENCE_DIR/"
fi
