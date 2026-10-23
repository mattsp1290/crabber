#!/bin/sh
set -eu
: "${CRABBER_REV:?supply a pushed 40-character crabber commit}"
export CRABBER_REV
python3 - <<'PY'
import os, re
if not re.fullmatch("[0-9a-f]{40}", os.environ["CRABBER_REV"]):
    raise SystemExit("CRABBER_REV must be a complete lowercase Git SHA")
PY
TEMPLATE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPOSITORY=$(git -C "$TEMPLATE" rev-parse --show-toplevel)
DEST=${CRABBER_TOOLS_CONSUMER_DIR:-$(mktemp -d)}
export DEST REPOSITORY
python3 - <<'PY'
import os
from pathlib import Path
dest = Path(os.environ["DEST"]).resolve()
repo = Path(os.environ["REPOSITORY"]).resolve()
if dest.is_relative_to(repo) or (dest.exists() and any(dest.iterdir())):
    raise SystemExit("consumer destination must be empty and outside the repository")
dest.mkdir(parents=True, exist_ok=True)
PY
DEST=$(CDPATH= cd -- "$DEST" && pwd)
export DEST
printf 'Consumer working directory: %s\n' "$DEST"
cp "$TEMPLATE/Cargo.toml" "$DEST/"
cp -R "$TEMPLATE/src" "$DEST/src"
cp "$REPOSITORY/examples/mount-standard/src/probe.rs" "$REPOSITORY/examples/mount-standard/src/contexts.rs" "$DEST/src/"
python3 - <<'PY'
import os
from pathlib import Path
manifest = Path(os.environ["DEST"]) / "Cargo.toml"
manifest.write_text(manifest.read_text().replace("CRABBER_REV", os.environ["CRABBER_REV"]))
PY
cd "$DEST"
export CARGO_NET_GIT_FETCH_WITH_CLI=true
export GIT_CONFIG_COUNT=1
export GIT_CONFIG_KEY_0=url.https://github.com/mattsp1290/crabber.git.insteadOf
export GIT_CONFIG_VALUE_0=ssh://git@github.com/mattsp1290/crabber.git
cargo generate-lockfile
cargo metadata --locked --format-version 1 > metadata.json
cargo tree --locked -d --depth 0 > tree-duplicate-roots.txt
python3 - <<'PY'
import json, os
from pathlib import Path
revision = os.environ["CRABBER_REV"]
expected = f"git+ssh://git@github.com/mattsp1290/crabber.git?rev={revision}#{revision}"
metadata = json.loads(Path("metadata.json").read_text())
packages = [p for p in metadata["packages"] if p["name"] == "crabber" or p["name"].startswith("crabber-")]
identities = {}
for package in packages:
    name = package["name"]
    identities.setdefault(name, set()).add((package["version"], package["source"]))
    if package["source"] != expected:
        raise SystemExit(f"{name}: expected the single qualified Crabber source")
if not {"crabber", "crabber-tools-catalog", "crabber-extensions"}.issubset(identities):
    raise SystemExit("consumer must resolve runtime, tools, and native extensions")
if any(len(values) != 1 for values in identities.values()):
    raise SystemExit("duplicate Crabber version/source identity")
if any(line.startswith(("crabber ", "crabber-")) for line in Path("tree-duplicate-roots.txt").read_text().splitlines()):
    raise SystemExit("duplicate Crabber tuple package")
Path("identities.json").write_text(json.dumps({name: sorted(values) for name, values in sorted(identities.items())}, indent=2) + "\n")
print(json.dumps({"crabber_sources": 1, "tuple_packages": len(identities)}))
PY
cargo run --locked
cargo --version > toolchain.txt
rustc --version >> toolchain.txt
python3 - <<'PY'
import hashlib
from pathlib import Path
Path("lock.sha256").write_text(hashlib.sha256(Path("Cargo.lock").read_bytes()).hexdigest() + "  Cargo.lock\n")
PY
printf 'Consumer evidence: %s\n' "$DEST"
if [ -n "${CRABBER_TOOLS_EVIDENCE_DIR:-}" ]; then
    mkdir -p "$CRABBER_TOOLS_EVIDENCE_DIR"
    cp Cargo.lock lock.sha256 metadata.json identities.json toolchain.txt tree-duplicate-roots.txt "$CRABBER_TOOLS_EVIDENCE_DIR/"
fi
