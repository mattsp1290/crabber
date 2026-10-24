"""Verify every Crabber package resolves to one immutable repository revision."""
import json
import os
from pathlib import Path

metadata = json.loads(Path("metadata.json").read_text())
packages = [p for p in metadata["packages"] if p["name"] == "crabber" or p["name"].startswith("crabber-")]
revision = os.environ["CRABBER_REV"]
expected = f"git+ssh://git@github.com/mattsp1290/crabber.git?rev={revision}#{revision}"
identities = {}
for package in packages:
    name = package["name"]
    identities.setdefault(name, set()).add((package["version"], package["source"]))
    if package["source"] != expected:
        raise SystemExit(f"{name}: expected the single qualified Crabber source")
for name, values in identities.items():
    if len(values) != 1:
        raise SystemExit(f"{name}: duplicate package identities: {values}")
if not {"crabber", "crabber-extensions"}.issubset(identities):
    raise SystemExit("consumer must resolve both Crabber and native extensions")
if any(line.startswith(("crabber ", "crabber-")) for line in Path("tree-duplicate-roots.txt").read_text().splitlines()):
    raise SystemExit("duplicate Crabber tuple package")
Path("identities.json").write_text(json.dumps({name: sorted(values) for name, values in sorted(identities.items())}, indent=2) + "\n")
print(json.dumps({"crabber_sources": 1, "tuple_packages": len(identities)}))
