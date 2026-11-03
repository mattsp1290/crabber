#!/usr/bin/env python3
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tempfile
import uuid

sys.dont_write_bytecode = True
ROOTS = (
    "storetest", "admission_contract", "execution_binding_contract",
    "typed_admission_contract", "revision_contract", "public_state_contract",
    "attempt_contract", "phase_contract", "attempt_control_contract",
    "approval_contract", "interaction_contract", "control_contract",
    "session_management_contract", "session_owner_contract",
    "tool_continuation_contract", "run_control_contract", "management_contract",
)
LIMIT = 64 << 20


def no_symlinks(path):
    for ancestor in (path, *path.parents):
        if ancestor.exists() or ancestor.is_symlink():
            assert not ancestor.is_symlink(), "probe path traverses a symlink"


def unique(pairs):
    value = {}
    for key, item in pairs:
        assert key not in value, "duplicate compiler JSON key"
        value[key] = item
    return value


def records(path):
    with path.open("rb") as stream:
        data = stream.read(LIMIT + 1)
    assert len(data) <= LIMIT, "compiler output exceeds the probe bound"
    return [json.loads(line, object_pairs_hook=unique)
            for line in data.decode("utf-8").splitlines() if line.strip()]


def completed(rows, success):
    finish = [i for i, row in enumerate(rows)
              if row.get("reason") == "build-finished"]
    assert len(finish) == 1 and finish[0] == len(rows) - 1, "incomplete compiler stream"
    assert rows[finish[0]].get("success") is success, "unexpected compiler completion"


def session_artifact(rows, workspace, features):
    completed(rows, True)
    artifacts = [row for row in rows if row.get("reason") == "compiler-artifact"
                 and row.get("target", {}).get("name") == "crabber_session"]
    assert len(artifacts) == 1, "one actual Session library artifact is required"
    artifact = artifacts[0]
    target = artifact["target"]
    assert target["kind"] == ["lib"], "Session library target required"
    assert Path(target["src_path"]).resolve() == workspace / "crates/crabber-session/src/lib.rs", "wrong Session source"
    assert artifact["profile"]["test"] is False, "dependency artifact must not be a test"
    assert isinstance(artifact["fresh"], bool), "compiler freshness required"
    assert artifact["features"] == sorted(features), "wrong Session build selection"
    return artifact


def bounded_tail(path):
    with path.open("rb") as stream:
        stream.seek(max(0, path.stat().st_size - 8192))
        return stream.read(8192).decode("utf-8", errors="replace")


def check(workspace, cache):
    identity = uuid.uuid4().hex
    scratch = Path(tempfile.mkdtemp(prefix="scratch-", dir=cache))
    owned = scratch.stat()
    report = cache / "reports" / identity
    report.mkdir(parents=True, mode=0o700)
    source = scratch / "src/bin"
    source.mkdir(parents=True)
    manifest = scratch / "Cargo.toml"
    manifest.write_text(f'''[package]
name = "session-conformance-probe-{identity}"
version = "0.0.0"
edition = "2024"
publish = false

[workspace]
resolver = "3"

[features]
conformance = ["crabber-session/conformance"]
memory = ["crabber-session/memory", "crabber/memory"]
sqlite = ["crabber-session/sqlite"]

[dependencies]
crabber-session = {{ path = {json.dumps(str(workspace / 'crates/crabber-session'))}, default-features = false }}
crabber = {{ path = {json.dumps(str(workspace / 'crates/crabber'))}, default-features = false }}
''')
    shutil.copyfile(workspace / "Cargo.lock", scratch / "Cargo.lock")
    (source / "normal.rs").write_text('use crabber_session::{MemoryStore, Store};\nfn main() { let store = MemoryStore::default(); let _: &dyn Store = &store; }\n')
    for name in ROOTS:
        (source / f"{name}.rs").write_text(f'use crabber_session::{name};\nfn main() {{}}\n')
    imports = "".join(f"use crabber_session::{name} as _;\n" for name in ROOTS)
    (source / "qualified.rs").write_text(imports + 'fn main() { let _ = crabber_session::storetest::run_contract(|clock| crabber_session::MemoryStore::with_clock(clock)); }\n')
    (source / "backend_free.rs").write_text('use crabber_session::Store; fn main() { let _: Option<&dyn Store> = None; let _ = crabber::Agent::builder(); }\n')
    additions = {
        "memory_import": ('use crabber_session::MemoryStore; fn main() {}\n', "E0432"),
        "fixed_host_import": ('use crabber_session::session_owner::owned_scope::FixedMemoryInitialHost; fn main() {}\n', "E0432"),
        "memory_method": ('fn main() { let _ = crabber::Agent::builder().memory(); }\n', "E0599"),
    }
    for name, (text, _) in additions.items():
        (source / f"{name}.rs").write_text(text)
    (source / "sqlite_qualified.rs").write_text('use crabber_session::{SqliteStore, Store}; fn main() { fn selected(store: SqliteStore) { let _: &dyn Store = &store; let _ = crabber_session::storetest::run_contract(move |clock| store.clone().with_clock(clock)); } let _ = selected; }\n')
    spec = importlib.util.spec_from_file_location("diagnostic", workspace / "testdata/custom-http-feature-probe/check-diagnostic.py")
    diagnostic = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(diagnostic)
    observations = []

    def cargo(name, full=False, locked=True, features=None):
        argv = ["cargo", "check", "--quiet", "--manifest-path", str(manifest),
                "--bin", name, "--message-format=json"]
        if locked:
            argv.append("--locked")
        features = features if features is not None else (["conformance", "memory"] if full else ["memory"])
        if features:
            argv.extend(["--features", ",".join(features)])
        env = os.environ.copy()
        env["CARGO_TARGET_DIR"] = str(cache / "target")
        env["CARGO_BUILD_BUILD_DIR"] = str(cache / "target")
        case = f"{name}-{'-'.join(features) or 'none'}-{len(observations)}"
        output = report / f"{case}.jsonl"
        stderr = report / f"{case}.stderr"
        with output.open("xb") as out, stderr.open("xb") as err:
            result = subprocess.run(argv, cwd=workspace, env=env, stdout=out, stderr=err)
        assert stderr.stat().st_size <= LIMIT, "compiler stderr exceeds the probe bound"
        rows = records(output)
        observations.append({"bin": name, "status": result.returncode, "argv": argv,
                             "stdout_sha256": hashlib.sha256(output.read_bytes()).hexdigest()})
        if result.returncode and name in ("normal", "qualified"):
            print(bounded_tail(stderr), file=sys.stderr)
            print(bounded_tail(output), file=sys.stderr)
        return result.returncode, rows

    try:
        status, normal = cargo("normal", locked=False)
        assert status == 0, "normal production dependency failed"
        ordinary = session_artifact(normal, workspace, ["memory"])
        for name in ROOTS:
            status, rows = cargo(name)
            assert status != 0, f"normal build unexpectedly exposes {name}"
            completed(rows, False)
            assert diagnostic.rejected(rows, source / f"{name}.rs", "E0432"), f"{name} failed without its root import diagnostic"
        status, full = cargo("qualified", full=True)
        assert status == 0, "conformance dependency failed"
        qualified = session_artifact(full, workspace, ["conformance", "memory"])
        for name, (_, code) in additions.items():
            status, baseline = cargo("backend_free", features=[])
            assert status == 0, "memory-free baseline failed"
            session_artifact(baseline, workspace, [])
            status, positive = cargo(name, features=["memory"])
            assert status == 0, f"enabled {name} positive failed"
            session_artifact(positive, workspace, ["memory"])
            status, rows = cargo(name, features=[])
            assert status != 0, f"memory-free build unexpectedly exposes {name}"
            completed(rows, False)
            assert diagnostic.rejected(rows, source / f"{name}.rs", code), f"{name} failed without its source-bound {code} diagnostic"
        status, sql = cargo("sqlite_qualified", features=["sqlite", "conformance"])
        assert status == 0, "memory-free SQL conformance dependency failed"
        session_artifact(sql, workspace, ["sqlite", "conformance"])
        (report / "result.json").write_text(json.dumps({
            "schema": 1, "workspace": str(workspace), "normal": ordinary,
            "qualified": qualified, "rejected_imports": list(ROOTS),
            "commands": observations, "memory_rejections": list(additions), "conformance_executed": False,
        }, indent=2) + "\n")
        shutil.copyfile(manifest, report / "Cargo.toml")
        shutil.copyfile(scratch / "Cargo.lock", report / "Cargo.lock")
        now = scratch.stat()
        assert (now.st_dev, now.st_ino, now.st_uid) == (owned.st_dev, owned.st_ino, os.getuid()), "probe scratch ownership changed"
        shutil.rmtree(scratch)
        print(f"Session conformance boundary passed; {len(ROOTS)} imports rejected; evidence {report}")
    except BaseException:
        print(f"Session conformance boundary failed; retained source {scratch}; evidence {report}", file=sys.stderr)
        raise


def main():
    workspace = Path(__file__).resolve().parents[2]
    target = Path(os.environ.get("CARGO_TARGET_DIR", workspace / "target"))
    if not target.is_absolute():
        target = workspace / target
    identity = hashlib.sha256(os.fsencode(workspace)).hexdigest()[:32]
    cache = target / "consumer-probes" / identity / "session-conformance"
    no_symlinks(cache)
    cache.mkdir(parents=True, exist_ok=True, mode=0o700)
    assert cache.stat().st_uid == os.getuid(), "foreign probe namespace"
    for name in ("target", "reports"):
        child = cache / name
        no_symlinks(child)
        if child.exists():
            assert child.is_dir() and child.stat().st_uid == os.getuid(), "foreign probe child"
    fd = os.open(cache / "probe.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "a+b") as lease:
        value = os.fstat(lease.fileno())
        assert stat.S_ISREG(value.st_mode) and value.st_uid == os.getuid() and value.st_nlink == 1, "invalid probe lease"
        fcntl.flock(lease, fcntl.LOCK_EX)
        check(workspace, cache)


if __name__ == "__main__":
    main()
