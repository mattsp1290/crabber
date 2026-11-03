#!/usr/bin/env python3
"""Build and execute ordinary, independently selected backend consumers."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import stat
import subprocess
import sys
import tomllib
import uuid

sys.dont_write_bytecode = True
LIMIT = 64 << 20
MATRIX = (
    {"name": "default", "lane": "quality-contracts", "features": None,
     "facade": ["default", "memory"], "session": ["memory"], "runtime": [],
     "backend": "memory", "cases": ("fallback", "memory-method", "memory-store")},
    {"name": "memory", "lane": "quality-contracts", "features": ("memory",),
     "facade": ["memory"], "session": ["memory"], "runtime": [],
     "backend": "memory", "cases": ("fallback", "memory-method", "memory-store")},
    {"name": "none", "lane": "quality-contracts", "features": (),
     "facade": [], "session": [], "runtime": [], "backend": "none",
     "cases": ("missing-store", "runtime-missing-store")},
    {"name": "conformance-only", "lane": "quality-contracts", "features": ("conformance",),
     "facade": [], "session": ["conformance"], "runtime": [], "backend": "none",
     "cases": ()},
    {"name": "sqlite", "lane": "quality-contracts", "features": ("sqlite",),
     "facade": ["sqlite"], "session": ["sqlite"], "runtime": ["sqlite"],
     "backend": "sqlite", "cases": ()},
    {"name": "sqlite-conformance", "lane": "quality-contracts", "features": ("sqlite", "conformance"),
     "facade": ["sqlite"], "session": ["conformance", "sqlite"], "runtime": ["sqlite"],
     "backend": "sqlite", "cases": ("agent", "runtime", "custom-store", "queue", "contract-management", "contract-store", "contract-workspace-identity", "contract-storable-records", "contract-admission", "contract-execution-binding", "contract-typed-admission", "contract-revision", "contract-public-state", "contract-attempt", "contract-phase", "contract-attempt-control", "contract-approval", "contract-interaction", "contract-control", "contract-session-owner", "native-register", "native-register-keyed", "native-scripted", "native-scripted-keyed")},
    {"name": "postgres", "lane": "postgres", "features": ("postgres",),
     "facade": ["postgres"], "session": ["postgres"], "runtime": [],
     "backend": "postgres", "cases": ()},
    {"name": "postgres-conformance", "lane": "postgres", "features": ("postgres", "conformance"),
     "facade": ["postgres"], "session": ["conformance", "postgres"], "runtime": [],
     "backend": "postgres", "cases": ("agent", "runtime", "custom-store", "queue", "contract-management", "contract-store", "contract-workspace-identity", "contract-storable-records", "contract-admission", "contract-execution-binding", "contract-typed-admission", "contract-revision", "contract-public-state", "contract-attempt", "contract-phase", "contract-attempt-control", "contract-approval", "contract-interaction", "contract-control", "contract-session-owner", "native-register", "native-register-keyed", "native-scripted", "native-scripted-keyed")},
)
PACKAGES = {"facade": "crabber", "session": "crabber-session", "runtime": "crabber-runtime"}
MEMORY_LEDGERS = (
    "src/continuation_input/ledger.rs",
    "src/native_enrollment/pending_initial/ledger.rs",
    "src/native_enrollment/initial_consumer/ledger.rs",
    "src/queue/source/ledger.rs",
    "src/session_owner/ledger.rs",
)


def unique(pairs):
    value = {}
    for key, item in pairs:
        assert key not in value, "duplicate JSON key"
        value[key] = item
    return value


def no_symlinks(path):
    for entry in (path, *path.parents):
        assert not entry.is_symlink(), "probe path traverses a symlink"


def owned_directory(path):
    no_symlinks(path)
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    assert path.is_dir() and path.stat().st_uid == os.getuid(), "foreign probe directory"


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def snapshot(workspace):
    result = subprocess.run(["git", "ls-files", "-z"], cwd=workspace, check=True, capture_output=True)
    files = {workspace / os.fsdecode(name) for name in result.stdout.split(b"\0") if name}
    files.update((workspace / "testdata/backend-selection-probe").rglob("*.rs"))
    files.update(workspace / "testdata/backend-selection-probe" / name for name in ("Cargo.toml", "Cargo.lock", "check.py"))
    selected = sorted(path for path in files if path.is_file())
    sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=workspace, check=True, capture_output=True, text=True).stdout.strip()
    return {"head": sha, "files": {str(path.relative_to(workspace)): digest(path) for path in selected}}


def records(path):
    assert path.stat().st_size <= LIMIT, "compiler output exceeds the probe bound"
    return [json.loads(line, object_pairs_hook=unique) for line in path.read_text().splitlines() if line.strip()]


def completed(rows):
    finish = [i for i, row in enumerate(rows) if row.get("reason") == "build-finished"]
    assert finish == [len(rows) - 1] and rows[-1].get("success") is True, "incomplete or unsuccessful compiler stream"


def tail(path):
    with path.open("rb") as stream:
        stream.seek(max(0, path.stat().st_size - 8192))
        return stream.read().decode("utf-8", errors="replace")


def command(argv, workspace, env, report, label):
    stdout, stderr = report / (label + ".jsonl"), report / (label + ".stderr")
    with stdout.open("xb") as out, stderr.open("xb") as err:
        process = subprocess.Popen(argv, cwd=workspace, env=env, stdout=out, stderr=err, start_new_session=True)
        try:
            status = process.wait(timeout=600)
        except BaseException:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
            raise
    assert stdout.stat().st_size <= LIMIT and stderr.stat().st_size <= LIMIT, "probe output exceeds its bound"
    if stderr.stat().st_size:
        print(tail(stderr), file=sys.stderr)
    if status:
        print(tail(stdout), file=sys.stderr)
    assert status == 0, f"{label} failed; evidence {report}"
    return stdout, {"argv": argv, "status": status, "stdout_sha256": digest(stdout), "stderr_sha256": digest(stderr)}


def arguments(mode):
    return [] if mode["features"] is None else ["--no-default-features"] + (["--features", ",".join(mode["features"])] if mode["features"] else [])


def dep_paths(path):
    text = path.read_text().replace("\\\n", "")
    line = next((line for line in text.splitlines() if ": " in line), None)
    assert line is not None, "missing make-style dependency rule"
    escaped = False
    boundary = None
    for index, char in enumerate(line):
        if char == ":" and not escaped:
            boundary = index
            break
        escaped = char == "\\" and not escaped
        if char != "\\":
            escaped = False
    assert boundary is not None, "missing dep-info target separator"
    result, token = [], []
    escaped = False
    for char in line[boundary + 1:] + " ":
        if escaped:
            token.append(char)
            escaped = False
        elif char == "\\":
            escaped = True
        elif char.isspace():
            if token:
                result.append("".join(token))
                token = []
        else:
            token.append(char)
    assert not escaped and result, "invalid make-style dependency path"
    return result


def artifact(rows, package, workspace, expected):
    selected = [row for row in rows if row.get("reason") == "compiler-artifact" and row.get("target", {}).get("name") == package.replace("-", "_") and row.get("target", {}).get("kind") == ["lib"]]
    assert len(selected) == 1, f"one actual {package} library artifact required"
    value = selected[0]
    source = workspace / "crates" / package / "src/lib.rs"
    assert Path(value["target"]["src_path"]).resolve() == source.resolve(), "wrong canonical library source"
    assert value["profile"]["test"] is False and isinstance(value["fresh"], bool), "normal non-test artifact required"
    assert value["features"] == expected, f"wrong {package} selection {value['features']}"
    assert value["package_id"].startswith("path+file://"), "canonical local package identity required"
    return value


def exclusion(session, workspace, target):
    outputs = [Path(name) for name in session["filenames"] if name.endswith((".rlib", ".rmeta"))]
    assert outputs, "Session normal library output missing"
    candidates = {path.with_name(path.name.removeprefix("lib")).with_suffix(".d") for path in outputs}
    assert len(candidates) == 1, "ambiguous Session dep-info identity"
    depinfo = candidates.pop()
    no_symlinks(depinfo)
    assert depinfo.is_file() and depinfo.resolve().is_relative_to(target.resolve()), "missing or foreign Session dep-info"
    root = (workspace / "crates/crabber-session").resolve()
    sources = []
    for name in dep_paths(depinfo):
        path = Path(name)
        if not path.is_absolute():
            path = root / path
        path = path.resolve()
        assert path.is_relative_to(root), "wrong-source Session dependency"
        sources.append(path.relative_to(root).as_posix())
    assert "src/lib.rs" in sources, "dep-info is not the selected Session source"
    forbidden = [name for name in sources if name == "src/memory.rs" or name.startswith("src/memory/") or re.search(r"(^|/)memory_snapshot(?:[./]|$)", name) or re.search(r"(^|/)(fixed_memory|fixed_preparation)(?:[./]|$)", name)]
    forbidden.extend(name for name in sources if any(name == root or name.startswith(root.removesuffix(".rs") + "/") for root in MEMORY_LEDGERS))
    assert not forbidden, f"memory implementation remains in production dep-info {forbidden}"
    return {"path": str(depinfo), "sha256": digest(depinfo), "sources": sources}


def graph(path, mode, workspace, source):
    data = json.loads(path.read_text(), object_pairs_hook=unique)
    packages = {item["id"]: item for item in data["packages"]}
    nodes = {item["id"]: item for item in data["resolve"]["nodes"]}
    root = data["resolve"]["root"]
    assert packages[root]["name"] == "backend-selection-probe"
    assert Path(packages[root]["manifest_path"]).resolve() == source / "Cargo.toml"
    reached, pending = set(), [root]
    while pending:
        current = pending.pop()
        if current in reached:
            continue
        reached.add(current)
        for dep in nodes[current]["deps"]:
            if any(kind["kind"] in (None, "build") for kind in dep["dep_kinds"]):
                pending.append(dep["pkg"])
    selected = {}
    for key, name in PACKAGES.items():
        ids = [identity for identity in reached if packages[identity]["name"] == name]
        assert len(ids) == 1, "duplicate or missing canonical SDK package"
        identity = ids[0]
        assert packages[identity]["source"] is None
        assert Path(packages[identity]["manifest_path"]).resolve() == workspace / "crates" / name / "Cargo.toml"
        assert nodes[identity]["features"] == mode[key], "normal/build graph selection differs"
        selected[name] = {"id": identity, "features": nodes[identity]["features"]}
    return selected


def compare_locks(workspace):
    original = tomllib.loads((workspace / "Cargo.lock").read_text())
    probe = tomllib.loads((workspace / "testdata/backend-selection-probe/Cargo.lock").read_text())
    root = {(item["name"], item["version"], item.get("source")): item.get("checksum") for item in original["package"]}
    for item in probe["package"]:
        key = item["name"], item["version"], item.get("source")
        if item.get("source"):
            assert key in root and root[key] == item.get("checksum"), f"standalone lock drift for {item['name']}"


def prepare(workspace, lane):
    source = lane / "source"
    owned_directory(source)
    owned_directory(source / "src")
    original = workspace / "testdata/backend-selection-probe"
    sources = tuple((original / "src").glob("*.rs"))
    assert sources, "canonical probe sources missing"
    assert {path.name for path in source.iterdir()} <= {"Cargo.toml", "Cargo.lock", "src"}, "unexpected mutable probe root input"
    assert {path.name for path in (source / "src").iterdir()} <= {path.name for path in sources}, "unexpected mutable probe source input"
    for destination in (source / "Cargo.toml", source / "Cargo.lock", *(source / "src" / path.name for path in sources)):
        no_symlinks(destination)
        if destination.exists():
            value = destination.stat()
            assert stat.S_ISREG(value.st_mode) and value.st_uid == os.getuid() and value.st_nlink == 1, "foreign mutable probe input"
    manifest = (original / "Cargo.toml").read_text()
    manifest = re.sub(r'path = "../../(crates/[^"]+)"', lambda match: 'path = ' + json.dumps(str(workspace / match.group(1))), manifest)
    (source / "Cargo.toml").write_text(manifest)
    shutil.copyfile(original / "Cargo.lock", source / "Cargo.lock")
    for path in sources:
        text = path.read_text().replace('"../../../examples/server-host-probe/src"', json.dumps(str(workspace / "examples/server-host-probe/src")))
        (source / "src" / path.name).write_text(text)
    return source


def build(mode, workspace, source, target, env, report):
    manifest = source / "Cargo.toml"
    args = arguments(mode)
    metadata, meta_call = command(["cargo", "metadata", "--format-version", "1", "--locked", "--manifest-path", str(manifest), *args], workspace, env, report, mode["name"] + "-metadata")
    selected = graph(metadata, mode, workspace, source)
    output, build_call = command(["cargo", "build", "--locked", "--manifest-path", str(manifest), "--message-format=json", *args], workspace, env, report, mode["name"] + "-build")
    rows = records(output)
    completed(rows)
    libs = {key: artifact(rows, name, workspace, mode[key]) for key, name in PACKAGES.items()}
    for key, value in libs.items():
        assert value["package_id"] == selected[PACKAGES[key]]["id"], "compiler/graph package identity differs"
    dependency = None
    if "memory" not in mode["session"]:
        dependency = exclusion(libs["session"], workspace, target)
    roots = [row for row in rows if row.get("reason") == "compiler-artifact" and row.get("target", {}).get("name") == "backend-selection-probe" and row.get("target", {}).get("kind") == ["bin"]]
    assert len(roots) == 1 and roots[0]["profile"]["test"] is False, "one normal probe root required"
    assert Path(roots[0]["target"]["src_path"]).resolve() == source / "src/main.rs"
    executable = Path(roots[0]["executable"])
    no_symlinks(executable)
    assert executable.is_file() and executable.resolve().is_relative_to(target.resolve()) and os.access(executable, os.X_OK), "foreign or missing selected executable"
    expected_root = sorted((mode["features"] if mode["features"] is not None else ("default",)))
    assert roots[0]["features"] == expected_root, "wrong root features"
    return executable, {"mode": mode["name"], "graph": selected, "artifacts": libs, "dep_info": dependency, "commands": [meta_call, build_call], "executable_sha256": digest(executable)}


def primary(mode, workspace, env, report):
    args = ["cargo", "check", "--locked", "--lib", "--message-format=json"]
    if mode["features"] is not None:
        args.append("--no-default-features")
    for name in PACKAGES.values():
        args += ["-p", name]
    selected = []
    for key, name in PACKAGES.items():
        selected.extend(name + "/" + feature for feature in mode[key] if feature != "default")
    if selected:
        args += ["--features", ",".join(selected)]
    strict = dict(env)
    strict["RUSTFLAGS"] = (strict.get("RUSTFLAGS", "") + " -D warnings").strip()
    output, call = command(args, workspace, strict, report, mode["name"] + "-primary")
    rows = records(output)
    completed(rows)
    for key, name in PACKAGES.items():
        artifact(rows, name, workspace, sorted(set(mode[key]) | {"default"}) if mode["features"] is None else mode[key])
    return call


def check(workspace, lane, phase):
    compare_locks(workspace)
    frozen = snapshot(workspace)
    report = lane / "reports" / uuid.uuid4().hex
    owned_directory(report)
    source = prepare(workspace, lane)
    target = lane / "target"
    owned_directory(target)
    env = os.environ.copy()
    env["CARGO_TARGET_DIR"] = str(target)
    env["CARGO_BUILD_BUILD_DIR"] = str(target)
    modes = [mode for mode in MATRIX if mode["lane"] == lane.name]
    results = []
    for mode in modes:
        if phase != "build" and not mode["cases"]:
            continue
        assert snapshot(workspace) == frozen, "SDK source changed before mode"
        executable, result = build(mode, workspace, source, target, env, report)
        assert snapshot(workspace) == frozen, "SDK source changed during build before runtime effects"
        if phase == "build":
            result["primary"] = primary(mode, workspace, env, report)
        else:
            output, runtime_call = command([str(executable), mode["backend"], *mode["cases"]], workspace, env, report, mode["name"] + "-runtime")
            receipts = records(output)
            assert receipts == [{"case": case, "backend": mode["backend"], "result": "passed"} for case in mode["cases"]], "missing, reordered or extra runtime case receipt"
            result["runtime"] = runtime_call
            result["cases"] = list(mode["cases"])
        assert snapshot(workspace) == frozen, "SDK source changed during mode"
        results.append(result)
    (report / "result.json").write_text(json.dumps({"schema": 1, "phase": phase, "lane": lane.name, "source": frozen, "results": results, "runtime_cases": sum(len(result.get("cases", ())) for result in results)}, indent=2) + "\n")
    print(f"Backend selection {phase} passed for {len(results)} selections; evidence {report}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("build", "local", "postgres"))
    parser.add_argument("--lane", choices=("quality-contracts", "postgres"))
    args = parser.parse_args()
    assert (args.phase == "build") == (args.lane is not None), "only build selects a lane"
    workspace = Path(__file__).resolve().parents[2]
    target = Path(os.environ.get("CARGO_TARGET_DIR", workspace / "target"))
    if not target.is_absolute():
        target = workspace / target
    identity = hashlib.sha256(os.fsencode(workspace)).hexdigest()[:32]
    lane = target / "consumer-probes" / identity / "backend-selection" / (args.lane or ("postgres" if args.phase == "postgres" else "quality-contracts"))
    owned_directory(lane)
    fd = os.open(lane / "probe.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "a+b") as lease:
        value = os.fstat(lease.fileno())
        assert stat.S_ISREG(value.st_mode) and value.st_uid == os.getuid() and value.st_nlink == 1, "invalid probe lease"
        fcntl.flock(lease, fcntl.LOCK_EX)
        check(workspace, lane, args.phase)


if __name__ == "__main__":
    main()
