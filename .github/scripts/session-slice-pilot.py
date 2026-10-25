#!/usr/bin/env python3
"""Diagnostic all-library build and Session execution; never a full-suite qualification."""
import hashlib
import importlib.util
import json
import os
from collections import Counter
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time

sys.dont_write_bytecode = True

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / 'target/test-performance' / ('session-slice-' + os.environ.get('GITHUB_RUN_ID', 'local'))
CANONICAL_REVISION = 'ecfa40cb7381c32c44b2deeb7ad3b5b417a7f351'
CANONICAL_SUFFIX = {'aarch64': '51a1db06d1c8de43', 'x86_64': '7a249514c276ccb4'}
INVENTORY = 'd36f3d2ce23f2f735f0b1048562dd59a0f083766d3f5bea22328a9aaac3df5fe'


def save(name, value):
    (OUT / name).write_text(json.dumps(value, indent=2) + '\n')


def command(argv, name, capture=False):
    started = time.monotonic()
    result = subprocess.run(argv, cwd=ROOT, capture_output=capture, text=True)
    save(name + '-phase.json', {'argv': argv, 'seconds': time.monotonic() - started, 'exit': result.returncode})
    if capture:
        (OUT / (name + '-stdout.log')).write_text(result.stdout)
        (OUT / (name + '-stderr.log')).write_text(result.stderr)
    result.check_returncode()
    return result.stdout


def features():
    source = (ROOT / 'xtask/src/complete.rs').read_text()
    section = source.split('const FEATURES: &str = concat!(', 1)[1].split(');', 1)[0]
    return ''.join(re.findall(r'"([^"]*)"', section))


def fingerprint(suffix):
    path = ROOT / 'target/debug/.fingerprint' / ('crabber-session-' + suffix) / 'test-lib-crabber_session.json'
    value = json.loads(path.read_text())
    value.pop('local', None)
    return value


def cargo(args):
    real = os.environ['SESSION_SLICE_REAL_CARGO']
    if args[:1] == ['test'] and '--doc' in args:
        assert args[args.index('-p') + 1] == 'crabber-session'
        save('excluded-doctest.json', {'argv': args, 'reason': 'pilot covers native Session cases only; full-check doctest gate remains outside this diagnostic'})
        return 0
    if args[:2] != ['nextest', 'run']:
        os.execv(real, [real, *args])
    metadata = OUT / 'cargo-metadata.json'
    binaries = OUT / 'binaries.json'
    if not binaries.exists():
        metadata.write_text(command([real, 'metadata', '--format-version', '1', '--locked', '--offline', '--features', features()], 'metadata', True))
        raw = command([real, 'nextest', 'list', '--workspace', '--lib', '--features', features(), '--list-type', 'binaries-only', '--message-format', 'json', '--cargo-message-format', 'json-render-diagnostics'], 'build', True)
        decoder = json.JSONDecoder()
        values = []
        while raw.strip():
            raw = raw.lstrip()
            value, end = decoder.raw_decode(raw)
            values.append(value)
            raw = raw[end:]
        artifacts = [{key: value[key] for key in ['reason', 'package_id', 'target', 'profile', 'features', 'filenames', 'executable', 'fresh']} for value in values if value.get('reason') == 'compiler-artifact']
        save('artifacts.json', artifacts)
        summary = next(x for x in values if 'rust-binaries' in x)
        (OUT / 'cargo-output.jsonl').write_text(''.join(json.dumps(value) + '\n' for value in [*artifacts, summary]))
        session = summary['rust-binaries']['crabber-session']
        suffix = Path(session['binary-path']).name.rsplit('-', 1)[1]
        assert suffix == CANONICAL_SUFFIX[os.uname().machine], 'Session graph changed its canonical binary identity'
        assert fingerprint(suffix) == json.loads((OUT / 'canonical-fingerprint.json').read_text()), 'Session dependency/features/profile fingerprints changed'
        session_artifacts = [x for x in artifacts if x.get('executable') == session['binary-path']]
        assert len(session_artifacts) == 1 and not session_artifacts[0]['fresh'], 'Session test target must actually rebuild'
        profile = session_artifacts[0]['profile']
        assert profile['debug_assertions'] and profile['overflow_checks'] and profile['opt_level'] == '0'
        save('graph-verdict.json', {'canonical_revision': CANONICAL_REVISION, 'session_fingerprint_equal': True, 'session_binary': session, 'session_profile': profile, 'emitted_executables': [{'target': x['target']['name'], 'kind': x['target']['kind'], 'profile': x['profile'], 'executable': x['executable']} for x in artifacts if x.get('executable')], 'scope': 'all library test targets compiled; only Session tests executed'})
        binaries.write_text(json.dumps(summary))
        manifest = json.loads(command([real, 'nextest', 'list', '--cargo-metadata', str(metadata), '--binaries-metadata', str(binaries), '--profile', 'complete-ci', '-E', 'package(=crabber-session)', '--message-format', 'json'], 'inventory', True))
        save('tests.json', manifest)
        cases = manifest['rust-suites']['crabber-session']['testcases']
        reduced = {name: {key: row[key] for key in ['ignored', 'kind']} for name, row in cases.items()}
        digest = hashlib.sha256(json.dumps(reduced, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        assert len(cases) == 1256 and sum(not x['ignored'] for x in cases.values()) == 1255 and digest == INVENTORY, 'Session inventory differs from canonical CI'
        save('inventory-verdict.json', {'total': 1256, 'nonignored': 1255, 'sha256': digest, 'canonical_equal': True})
    original_filter = args[args.index('-E') + 1]
    selected = 'package(=crabber-session) & (' + original_filter + ')'
    argv = [real, 'nextest', 'run', '--cargo-metadata', str(metadata), '--binaries-metadata', str(binaries), '--profile', 'complete-ci', '--test-threads', '8', '--retries', '0', '--no-fail-fast', '--no-tests', 'fail', '-E', selected]
    name = 'role' if original_filter.startswith('test(') else 'session'
    command(argv, name)
    return 0


def run():
    restored = json.loads((ROOT / 'target/test-performance/cache-restore.json').read_text())
    assert CANONICAL_REVISION in (restored.get('restored_key') or ''), 'requires the actual ecfa diagnostic compiler snapshot'
    assert os.cpu_count() == 4 and len(os.sched_getaffinity(0)) == 4
    suffix = CANONICAL_SUFFIX[os.uname().machine]
    save('canonical-fingerprint.json', fingerprint(suffix))
    owner = ROOT / 'target/debug/xtask'
    dependency = (ROOT / 'target/debug/xtask.d').read_text()
    assert str(ROOT / 'xtask/src/main.rs') in dependency, 'cached owner belongs to another checkout'
    owner_sha = hashlib.sha256(owner.read_bytes()).hexdigest()
    owner_matches = []
    for path in (ROOT / 'target/debug/.fingerprint').glob('xtask-*/bin-xtask.json'):
        value = json.loads(path.read_text())
        binary = ROOT / 'target/debug/deps' / path.parent.name
        if 'session-fixtures' in value['features'] and binary.is_file() and hashlib.sha256(binary.read_bytes()).hexdigest() == owner_sha:
            owner_matches.append(str(path))
    assert owner_matches, 'cached owner must be the Session-enabled executable'
    command(['git', 'diff', '--exit-code', CANONICAL_REVISION, 'HEAD', '--', 'xtask'], 'owner-source')
    save('owner.json', {'sha256': owner_sha, 'fingerprints': owner_matches, 'source_equivalence': 'xtask Rust source unchanged from ecfa; cached owner is inside the measured dependency cache', 'canonical_revision': CANONICAL_REVISION})
    real = shutil.which('cargo')
    spec = importlib.util.spec_from_file_location('prepare_rebuild', ROOT / '.github/scripts/prepare-rebuild.py')
    rebuild = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(rebuild)
    rows = rebuild.inventory(ROOT, ROOT / 'target', 'crabber-session')
    signature = b'Signature: 8a477f597d28d172789f06886806bc55\n'
    tag_bytes = signature + b'# This file is a cache directory tag created by cargo.\n# For information about cache directory tags see https://bford.info/cachedir/\n'
    tags = []
    for row in rows:
        target = rebuild.owned(ROOT, Path(row['target']))
        tag = target / 'CACHEDIR.TAG'
        assert not tag.is_symlink(), 'cache tag must not be a symlink'
        if tag.exists():
            assert tag.read_bytes().startswith(signature), 'existing Cargo cache tag is invalid'
        tags.append({'target': str(target), 'tag': str(tag), 'existed': tag.exists()})
    for row in tags:
        if not row['existed']:
            with Path(row['tag']).open('xb') as stream:
                stream.write(tag_bytes)
    save('cache-tags.json', {'ownership': 'existing prepare-rebuild inventory requires current-source package/dependent fingerprints inside nonsymlink owned targets', 'tags': tags, 'created_tag_sha256': hashlib.sha256(tag_bytes).hexdigest()})
    command([sys.executable, '-I', '-S', str(ROOT / '.github/scripts/prepare-rebuild.py'), 'crabber-session', '--report', str(OUT / 'invalidation.json')], 'invalidation')
    shim = OUT / 'bin'
    shim.mkdir()
    wrapper = shim / 'cargo'
    wrapper.write_text('#!/bin/sh\nexec python3 -I -S "' + str(Path(__file__).resolve()) + '" cargo "$@"\n')
    wrapper.chmod(0o755)
    os.environ['SESSION_SLICE_REAL_CARGO'] = real
    os.environ['PATH'] = str(shim) + os.pathsep + os.environ['PATH']
    try:
        command([str(owner), 'check-postgres', '--package', 'crabber-session', '--runner', 'nextest', '--test-threads', '4', '--fixture-metrics', str(OUT / 'fixtures.jsonl')], 'owned-pg')
    finally:
        ledger = OUT / 'fixtures.jsonl'
        events = [json.loads(line) for line in ledger.read_text().splitlines()] if ledger.exists() else []
        created = Counter(x['identity'] for x in events if x['phase'] == 'created')
        finished = Counter(x['identity'] for x in events if x['phase'] == 'finished')
        remaining = command(['docker', 'ps', '-aq', '--filter', 'label=crabber.test-runner'], 'terminal-container-check', True).split()
        save('cleanup.json', {'created': len(created), 'finished': len(finished), 'matched': created == finished, 'remaining_runner_containers': remaining, 'phases': [x['phase'] for x in events]})
        assert not remaining
    assert hashlib.sha256(owner.read_bytes()).hexdigest() == owner_sha
    assert created and created == finished and all(x == 1 for x in created.values())
    assert all(any(x['phase'] == phase for x in events) for phase in ['run_cleanup', 'server_cleanup', 'gate_passed'])
    return 0


def launch():
    OUT.mkdir(parents=True, exist_ok=False)
    source_paths = {'before_supervisor_import': subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT, text=True).splitlines()}
    spec = importlib.util.spec_from_file_location('check_supervisor', ROOT / '.github/scripts/check.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    source_paths['after_supervisor_import'] = subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT, text=True).splitlines()
    save('source-paths.json', source_paths)
    elapsed = time.time() - float(os.environ['CHECK_STARTED'])
    report = OUT / 'report.json'
    code = module.supervise([sys.executable, '-I', '-S', str(Path(__file__).resolve()), 'run'], ROOT, report, budget=4800, reserve=20, elapsed=elapsed)
    value = json.loads(report.read_text())
    source_paths['after_run'] = subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT, text=True).splitlines()
    save('source-paths.json', source_paths)
    value.update(qualified=False, coverage_scope='diagnostic Session execution only; all-library compilation', timing_target_seconds=600, timing_target_met=False, source_end=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(), source_end_clean=not subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT, text=True).strip())
    stable = value['source_clean'] and value['source_end_clean'] and value['source_revision'] == value['source_end']
    if not stable:
        value.update(status='failed', reason='source-changed-or-dirty')
        code = code or 1
    value['pilot_slice_within_600'] = code == 0 and value['status'] == 'passed' and stable and value['wall_seconds'] <= 600
    save('report.json', value)
    print('Session slice diagnostic ' + value['status'] + ' in ' + str(value['wall_seconds']) + ' seconds; full-suite qualified=false', flush=True)
    return code


if __name__ == '__main__':
    mode = sys.argv[1]
    raise SystemExit(cargo(sys.argv[2:]) if mode == 'cargo' else run() if mode == 'run' else launch())
