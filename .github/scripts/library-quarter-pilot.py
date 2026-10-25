#!/usr/bin/env python3
"""Diagnostic all-library build and first hash quarter; never a full-suite qualification."""
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
OUT = ROOT / 'target/test-performance' / ('library-quarter-' + os.environ.get('GITHUB_RUN_ID', 'local'))
CANONICAL_REVISION = 'cbbbbbded5e2b3290346863ef1e51ff7ed6b2b03'
CANONICAL = json.loads((ROOT / '.github/scripts/library-quarter-canonical.json').read_text())[os.uname().machine]
ROLE = 'postgres::isolation_tests::migration::migration_without_createrole_fails_clearly'


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


def fingerprints():
    result = {}
    for bid, record in CANONICAL['libraries'].items():
        binary = record['binary']
        suffix = Path(binary['binary-path']).name.rsplit('-', 1)[1]
        path = ROOT / 'target/debug/.fingerprint' / (record['package_name'] + '-' + suffix) / ('test-lib-' + binary['binary-name'] + '.json')
        value = json.loads(path.read_text())
        value.pop('local', None)
        result[bid] = value
    return result


def records(manifest):
    return sorted([bid, name, row['ignored'], row['kind']]
                  for bid, suite in manifest['rust-suites'].items()
                  for name, row in suite['testcases'].items())


def selected_cases(manifest):
    return sorted([bid, name] for bid, suite in manifest['rust-suites'].items()
                  for name, row in suite['testcases'].items()
                  if not row['ignored'] and row['filter-match']['status'] == 'matches')


def cargo(args):
    real = os.environ['LIBRARY_QUARTER_REAL_CARGO']
    if args[:1] == ['test'] and '--doc' in args:
        assert args[args.index('-p') + 1] == 'crabber-session'
        save('excluded-doctest.json', {'argv': args, 'reason': 'pilot covers only the first native library hash quarter; full-check doctest gate remains outside this diagnostic'})
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
        actual = summary['rust-binaries']
        expected = {bid: value['binary'] for bid, value in CANONICAL['libraries'].items()}
        assert actual == expected, 'library binary identities differ from the complete canonical graph'
        after = fingerprints()
        save('rebuilt-fingerprints.json', after)
        assert after == json.loads((OUT / 'canonical-fingerprints.json').read_text()), 'library dependency/features/profile fingerprints changed'
        executable_artifacts = [x for x in artifacts if x.get('executable')]
        assert len(executable_artifacts) == 29
        session = [x for x in executable_artifacts if x['target']['name'] == 'crabber_session']
        assert len(session) == 1 and not session[0]['fresh'], 'Session test target must actually rebuild'
        actual_profiles = {x['executable']: x['profile'] for x in executable_artifacts}
        assert all(actual_profiles[v['binary']['binary-path']] == v['profile'] and v['profile']['debug_assertions'] and v['profile']['overflow_checks'] for v in CANONICAL['libraries'].values())
        assert session[0]['profile']['opt_level'] == '0'
        save('graph-verdict.json', {'canonical_revision': CANONICAL_REVISION, 'all_29_fingerprints_equal': True, 'session_rebuilt': True, 'emitted_executables': executable_artifacts, 'scope': 'all library test targets compiled; first hash quarter executed'})
        binaries.write_text(json.dumps(summary))
        list_args = [real, 'nextest', 'list', '--cargo-metadata', str(metadata), '--binaries-metadata', str(binaries), '--profile', 'complete-ci', '--message-format', 'json']
        manifest = json.loads(command(list_args, 'inventory', True))
        save('tests.json', manifest)
        inventory = records(manifest)
        digest = hashlib.sha256(json.dumps(inventory, separators=(',', ':')).encode()).hexdigest()
        assert len(inventory) == CANONICAL['total'] and sum(not x[2] for x in inventory) == CANONICAL['nonignored'] and digest == CANONICAL['inventory_sha256'], 'library inventory differs from canonical CI'
        selected = json.loads(command([*list_args, '-E', 'not test(=' + ROLE + ')', '--partition', 'hash:1/4'], 'partition-inventory', True))
        save('partition-tests.json', selected)
        selected_ids = selected_cases(selected)
        assert selected_ids and all(name != ROLE for _, name in selected_ids)
        save('inventory-verdict.json', {'total': len(inventory), 'nonignored': CANONICAL['nonignored'], 'sha256': digest, 'canonical_equal': True, 'partition': 'hash:1/4', 'partition_cases': selected_ids, 'partition_count': len(selected_ids)})

    original_filter = args[args.index('-E') + 1]
    role = original_filter.startswith('test(')
    selected = 'test(=' + ROLE + ')' if role else 'not test(=' + ROLE + ')'
    argv = [real, 'nextest', 'run', '--cargo-metadata', str(metadata), '--binaries-metadata', str(binaries), '--profile', 'complete-ci', '--retries', '0', '--no-fail-fast', '--no-tests', 'fail', '-E', selected]
    if not role:
        argv += ['--partition', 'hash:1/4']
    phase = 'role' if role else 'quarter'
    output = command([*argv, '--status-level', 'pass'], phase, True)
    stderr = (OUT / (phase + '-stderr.log')).read_text()
    clean = re.sub(r'\x1b\[[0-9;]*m', '', output + '\n' + stderr)
    actual = Counter((match[1], match[2]) for match in re.finditer(r'^\s*PASS\s+\[\s*[0-9.]+s\]\s+(?:\(\s*\d+/\d+\)\s+)?(\S+)\s+(.+?)\s*$', clean, re.MULTILINE))
    expected = [('crabber-session', ROLE)] if role else [tuple(x) for x in json.loads((OUT / 'inventory-verdict.json').read_text())['partition_cases']]
    save(phase + '-execution.json', {'expected_count': len(expected), 'actual_count': sum(actual.values()), 'actual_cases': [list(key) for key in sorted(actual)], 'exact_equal': actual == Counter(expected)})
    assert actual == Counter(expected), 'actual passing cases differ from the selected library partition'
    print(phase + ': ' + str(len(expected)) + ' exact cases passed', flush=True)
    return 0


def run():
    restored = json.loads((ROOT / 'target/test-performance/cache-restore.json').read_text())
    assert CANONICAL_REVISION in (restored.get('restored_key') or ''), 'requires the actual cbbb diagnostic compiler snapshot'
    assert os.cpu_count() == 4 and len(os.sched_getaffinity(0)) == 4
    import tomllib
    config = tomllib.loads((ROOT / '.config/nextest.toml').read_text())
    assert config['profile']['complete-ci']['test-threads'] == 4
    command(['git', 'diff', '--exit-code', CANONICAL_REVISION, 'HEAD', '--', '.config/nextest.toml'], 'scheduler-source')
    save('canonical-fingerprints.json', fingerprints())
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
    save('owner.json', {'sha256': owner_sha, 'fingerprints': owner_matches, 'source_equivalence': 'xtask Rust source unchanged from cbbb; cached owner is inside the measured dependency cache', 'canonical_revision': CANONICAL_REVISION})
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
    os.environ['LIBRARY_QUARTER_REAL_CARGO'] = real
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
    code = module.supervise([sys.executable, '-I', '-S', str(Path(__file__).resolve()), 'run'], ROOT, report, budget=600, reserve=20, elapsed=elapsed)
    value = json.loads(report.read_text())
    source_paths['after_run'] = subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT, text=True).splitlines()
    save('source-paths.json', source_paths)
    value.update(qualified=False, coverage_scope='diagnostic library hash:1/4 plus isolated role; all-library compilation', timing_target_seconds=600, timing_target_met=False, source_end=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(), source_end_clean=not subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT, text=True).strip())
    stable = value['source_clean'] and value['source_end_clean'] and value['source_revision'] == value['source_end']
    if not stable:
        value.update(status='failed', reason='source-changed-or-dirty')
        code = code or 1
    value['pilot_slice_within_600'] = code == 0 and value['status'] == 'passed' and stable and value['wall_seconds'] <= 600
    save('report.json', value)
    print('Library quarter diagnostic ' + value['status'] + ' in ' + str(value['wall_seconds']) + ' seconds; full-suite qualified=false', flush=True)
    return code


if __name__ == '__main__':
    mode = sys.argv[1]
    raise SystemExit(cargo(sys.argv[2:]) if mode == 'cargo' else run() if mode == 'run' else launch())
