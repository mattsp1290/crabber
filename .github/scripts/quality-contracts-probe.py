#!/usr/bin/env python3
"""Draft-only exact-cache contracts cost probe; NEVER MERGE or qualify the suite."""
import errno
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import zipfile

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / 'target/test-performance/quality-contracts-probe'
SOURCE = 'f0cfc61dfec018c24be0124ad6f80856f50be47e'
MAIN = '8868d6ec1bf4b8e1a7f0684ceb970a399c80eb90'
RUN = 40181958569
ARTIFACT = 12596077587
DIGEST = '8e46ace367ae736921368c96e03298fbd8a0971e9f9f787e1b6dfd8218aa117d'
CACHE_ID = 9474387290
CACHE = 'rust-Linux-X64-b569b0c98ea3133c7252b4b8ea6b39b7b353813df0dd5f5c412c355691424112-dev0-test0-inc0-combined-v1-2140fed529d2fc7a4b7788fa8a0ae04e871570c1df6d03fa97d46e9e82ac09e0-8868d6ec1bf4b8e1a7f0684ceb970a399c80eb90-40181958569-1'
ALLOWED = {'.github/workflows/ci.yml', '.github/scripts/quality-contracts-probe.py'}


def read(path):
    return json.loads(path.read_text())


def cached(relative):
    path = ROOT / relative
    for parent in [path, *path.parents]:
        if parent == ROOT:
            break
        assert not parent.is_symlink(), 'cached input may not traverse a symlink'
    assert path.is_file(), 'required exact cached input absent'
    return path


def save(name, value):
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / name).write_text(json.dumps(value, indent=2) + '\n')


def git(*args):
    return subprocess.check_output(['git', *args], cwd=ROOT, text=True).strip()


def api(path):
    return json.loads(subprocess.check_output(['gh', 'api', 'repos/mattsp1290/crabber/' + path], text=True))


def source_guard():
    assert Path.cwd().resolve() == ROOT and os.uname().machine == 'x86_64'
    assert os.environ['GITHUB_REPOSITORY'] == 'mattsp1290/crabber'
    assert os.environ['GITHUB_EVENT_NAME'] == 'pull_request'
    event = read(Path(os.environ['GITHUB_EVENT_PATH']))['pull_request']
    assert event['draft'] is True and event['head']['repo']['full_name'] == 'mattsp1290/crabber'
    assert event['head']['ref'] == 'diagnostics/quality-contracts-f0'
    assert event['base']['ref'] == 'main' and event['base']['sha'] == MAIN
    assert not git('status', '--porcelain'), 'clean diagnostic source required'
    git('merge-base', '--is-ancestor', SOURCE, 'HEAD')
    assert set(git('diff', '--name-only', '--no-renames', '-z', SOURCE, 'HEAD').rstrip('\0').split('\0')) <= ALLOWED
    assert os.environ.get('CARGO_TARGET_DIR') == str(ROOT / 'target')
    for name, value in {'CARGO_BUILD_JOBS': '4', 'CARGO_INCREMENTAL': '0', 'CARGO_PROFILE_DEV_DEBUG': '0', 'CARGO_PROFILE_TEST_DEBUG': '0'}.items():
        assert os.environ.get(name) == value, 'canonical profile environment required'
    assert not any(os.environ.get(name) for name in ['RUSTFLAGS', 'CARGO_ENCODED_RUSTFLAGS', 'RUSTC_WRAPPER', 'RUSTC_WORKSPACE_WRAPPER', 'CARGO_BUILD_TARGET', 'CARGO_BUILD_BUILD_DIR'])
    assert {name for name in os.environ if name.startswith('CARGO_PROFILE_')} == {'CARGO_PROFILE_DEV_DEBUG', 'CARGO_PROFILE_TEST_DEBUG'}


def baseline():
    source_guard()
    cache = api('actions/caches?key=' + CACHE + '&ref=refs%2Fheads%2Fmain&per_page=100')
    save('cache-preflight.json', cache)
    matches = [row for row in cache['actions_caches'] if row['id'] == CACHE_ID and row['key'] == CACHE and row['ref'] == 'refs/heads/main']
    assert len(matches) == 1, 'exact Main cache unavailable; no fallback or workspace work permitted'
    run = api(f'actions/runs/{RUN}')
    assert run['head_sha'] == MAIN and run['head_branch'] == 'main' and run['event'] == 'push' and run['run_attempt'] == 1
    artifact = api(f'actions/artifacts/{ARTIFACT}')
    assert artifact['name'] == f'complete-check-X64-{RUN}-1' and not artifact['expired']
    assert artifact['workflow_run']['id'] == RUN and artifact['digest'] == 'sha256:' + DIGEST
    archive = OUT / 'normal.zip'
    with archive.open('xb') as stream:
        subprocess.run(['gh', 'api', f'repos/mattsp1290/crabber/actions/artifacts/{ARTIFACT}/zip'], stdout=stream, check=True)
    assert hashlib.file_digest(archive.open('rb'), 'sha256').hexdigest() == DIGEST
    with zipfile.ZipFile(archive) as zipped:
        for entry in zipped.infolist():
            path = Path(entry.filename)
            assert not path.is_absolute() and '..' not in path.parts
            assert (entry.external_attr >> 16) & 0o170000 != 0o120000
        zipped.extractall(OUT / 'normal')
    checks = list((OUT / 'normal').glob('ci-*/check.json'))
    assert len(checks) == 1
    normal = read(checks[0])
    assert normal['status'] == 'passed' and normal['child_exit_code'] == 0
    assert normal['source_revision'] == normal['end_revision'] == MAIN and normal['source_clean'] and normal['end_clean']
    phases = read(checks[0].parent / 'check-phases.json')
    assert phases['coverage_scope'] == 'complete' and len(phases['gates']) == 72 and all(g['status'] == 'passed' for g in phases['gates'])
    save('baseline.json', {'source': SOURCE, 'main': MAIN, 'run': RUN, 'artifact': ARTIFACT, 'archive_sha256': DIGEST, 'cache_id': CACHE_ID, 'cache_key': CACHE, 'normal_check': str(checks[0])})


def restored():
    source_guard()
    assert os.environ.get('RESTORED_KEY') == CACHE and os.environ.get('CACHE_EXACT_HIT') == 'true'
    assert read(OUT / 'baseline.json')['cache_key'] == CACHE
    marker = read(cached('target/test-tools/complete-cache.json'))
    assert marker['revision'] == MAIN and marker['complete'] is True
    assert marker['check'] == read(Path(read(OUT / 'baseline.json')['normal_check']))
    timestamps = read(cached('target/test-tools/source-times.json'))
    assert timestamps['version'] == 2 and timestamps['workspace'] == str(ROOT)
    for entry in git('ls-tree', '-r', '-z', MAIN).rstrip('\0').split('\0'):
        metadata, name = entry.split('\t', 1)
        mode, kind, blob = metadata.split()
        if mode in {'100644', '100755'}:
            saved = timestamps['files'].get(name, {})
            assert saved.get('blob') == blob and saved.get('mode') == mode, 'restored source provenance is not exact Main'
    pins = read(ROOT / '.config/test-tools.json')
    for binary, argv, expected in [
        (cached('target/test-tools/cargo-nextest'), ['nextest', '--version'], 'cargo-nextest ' + pins['nextest']['version']),
        (cached('target/test-tools/bin/wasm-tools'), ['--version'], 'wasm-tools ' + pins['wasm-tools']['version']),
    ]:
        assert subprocess.check_output([str(binary), *argv], text=True).strip().startswith(expected)
    rustc = subprocess.check_output(['rustc', '-Vv'])
    assert hashlib.sha256(rustc).hexdigest() == CACHE.split('-dev0-test0')[0].split('-')[-1]
    save('restore-proof.json', {'exact_hit': True, 'cache_id': CACHE_ID, 'cache_key': CACHE, 'source_profile': SOURCE, 'workspace_compilation_in_setup': False})


def measure():
    source_guard()
    assert read(OUT / 'restore-proof.json')['exact_hit'] is True
    credentials = ['GH_TOKEN', 'GITHUB_TOKEN', 'ANTHROPIC_API_KEY', 'OPENAI_API_KEY', 'OPENCODE_GO_API_KEY', 'DD_API_KEY', 'DD_APP_KEY']
    assert not any(os.environ.get(name) for name in credentials), 'measurement must not inherit credentials'
    spec = importlib.util.spec_from_file_location('contracts_check', ROOT / '.github/scripts/check.py')
    check = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(check)
    history, inspection_errors, environments = set(), [], []
    requirements = ['CRABBER_REQUIRE_POSTGRES', 'BACKGROUND_JOBS_REQUIRE_SHELL', 'PYTHON_REPL_REQUIRE_PYTHON', 'CRABBER_TOOLS_REQUIRE_LOGIN_PROFILE']
    assert all(os.environ.get(name) == '1' for name in requirements)
    original_collect, original_supervise = check.collect, check.supervise

    def strict_identity(pid):
        try:
            raw = Path(f'/proc/{pid}/stat').read_text()
            fields = raw[raw.rfind(')') + 2:].split()
            return (int(fields[1]), int(fields[19]), fields[0], int(fields[21]) * os.sysconf('SC_PAGE_SIZE'), int(fields[17]))
        except OSError as error:
            if error.errno not in (errno.ENOENT, errno.ESRCH):
                inspection_errors.append({'pid': pid, 'error': type(error).__name__})
        except (ValueError, IndexError) as error:
            inspection_errors.append({'pid': pid, 'error': type(error).__name__})
        return None

    def collect(owned):
        history.update(owned.items())
        current = original_collect(owned)
        history.update((pid, row[1]) for pid, row in current.items())
        if not environments:
            for pid, row in current.items():
                try:
                    argv = Path(f'/proc/{pid}/cmdline').read_bytes().split(b'\0')
                    if b'--part' not in argv or b'quality-contracts' not in argv:
                        continue
                    values = dict(item.split(b'=', 1) for item in Path(f'/proc/{pid}/environ').read_bytes().split(b'\0') if b'=' in item)
                    verified = strict_identity(pid)
                    if verified is None or verified[1] != row[1]:
                        continue
                    environments.append({'pid': pid, 'start': row[1],
                        'requirements_enabled': {name: values.get(name.encode()) == b'1' for name in requirements},
                        'credentials_absent': {name: not values.get(name.encode()) for name in credentials}})
                    break
                except OSError as error:
                    if error.errno not in (errno.ENOENT, errno.ESRCH):
                        inspection_errors.append({'pid': pid, 'error': type(error).__name__})
        return current

    def supervise(*args, **kwargs):
        code = original_supervise(*args, **kwargs)
        terminal_started = time.monotonic_ns()
        survivors = []
        for pid, start in sorted(history):
            row = strict_identity(pid)
            if row is not None and row[1] == start:
                survivors.append({'pid': pid, 'start': start, 'state': row[2]})
        valid = bool(history) and not survivors and not inspection_errors and bool(environments)
        valid = valid and all(all(row['requirements_enabled'].values()) and all(row['credentials_absent'].values()) for row in environments)
        phases = read(OUT / 'check-phases.json') if (OUT / 'check-phases.json').exists() else {}
        if code == 0:
            groups = phases.get('ownership', {})
            expected = groups.get('quality-contracts', [])
            actual = phases.get('gates', [])
            union = [item for group in groups.values() for item in group]
            valid = valid and phases.get('coverage_scope') == 'quality-contracts' and phases.get('qualified') is False and phases.get('processes_drained') is True
            valid = valid and phases.get('full_native_suite_executed') is False and phases.get('owned_postgres_run') is False
            valid = valid and len(expected) == len(actual) == 41 and len(union) == len(set(union)) == 72
            valid = valid and [g['id'] for g in actual] == expected and all(g['status'] == 'passed' for g in actual)
        save('terminal-proof.json', {'valid': valid, 'observed_pid_start_identities': sorted(history), 'survivors_including_zombies': survivors, 'inspection_errors': inspection_errors, 'actual_partial_command_environments': environments, 'terminal_verification_started_monotonic_ns': terminal_started, 'terminal_verification_finished_monotonic_ns': time.monotonic_ns(), 'clock_scope': 'inside check.main supervise call before its final elapsed/budget decision', 'scope': 'sampled owned descendants only; no PostgreSQL container was owned or claimed absent'})
        report = read(OUT / 'check.json')
        report['terminal_proof_valid'] = valid
        if code == 0 and not valid:
            code = 1
            report.update(status='failed', reason='diagnostic-terminal-or-scope-proof-failed')
        check.persist(OUT / 'check.json', report)
        return code

    check.identity, check.collect, check.supervise = strict_identity, collect, supervise
    sys.argv = ['check.py', '--part', 'quality-contracts', '--budget-seconds', '600', '--cache-state', 'unclassified', '--ci-start-time', os.environ['CHECK_STARTED'], '--report', str(OUT / 'check.json')]
    return check.main()


if __name__ == '__main__':
    operation = sys.argv[1]
    if operation == 'run':
        raise SystemExit(measure())
    {'baseline': baseline, 'restored': restored}[operation]()
