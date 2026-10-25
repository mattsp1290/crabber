"""Run one cold and three paired warm complete gates on frozen checkouts.

Only the explicit benchmark output roots are archived or removed. Inactive
compiler archives live outside measured storage; restored outputs retain mtimes.
"""
import argparse
import atexit
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--baseline', type=Path, required=True)
parser.add_argument('--baseline-revision', default='58535cfc234c2a3d514487b0f72d646e4ba33110')
parser.add_argument('--candidate', type=Path, required=True)
parser.add_argument('--candidate-revision', required=True)
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--archives', type=Path, required=True)
parser.add_argument('--incremental', choices=['0', '1'], required=True)
parser.add_argument('--build-jobs', type=int, default=4)
args = parser.parse_args()
assert args.build_jobs > 0
checkouts = {'baseline': args.baseline.resolve(), 'candidate': args.candidate.resolve()}
assert len(set(checkouts.values())) == 2
assert not checkouts['baseline'].is_relative_to(checkouts['candidate'])
assert not checkouts['candidate'].is_relative_to(checkouts['baseline'])
base_revision = '58535cfc234c2a3d514487b0f72d646e4ba33110'
instrumented_revision = '9890af9c0f6ec22e94687f13a0d8918f6a0b19c4'
assert args.baseline_revision in {base_revision, instrumented_revision}
revisions = {'baseline': args.baseline_revision, 'candidate': args.candidate_revision}
here = Path(__file__).resolve().parent
env = dict(os.environ)
for name in list(env):
    if (name.startswith(('CRABBER_TEST_', 'OTEL_EXPORTER_OTLP_')) or name in {
        'CARGO_TARGET_DIR', 'RUSTFLAGS', 'CARGO_ENCODED_RUSTFLAGS', 'RUSTC_WRAPPER',
        'RUSTC_WORKSPACE_WRAPPER', 'ANTHROPIC_API_KEY', 'OPENAI_API_KEY',
        'OPENCODE_GO_API_KEY', 'DD_API_KEY', 'DD_APP_KEY', 'DD_ENV', 'DD_SERVICE',
        'DD_VERSION', 'CRABBER_POSTGRES_URL',
        'CRABBER_REQUIRE_POSTGRES', 'CRABBER_TRACE_LIVE', 'CRABBER_OBS_VERIFY_MARKER',
    }):
        env.pop(name)
env.update(CARGO_PROFILE_DEV_DEBUG='0', CARGO_PROFILE_TEST_DEBUG='0',
           CARGO_INCREMENTAL=args.incremental, CARGO_BUILD_JOBS=str(args.build_jobs))
env['PATH'] = str(checkouts['candidate'] / 'target/test-tools') + os.pathsep + env['PATH']
out = args.output.resolve()
archives = args.archives.resolve()
roots = [
    'target/benchmark-offline', 'target/benchmark-postgres', 'fixtures/wasm/target',
    'testdata/custom-http-feature-probe/target',
    'testdata/external-consumer/target', 'testdata/server-host-consumer/target',
    'fixtures/wasm/manifest.sha256',
]
for root in checkouts.values():
    for directory in [out, archives]:
        assert not directory.is_relative_to(root / 'fixtures/wasm')
        assert not any(directory.is_relative_to(root / name) for name in roots)
out.mkdir(parents=True, exist_ok=False)
archives.mkdir(parents=True, exist_ok=False, mode=0o700)
schedule = [('cold', 'baseline'), ('cold', 'candidate'),
            ('warm-1', 'baseline'), ('warm-1', 'candidate'),
            ('warm-2', 'candidate'), ('warm-2', 'baseline'),
            ('warm-3', 'baseline'), ('warm-3', 'candidate')]
report = {'status': 'preparing', 'revisions': revisions,
          'baseline_product_revision': base_revision,
          'profile': {k: env[k] for k in ['CARGO_PROFILE_DEV_DEBUG', 'CARGO_PROFILE_TEST_DEBUG',
                                         'CARGO_INCREMENTAL', 'CARGO_BUILD_JOBS']},
          'output_roots': roots,
          'transient_sources': 'The original HTTP script recreates and removes .probe-tmp itself; '
                               'local source probes own disposable temporary directories. '
                               'Their compiler outputs still use the listed target roots.',
          'conditions': 'Separate revision/lane targets; cached registry, tools and PG image; '
                        'inactive compressed artifacts outside measured filesystem; no concurrent gates.',
          'runs': [{'sample': s, 'variant': v, 'status': 'pending'} for s, v in schedule]}

def save():
    temporary = out / 'summary.tmp'
    temporary.write_text(json.dumps(report, indent=2) + '\n')
    temporary.replace(out / 'summary.json')

def incomplete():
    if report['status'] not in {'passed', 'failed'}:
        report['status'] = 'failed'
        report['reason'] = 'Coordinator stopped before completing the comparison'
        for row in report['runs']:
            if row['status'] == 'pending':
                row['status'] = 'cancelled'
            elif row['status'] == 'running':
                row['status'] = 'failed'
        save()

atexit.register(incomplete)

active = None

def stop(signum, _frame):
    if active is None:
        raise SystemExit(128 + signum)
    try:
        os.killpg(active.pid, signum)
    except ProcessLookupError:
        pass

signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)

def run_command(argv, *, check=False, **kwargs):
    global active
    active = subprocess.Popen(argv, start_new_session=True, **kwargs)
    try:
        code = active.wait()
    finally:
        active = None
    if check and code:
        raise subprocess.CalledProcessError(code, argv)
    return code

def quiescent():
    competing = []
    for process in Path('/proc').iterdir():
        if not process.name.isdecimal():
            continue
        try:
            argv = (process / 'cmdline').read_bytes().split(b'\0')
            name = Path(os.fsdecode(argv[0])).name
        except (OSError, IndexError):
            continue
        if name.startswith(('cargo', 'xtask')) or name in {'rustc', 'clippy-driver', 'rustdoc'}:
            competing.append({'pid': int(process.name), 'executable': name})
    if competing:
        report['competing_processes'] = competing
        raise RuntimeError('A build or test runner is already active; comparison was not started')

def frozen(variant):
    root = checkouts[variant]
    assert Path(subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], cwd=root, text=True).strip()).resolve() == root
    assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip() == revisions[variant]
    assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=root, text=True).strip()

def paths(root, lane=None):
    selected = roots
    if lane == 'postgres':
        selected = ['target/benchmark-postgres']
    elif lane == 'offline':
        selected = [name for name in roots if name != 'target/benchmark-postgres']
    aliases = [] if lane == 'postgres' else sorted((root / 'fixtures/wasm').glob('*.wasm'))
    return [root / name for name in selected] + aliases

def safe(root, path):
    assert path != root and path.resolve().is_relative_to(root) and not path.is_symlink()
    relative = str(path.relative_to(root))
    subprocess.run(['git', 'check-ignore', '-q', '--', relative], cwd=root, check=True)
    assert not subprocess.check_output(['git', 'ls-files', '--', relative], cwd=root, text=True).strip()

def clear_outputs(root, lane=None):
    for path in paths(root, lane):
        safe(root, path)
        if path.exists():
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()

def snapshot(variant, lane):
    root = checkouts[variant]
    available = [p for p in paths(root, lane) if p.exists()]
    for path in available:
        safe(root, path)
    manifest = archives / f'{variant}-{lane}.paths'
    manifest.write_bytes(b'\0'.join(str(p.relative_to(root)).encode() for p in available) + b'\0')
    archive = archives / f'{variant}-{lane}.tar.zst'
    temporary = archive.with_suffix('.partial')
    run_command(['tar', '--format=pax', '-I', 'zstd -T2 -1', '-cf', str(temporary),
                 '--null', '-T', str(manifest)], cwd=root, check=True)
    run_command(['zstd', '-t', str(temporary)], check=True)
    temporary.replace(archive)
    clear_outputs(root, lane)

def restore(variant, lane):
    root = checkouts[variant]
    archive = archives / f'{variant}-{lane}.tar.zst'
    assert archive.exists()
    clear_outputs(root, lane)
    run_command(['tar', '--delay-directory-restore', '-I', 'zstd', '-xf', str(archive)],
                cwd=root, check=True)

save()
if args.baseline_revision == instrumented_revision:
    identical = {}
    for name in ['crates', 'examples', 'testdata', 'fixtures', 'wit', '.github',
                 '.cargo', 'Cargo.toml', 'README.md', '.gitignore']:
        before = subprocess.check_output(['git', 'rev-parse', base_revision + ':' + name],
                                         cwd=checkouts['baseline'], text=True).strip()
        measured = subprocess.check_output(['git', 'rev-parse', instrumented_revision + ':' + name],
                                           cwd=checkouts['baseline'], text=True).strip()
        assert before == measured, name
        identical[name] = before
    report['baseline_instrumentation'] = {
        'scope': 'WP1 unchanged original command schedule with timing/failure reporting; '
                 'all product and behavioral test trees identical to the agreed base',
        'identical_trees': identical,
    }
    save()
for variant in checkouts:
    frozen(variant)
    quiescent()
    clear_outputs(checkouts[variant])
report['status'] = 'running'
save()
for record in report['runs']:
    variant, sample = record['variant'], record['sample']
    root = checkouts[variant]
    frozen(variant)
    quiescent()
    record.update(status='running', phases=[])
    save()
    # The cold PostgreSQL lane must succeed before the offline lane generates
    # WASM aliases or consumer outputs that are absent in its independent CI job.
    for lane in ['postgres', 'offline']:
        quiescent()
        if sample != 'cold':
            restore(variant, lane)
        destination = out / f'{sample}-{variant}' / lane
        lane_env = dict(env, CARGO_TARGET_DIR=str(root / f'target/benchmark-{lane}'))
        if lane == 'offline':
            command = ['cargo', 'xtask', 'check']
            if variant == 'candidate' or args.baseline_revision == instrumented_revision:
                command.extend(['--timings', str(destination / 'timings.json')])
        elif variant == 'baseline':
            command = ['python3', '-I', '-S', str(here / 'baseline-postgres.py'),
                       '--output', str(destination / 'commands')]
        else:
            command = ['cargo', 'run', '-p', 'xtask', '--features', 'postgres-check',
                       '--', 'check-postgres', '--fixture-metrics', str(destination / 'fixtures.jsonl')]
        started = time.monotonic()
        print(f'{sample} {variant} {lane} starting', flush=True)
        code = run_command(['python3', '-I', '-S', str(here / 'measure.py'),
                            '--cwd', str(root), '--output', str(destination), '--', *command],
                           env=lane_env)
        measurement = json.loads((destination / 'summary.json').read_text())
        record['phases'].append({'lane': lane, 'exit_code': code, 'measurement': measurement,
                                 'observer_end_to_end_seconds': time.monotonic() - started})
        save()
        if code != 0 or measurement['status'] != 'passed':
            record['status'] = report['status'] = 'failed'
            for pending in report['runs']:
                if pending['status'] == 'pending':
                    pending['status'] = 'cancelled'
            save()
            raise SystemExit(code or 1)
        frozen(variant)
        snapshot(variant, lane)
    record['status'] = 'passed'
    frozen(variant)
    save()
report['status'] = 'passed'
save()
print('Cold and all three paired warm complete-gate comparisons passed', flush=True)
