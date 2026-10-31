#!/usr/bin/env python3
"""Run bounded off/empty/warm complete-check trials in one fresh owned worktree."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import signal
import statistics
import subprocess
import sys
import time

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('compiler_cache', Path(__file__).with_name('compiler-cache.py'))
cache = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cache)
ROOT = Path(__file__).resolve().parents[2]
BUDGET = 600


class Cancelled(KeyboardInterrupt):
    def __init__(self, number):
        self.number = number


def run_check(command, cwd, env, log):
    with log.open('xb') as output:
        child = subprocess.Popen(command, cwd=cwd, env=env, stdout=output, stderr=output, start_new_session=True)
        try:
            return child.wait()
        except BaseException as error:
            if child.poll() is None:
                os.killpg(child.pid, error.number if isinstance(error, Cancelled) else signal.SIGTERM)
                child.wait()
            raise


def stable_context(record, revision):
    report = record['measurement']
    return (isinstance(report, dict) and report.get('coverage_scope') == 'complete' and report.get('budget_seconds') == BUDGET
            and report.get('source_revision') == report.get('end_revision') == revision
            and report.get('source_clean') is True and report.get('end_clean') is True
            and isinstance(report.get('profile'), dict) and report['profile'].get('CARGO_INCREMENTAL') == '0')


def complete(record, revision):
    report = record['measurement']
    return (stable_context(record, revision) and record['exit_code'] == 0 and report.get('status') == 'passed'
            and report.get('timing_target_met') is True and report.get('budget_breached') is False
            and record['total_wall_seconds'] <= BUDGET and report.get('wall_seconds', BUDGET + 1) <= BUDGET)


def timed_out(record, revision):
    report = record['measurement']
    return (stable_context(record, revision) and record['exit_code'] == 124 and report.get('status') == 'failed'
            and isinstance(report.get('reason'), str) and report['reason'].startswith('budget-'))


def closed_seed(record):
    local = record['measurement'].get('compiler_cache', {})
    return (isinstance(local, dict) and local.get('mode') == 'local' and local.get('state') == 'closed'
            and local.get('cleanup_verified') is True and isinstance(local.get('published'), str))


def attempt(checkout, output, pair, role, env, previous):
    started_epoch, started = time.time(), time.monotonic()
    name = f'{pair:02d}-{role}'
    directory = output / name
    record = {'pair': pair, 'role': role, 'exit_code': None, 'measurement': None,
              'reason': None, 'interrupted_signal': None}
    try:
        directory.mkdir(mode=0o700)
        target = checkout / 'target/compiler-cache-qualification' / name
        cache.require(not target.exists(), 'qualification requires a new private Cargo target')
        target.mkdir(parents=True, mode=0o700)
        fixture = checkout / 'fixtures/wasm/target'
        if fixture.exists():
            cache.require(previous is not None and not fixture.is_symlink(), 'unexpected preexisting WASM target')
            cache.owned_directory(fixture)
            fixture.rename(previous / 'wasm-target')
        elif fixture.is_symlink():
            raise ValueError('WASM target must not be a symlink')
        child_env = {**cache.clean_environment(env), 'CARGO_INCREMENTAL': '0',
                     'CARGO_TARGET_DIR': str(target), 'CARGO_BUILD_BUILD_DIR': str(target),
                     'CRABBER_COMPILER_CACHE': 'off' if role == 'off' else 'local',
                     'CRABBER_COMPILER_CACHE_DIR': str(output / 'compiler-snapshots' / str(pair))}
        child_env.setdefault('CARGO_PROFILE_DEV_DEBUG', '0')
        child_env.setdefault('CARGO_PROFILE_TEST_DEBUG', '0')
        measurement = directory / 'check.json'
        command = [sys.executable, '-I', '-S', str(checkout / '.github/scripts/check.py'),
                   '--budget-seconds', str(BUDGET), '--cache-state', 'unclassified',
                   '--ci-start-time', str(started_epoch), '--report', str(measurement)]
        print(f'Compiler cache trial {pair}, {role}, full schedule and {BUDGET}s total budget.', flush=True)
        record['exit_code'] = run_check(command, checkout, child_env, directory / 'console.log')
        result = json.loads(measurement.read_text())
        cache.require(isinstance(result, dict), 'check report must be an object')
        record['measurement'] = result
    except KeyboardInterrupt as error:
        record.update(reason='caller-interrupted', interrupted_signal=getattr(error, 'number', signal.SIGINT))
        record['exit_code'] = 128 + record['interrupted_signal']
    except Exception as error:
        record['reason'] = 'missing-or-invalid-report-' + type(error).__name__
    record['total_wall_seconds'] = time.monotonic() - started
    return record, directory


def conditions(checkout, output, trials, env, revision):
    report = {'version': 1, 'status': 'running', 'qualified': False, 'pilot': trials < 5,
              'revision': revision, 'budget_seconds': BUDGET, 'requested_trials': trials,
              'pair_orders': [], 'cases': [], 'reason': None,
              'acceptance_remaining': ['paired resource/limiter review', 'target-only/combined controls',
                                       'unchanged/runtime/session qualification', 'Linux x64 and arm64 CI persistence'],
              'timing_scope': 'each complete command includes preparation, import, server startup, compilation, full schedule, cleanup and publication'}
    def save():
        cache.write_json(output / 'summary.json', report)
    def stop(reason):
        report.update(status='failed', reason=reason)
        save()
        return report
    save()
    previous = None
    for pair in range(1, trials + 1):
        seed = None
        order = ['off', 'empty', 'warm'] if pair % 2 else ['empty', 'warm', 'off']
        report['pair_orders'].append({'pair': pair, 'roles': order})
        for role in order:
            name = f'{pair:02d}-{role}'
            record, directory = attempt(checkout, output, pair, role, env, previous)
            report['cases'].append(record)
            previous = directory
            save()
            if record['reason'] is not None:
                return stop(f'{name}-{record["reason"]}')
            result = record['measurement']
            if role == 'off':
                if result.get('compiler_cache') != {'version': 1, 'mode': 'off'}:
                    return stop(f'{name}-off-control-not-disabled')
                if not complete(record, revision) and not timed_out(record, revision):
                    return stop(f'{name}-off-control-failed')
            elif role == 'empty':
                if not closed_seed(record) or result['compiler_cache'].get('restored') is not None:
                    return stop(f'{name}-seed-not-closed-or-not-empty')
                if not complete(record, revision) and not timed_out(record, revision):
                    return stop(f'{name}-seed-failed-without-bounded-timeout')
                seed = result
            else:
                local = result.get('compiler_cache', {})
                if not isinstance(local, dict) or not isinstance(local.get('restored'), dict):
                    return stop(f'{name}-invalid-warm-cache-report')
                restored = local.get('restored') or {}
                stats = local.get('stats') or {}
                try:
                    hits = stats.get('cache_hits', {}).get('counts', {}).get('Rust', 0)
                except AttributeError:
                    return stop(f'{name}-invalid-warm-statistics')
                if (not complete(record, revision) or not closed_seed(record) or seed is None
                        or local.get('namespace') != seed['compiler_cache']['namespace']
                        or restored.get('snapshot') != seed['compiler_cache']['published']
                        or restored.get('check_status') != seed['status']
                        or type(hits) is not int or hits <= 0):
                    return stop(f'{name}-warm-failed-or-no-seed-library-hit')
    report['status'] = 'passed'
    completed = {role: [case['total_wall_seconds'] for case in report['cases']
                        if case['role'] == role and complete(case, revision)] for role in ['off', 'empty', 'warm']}
    report['total_wall_summary'] = {role: {'completed': len(values), 'median_seconds': statistics.median(values),
                                         'minimum_seconds': min(values), 'maximum_seconds': max(values)}
                                  for role, values in completed.items() if values}
    off_complete = len(completed['off']) == trials
    all_off_timed_out = all(timed_out(case, revision) for case in report['cases'] if case['role'] == 'off')
    report['fresh_target_threshold_met'] = (
        statistics.median(completed['warm']) <= .8 * statistics.median(completed['off'])
        if off_complete else all_off_timed_out)
    report['verdict'] = 'unqualified pilot' if trials < 5 else 'pending resource review and remaining acceptance controls'
    if trials >= 5 and not report['fresh_target_threshold_met']:
        report.update(status='failed', reason='fresh-target-threshold-not-met-or-mixed-censoring')
    save()
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--trials', choices=[1, 5], type=int, default=5)
    parser.add_argument('--offline', action='store_true', required=True,
                        help='use prepared dependency sources so download warming cannot affect comparisons')
    args = parser.parse_args()
    cache.require(sys.platform == 'linux', 'complete compiler-cache qualification requires Linux')
    cache.require(not subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT, text=True).strip(),
                  'qualification requires a clean committed source snapshot')
    revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    env = dict(os.environ)
    env['CRABBER_SCCACHE_PATH'] = str(Path(env.get('CRABBER_SCCACHE_PATH', str(ROOT / 'target/test-tools/sccache'))).absolute())
    cache.verify_executable(Path(env['CRABBER_SCCACHE_PATH']), env=env)
    env['CARGO_NET_OFFLINE'] = 'true'
    output = args.output.absolute()
    cache.require(not output.exists() and not output.is_symlink(), 'use a new qualification output directory')
    cache.private_directory(output)
    checkout = output / 'checkout'
    subprocess.run(['git', 'worktree', 'add', '--detach', str(checkout), revision], cwd=ROOT, check=True)
    cache.write_json(output / 'ownership.json', {'version': 1, 'source_revision': revision,
                                               'purpose': 'disposable compiler-cache qualification worktree'})
    def interrupt(number, _frame):
        raise Cancelled(number)
    for number in [signal.SIGINT, signal.SIGTERM]:
        signal.signal(number, interrupt)
    result = conditions(checkout, output, args.trials, env, revision)
    print(f"Compiler-cache experiment {result['status']}; qualified=false; reports retained for review.", flush=True)
    interrupted = next((case['interrupted_signal'] for case in result.get('cases', []) if case['interrupted_signal']), None)
    return 128 + interrupted if interrupted else (0 if result['status'] == 'passed' else 1)


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        raise SystemExit(f'compiler-cache experiment refused: {error}')
