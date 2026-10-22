#!/usr/bin/env python3
"""Qualify the complete check as no-op, runtime rebuild and session rebuild."""
import argparse
import json
import os
from pathlib import Path
import resource
import signal
import subprocess
import sys
import time


def profile_differences(previous, current):
    required = {'CARGO_BUILD_JOBS', 'CARGO_INCREMENTAL',
                'CARGO_PROFILE_DEV_DEBUG', 'CARGO_PROFILE_TEST_DEBUG'}
    names = set(previous) | required | {
        name for name in current if name.startswith('CARGO_PROFILE_')}
    different = []
    for name in sorted(names):
        value = current.get(name)
        if value is None and name in {'CARGO_PROFILE_DEV_DEBUG', 'CARGO_PROFILE_TEST_DEBUG'}:
            value = '0'
        if value != previous.get(name):
            different.append(name)
    return different


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--warm-report', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--ci-start-time', type=float)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip()
    if subprocess.check_output(['git', 'status', '--porcelain'], cwd=root, text=True).strip():
        parser.error('qualification requires a clean committed checkout')
    warm = json.loads(args.warm_report.read_text())
    if 'previous_complete_result' in warm:
        marker = warm['previous_complete_result']
        if not marker or not marker.get('complete'):
            parser.error('CI needs a completed same-revision compiler snapshot before qualification')
        warm = marker.get('check', {})
    if (warm.get('status') != 'passed' or warm.get('source_revision') != revision
            or not warm.get('source_clean') or not warm.get('end_clean')
            or warm.get('end_revision') != revision):
        parser.error('the no-op case needs a previous complete passing report for this clean revision')
    differences = profile_differences(warm.get('profile', {}), os.environ)
    if differences:
        parser.error('qualification must preserve the previous profile settings: '
                     + ', '.join(differences))
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    summary = {'version': 1, 'status': 'running', 'revision': revision,
               'condition': 'same-revision completed target, then package-scoped invalidation of every affected owned target; dependencies retained',
               'warm_report': str(args.warm_report), 'cases': []}
    started = time.monotonic()
    usage_before = resource.getrusage(resource.RUSAGE_CHILDREN)

    def save():
        temporary = output / 'summary.tmp'
        temporary.write_text(json.dumps(summary, indent=2) + '\n')
        temporary.replace(output / 'summary.json')

    save()
    code = 1
    active = None
    interrupted = None
    def stop(number, _frame):
        nonlocal interrupted
        interrupted = number
        if active is not None:
            try:
                os.killpg(active.pid, number)
            except ProcessLookupError:
                pass
    for number in [signal.SIGINT, signal.SIGTERM]:
        signal.signal(number, stop)

    def run(command):
        nonlocal active
        if interrupted is not None:
            raise SystemExit(128 + interrupted)
        active = subprocess.Popen(command, cwd=root, start_new_session=True)
        try:
            status = active.wait()
        finally:
            active = None
        if interrupted is not None:
            return 128 + interrupted
        return status if status >= 0 else 128 - status

    try:
        for case, package in [('unchanged', None), ('runtime-rebuild', 'crabber-runtime'),
                              ('session-rebuild', 'crabber-session')]:
            record = {'case': case, 'status': 'running'}
            summary['cases'].append(record)
            save()
            evidence = output / f'{case}-invalidation.json'
            if package:
                prepare_started = time.monotonic()
                prepared = run([sys.executable, '-I', '-S', '.github/scripts/prepare-rebuild.py',
                                package, '--report', str(evidence)])
                if prepared:
                    code = prepared
                    return code
                record['preparation_seconds'] = time.monotonic() - prepare_started
                record['invalidation_report'] = str(evidence)
            report = output / f'{case}.json'
            command = [sys.executable, '-I', '-S', '.github/scripts/check.py',
                       '--cache-state', case, '--report', str(report)]
            if case == 'unchanged' and args.ci_start_time is not None:
                command.extend(['--ci-start-time', str(args.ci_start_time)])
            print(f'Qualifying complete {case} check', flush=True)
            child_code = run(command)
            measurement = json.loads(report.read_text())
            record['measurement'] = measurement
            if child_code or measurement.get('status') != 'passed' or not measurement.get('qualified'):
                record['status'] = 'failed'
                save()
                code = child_code or 1
                return code
            if package:
                verified = run([sys.executable, '-I', '-S', '.github/scripts/prepare-rebuild.py',
                                package, '--report', str(evidence), '--verify-check', str(report)])
                if verified:
                    code = verified
                    return code
            record['status'] = 'passed'
            save()
        summary['status'] = 'passed'
        code = 0
        return 0
    finally:
        if summary['status'] != 'passed':
            summary['status'] = 'failed'
            for record in summary['cases']:
                if record['status'] == 'running':
                    record['status'] = 'failed'
        usage = resource.getrusage(resource.RUSAGE_CHILDREN)
        summary.update(wall_seconds=time.monotonic() - started,
                       cpu_user_seconds=usage.ru_utime - usage_before.ru_utime,
                       cpu_system_seconds=usage.ru_stime - usage_before.ru_stime,
                       exit_code=code)
        save()


if __name__ == '__main__':
    raise SystemExit(main())
