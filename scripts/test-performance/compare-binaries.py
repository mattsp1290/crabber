#!/usr/bin/env python3
"""Compare exact Rust tests in frozen binaries, excluding Cargo compilation."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import signal
import statistics
import subprocess
import time


def digest(path):
    with path.open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


def save():
    temporary = args.output / 'summary.tmp'
    temporary.write_text(json.dumps(report, indent=2) + '\n')
    temporary.replace(args.output / 'summary.json')


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--baseline', type=Path, required=True)
parser.add_argument('--candidate', type=Path, required=True)
parser.add_argument('--test', action='append', required=True)
parser.add_argument('--runs', type=int, default=5)
parser.add_argument('--timeout', type=float, default=180)
parser.add_argument('--output', type=Path, required=True)
args = parser.parse_args()
if args.runs < 1 or args.timeout <= 0 or len(set(args.test)) != len(args.test):
    parser.error('runs and timeout must be positive; test names must be unique')
args.output = args.output.resolve()
args.output.mkdir(parents=True, exist_ok=False)
report = {'status': 'running', 'binaries': {}, 'tests': args.test,
          'pairs': args.runs, 'affinity': sorted(os.sched_getaffinity(0)),
          'load_average_at_start': list(os.getloadavg()), 'runs': [],
          'passed_tests': 0, 'failed_tests': 0, 'errors': 0,
          'scope': 'serial exact tests in frozen binaries; excludes compilation and full-suite contention'}
exit_code = 1
save()
try:
    for variant in ['baseline', 'candidate']:
        binary = getattr(args, variant).resolve(strict=True)
        report['binaries'][variant] = {'path': str(binary), 'sha256': digest(binary)}
    save()
    for pair in range(args.runs):
        order = ['baseline', 'candidate'] if pair % 2 == 0 else ['candidate', 'baseline']
        for variant in order:
            for index, test in enumerate(args.test):
                binary = report['binaries'][variant]
                if digest(Path(binary['path'])) != binary['sha256']:
                    raise RuntimeError(f'{variant} binary changed before execution')
                log = args.output / f'{pair + 1:02}-{variant}-{index + 1:02}.log'
                row = {'pair': pair + 1, 'variant': variant, 'test': test,
                       'log': log.name, 'status': 'running'}
                report['runs'].append(row)
                save()
                before = resource.getrusage(resource.RUSAGE_CHILDREN)
                started = time.perf_counter()
                with log.open('w') as output:
                    child = subprocess.Popen([binary['path'], '--exact', test, '--test-threads=1',
                                              '--nocapture', '--color=never'], stdout=output,
                                             stderr=subprocess.STDOUT, start_new_session=True)
                    try:
                        code = child.wait(timeout=args.timeout)
                    except BaseException:
                        try:
                            os.killpg(child.pid, signal.SIGTERM)
                        except ProcessLookupError:
                            pass
                        try:
                            child.wait(timeout=3)
                        except subprocess.TimeoutExpired:
                            pass
                        try:
                            os.killpg(child.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                        child.wait()
                        raise
                after = resource.getrusage(resource.RUSAGE_CHILDREN)
                row.update(wall_seconds=time.perf_counter() - started, exit_code=code,
                           user_seconds=after.ru_utime - before.ru_utime,
                           system_seconds=after.ru_stime - before.ru_stime)
                text = log.read_text(errors='replace')
                summary = re.search(r'test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured;', text)
                counts = tuple(map(int, summary.groups())) if summary else (0, 0, 0, 0)
                row.update(passed=counts[0], failed=counts[1], ignored=counts[2], measured=counts[3])
                report['passed_tests'] += counts[0]
                report['failed_tests'] += counts[1]
                unchanged = digest(Path(binary['path'])) == binary['sha256']
                good = code == 0 and counts == (1, 0, 0, 0) and unchanged and 'skipping PostgreSQL test:' not in text and 'SKIPPED:' not in text
                row['status'] = 'passed' if good else 'failed'
                save()
                if not good:
                    raise RuntimeError(f'{variant} test failed, skipped, selected incorrectly, or binary changed; see {log.name}')
    report['summary'] = {}
    for variant in ['baseline', 'candidate']:
        samples = [sum(row['wall_seconds'] for row in report['runs']
                       if row['variant'] == variant and row['pair'] == pair + 1)
                   for pair in range(args.runs)]
        report['summary'][variant] = {'median_seconds': statistics.median(samples),
                                      'min_seconds': min(samples), 'max_seconds': max(samples),
                                      'samples_seconds': samples}
    report['status'] = 'passed'
    exit_code = 0
except BaseException as error:
    report.update(status='failed', error=f'{type(error).__name__}: {error}', errors=report['errors'] + 1)
    if report['runs'] and report['runs'][-1]['status'] == 'running':
        report['runs'][-1]['status'] = 'failed'
finally:
    report['load_average_at_end'] = list(os.getloadavg())
    save()
print(json.dumps({key: report.get(key) for key in ['status', 'passed_tests', 'failed_tests', 'errors', 'summary']}), flush=True)
raise SystemExit(exit_code)
