#!/usr/bin/env python3
"""Measure one complete Linux gate without treating a failed run as a sample."""

import argparse
import datetime
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time


def processes():
    result = {}
    for entry in Path('/proc').iterdir():
        if not entry.name.isdecimal():
            continue
        try:
            data = (entry / 'stat').read_text()
            fields = data[data.rfind(')') + 2:].split()
            result[int(entry.name)] = {
                'parent': int(fields[1]), 'start': int(fields[19]),
                'rss': int(fields[21]) * os.sysconf('SC_PAGE_SIZE'),
                'threads': int(fields[17]),
            }
        except (OSError, ValueError, IndexError):
            continue
    return result


def descendants(snapshot, root, seen):
    owned = {pid for pid, record in snapshot.items()
             if seen.get(pid) == record['start']}
    if root in snapshot:
        owned.add(root)
    while True:
        added = {pid for pid, record in snapshot.items()
                 if record['parent'] in owned} - owned
        if not added:
            break
        owned.update(added)
    for pid in owned:
        seen[pid] = snapshot[pid]['start']
    return [snapshot[pid] for pid in owned]


def capture(argv, cwd):
    return subprocess.run(argv, cwd=cwd, check=True, text=True,
                          stdout=subprocess.PIPE, stderr=subprocess.DEVNULL).stdout.strip()


def write_report(path, report):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(report, indent=2) + '\n')
    os.replace(temporary, path)


def bytes_used(value):
    number, unit = re.fullmatch(r'([0-9.]+)([A-Za-z]+)', value.split(' / ')[0]).groups()
    units = {'B': 1, 'kB': 1000, 'MB': 1000**2, 'GB': 1000**3,
             'KiB': 1024, 'MiB': 1024**2, 'GiB': 1024**3}
    return int(float(number) * units[unit])


def sample_container(container, cwd, report):
    try:
        row = json.loads(capture(['docker', 'stats', '--no-stream', '--format', '{{json .}}', container], cwd))
        report['peak_container_memory_bytes'] = max(report.get('peak_container_memory_bytes', 0), bytes_used(row['MemUsage']))
        report['peak_container_cpu_percent'] = max(
            report.get('peak_container_cpu_percent', 0), float(row['CPUPerc'].removesuffix('%')))
        # The observer connects to the administrative database; subtract itself.
        count = int(capture(['docker', 'exec', container, 'psql', '-U', 'postgres', '-d', 'postgres', '-Atc',
                             "SELECT count(*)-1 FROM pg_stat_activity WHERE backend_type='client backend'"], cwd))
        report['peak_server_client_connections'] = max(report.get('peak_server_client_connections', 0), count)
    except (subprocess.CalledProcessError, ValueError, KeyError, AttributeError):
        report['container_observation_errors'] += 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cwd', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    if not command:
        parser.error('a command is required after --')
    cwd = args.cwd.resolve()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    report = {
        'status': 'running', 'started_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'revision': capture(['git', 'rev-parse', 'HEAD'], cwd),
        'clean': not capture(['git', 'status', '--porcelain'], cwd),
        'rustc': capture(['rustc', '-Vv'], cwd),
        'cargo': capture(['cargo', '-V'], cwd),
        'tools': {
            'nextest_installed': capture(['cargo', 'nextest', '--version'], cwd),
            'wasm_tools': capture(['wasm-tools', '--version'], cwd),
        },
        'hardware': {
            'architecture': os.uname().machine,
            'kernel': os.uname().release,
            'physical_memory_bytes': os.sysconf('SC_PHYS_PAGES') * os.sysconf('SC_PAGE_SIZE'),
            'load_average_at_start': list(os.getloadavg()),
            'memory_at_start': {
                line.split(':', 1)[0]: line.split(':', 1)[1].strip()
                for line in Path('/proc/meminfo').read_text().splitlines()
                if line.startswith(('MemTotal:', 'MemAvailable:', 'SwapTotal:', 'SwapFree:'))
            },
        },
        'command': [arg if '://' not in arg else '<endpoint>' for arg in command],
        'cpu_count': os.cpu_count(),
        'profile': {key: os.environ.get(key) for key in
                    ['CARGO_INCREMENTAL', 'CARGO_PROFILE_DEV_DEBUG', 'CARGO_PROFILE_TEST_DEBUG', 'CARGO_BUILD_JOBS']},
        'peak_aggregate_process_rss_bytes': 0, 'peak_processes': 0, 'peak_threads': 0,
        'container_observation_errors': 0,
        'measurement': 'sampled process-tree RSS sum (shared pages can be counted more than once); CPU from wait4; container observed separately',
    }
    path = output / 'summary.json'
    write_report(path, report)
    started = time.monotonic()
    seen = {}
    container = None
    last_container_sample = started
    interrupted = None
    with (output / 'raw.log').open('w') as raw:
        child = subprocess.Popen(command, cwd=cwd, stdout=raw, stderr=subprocess.STDOUT,
                                 start_new_session=True)

        def forward(signum, _frame):
            nonlocal interrupted
            interrupted = signum
            try:
                os.killpg(child.pid, signum)
            except ProcessLookupError:
                pass

        signal.signal(signal.SIGINT, forward)
        signal.signal(signal.SIGTERM, forward)
        while True:
            tree = descendants(processes(), child.pid, seen)
            report['peak_aggregate_process_rss_bytes'] = max(report['peak_aggregate_process_rss_bytes'], sum(p['rss'] for p in tree))
            report['peak_processes'] = max(report['peak_processes'], len(tree))
            report['peak_threads'] = max(report['peak_threads'], sum(p['threads'] for p in tree))
            if container is None:
                text = (output / 'raw.log').read_text(errors='replace')
                match = re.search(r'managed PostgreSQL container=([0-9a-f]{64}) owner=([0-9]+)-([0-9]+)', text)
                if match and int(match[2]) in seen:
                    candidate, owner = match[1], f'{match[2]}-{match[3]}'
                    try:
                        label = capture(['docker', 'inspect', '--format', '{{index .Config.Labels "crabber.test-runner"}}', candidate], cwd)
                    except (subprocess.CalledProcessError, FileNotFoundError):
                        report['container_observation_errors'] += 1
                        label = None
                    if label == owner:
                        container = candidate
            if container and time.monotonic() - last_container_sample >= 2:
                sample_container(container, cwd, report)
                last_container_sample = time.monotonic()
            reaped, status, usage = os.wait4(child.pid, os.WNOHANG)
            if reaped:
                child.returncode = os.waitstatus_to_exitcode(status)
                report.update(cpu_user_seconds=usage.ru_utime, cpu_system_seconds=usage.ru_stime,
                              major_page_faults=usage.ru_majflt, minor_page_faults=usage.ru_minflt,
                              block_input_operations=usage.ru_inblock,
                              block_output_operations=usage.ru_oublock)
                break
            report['elapsed_seconds'] = time.monotonic() - started
            write_report(path, report)
            time.sleep(.25)
    end_revision = capture(['git', 'rev-parse', 'HEAD'], cwd)
    end_clean = not capture(['git', 'status', '--porcelain'], cwd)
    unchanged = report['clean'] and end_clean and end_revision == report['revision']
    report.update(end_revision=end_revision, end_clean=end_clean, source_unchanged=unchanged)
    report['load_average_at_end'] = list(os.getloadavg())
    report.update(wall_seconds=time.monotonic() - started, exit_code=child.returncode,
                  interrupted_signal=interrupted,
                  status='passed' if child.returncode == 0 and interrupted is None and unchanged else 'failed')
    write_report(path, report)
    if interrupted is not None:
        return 128 + interrupted
    if child.returncode == 0 and not unchanged:
        return 1
    return child.returncode if child.returncode >= 0 else 128 - child.returncode


if __name__ == '__main__':
    raise SystemExit(main())
