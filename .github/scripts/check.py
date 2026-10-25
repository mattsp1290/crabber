#!/usr/bin/env python3
"""Run a full or partial check with a supervised deadline, defaulting to 600 seconds."""
import argparse
import ctypes
import json
import os
from pathlib import Path
import resource
import signal
import subprocess
import time
import tomllib


def identity(pid):
    try:
        data = Path(f'/proc/{pid}/stat').read_text()
        fields = data[data.rfind(')') + 2:].split()
        return (int(fields[1]), int(fields[19]), fields[0],
                int(fields[21]) * os.sysconf('SC_PAGE_SIZE'), int(fields[17]))
    except (OSError, ValueError, IndexError):
        return None


def collect(owned):
    # Follow only this launcher's descendants, including children adopted by its
    # subreaper. Reading every process on the host would add observer CPU cost.
    pending = [os.getpid()]
    current = {}
    visited = set()
    for pid, start in list(owned.items()):
        row = identity(pid)
        if row is not None and row[1] == start:
            current[pid] = row
            pending.append(pid)
        else:
            # Thousands of short-lived test processes must not become a growing
            # list of dead /proc lookups on every subsequent supervision poll.
            # Live escaped/adopted descendants retain their original identity.
            del owned[pid]
    while pending:
        parent = pending.pop()
        if parent in visited:
            continue
        visited.add(parent)
        try:
            tasks = list(Path(f'/proc/{parent}/task').iterdir())
        except OSError:
            continue
        for task in tasks:
            try:
                children = (task / 'children').read_text().split()
            except OSError:
                continue
            for child in children:
                pid = int(child)
                row = identity(pid)
                if row is not None:
                    current[pid] = row
                    owned[pid] = row[1]
                    pending.append(pid)
    return current


def send(pid, start, number):
    row = identity(pid)
    if row is not None and row[1] == start:
        try:
            os.kill(pid, number)
        except ProcessLookupError:
            pass


def persist(path, report):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(report, indent=2) + '\n')
    temporary.replace(path)


def cargo_profiles(cwd):
    manifest = Path(cwd) / 'Cargo.toml'
    if not manifest.exists():
        return {}
    try:
        return tomllib.loads(manifest.read_text()).get('profile', {})
    except (OSError, UnicodeError, tomllib.TOMLDecodeError) as error:
        # Preserve supervision/reporting when Cargo itself must reject a bad
        # manifest; this field is evidence, not another manifest validator.
        return {'unavailable': type(error).__name__}


def supervise(command, cwd, report_path, budget=600., reserve=20., elapsed=0., env=None):
    started = time.monotonic()
    initial_usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    initial_self_usage = resource.getrusage(resource.RUSAGE_SELF)
    child_env = dict(os.environ if env is None else env)
    report_path = Path(report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    if report_path.exists():
        raise FileExistsError('use a new check report path')
    report = {'version': 1, 'status': 'running', 'budget_seconds': budget,
              'cleanup_reserve_seconds': reserve, 'ci_elapsed_before_launcher_seconds': elapsed,
              'started_unix_seconds': time.time(), 'command': command,
              'source_revision': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=cwd, text=True).strip(),
              'source_clean': not subprocess.check_output(['git', 'status', '--porcelain'], cwd=cwd, text=True).strip(),
              'cpu_count': os.cpu_count(), 'platform': list(os.uname()),
              'peak_process_rss_sum_bytes': 0, 'peak_processes': 0, 'peak_threads': 0,
              'resource_scope': 'launcher and owned process descendants; PostgreSQL server is outside this process tree; shared RSS pages may be counted twice',
              'cpu_affinity_count': len(os.sched_getaffinity(0)),
              'physical_memory_bytes': os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES'),
              'cgroup_limits': {name: path.read_text().strip()
                                for name in ['cpu.max', 'memory.max', 'cpuset.cpus.effective']
                                if (path := Path('/sys/fs/cgroup') / name).is_file()},
              'profile': {k: child_env.get(k) for k in sorted(
                  {'CARGO_BUILD_JOBS', 'CARGO_INCREMENTAL',
                   'CARGO_PROFILE_DEV_DEBUG', 'CARGO_PROFILE_TEST_DEBUG'}
                  | {name for name in child_env if name.startswith('CARGO_PROFILE_')})},
              'cargo_profiles': cargo_profiles(cwd)}
    interrupted = None
    child = None
    owned = {}
    termination_started = None
    reason = None
    old_handlers = {}

    def interrupt(number, _frame):
        nonlocal interrupted
        interrupted = number

    if not Path('/proc/self/stat').exists():
        raise RuntimeError('complete-check supervision requires Linux /proc')
    # PR_SET_CHILD_SUBREAPER; reused child groups are never signalled by stale ID.
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), 'cannot become a child subreaper')
    for number in [signal.SIGINT, signal.SIGTERM]:
        old_handlers[number] = signal.signal(number, interrupt)
    persist(report_path, report)
    try:
        remaining = budget - elapsed
        child_env['CRABBER_CHECK_WORK_DEADLINE_NS'] = str(
            time.monotonic_ns() + max(0, int((remaining - reserve) * 1_000_000_000)))
        if remaining <= reserve:
            reason = 'budget-exhausted-before-cargo'
        else:
            child = subprocess.Popen(command, cwd=cwd, env=child_env, start_new_session=True)
        while child is not None:
            tree = collect(owned)
            code = child.poll()
            if code is not None:
                # Exit can adopt a helper forked after the previous snapshot.
                # Recollect after reaping the leader before declaring drained.
                tree = collect(owned)
            report['peak_process_rss_sum_bytes'] = max(report['peak_process_rss_sum_bytes'], sum(row[3] for row in tree.values()))
            report['peak_processes'] = max(report['peak_processes'], len(tree))
            report['peak_threads'] = max(report['peak_threads'], sum(row[4] for row in tree.values()))
            live = {pid: row for pid, row in tree.items()
                    if row[2] != 'Z' and not (pid == child.pid and code is not None)}
            total = elapsed + time.monotonic() - started
            if termination_started is None:
                if interrupted is not None:
                    reason = 'interrupted'
                elif total >= budget - reserve:
                    reason = 'budget-expired'
                elif code is not None and live:
                    reason = 'leaked-descendants'
                if reason:
                    termination_started = time.monotonic()
                    supervisors = []
                    for pid, row in live.items():
                        try:
                            # Cargo can replace xtask during the full schedule;
                            # the running inode then has Linux's deleted suffix.
                            executable = Path(f'/proc/{pid}/exe').resolve().name
                            if executable.removesuffix(' (deleted)') == 'xtask':
                                supervisors.append((pid, row[1]))
                        except OSError:
                            pass
                    # Once xtask exists, let its fixture/descendant cleanup run.
                    # Before that, terminate owned Cargo/compiler descendants.
                    targets = supervisors or [(pid, row[1]) for pid, row in live.items()]
                    for pid, start in targets:
                        send(pid, start, signal.SIGTERM)
            if termination_started is not None and (
                    total >= budget or time.monotonic() - termination_started >= reserve):
                for pid, row in live.items():
                    send(pid, row[1], signal.SIGKILL)
            # Cargo is reaped through Popen; reap adopted helper descendants too.
            for pid in tree:
                if pid != child.pid:
                    try:
                        os.waitpid(pid, os.WNOHANG)
                    except ChildProcessError:
                        pass
            if code is not None and not live:
                break
            # A 100ms sample bounds cancellation latency well inside the 20s
            # cleanup reserve without repeatedly scanning every Tokio thread.
            time.sleep(.1)
        code = child.wait() if child is not None else None
    except BaseException:
        # A reporting/spawn failure cannot leave an owned compiler/test running.
        if child is not None:
            for pid, row in collect(owned).items():
                send(pid, row[1], signal.SIGKILL)
            child.wait()
        reason = reason or 'launcher-error'
        raise
    finally:
        for number, previous in old_handlers.items():
            signal.signal(number, previous)
        wall = elapsed + time.monotonic() - started
        usage = resource.getrusage(resource.RUSAGE_CHILDREN)
        own_usage = resource.getrusage(resource.RUSAGE_SELF)
        own_user = own_usage.ru_utime - initial_self_usage.ru_utime
        own_system = own_usage.ru_stime - initial_self_usage.ru_stime
        report.update(status='passed' if reason is None and child is not None and child.returncode == 0 and wall <= budget else 'failed',
                      reason=reason, child_exit_code=child.returncode if child is not None else None,
                      wall_seconds=wall, launcher_wall_seconds=time.monotonic() - started,
                      ended_unix_seconds=time.time(), budget_breached=wall > budget,
                      interrupted_signal=interrupted, launcher_cpu_user_seconds=own_user, launcher_cpu_system_seconds=own_system,
                      cpu_user_seconds=usage.ru_utime - initial_usage.ru_utime + own_user,
                      cpu_system_seconds=usage.ru_stime - initial_usage.ru_stime + own_system)
        persist(report_path, report)
    if interrupted is not None:
        return 128 + interrupted
    if report['status'] != 'passed':
        return 124 if reason and reason.startswith('budget-') else (code if code and code > 0 else 1)
    return 0


def main():
    main_started = time.monotonic()
    main_started_epoch = time.time()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--report', type=Path)
    parser.add_argument('--part', choices=['quality-contracts', 'quality-artifacts'])
    parser.add_argument('--budget-seconds', type=int, default=600)
    parser.add_argument('--ci-start-time', type=float)
    parser.add_argument('--cache-state', choices=['unclassified', 'unchanged', 'runtime-rebuild', 'session-rebuild', 'bootstrap'], default='unclassified')
    args = parser.parse_args()
    if args.budget_seconds <= 20:
        parser.error('--budget-seconds must exceed the 20-second cleanup reserve')
    cwd = Path(__file__).resolve().parents[2]
    elapsed = 0.
    if args.ci_start_time is not None:
        elapsed = main_started_epoch - args.ci_start_time
        if not 0 <= elapsed <= 86400:
            parser.error('--ci-start-time must be a valid earlier timestamp from this CI invocation')
    report = args.report or cwd / 'target/test-performance' / f'complete-{time.time_ns()}.json'
    report = report if report.is_absolute() else (cwd / report).resolve()
    inner = report.with_name(report.stem + '-phases.json')
    env = dict(os.environ)
    # Keep incremental compilation at Cargo's local default. Debug symbols are
    # unnecessary for this gate; explicit caller settings remain authoritative.
    env.setdefault('CARGO_PROFILE_DEV_DEBUG', '0')
    env.setdefault('CARGO_PROFILE_TEST_DEBUG', '0')
    command = ['cargo', 'xtask', 'check']
    if args.part:
        command += ['--part', args.part]
    command += ['--timings', str(inner)]
    code = supervise(command, cwd, report, budget=args.budget_seconds, elapsed=elapsed + time.monotonic() - main_started, env=env)
    result = json.loads(report.read_text())
    result['cache_state'] = args.cache_state
    result['phases_report'] = str(inner)
    if code == 0:
        try:
            phases = json.loads(inner.read_text())
        except (OSError, ValueError):
            phases = {}
        expected_scope = args.part or 'complete'
        if (phases.get('coverage_scope') != expected_scope or phases.get('status') != 'passed'
                or (args.part and (phases.get('qualified') is not False
                                  or phases.get('processes_drained') is not True
                                  or phases.get('full_native_suite_executed') is not False
                                  or phases.get('owned_postgres_run') is not (args.part == 'quality-artifacts')))):
            result.update(status='failed', reason='partial-schedule-report-missing' if args.part else 'complete-schedule-report-missing')
            code = 1
    result['end_revision'] = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=cwd, text=True).strip()
    result['end_clean'] = not subprocess.check_output(['git', 'status', '--porcelain'], cwd=cwd, text=True).strip()
    result['launcher_wall_seconds'] = time.monotonic() - main_started
    result['wall_seconds'] = elapsed + result['launcher_wall_seconds']
    result['ended_unix_seconds'] = time.time()
    result['budget_breached'] = result['wall_seconds'] > args.budget_seconds
    result['timing_target_seconds'] = 600
    if result['budget_breached']:
        result.update(status='failed', reason='budget-expired')
        code = 124
    source_stable = result['source_clean'] and result['end_clean'] and result['end_revision'] == result['source_revision']
    if code == 0 and not source_stable:
        result.update(status='failed', reason='source-changed-or-dirty')
        code = 1
    result['coverage_scope'] = args.part or 'complete'
    result['timing_target_met'] = args.part is None and code == 0 and result['status'] == 'passed' and source_stable and result['wall_seconds'] <= result['timing_target_seconds']
    result['qualified'] = args.budget_seconds == result['timing_target_seconds'] and result['timing_target_met'] and args.cache_state in {'unchanged', 'runtime-rebuild', 'session-rebuild'}
    persist(report, result)
    label = f'Partial check ({args.part})' if args.part else 'Complete check'
    print(f"{label}: {result['status']} in {result['wall_seconds']:.3f}s ({args.budget_seconds}s supervision budget; cache state {args.cache_state})", flush=True)
    return code


if __name__ == '__main__':
    raise SystemExit(main())
