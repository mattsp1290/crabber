#!/usr/bin/env python3
"""Record actual setup cache use without granting Native or Full credit."""
import argparse
import base64
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import stat
import subprocess

ROOT = Path(__file__).resolve().parents[2]
PROFILE = {'CARGO_BUILD_JOBS': '4', 'CARGO_INCREMENTAL': '0',
           'CARGO_PROFILE_DEV_DEBUG': '0', 'CARGO_PROFILE_TEST_DEBUG': '0',
           'CARGO_PROFILE_DEV_STRIP': 'symbols', 'CARGO_PROFILE_TEST_STRIP': 'symbols'}
MAX_INPUT = 8 * 1024 * 1024
MAX_MARKER = 1024 * 1024
MAX_RECEIPT = 20 * 1024 * 1024
CHECK_FIELDS = set('''version status budget_seconds cleanup_reserve_seconds ci_elapsed_before_launcher_seconds
started_unix_seconds command source_revision source_clean cpu_count platform peak_process_rss_sum_bytes
peak_processes peak_threads resource_scope cpu_affinity_count physical_memory_bytes cgroup_limits profile
cargo_profiles reason child_exit_code wall_seconds launcher_wall_seconds ended_unix_seconds budget_breached
interrupted_signal launcher_cpu_user_seconds launcher_cpu_system_seconds cpu_user_seconds cpu_system_seconds
cache_state phases_report end_revision end_clean timing_target_seconds coverage_scope timing_target_met qualified compiler_cache'''.split())
ASSOCIATION = 'Source-trusted restore and producer correlation; cache origin is not server-attested.'


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def exact(value, fields):
    require(isinstance(value, dict) and set(value) == set(fields), 'unexpected receipt fields')


def integer(value):
    return type(value) is int and value >= 0


def revision(value):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{40}', value)


def save(path, value, exclusive=False):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('x' if exclusive else 'w') as stream:
        stream.write(json.dumps(value, indent=2) + '\n')


def read(path, limit=MAX_RECEIPT):
    require(stat.S_ISREG(path.lstat().st_mode), 'receipt input must be a regular file')
    with path.open('rb') as stream:
        raw = stream.read(limit + 1)
    require(len(raw) <= limit, 'receipt input exceeds fixed bound')
    return raw


def capture(path, limit):
    if not path.exists() and not path.is_symlink():
        return {'kind': 'absent'}
    raw = read(path, limit)
    return {'kind': 'captured', 'raw_utf8': raw.decode('utf-8'), 'sha256': sha(raw)}


def captured(value, limit):
    require(isinstance(value, dict), 'missing captured input')
    if value.get('kind') == 'absent':
        exact(value, ['kind'])
        return None, None
    exact(value, ['kind', 'raw_utf8', 'sha256'])
    require(value['kind'] == 'captured' and isinstance(value['raw_utf8'], str), 'invalid captured input')
    raw = value['raw_utf8'].encode('utf-8')
    require(len(raw) <= limit and sha(raw) == value['sha256'], 'captured input digest mismatch')
    return raw, json.loads(value['raw_utf8'])


def marker(value):
    raw, parsed = captured(value, MAX_MARKER)
    if raw is None:
        return {'kind': 'absent'}
    exact(parsed, ['revision', 'complete', 'check'])
    require(revision(parsed['revision']) and type(parsed['complete']) is bool, 'invalid marker identity')
    check = parsed['check']
    require(isinstance(check, dict) and not set(check) - CHECK_FIELDS, 'unexpected marker check fields')
    require(type(check.get('version')) is int and check['version'] in [1, 2]
            and check.get('status') in ['passed', 'failed'], 'unfinished marker check')
    if check['version'] == 2:
        compiler_result(check.get('compiler_cache'))
    else:
        require('compiler_cache' not in check, 'unexpected legacy compiler cache field')
    require(revision(check.get('source_revision')) and revision(check.get('end_revision')), 'invalid marker source')
    if parsed['complete']:
        require(check['status'] == 'passed' and check.get('child_exit_code') == 0
                and check.get('reason') is None and check.get('interrupted_signal') is None
                and check['source_revision'] == check['end_revision'] == parsed['revision']
                and check.get('source_clean') is True and check.get('end_clean') is True
                and check.get('coverage_scope') == 'complete' and check.get('budget_breached') is False,
                'contradictory complete marker')
    return {'kind': 'complete' if parsed['complete'] else 'non-complete',
            'revision': parsed['revision'], 'check_status': check['status']}


def compiler_result(value, off_only=False):
    require(isinstance(value, dict) and type(value.get('version')) is int and value['version'] == 1,
            'invalid compiler-result report version')
    if value.get('mode') == 'off':
        exact(value, ['version', 'mode'])
        return
    require(not off_only and value.get('mode') == 'local', 'Native diagnostics require compiler-result caching off')
    exact(value, ['version', 'mode', 'namespace', 'restored', 'published', 'state', 'stats',
                  'cleanup_verified', 'error', 'restore_seconds', 'startup_seconds', 'cleanup_seconds'])
    require(isinstance(value['namespace'], str) and re.fullmatch('[0-9a-f]{64}', value['namespace']) is not None
            and value['state'] in ['closed', 'failed'] and type(value['cleanup_verified']) is bool,
            'invalid compiler-result lifecycle evidence')
    require(value['published'] is None or (isinstance(value['published'], str)
            and value['state'] == 'closed' and value['cleanup_verified']
            and re.fullmatch('[0-9a-f]{32}', value['published']) is not None), 'compiler snapshot published without cleanup')
    stats = value['stats']
    if stats is not None:
        counters = set('''compile_requests requests_unsupported_compiler requests_not_compile requests_not_cacheable
        requests_executed cache_timeouts cache_read_errors non_cacheable_compilations forced_recaches cache_write_errors
        cache_writes compilations compile_fails'''.split())
        exact(stats, counters | {'cache_errors', 'cache_hits', 'cache_misses', 'not_cached'})
        require(all(integer(stats[name]) for name in counters), 'invalid numeric invocation statistics')
        for name in ['cache_errors', 'cache_hits', 'cache_misses']:
            exact(stats[name], ['counts'])
            exact(stats[name]['counts'], ['Rust'])
            require(integer(stats[name]['counts']['Rust']), 'invalid Rust invocation statistics')
        exact(stats['not_cached'], ['crate-type', 'incremental', 'other'])
        require(all(integer(value) for value in stats['not_cached'].values()), 'invalid non-cacheable statistics')
    require(value['error'] is None or isinstance(value['error'], str), 'invalid compiler error')
    for name in ['restore_seconds', 'startup_seconds', 'cleanup_seconds']:
        require(type(value[name]) in [int, float] and math.isfinite(value[name]) and value[name] >= 0,
                'invalid compiler-cache duration')
    restored = value['restored']
    if restored is not None:
        exact(restored, ['snapshot', 'check_status', 'producer', 'bytes'])
        require(isinstance(restored['snapshot'], str) and re.fullmatch('[0-9a-f]{32}', restored['snapshot'])
                and restored['check_status'] in ['passed', 'failed'] and integer(restored['bytes']), 'invalid compiler restore')
        producer = restored['producer']
        exact(producer, ['source_revision', 'source_clean'])
        require(revision(producer['source_revision']) and type(producer['source_clean']) is bool,
                'invalid compiler snapshot producer')


def source_manifest(value):
    raw, parsed = captured(value, MAX_INPUT)
    if raw is None:
        return None, {}
    exact(parsed, ['version', 'workspace', 'files'])
    require(integer(parsed['version']) and isinstance(parsed['workspace'], str)
            and Path(parsed['workspace']).is_absolute() and isinstance(parsed['files'], dict), 'invalid source-time input')
    for name, item in parsed['files'].items():
        require(isinstance(name, str) and name not in ['', '.'] and str(PurePosixPath(name)) == name
                and not PurePosixPath(name).is_absolute()
                and '..' not in PurePosixPath(name).parts and isinstance(item, dict), 'invalid source-time path')
        require(set(item) <= {'blob', 'mode', 'mtime_ns'} and revision(item.get('blob'))
                and integer(item.get('mtime_ns')) and item.get('mode') in [None, '100644', '100755'],
                'invalid source-time record')
        require(parsed['version'] != 2 or item.get('mode') in ['100644', '100755'], 'missing source-time mode')
    return raw, parsed


def context(value):
    exact(value, ['repository', 'run', 'attempt', 'source', 'tree', 'workspace', 'runner_os',
                  'runner_arch', 'compiler_stdout_base64', 'compiler_sha256', 'profile', 'keys'])
    require(isinstance(value['repository'], str) and re.fullmatch('[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', value['repository']), 'invalid repository')
    require(integer(value['run']) and value['run'] > 0 and integer(value['attempt']) and value['attempt'] > 0,
            'invalid run identity')
    require(revision(value['source']) and revision(value['tree']) and isinstance(value['workspace'], str)
            and Path(value['workspace']).is_absolute(), 'invalid source context')
    require(value['runner_os'] == 'Linux' and value['runner_arch'] in ['X64', 'ARM64']
            and value['profile'] == PROFILE, 'incompatible runner or profile')
    require(isinstance(value['compiler_stdout_base64'], str)
            and len(value['compiler_stdout_base64']) <= 21848, 'invalid compiler encoding')
    raw = base64.b64decode(value['compiler_stdout_base64'], validate=True)
    require(0 < len(raw) <= 16384 and sha(raw) == value['compiler_sha256'], 'compiler bytes mismatch')
    text = raw.decode('utf-8')
    host = 'x86_64' if value['runner_arch'] == 'X64' else 'aarch64'
    require(text.startswith('rustc ') and '\nhost: ' + host + '-unknown-linux-gnu\n' in text,
            'compiler architecture mismatch')
    keys = value['keys'];exact(keys, ['base', 'graph', 'primary', 'graph_hash'])
    base = f"rust-{value['runner_os']}-{value['runner_arch']}-{value['compiler_sha256']}-dev0-test0-strip-symbols-inc0-combined-v2-"
    require(keys['base'] == base and isinstance(keys['graph_hash'], str)
            and re.fullmatch('[0-9a-f]{64}', keys['graph_hash']), 'incompatible cache base')
    require(keys['graph'] == base + keys['graph_hash'] + '-'
            and keys['primary'] == keys['graph'] + f"{value['source']}-{value['run']}-{value['attempt']}",
            'inconsistent cache keys')
    return raw


def refresh(value, source_input, expected):
    exact(value, ['version', 'status', 'workspace', 'source', 'tree', 'input_sha256', 'current_files',
                  'tracked_source_clean', 'reused_files', 'invalidated_files', 'changed_time_ns', 'max_invocation_ns'])
    require(type(value['version']) is int and value['version'] == 1 and value['status'] == 'completed'
            and value['tracked_source_clean'] is True
            and all(value[key] == expected[key] for key in ['workspace', 'source', 'tree']), 'unfinished or foreign refresh')
    raw, saved = source_manifest(source_input)
    require(value['input_sha256'] == (sha(raw) if raw is not None else None), 'refresh input changed after capture')
    current = value['current_files'];require(isinstance(current, dict), 'invalid refresh file map')
    reusable = saved.get('files', {}) if saved.get('version') == 2 and saved.get('workspace') == expected['workspace'] else {}
    count = 0
    for name, item in current.items():
        require(isinstance(name, str) and name not in ['', '.'] and str(PurePosixPath(name)) == name
                and not PurePosixPath(name).is_absolute()
                and '..' not in PurePosixPath(name).parts, 'invalid refresh path')
        exact(item, ['blob', 'mode']);require(revision(item['blob']) and item['mode'] in ['100644', '100755'], 'invalid refresh file')
        previous = reusable.get(name)
        count += bool(previous and all(previous.get(key) == item[key] for key in ['blob', 'mode']))
    require(all(integer(value[key]) for key in ['reused_files', 'invalidated_files', 'changed_time_ns', 'max_invocation_ns']),
            'invalid refresh counters')
    require(value['reused_files'] == count and value['invalidated_files'] == len(current) - count
            and value['changed_time_ns'] > value['max_invocation_ns'], 'contradictory refresh counters')


def admit(value, expected=None, compiler_text=None, requested=None):
    exact(value, ['version', 'status', 'context', 'request', 'result', 'action', 'inputs', 'marker',
                  'refresh', 'cache_state', 'producer_association'])
    require(type(value['version']) is int and value['version'] == 1 and value['status'] == 'finalized'
            and value['cache_state'] == 'unclassified' and value['producer_association'] == ASSOCIATION,
            'unfinished cache-use receipt')
    compiler = context(value['context'])
    if expected is not None:
        require(isinstance(expected, dict) and set(expected) <= set(value['context'])
                and all(value['context'][key] == item for key, item in expected.items()), 'cache-use context mismatch')
    if compiler_text is not None:
        require(compiler.decode('utf-8').strip() == compiler_text, 'bundle compiler identity mismatch')
    if requested is not None:
        require(requested in ['off', 'restore-compatible'] and value['request'] == requested,
                'cache request differs from caller policy')
    action = value['action'];exact(action, ['outcome', 'matched_key'])
    inputs = value['inputs'];exact(inputs, ['marker', 'source_times'])
    if value['result'] == 'disabled':
        require(value['request'] == 'off' and action == {'outcome': 'skipped', 'matched_key': ''}
                and inputs == {'marker': {'kind': 'not-consumed'}, 'source_times': {'kind': 'not-consumed'}}
                and value['marker'] == {'kind': 'not-consumed'} and value['refresh'] == {'kind': 'not-run'},
                'disabled receipt claims cache activity')
    else:
        require(value['request'] == 'restore-compatible' and action['outcome'] == 'success'
                and isinstance(action['matched_key'], str), 'restore did not succeed')
        require(value['result'] == ('restored' if action['matched_key'] else 'miss'), 'cache result contradicts matched key')
        if action['matched_key']:
            require(action['matched_key'].startswith(value['context']['keys']['base'])
                    and len(action['matched_key']) <= 512, 'incompatible matched key')
        require(value['marker'] == marker(inputs['marker']), 'marker projection mismatch')
        refresh(value['refresh'], inputs['source_times'], value['context'])
    require(len(json.dumps(value).encode()) <= MAX_RECEIPT, 'receipt exceeds fixed bound')
    return value


def run_context(root):
    value = {'repository': os.environ['GITHUB_REPOSITORY'], 'run': int(os.environ['GITHUB_RUN_ID']),
             'attempt': int(os.environ['GITHUB_RUN_ATTEMPT']), 'source': os.environ['GITHUB_SHA'],
             'tree': subprocess.check_output(['git', 'rev-parse', 'HEAD^{tree}'], cwd=root, text=True).strip(),
             'workspace': str(root), 'runner_os': os.environ['RUNNER_OS'], 'runner_arch': os.environ['RUNNER_ARCH'],
             'profile': {key: os.environ.get(key) for key in sorted(
                 set(PROFILE) | {name for name in os.environ if name.startswith('CARGO_PROFILE_')})}}
    require(subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip() == value['source']
            and not subprocess.check_output(['git', 'status', '--porcelain'], cwd=root, text=True).strip(), 'setup source differs')
    return value


def identity(root, compiler, keys):
    require(isinstance(compiler, Path), 'missing captured compiler output')
    raw = read(compiler, 16384)
    value = run_context(root) | {'compiler_stdout_base64': base64.b64encode(raw).decode('ascii'),
                                 'compiler_sha256': sha(raw), 'keys': keys}
    context(value)
    return value


def begin(root, directory, restore, compiler, keys):
    require(restore in ['true', 'false'], 'invalid restore request')
    value = {'phase': 'requested', 'context': identity(root, compiler, keys),
             'request': 'restore-compatible' if restore == 'true' else 'off'}
    save(directory / 'pending.json', value, exclusive=True)


def capture_restored(root, directory, outcome, matched):
    pending = json.loads(read(directory / 'pending.json'))
    exact(pending, ['phase', 'context', 'request'])
    require(pending['phase'] == 'requested', 'cache inputs already captured')
    require(pending['request'] in ['off', 'restore-compatible'], 'unknown cache request')
    context(pending['context'])
    if pending['request'] == 'off':
        require(outcome == 'skipped' and matched == '', 'disabled restore action conflict')
        inputs = {'marker': {'kind': 'not-consumed'}, 'source_times': {'kind': 'not-consumed'}}
    else:
        require(outcome == 'success', 'restore action failed')
        inputs = {'marker': capture(root / 'target/test-tools/complete-cache.json', MAX_MARKER),
                  'source_times': capture(root / 'target/test-tools/source-times.json', MAX_INPUT)}
        marker(inputs['marker']);source_manifest(inputs['source_times'])
    pending.update(phase='captured', action={'outcome': outcome, 'matched_key': matched}, inputs=inputs)
    save(directory / 'pending.json', pending)


def finish(root, directory):
    pending = json.loads(read(directory / 'pending.json'))
    exact(pending, ['phase', 'context', 'request', 'action', 'inputs'])
    require(pending['phase'] == 'captured', 'cache inputs were not captured')
    ctx = pending['context']
    disabled = pending['request'] == 'off'
    value = {'version': 1, 'status': 'finalized', 'context': ctx, 'request': pending['request'],
             'result': 'disabled' if disabled else ('restored' if pending['action']['matched_key'] else 'miss'),
             'action': pending['action'], 'inputs': pending['inputs'],
             'marker': {'kind': 'not-consumed'} if disabled else marker(pending['inputs']['marker']),
             'refresh': {'kind': 'not-run'} if disabled else json.loads(read(directory / 'source-refresh.json', MAX_INPUT)),
             'cache_state': 'unclassified', 'producer_association': ASSOCIATION}
    admit(value, run_context(root))
    save(directory / 'receipt.json', value, exclusive=True)
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['begin', 'capture', 'finish', 'validate'])
    parser.add_argument('--directory', type=Path, default=ROOT / 'target/test-performance/cache-use')
    parser.add_argument('--restore', choices=['true', 'false'])
    parser.add_argument('--compiler', type=Path)
    parser.add_argument('--base-key');parser.add_argument('--graph-key');parser.add_argument('--primary-key');parser.add_argument('--graph-hash')
    parser.add_argument('--action-outcome');parser.add_argument('--matched-key', default='')
    args = parser.parse_args()
    try:
        directory = args.directory.absolute()
        require(directory.is_relative_to(ROOT / 'target/test-performance') and directory.resolve() == directory,
                'receipt state must be outside restored paths')
        if args.action == 'begin':
            begin(ROOT, directory, args.restore, args.compiler,
                  {'base': args.base_key, 'graph': args.graph_key, 'primary': args.primary_key, 'graph_hash': args.graph_hash})
        elif args.action == 'capture':
            capture_restored(ROOT, directory, args.action_outcome, args.matched_key)
        elif args.action == 'finish':
            finish(ROOT, directory)
        else:
            admit(json.loads(read(directory / 'receipt.json')))
    except (ValueError, TypeError, KeyError, OSError, subprocess.CalledProcessError):
        parser.exit(1, 'Cache-use receipt rejected; inspect source-bound setup inputs.\n')


if __name__ == '__main__':
    main()
