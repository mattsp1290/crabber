#!/usr/bin/env python3
"""Preserve Cargo freshness for identical Git inputs across CI checkouts."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import time


def tracked(root):
    # The index already identifies file contents. Never reuse timestamps for
    # an unstaged edit, a changed blob/mode, a symlink, or a different checkout
    # path. Cargo still checks its own compiler, feature and dependency inputs.
    subprocess.run(['git', 'update-index', '--refresh'], cwd=root, check=True, capture_output=True)
    subprocess.run(['git', 'diff-files', '--quiet'], cwd=root, check=True)
    output = subprocess.check_output(['git', 'ls-files', '--stage', '-z'], cwd=root)
    result = {}
    for entry in output.split(b'\0'):
        if not entry:
            continue
        metadata, name = entry.split(b'\t', 1)
        mode, blob, stage = metadata.decode().split()
        if stage != '0':
            raise ValueError('unresolved index entries cannot populate a source cache')
        name = os.fsdecode(name)
        path = root / name
        info = path.lstat()
        if stat.S_ISREG(info.st_mode) and mode in {'100644', '100755'}:
            result[name] = {'blob': blob, 'mode': mode, 'mtime_ns': info.st_mtime_ns}
    return result


def capture(root, destination):
    value = {'version': 2, 'workspace': str(root), 'files': tracked(root)}
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(value) + '\n')
    return len(value['files'])


def restore(root, source, report=None):
    if report is not None and (report.exists() or report.is_symlink()):
        raise FileExistsError('use a new source-time report path')
    if report is not None:
        revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip()
        tree = subprocess.check_output(['git', 'rev-parse', 'HEAD^{tree}'], cwd=root, text=True).strip()
    raw = source.read_bytes() if source.exists() else None
    saved = json.loads(raw.decode()) if raw is not None else {}
    files = saved.get('files', {}) if saved.get('version') == 2 and saved.get('workspace') == str(root) else {}
    # A compatible fallback can have been built after this job's checkout.
    # Unknown/changed inputs must be newer than every restored invocation,
    # including scoped probes and WASM targets, regardless of clock skew.
    changed_time = time.time_ns()
    max_invocation = 0
    for target in [root / 'target', root / 'fixtures/wasm/target']:
        for invoked in target.glob('**/.fingerprint/*/invoked.timestamp'):
            max_invocation = max(max_invocation, invoked.stat().st_mtime_ns)
    changed_time = max(changed_time, max_invocation + 1)
    restored = 0
    current_files = tracked(root)
    for name, current in current_files.items():
        previous = files.get(name)
        path = root / name
        info = path.lstat()
        if previous and all(previous.get(k) == current[k] for k in ['blob', 'mode']):
            timestamp = previous['mtime_ns']
            restored += 1
        else:
            timestamp = changed_time
        os.utime(path, ns=(info.st_atime_ns, timestamp), follow_symlinks=False)
    if report is not None:
        value = {'version': 1, 'status': 'completed', 'workspace': str(root),
                 'source': revision, 'tree': tree,
                 'tracked_source_clean': not subprocess.check_output(['git', 'status', '--porcelain'], cwd=root, text=True).strip(),
                 'input_sha256': hashlib.sha256(raw).hexdigest() if raw is not None else None,
                 'current_files': {name: {key: item[key] for key in ['blob', 'mode']}
                                   for name, item in current_files.items()},
                 'reused_files': restored, 'invalidated_files': len(current_files) - restored,
                 'changed_time_ns': changed_time, 'max_invocation_ns': max_invocation}
        report.parent.mkdir(parents=True, exist_ok=True)
        with report.open('x') as stream:
            stream.write(json.dumps(value) + '\n')
    return restored


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['capture', 'restore'])
    parser.add_argument('--report', type=Path)
    args = parser.parse_args()
    if args.report is not None and args.action != 'restore':
        parser.error('--report applies only to restore')
    root = Path(__file__).resolve().parents[2]
    destination = root / 'target/test-tools/source-times.json'
    count = (capture(root, destination) if args.action == 'capture'
             else restore(root, destination, args.report))
    print(f'{args.action}: {count} identical tracked compiler inputs')


if __name__ == '__main__':
    main()
