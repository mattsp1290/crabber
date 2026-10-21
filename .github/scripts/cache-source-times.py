#!/usr/bin/env python3
"""Preserve Cargo freshness for identical Git inputs across CI checkouts."""
import argparse
import json
import os
from pathlib import Path
import stat
import subprocess


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
    value = {'version': 1, 'workspace': str(root), 'files': tracked(root)}
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(value) + '\n')
    return len(value['files'])


def restore(root, source):
    if not source.exists():
        return 0
    saved = json.loads(source.read_text())
    if saved.get('version') != 1 or saved.get('workspace') != str(root):
        return 0
    restored = 0
    for name, current in tracked(root).items():
        previous = saved['files'].get(name)
        if not previous or any(previous.get(k) != current[k] for k in ['blob', 'mode']):
            continue
        path = root / name
        info = path.lstat()
        os.utime(path, ns=(info.st_atime_ns, previous['mtime_ns']), follow_symlinks=False)
        restored += 1
    return restored


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['capture', 'restore'])
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    destination = root / 'target/test-tools/source-times.json'
    count = (capture if args.action == 'capture' else restore)(root, destination)
    print(f'{args.action}: {count} identical tracked compiler inputs')


if __name__ == '__main__':
    main()
