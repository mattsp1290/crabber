#!/usr/bin/env python3
"""Remove obsolete workspace test executables after a terminal CI check."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import stat
import subprocess


def read_json(path):
    if path.is_symlink():
        raise ValueError('metadata must not be a symlink')
    return json.loads(path.read_text())


def within(root, path):
    path = Path(path)
    path = path if path.is_absolute() else root / path
    if path.is_symlink() or not path.resolve().is_relative_to(root):
        raise ValueError('path is outside the current workspace')
    return path.resolve()


def inventory(root, revision, directory, audit=None):
    """Bind native discovery to the supplied terminal run, never a latest glob."""
    qualification = directory / 'qualification'
    if qualification.exists():
        summary = read_json(qualification / 'summary.json')
        if summary['status'] not in {'passed', 'failed'} or summary['revision'] != revision:
            raise ValueError('qualification is not terminal at this revision')
        case = summary['cases'][-1]
        if case['case'] not in {'unchanged', 'runtime-rebuild', 'session-rebuild'}:
            raise ValueError('unknown qualification case')
        report = read_json(qualification / (case['case'] + '.json'))
        if report != case['measurement']:
            raise ValueError('qualification measurement does not match its report')
    else:
        report = read_json(directory / 'check.json')
    if (report['status'] not in {'passed', 'failed'}
            or report['source_revision'] != revision or report['end_revision'] != revision
            or not report['source_clean'] or not report['end_clean']
            or not report['ended_unix_seconds']):
        raise ValueError('check is not terminal for this clean revision')
    phases_path = within(root, report['phases_report'])
    if not phases_path.is_relative_to(directory):
        raise ValueError('phase report does not belong to the supplied run')
    if report['command'] != ['cargo', 'xtask', 'check', '--timings', str(phases_path)]:
        raise ValueError('launcher did not invoke this exact phase report')
    phases = read_json(phases_path)
    if (phases['coverage_scope'] != 'complete' or phases['status'] not in {'passed', 'failed'}
            or phases['source']['revision'] != revision
            or not phases['source']['tracked_source_clean']):
        raise ValueError('phase report has incompatible provenance')
    manifest_dir = within(root, phases['fixture_metrics']).parent
    if manifest_dir.parent != root / 'target/test-performance':
        raise ValueError('unexpected complete manifest directory')
    gates = phases['gates']
    compiled = [g for g in gates if g.get('argv', [])[:4] == ['cargo', 'nextest', 'list', '--workspace']
                and '--list-type' in g['argv'] and 'binaries-only' in g['argv']]
    listed = [g for g in gates if g.get('argv', [])[:4] == ['cargo', 'nextest', 'list', '--cargo-metadata']
              and '-E' not in g['argv']]
    coverage = [g for g in gates if g['id'] == 'required-coverage-and-features']
    if any(len(items) != 1 or items[0]['status'] != 'passed'
           for items in [compiled, listed, coverage]):
        raise ValueError('current native discovery and coverage did not complete')
    args = listed[0]['argv']
    metadata_path = within(root, args[args.index('--cargo-metadata') + 1])
    binaries_path = within(root, args[args.index('--binaries-metadata') + 1])
    if metadata_path != manifest_dir / 'cargo-metadata.json' or binaries_path != manifest_dir / 'binaries.json':
        raise ValueError('native discovery paths differ from the current fixture run')
    metadata = read_json(metadata_path)
    target = root / 'target'
    if Path(metadata['workspace_root']) != root or Path(metadata['target_directory']) != target:
        raise ValueError('Cargo metadata uses a different workspace or target')
    packages = {p['id']: p for p in metadata['packages'] if p['id'] in metadata['workspace_members']}
    binaries = read_json(binaries_path)['rust-binaries']
    if not binaries:
        raise ValueError('native binary inventory is empty')
    keep = set()
    for binary in binaries.values():
        package = packages[binary['package-id']]
        if not any(t['name'].replace('-', '_') == binary['binary-name'].replace('-', '_')
                   for t in package['targets']):
            raise ValueError('native binary is not a current workspace target')
        path = within(root, binary['binary-path'])
        if path.parent != target / 'debug/deps' or not path.is_file():
            raise ValueError('a current native binary is missing or outside the native target')
        keep.add(path)
    if audit is not None:
        audit.update(workspace_root=str(root), manifest_directory=str(manifest_dir),
                     inputs={str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                             for p in [phases_path, metadata_path, binaries_path]},
                     retained_paths=sorted(str(p.relative_to(root)) for p in keep))
    return target, packages.values(), keep


def candidates(target, packages, keep):
    """Recognize only current workspace package/target test fingerprints."""
    debug = target / 'debug'
    for directory in [target, debug, debug / 'deps', debug / '.fingerprint']:
        if directory.is_symlink():
            raise ValueError('target directories must not be symlinks')
    protected_inodes = {(p.stat().st_dev, p.stat().st_ino) for p in keep}
    for path in debug.iterdir():
        if path.is_file():
            info = path.stat()
            protected_inodes.add((info.st_dev, info.st_ino))
    result = []
    for package in packages:
        for fingerprint in (debug / '.fingerprint').glob(package['name'] + '-*'):
            if fingerprint.is_symlink() or not re.fullmatch(re.escape(package['name']) + r'-[0-9a-f]{16}', fingerprint.name):
                continue
            suffix = fingerprint.name.rsplit('-', 1)[1]
            for target_info in package['targets']:
                if not target_info['test']:
                    continue
                kinds = target_info['kind']
                label = 'integration-test' if kinds == ['test'] else kinds[0]
                if label not in {'integration-test', 'lib', 'bin'}:
                    continue
                name = target_info['name'].replace('-', '_')
                marker = fingerprint / ('test-' + label + '-' + name + '.json')
                # Cargo's normal-bin/library output is never a pruning candidate.
                if not marker.is_file() or marker.is_symlink() or any(fingerprint.glob('bin-*.json')) or any(fingerprint.glob('lib-*.json')):
                    continue
                value = read_json(marker)
                if not {'rustc', 'target', 'profile', 'deps'}.issubset(value):
                    continue
                executable = debug / 'deps' / (name + '-' + suffix)
                if executable in keep or executable.is_symlink() or not executable.is_file():
                    continue
                info = executable.stat()
                if not stat.S_ISREG(info.st_mode) or not info.st_mode & 0o111 or (info.st_dev, info.st_ino) in protected_inodes:
                    continue
                with executable.open('rb') as stream:
                    if stream.read(4) != b'\x7fELF':
                        continue
                result.append((executable, info.st_size))
    return sorted(set(result))


def remove_obsolete(result, obsolete):
    """Report actual removals even when maintenance stops on an I/O error."""
    for path, size in obsolete:
        try:
            path.unlink()
        except OSError as error:
            result.update(status='partial', reason=type(error).__name__ + ': ' + str(error))
            return
        result['removed_bytes'] += size
        result['removed_executables'].append(str(path))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-directory', required=True, type=Path)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    result = {'status': 'skipped', 'removed_bytes': 0, 'executables': [], 'removed_executables': []}
    try:
        directory = within(root, args.run_directory)
        if directory.parent != root / 'target/test-performance':
            raise ValueError('unexpected CI run directory')
        revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip()
        if subprocess.check_output(['git', 'status', '--porcelain'], cwd=root, text=True).strip():
            raise ValueError('checkout is not clean')
        target, packages, keep = inventory(root, revision, directory, result)
        obsolete = candidates(target, packages, keep)
        result.update(status='applied' if args.apply else 'dry-run', revision=revision,
                      retained_executables=len(keep), candidate_bytes=sum(n for _, n in obsolete),
                      executables=[str(p.relative_to(root)) for p, _ in obsolete])
        if args.apply:
            remove_obsolete(result, obsolete)
    except (OSError, ValueError, KeyError, IndexError, TypeError, subprocess.CalledProcessError) as error:
        result['reason'] = type(error).__name__ + ': ' + str(error)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
