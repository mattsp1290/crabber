#!/usr/bin/env python3
"""Invalidate an affected workspace package in every retained owned Cargo target."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time


def read(path):
    return json.loads(Path(path).read_text())


def owned(root, path):
    absolute = path.absolute()
    if absolute.is_relative_to(root):
        current = absolute
        while current != root:
            if current.is_symlink():
                raise ValueError(f'qualification target must not traverse a symlink: {path}')
            current = current.parent
    resolved = path.resolve()
    if not resolved.is_relative_to(root):
        raise ValueError(f'qualification target is outside this checkout: {path}')
    return resolved


def all_fingerprints(target):
    # Nested probe targets retain separate ownership and compiler graphs.
    return [p for pattern in ['*/.fingerprint/*', '*/*/.fingerprint/*']
            for p in target.glob(pattern) if p.is_dir()]


def fingerprints(target, package):
    return sorted(p for p in all_fingerprints(target) if p.name.startswith(package + '-'))


def current_package(target, package, source):
    result = []
    module = package.replace('-', '_')
    for fingerprint in fingerprints(target, package):
        suffix = fingerprint.name.removeprefix(package + '-')
        profile = fingerprint.parent.parent
        dependency = profile / 'deps' / f'{module}-{suffix}.d'
        if not dependency.exists():
            continue
        text = dependency.read_text(errors='replace')
        absolute = str(source).replace(' ', '\\ ') + '/'
        relative = str(source.relative_to(source.parents[2])).replace(' ', '\\ ') + '/'
        if absolute in text or relative in text:
            result.append(fingerprint)
    return result


def stamps(paths):
    return {str(p): [p.stat().st_size, p.stat().st_mtime_ns]
            for directory in paths for p in directory.glob('*.json')}


def dependent_stamps(target, package):
    module = package.replace('-', '_')
    result = {}
    for directory in all_fingerprints(target):
        for path in directory.glob('*.json'):
            value = read(path)
            if any(dependency[1] == module for dependency in value.get('deps', [])):
                result[str(path)] = [path.stat().st_size, path.stat().st_mtime_ns]
    return result


def manifest_for(root, target, main):
    if target == main:
        return root / 'Cargo.toml'
    local = target.parent / 'source/Cargo.toml'
    if local.is_file():
        return local
    if 'consumer-probes' in target.parts and target.parent.name == 'custom-http':
        return root / 'testdata/custom-http-feature-probe/Cargo.toml'
    parent = target.parent / 'Cargo.toml'
    if parent.is_file():
        return parent
    raise ValueError(f'cannot identify the retained manifest for {target}')


def inventory(root, main, package):
    # Complete checks route standalone native consumers into the main target.
    # Local/API probes retain explicit child targets; WASM guests have one
    # explicit target. Obsolete default consumer targets are not retained.
    bases = [main, root / 'fixtures/wasm/target']
    targets = {owned(root, main)}
    for base in bases:
        if base.exists():
            targets.update(owned(root, p.parent) for p in base.rglob('.rustc_info.json'))
    rows = []
    source = root / 'crates' / package / 'src'
    for target in sorted(targets):
        affected = current_package(target, package, source)
        if not affected:
            continue
        manifest = owned(root, manifest_for(root, target, main))
        # These dependencies are registry packages in the retained manifests.
        # Their metadata must survive package-scoped cleanup unchanged.
        dependencies = []
        for name in ['tokio', 'serde', 'sqlx', 'wasmtime', 'reqwest']:
            dependencies += fingerprints(target, name)
        before = stamps(affected)
        dependents = dependent_stamps(target, package)
        if not dependents:
            raise ValueError(f'no declared dependent artifacts were found in {target}')
        rows.append({'target': str(target), 'manifest': str(manifest),
                     'affected_before': before, 'dependency_before': stamps(dependencies),
                     'dependents_before': dependents})
    if not rows:
        raise ValueError(f'no current-checkout {package} artifacts were found')
    return rows


def save(path, report):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(report, indent=2) + '\n')
    temporary.replace(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('package', choices=['crabber-runtime', 'crabber-session'])
    parser.add_argument('--report', required=True, type=Path)
    parser.add_argument('--verify-check', type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip()
    source = root / 'crates' / args.package / 'src'
    if args.verify_check:
        report = read(args.report)
        check = read(args.verify_check)
        if report['revision'] != revision or report['package'] != args.package:
            raise ValueError('rebuild evidence does not match the current source/package')
        if check['status'] != 'passed' or check['source_revision'] != revision:
            raise ValueError('the complete required rebuild check did not pass this revision')
        for row in report['targets']:
            target = owned(root, Path(row['target']))
            affected = stamps(current_package(target, args.package, source))
            if not affected:
                raise ValueError(f'affected package was not rebuilt in {target}')
            dependents = dependent_stamps(target, args.package)
            changed = {p: value for p, value in dependents.items()
                       if row['dependents_before'].get(p) != value}
            if not changed:
                raise ValueError(f'no dependent artifact rebuild was observed in {target}')
            for name, previous in row['dependency_before'].items():
                path = Path(name)
                if not path.exists() or [path.stat().st_size, path.stat().st_mtime_ns] != previous:
                    raise ValueError(f'third-party dependency was unnecessarily rebuilt: {path}')
            row.update(affected_rebuilt=affected, changed_dependent_fingerprints=changed)
        report.update(status='verified', completed_check=str(args.verify_check))
        save(args.report, report)
        print(f'Verified {args.package} and dependent rebuilding in {len(report["targets"])} owned targets')
        return
    if args.report.exists():
        raise FileExistsError('use a new rebuild evidence path')
    main_target = Path(os.environ.get('CARGO_TARGET_DIR', str(root / 'target')))
    main_target = owned(root, main_target if main_target.is_absolute() else root / main_target)
    report = {'version': 2, 'status': 'preparing', 'revision': revision,
              'package': args.package, 'started_unix_seconds': time.time(),
              'targets': inventory(root, main_target, args.package)}
    save(args.report, report)
    for row in report['targets']:
        command = ['cargo', 'clean', '--offline', '--locked', '--manifest-path', row['manifest'],
                   '--target-dir', row['target'], '-p', args.package]
        result = subprocess.run(command, cwd=root)
        if result.returncode:
            raise SystemExit(result.returncode)
        affected = current_package(Path(row['target']), args.package, source)
        if affected:
            raise ValueError(f'package cleanup left current-source artifacts in {row["target"]}')
        for name, previous in row['dependency_before'].items():
            path = Path(name)
            if not path.exists() or [path.stat().st_size, path.stat().st_mtime_ns] != previous:
                raise ValueError(f'package cleanup invalidated a third-party dependency: {path}')
        row['affected_removed'] = sorted(row['affected_before'])
    report.update(status='prepared', prepared_unix_seconds=time.time())
    save(args.report, report)
    print(f'Prepared {args.package} rebuild in {len(report["targets"])} owned targets')


if __name__ == '__main__':
    main()
