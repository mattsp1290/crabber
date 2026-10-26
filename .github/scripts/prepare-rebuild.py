#!/usr/bin/env python3
"""Invalidate an affected workspace package in every retained owned Cargo target."""
import argparse
import hashlib
import math
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


def clean_target_arguments(target, affected_before):
    arguments = set()
    for name in affected_before:
        parts = owned(target, Path(name)).relative_to(target).parts
        if len(parts) == 4 and parts[1] == '.fingerprint':
            arguments.add(())
        elif len(parts) == 5 and parts[2] == '.fingerprint':
            arguments.add(('--target', parts[0]))
        else:
            raise ValueError(f'cannot identify the retained Cargo target for {name}')
    if not arguments:
        raise ValueError('affected compiler fingerprints are required for cleanup')
    return sorted(arguments)


def dependent_stamps(target, package):
    module = package.replace('-', '_')
    result = {}
    for directory in all_fingerprints(target):
        for path in directory.glob('*.json'):
            value = read(path)
            if any(dependency[1] == module for dependency in value.get('deps', [])):
                result[str(path)] = [path.stat().st_size, path.stat().st_mtime_ns]
    return result


def legacy_manifest(root, target, main):
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


def artifact_key(artifact):
    if set(artifact) != {'package_id', 'target', 'profile', 'features', 'fresh', 'filenames'}:
        raise ValueError('unexpected consumer compiler fields')
    if not isinstance(artifact['package_id'], str) or type(artifact['fresh']) is not bool:
        raise ValueError('invalid consumer compiler identity')
    target = artifact['target']
    if set(target) != {'name', 'kind', 'src_path'} or not all(
            isinstance(target[name], str) for name in ['name', 'src_path']):
        raise ValueError('invalid consumer compiler target')
    if not Path(target['src_path']).is_absolute():
        raise ValueError('consumer compiler source must be absolute')
    for name, values in [('kind', target['kind']), ('features', artifact['features']),
                         ('filenames', artifact['filenames'])]:
        if (not isinstance(values, list) or not all(isinstance(v, str) for v in values)
                or (not values and name != 'features')):
            raise ValueError('invalid consumer compiler list')
    profile = artifact['profile']
    if set(profile) != {'opt_level', 'debuginfo', 'debug_assertions', 'overflow_checks', 'test'}:
        raise ValueError('invalid consumer compiler profile')
    if profile['opt_level'] not in ['0', '1', '2', '3', 's', 'z']:
        raise ValueError('invalid consumer optimization level')
    debug = profile['debuginfo']
    if not ((type(debug) is int and 0 <= debug <= 2)
            or debug in ['line-directives-only', 'line-tables-only']):
        raise ValueError('invalid consumer debug level')
    if not all(type(profile[name]) is bool for name in ['debug_assertions', 'overflow_checks', 'test']):
        raise ValueError('invalid consumer compiler profile flags')
    return json.dumps({name: value for name, value in artifact.items() if name != 'fresh'}, sort_keys=True)


def compiler_receipt(root, target, owner):
    manifest = owned(root, Path(owner['manifest']))
    receipt = read(owned(root, Path(owner['receipt'])))
    if set(receipt) != {'version', 'manifest', 'target', 'gate', 'manifest_sha256',
                        'completed_unix_seconds', 'artifacts'} or (type(receipt['version']) is not int or receipt['version'] != 1):
        raise ValueError('invalid consumer compiler receipt')
    if (owned(root, Path(receipt['manifest'])) != manifest
            or owned(root, Path(receipt['target'])) != target or receipt['gate'] != owner['gate']
            or receipt['manifest_sha256'] != hashlib.sha256(manifest.read_bytes()).hexdigest()):
        raise ValueError('consumer compiler receipt does not match its owner/cache')
    completed = receipt['completed_unix_seconds']
    if type(completed) not in (int, float) or not math.isfinite(completed):
        raise ValueError('invalid consumer compiler completion time')
    artifacts = receipt['artifacts']
    if not isinstance(artifacts, list) or not artifacts:
        raise ValueError('consumer compiler artifacts are missing')
    keys = set()
    for artifact in artifacts:
        key = artifact_key(artifact)
        if key in keys:
            raise ValueError('duplicate consumer compiler artifact')
        keys.add(key)
        if not all(Path(file).is_absolute() and owned(root, Path(file)).is_relative_to(target) and Path(file).exists()
                   for file in artifact['filenames']):
            raise ValueError('consumer compiler artifact escaped its cache')
    if not any(Path(a['target']['src_path']).is_relative_to(manifest.parent) for a in artifacts):
        raise ValueError('consumer root compiler artifact is missing')
    return receipt


def shared_probe_target(target):
    return target.name == 'shared-target' and target.parent.parent.name == 'local-probes'


def owners_for(root, target, main):
    if not shared_probe_target(target):
        return [{'manifest': str(owned(root, legacy_manifest(root, target, main)))}]
    cache = target.parent
    marker = read(owned(root, cache / 'owners.json'))
    if set(marker) != {'version', 'checkout', 'owners'} or (type(marker['version']) is not int or marker['version'] != 1):
        raise ValueError('invalid shared consumer cache ownership')
    if owned(root, Path(marker['checkout'])) != root or not isinstance(marker['owners'], dict) or not marker['owners']:
        raise ValueError('shared consumer cache belongs to a different checkout')
    owners = []
    for name, entry in sorted(marker['owners'].items()):
        if (not name or Path(name).name != name or name in {'.', '..'} or set(entry) != {'manifest', 'gate', 'receipt'}
                or entry != {'manifest': f'{name}/source/Cargo.toml', 'gate': f'local-probe-{name}',
                             'receipt': f'{name}/compiler.json'}):
            raise ValueError('invalid shared consumer owner identity')
        owner = {'manifest': str(owned(root, cache / entry['manifest'])), 'gate': entry['gate'],
                 'receipt': str(owned(root, cache / entry['receipt']))}
        owner['compiler_before'] = compiler_receipt(root, target, owner)
        owners.append(owner)
    return owners


def verify_consumer_compilation(root, row, package, started, finished):
    target = owned(root, Path(row['target']))
    source = root / 'crates' / package / 'src'
    rebuilt = set()
    required = set()
    for owner in row['owners']:
        before = owner['compiler_before']
        after = compiler_receipt(root, target, owner)
        if not (started <= after['completed_unix_seconds'] <= finished
                and after['completed_unix_seconds'] > before['completed_unix_seconds']):
            raise ValueError('consumer compiler receipt was not produced by the rebuild check')
        if {artifact_key(a) for a in before['artifacts']} != {artifact_key(a) for a in after['artifacts']}:
            raise ValueError('consumer compiler graph changed during package invalidation')
        affected = [a for a in before['artifacts'] if Path(a['target']['src_path']).is_relative_to(source)]
        required.update(artifact_key(a) for a in affected)
        rebuilt.update(artifact_key(a) for a in after['artifacts'] if not a['fresh'])
        if affected:
            manifest = Path(owner['manifest'])
            roots = [a for a in after['artifacts'] if Path(a['target']['src_path']).is_relative_to(manifest.parent)]
            if not any(not a['fresh'] for a in roots):
                raise ValueError(f'affected consumer root was not rebuilt: {manifest}')
        owner['compiler_after'] = after
    if not required.issubset(rebuilt):
        raise ValueError('an affected shared compiler variant was not rebuilt')


def verify_consumer_gates(root, row, check, phases, package):
    if (phases.get('coverage_scope') != 'complete' or phases.get('status') != 'passed'
            or phases.get('exit_code') != 0 or phases['source']['revision'] != check['source_revision']):
        raise ValueError('consumer rebuild needs the complete passing phase report')
    target = owned(root, Path(row['target']))
    gates = [g for g in phases['gates'] if g['id'].startswith('local-probe-')]
    expected = {owner['gate']: owner for owner in row['owners']}
    if (len(expected) != len(row['owners']) or len(gates) != len(expected)
            or {g['id'] for g in gates} != set(expected)):
        raise ValueError('complete consumer gates differ from prepared cache owners')
    for gate in gates:
        owner = expected[gate['id']]
        argv = gate.get('argv', [])
        if (gate.get('status') != 'passed' or gate.get('exit_code') != 0
                or owned(root, root / gate['working_directory']) != Path(owner['manifest']).parent
                or len(argv) != 7 or argv[:4] != ['cargo', 'check', '--all-targets', '--target-dir']
                or argv[5:] != ['--message-format', 'json-render-diagnostics']
                or owned(root, Path(argv[4])) != target
                or 'CARGO_BUILD_BUILD_DIR' not in gate.get('environment_names', [])
                or gate.get('kind') != 'captured-command'):
            raise ValueError('consumer gate did not check its prepared manifest/cache')
    verify_consumer_compilation(root, row, package,
                               phases['started_unix_seconds'],
                               phases['started_unix_seconds'] + phases['wall_seconds'] + 1)


def inventory(root, main, package):
    # Complete checks route standalone native consumers into the main target.
    # Local/API probes retain explicit child targets; WASM guests have one
    # explicit target. Obsolete default consumer targets are not retained.
    bases = [main, root / 'fixtures/wasm/target']
    targets = {owned(root, main)}
    for base in bases:
        if base.exists():
            targets.update(owned(root, p.parent) for p in base.rglob('.rustc_info.json'))
            targets.update(owned(root, target)
                           for target in base.glob('local-probes/*/shared-target'))
    rows = []
    source = root / 'crates' / package / 'src'
    for target in sorted(targets):
        affected = current_package(target, package, source)
        if not affected:
            continue
        owners = owners_for(root, target, main)
        # These dependencies are registry packages in the retained manifests.
        # Their metadata must survive package-scoped cleanup unchanged.
        dependencies = []
        for name in ['tokio', 'serde', 'sqlx', 'wasmtime', 'reqwest']:
            dependencies += fingerprints(target, name)
        before = stamps(affected)
        dependents = dependent_stamps(target, package)
        if not dependents:
            raise ValueError(f'no declared dependent artifacts were found in {target}')
        rows.append({'target': str(target), 'owners': owners,
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
    main_target = Path(os.environ.get('CARGO_TARGET_DIR', str(root / 'target')))
    main_target = owned(root, main_target if main_target.is_absolute() else root / main_target)
    if args.verify_check:
        report = read(args.report)
        check = read(args.verify_check)
        if (report.get('version') != 3 or report.get('status') not in ['prepared', 'verified']
                or not isinstance(report.get('targets'), list) or not report['targets']):
            raise ValueError('complete prepared rebuild inventory required')
        if report['revision'] != revision or report['package'] != args.package:
            raise ValueError('rebuild evidence does not match the current source/package')
        if (check['status'] != 'passed' or check['source_revision'] != revision
                or check['started_unix_seconds'] < report['prepared_unix_seconds']):
            raise ValueError('the complete required rebuild check did not pass this revision')
        phases = read(check['phases_report'])
        if (phases.get('coverage_scope') != 'complete' or phases.get('status') != 'passed'
                or phases.get('exit_code') != 0 or phases['source']['revision'] != revision):
            raise ValueError('rebuild verification requires the complete passing phase report')
        for row in report['targets']:
            target = owned(root, Path(row['target']))
            owners = row.get('owners')
            if shared_probe_target(target):
                required = {'manifest', 'gate', 'receipt', 'compiler_before'}
                if (not isinstance(owners, list) or not owners
                        or any(not isinstance(owner, dict) or not required.issubset(owner)
                               for owner in owners)):
                    raise ValueError('complete shared consumer owners required')
                verify_consumer_gates(root, row, check, phases, args.package)
            elif (not isinstance(owners, list) or len(owners) != 1
                  or set(owners[0]) != {'manifest'}):
                raise ValueError('one conventional target manifest required')
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
        prepared_targets = [owned(root, Path(row['target'])) for row in report['targets']]
        current_targets = {Path(row['target']) for row in inventory(root, main_target, args.package)}
        if len(set(prepared_targets)) != len(prepared_targets) or set(prepared_targets) != current_targets:
            raise ValueError('retained target inventory differs from prepared rebuild evidence')
        report.update(status='verified', completed_check=str(args.verify_check))
        save(args.report, report)
        print(f'Verified {args.package} and dependent rebuilding in {len(report["targets"])} owned targets')
        return
    if args.report.exists():
        raise FileExistsError('use a new rebuild evidence path')
    report = {'version': 3, 'status': 'preparing', 'revision': revision,
              'package': args.package, 'started_unix_seconds': time.time(),
              'targets': inventory(root, main_target, args.package)}
    save(args.report, report)
    for row in report['targets']:
        target_arguments = clean_target_arguments(Path(row['target']), row['affected_before'])
        for owner in row['owners']:
            if 'compiler_before' in owner and not any(
                    Path(a['target']['src_path']).is_relative_to(source)
                    for a in owner['compiler_before']['artifacts']):
                continue
            for target_argument in target_arguments:
                command = ['cargo', 'clean', '--offline', '--locked', '--manifest-path', owner['manifest'],
                           '--target-dir', row['target'], *target_argument, '-p', args.package]
                env = dict(os.environ, CARGO_BUILD_BUILD_DIR=row['target'])
                result = subprocess.run(command, cwd=root, env=env)
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
