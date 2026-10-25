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


class IncompleteDiscovery(ValueError):
    pass


def run_inventory(root, revision, directory):
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
    return report, phases_path, phases, manifest_dir


def inventory(root, revision, directory, audit=None):
    _, phases_path, phases, manifest_dir = run_inventory(root, revision, directory)
    gates = phases['gates']
    compiled = [g for g in gates if g.get('argv', [])[:4] == ['cargo', 'nextest', 'list', '--workspace']
                and '--list-type' in g['argv'] and 'binaries-only' in g['argv']]
    listed = [g for g in gates if g.get('argv', [])[:4] == ['cargo', 'nextest', 'list', '--cargo-metadata']
              and '-E' not in g['argv']]
    coverage = [g for g in gates if g['id'] == 'required-coverage-and-features']
    if any(len(items) != 1 or items[0]['status'] != 'passed'
           for items in [compiled, listed, coverage]):
        raise IncompleteDiscovery('current native discovery and coverage did not complete')
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


FINGERPRINT_CONTEXT = ('rustc', 'target', 'profile', 'path', 'features',
                       'declared_features', 'rustflags', 'config', 'compile_kind')


def fingerprint_context(path):
    if any(parent.is_symlink() for parent in [path, path.parent, path.parent.parent]):
        raise ValueError('native fingerprint context must not use symlinks')
    value = read_json(path)
    if not all(key in value for key in FINGERPRINT_CONTEXT):
        raise ValueError('incomplete native fingerprint context')
    return {key: value[key] for key in FINGERPRINT_CONTEXT}


def partial_inputs(root, revision, directory):
    """Bind the safe journal to one terminal, pinned native build."""
    report, phases_path, phases, manifest = run_inventory(root, revision, directory)
    builds = [g for g in phases['gates']
              if g.get('argv', [])[:4] == ['cargo', 'nextest', 'list', '--workspace']]
    if len(builds) != 1:
        raise ValueError('no unique native build')
    build = builds[0]
    features = build.get('features')
    expected = ['cargo', 'nextest', 'list', '--workspace', '--features', features,
                '--list-type', 'binaries-only', '--message-format', 'json',
                '--cargo-message-format', 'json-render-diagnostics']
    if (not isinstance(features, str) or build['argv'] != expected
            or build['status'] not in {'failed', 'cancelled', 'passed'}):
        raise ValueError('no supported streamed native build')
    journal = within(root, build.get('native_artifacts', ''))
    if journal != manifest / 'native-artifacts.jsonl':
        raise ValueError('native artifact journal differs from this build')
    raw = journal.read_bytes()
    if not raw.endswith(b'\n'):
        raise ValueError('native artifact reader did not finish')
    records = [json.loads(line) for line in raw.splitlines()]
    if (len(records) < 2 or records[0].get('kind') != 'header'
            or records[-1].get('kind') != 'reader-finished'
            or records[-1].get('invalid') is not False):
        raise ValueError('native artifact stream is incomplete or invalid')
    header = records[0]
    metadata_path = manifest / 'cargo-metadata.json'
    metadata = read_json(metadata_path)
    target = root / 'target'
    if (header.get('version') != 1 or header.get('workspace_root') != str(root)
            or header.get('source') != phases['source']
            or header.get('argv') != expected
            or header.get('metadata_sha256') != hashlib.sha256(metadata_path.read_bytes()).hexdigest()
            or metadata['workspace_root'] != str(root) or metadata['target_directory'] != str(target)):
        raise ValueError('native artifact provenance differs from the current run')
    metadata_builds = [g for g in phases['gates'] if g.get('argv') ==
                       ['cargo', 'metadata', '--format-version', '1', '--features', features]
                       and g['status'] == 'passed']
    version_checks = [g for g in phases['gates'] if g.get('argv') ==
                      ['cargo', 'nextest', '--version'] and g['status'] == 'passed']
    if len(metadata_builds) != 1 or len(version_checks) != 1:
        raise ValueError('native metadata or pinned runner was not verified')
    profile = header.get('profile', {})
    if (profile.get('CARGO_PROFILE_DEV_DEBUG') != '0'
            or profile.get('CARGO_PROFILE_TEST_DEBUG') != '0'
            or profile.get('CARGO_BUILD_TARGET') is not None
            or profile.get('CARGO_BUILD_BUILD_DIR') is not None):
        raise ValueError('unsupported native build profile or target')
    if any(profile.get(key) != value for key, value in report['profile'].items()):
        raise ValueError('native profile differs from launcher profile')
    host = re.search(r'^host: (\S+)$', phases['source'].get('rustc', ''), re.M)
    machine = {'x86_64': 62, 'aarch64': 183}.get(host[1].split('-')[0]) if host else None
    if machine is None:
        raise ValueError('unsupported native host')
    return phases_path, manifest, build, journal, metadata, target, records, machine


def observed_units(root, target, metadata, records, machine):
    """Keep all emitted paths; ambiguity removes the whole pair's authority."""
    packages = {p['id']: p for p in metadata['packages'] if p['id'] in metadata['workspace_members']}
    nodes = {n['id']: n for n in metadata['resolve']['nodes']}
    keep, blocked, observed = set(), set(), {}
    for record in records[1:-1]:
        if record.get('kind') != 'artifact':
            raise ValueError('unexpected native artifact record')
        package = packages.get(record.get('package_id'))
        if package is None:
            raise ValueError('artifact is not a workspace package')
        info = record.get('target', {})
        name = info.get('name')
        pair = (package['id'], name)
        executable = within(root, record.get('executable', ''))
        if record.get('profile', {}).get('test') is False:
            if executable.parent == target / 'debug/deps' and executable.is_file():
                keep.add(executable)
            continue
        if executable.parent != target / 'debug/deps' or not executable.is_file():
            blocked.add(pair)
            continue
        keep.add(executable)
        targets = [t for t in package['targets'] if t.get('test') and
                   all(t.get(key) == info.get(key) for key in ['name', 'kind', 'src_path'])]
        signature = record.get('profile')
        if (len(targets) != 1 or info.get('kind') not in [['lib'], ['bin'], ['test']]
                or signature != {'opt_level': '0', 'debuginfo': 0, 'debug_assertions': True,
                                 'overflow_checks': True, 'test': True}
                or not isinstance(record.get('fresh'), bool)
                or not isinstance(record.get('features'), list)
                or record['features'] != sorted(nodes[package['id']]['features'])):
            blocked.add(pair)
            continue
        match = re.fullmatch(re.escape(name.replace('-', '_')) + r'-([0-9a-f]{16})', executable.name)
        label = 'integration-test' if info['kind'] == ['test'] else info['kind'][0]
        if match is None:
            blocked.add(pair)
            continue
        marker = target / 'debug/.fingerprint' / (package['name'] + '-' + match[1]) / ('test-' + label + '-' + name.replace('-', '_') + '.json')
        try:
            context = fingerprint_context(marker)
            with executable.open('rb') as stream:
                elf = stream.read(20)
        except (OSError, ValueError):
            blocked.add(pair)
            continue
        if (len(elf) < 20 or elf[:6] != b'\x7fELF\x02\x01'
                or int.from_bytes(elf[18:20], 'little') != machine
                or not executable.stat().st_mode & 0o111
                or json.loads(context['features']) != record['features']):
            blocked.add(pair)
            continue
        identity = (info, signature, record['features'], context)
        previous = observed.get(pair)
        if previous is not None and (previous['identity'] != identity or previous['path'] != executable):
            blocked.add(pair)
        else:
            observed[pair] = {'package': package, 'target': targets[0], 'identity': identity,
                              'context': context, 'path': executable, 'label': label}
    return keep, blocked, observed


def partial_candidates(root, revision, directory, audit):
    """Only equivalent old variants of individually observed native test units."""
    phases_path, manifest, build, journal, metadata, target, records, machine = partial_inputs(root, revision, directory)
    keep, blocked, observed = observed_units(root, target, metadata, records, machine)
    obsolete, eligible = [], []
    for pair, unit in observed.items():
        if pair in blocked:
            continue
        package = dict(unit['package'], targets=[unit['target']])
        # The completed current test unit supersedes old dependency/source
        # generations in this exact context. Other features, profiles, source
        # paths, flags and native compilation contexts remain untouched.
        for path, size in candidates(target, [package], keep):
            suffix = path.name.rsplit('-', 1)[1]
            marker = target / 'debug/.fingerprint' / (package['name'] + '-' + suffix) / ('test-' + unit['label'] + '-' + unit['target']['name'].replace('-', '_') + '.json')
            try:
                equivalent = fingerprint_context(marker) == unit['context']
            except (OSError, ValueError):
                equivalent = False
            if equivalent:
                obsolete.append((path, size))
        eligible.append({'package_id': pair[0], 'target': unit['target'],
                         'current_executable': str(unit['path'].relative_to(root)),
                         'fingerprint_context': unit['context']})
    if not eligible:
        raise ValueError('no unambiguous completed native test units')
    audit.update(mode='partial-artifacts', revision=revision, build_status=build['status'],
                 workspace_root=str(root), manifest_directory=str(manifest),
                 inputs={str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                         for p in [phases_path, manifest / 'cargo-metadata.json', journal]},
                 retained_paths=sorted(str(p.relative_to(root)) for p in keep),
                 eligible_units=eligible, blocked_pairs=sorted(blocked),
                 unknown_pairs_retained=True, qualified=False)
    return sorted(set(obsolete))


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
        try:
            target, packages, keep = inventory(root, revision, directory, result)
            obsolete = candidates(target, packages, keep)
            result.update(mode='complete-discovery', retained_executables=len(keep))
        except IncompleteDiscovery:
            obsolete = partial_candidates(root, revision, directory, result)
        result.update(status='applied' if args.apply else 'dry-run', revision=revision,
                      candidate_bytes=sum(n for _, n in obsolete),
                      executables=[str(p.relative_to(root)) for p, _ in obsolete])
        if args.apply:
            remove_obsolete(result, obsolete)
    except (OSError, ValueError, KeyError, IndexError, TypeError, subprocess.CalledProcessError) as error:
        result['reason'] = type(error).__name__ + ': ' + str(error)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
