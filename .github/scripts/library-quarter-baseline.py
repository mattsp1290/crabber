#!/usr/bin/env python3
"""Bind a diagnostic library baseline to one successful run and exact cache."""
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import zipfile

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / 'target/test-performance/library-quarter-baseline'
REVISION = '49317956a606826999161c8ae283c5c1f5fce19f'
REBUILD_FIX = '901b1693e044b99c46486ba41fa140e04f85d3ce'
RUN = 40164010460
ARTIFACT = 12588359604
DIGEST = 'd417fa27a7986eb7f993f9af5cb94579194c2af703099a8aa28463816322e643'
CACHE = 'rust-Linux-X64-b569b0c98ea3133c7252b4b8ea6b39b7b353813df0dd5f5c412c355691424112-dev0-test0-inc0-combined-v1-2140fed529d2fc7a4b7788fa8a0ae04e871570c1df6d03fa97d46e9e82ac09e0-49317956a606826999161c8ae283c5c1f5fce19f-40164010460-1'
FIXED_SCRIPTS = {'.github/scripts/prepare-rebuild.py', '.github/scripts/test_check.py'}
ALLOWED = {'.github/workflows/ci.yml', '.github/scripts/library-quarter-baseline.py', '.github/scripts/library-quarter-pilot.py'} | FIXED_SCRIPTS


def read(path):
    return json.loads(path.read_text())


def git(*args):
    return subprocess.check_output(['git', *args], cwd=ROOT, text=True)


def api(path):
    return json.loads(subprocess.check_output(['gh', 'api', 'repos/mattsp1290/crabber/' + path], text=True))


def source_guard():
    assert not git('status', '--porcelain'), 'diagnostic source must be clean'
    changed = set(git('diff', '--name-only', REVISION, 'HEAD').splitlines())
    assert changed <= ALLOWED, 'only diagnostic files and the approved rebuild fix may differ from493'
    git('merge-base', '--is-ancestor', REBUILD_FIX, 'HEAD')
    for path in sorted(FIXED_SCRIPTS):
        assert git('ls-tree', 'HEAD', '--', path) == git('ls-tree', REBUILD_FIX, '--', path), 'rebuild scripts must match the approved901b blobs and modes exactly'
    assert os.environ.get('PILOT_BASE_SHA') == REVISION, 'diagnostic PR must target the exact493 feature branch'
    assert os.uname().machine == 'x86_64'


def download():
    source_guard()
    OUT.mkdir(parents=True, exist_ok=False)
    run = api(f'actions/runs/{RUN}')
    assert run['head_sha'] == REVISION and run['head_branch'] == 'feat/ten-minute-tests'
    assert run['event'] == 'push' and run['conclusion'] == 'success' and run['run_attempt'] == 1
    artifact = api(f'actions/artifacts/{ARTIFACT}')
    assert artifact['name'] == f'complete-check-X64-{RUN}-1' and not artifact['expired']
    assert artifact['digest'] == 'sha256:' + DIGEST and artifact['workflow_run']['id'] == RUN
    with (OUT / 'normal.zip').open('xb') as stream:
        subprocess.run(['gh', 'api', f'repos/mattsp1290/crabber/actions/artifacts/{ARTIFACT}/zip'], stdout=stream, check=True)
    assert hashlib.sha256((OUT / 'normal.zip').read_bytes()).hexdigest() == DIGEST, 'artifact archive digest mismatch'
    with zipfile.ZipFile(OUT / 'normal.zip') as archive:
        for entry in archive.infolist():
            path = Path(entry.filename)
            assert not path.is_absolute() and '..' not in path.parts
            assert (entry.external_attr >> 16) & 0o170000 != 0o120000, 'artifact may not contain symlinks'
        archive.extractall(OUT / 'normal')
    (OUT / 'download.json').write_text(json.dumps({'run': RUN, 'artifact': ARTIFACT, 'archive_sha256': DIGEST, 'source_revision': REVISION}, indent=2) + '\n')


def records(manifest):
    return sorted([bid, name, row['ignored'], row['kind']] for bid, suite in manifest['rust-suites'].items() for name, row in suite['testcases'].items())


def digest(rows):
    return hashlib.sha256(json.dumps(rows, separators=(',', ':')).encode()).hexdigest()


def owned(path):
    assert path.is_relative_to(ROOT)
    for parent in [path, *path.parents]:
        if parent == ROOT:
            break
        assert not parent.is_symlink(), 'cached inputs must not traverse symlinks'
    assert path.is_file(), 'required canonical cache input missing'
    return path


def prepare():
    source_guard()
    assert os.environ.get('RESTORED_KEY') == CACHE and os.environ.get('CACHE_EXACT_HIT') == 'true', 'requires exact normal493 cache; no fallback'
    caches = api('actions/caches?per_page=100')['actions_caches']
    assert any(c['id'] == 9468484999 and c['key'] == CACHE and c['ref'] == 'refs/heads/feat/ten-minute-tests' for c in caches), 'required cache identity absent'
    normal = OUT / 'normal'
    checks = list(normal.glob('ci-*/check.json'))
    complete = list(normal.glob('complete-*'))
    assert len(checks) == len(complete) == 1
    report = read(checks[0])
    assert report['status'] == 'passed' and report['child_exit_code'] == 0
    assert report['source_revision'] == report['end_revision'] == REVISION
    assert report['source_clean'] and report['end_clean']
    phases = read(checks[0].parent / 'check-phases.json')
    assert len(phases['gates']) == 72 and all(g['status'] == 'passed' for g in phases['gates'])
    complete = complete[0]
    tests, binaries, metadata = [read(complete / name) for name in ['tests.json', 'binaries.json', 'cargo-metadata.json']]
    journal = [json.loads(line) for line in (complete / 'native-artifacts.jsonl').read_text().splitlines()]
    header = journal[0]
    assert header['kind'] == 'header' and journal[-1]['kind'] == 'reader-finished' and not journal[-1]['invalid']
    assert header['source']['revision'] == REVISION and header['source']['tracked_source_clean']
    assert header['source']['tree'] == git('rev-parse', REVISION + '^{tree}').strip()
    assert header['workspace_root'] == str(ROOT) and header['metadata_sha256'] == hashlib.sha256((complete / 'cargo-metadata.json').read_bytes()).hexdigest()
    assert header['profile'] == {'CARGO_BUILD_BUILD_DIR': None, 'CARGO_BUILD_JOBS': '4', 'CARGO_BUILD_TARGET': None, 'CARGO_INCREMENTAL': '0', 'CARGO_PROFILE_DEV_DEBUG': '0', 'CARGO_PROFILE_TEST_DEBUG': '0'}
    assert header['source']['rustc'] == subprocess.check_output(['rustc', '-Vv'], text=True).strip()
    assert header['source']['cargo'] == subprocess.check_output(['cargo', '-V'], text=True).strip()
    features = ''.join(re.findall(r'"([^"]*)"', (ROOT / 'xtask/src/complete.rs').read_text().split('const FEATURES: &str = concat!(', 1)[1].split(');', 1)[0]))
    assert header['argv'] == ['cargo', 'nextest', 'list', '--workspace', '--features', features, '--list-type', 'binaries-only', '--message-format', 'json', '--cargo-message-format', 'json-render-diagnostics']
    source_times = read(owned(ROOT / 'target/test-tools/source-times.json'))
    assert source_times['version'] == 2 and source_times['workspace'] == str(ROOT)
    for entry in git('ls-files', '--stage', '-z').split('\0'):
        if not entry:
            continue
        index, name = entry.split('\t', 1)
        mode, blob, stage = index.split()
        if name not in ALLOWED and mode in {'100644', '100755'}:
            saved = source_times['files'].get(name, {})
            assert stage == '0' and saved.get('blob') == blob and saved.get('mode') == mode, 'cached source provenance differs from493'
    full = records(tests)
    assert len(full) == 3805 and sum(not row[2] for row in full) == 3800
    assert digest(full) == '8d62354fc27a780f3ecb527840b925ade24f8408d58395924f073c76cc74cd42'
    assert len(binaries['rust-binaries']) == 146
    artifacts = [row for row in journal if row.get('kind') == 'artifact' and row.get('executable')]
    assert len(artifacts) == 154
    assert len({row['executable'] for row in artifacts}) == 154
    assert all(row['profile']['debug_assertions'] and row['profile']['overflow_checks'] for row in artifacts)
    packages = {p['id']: p['name'] for p in metadata['packages']}
    libraries, fingerprints, hashes = {}, {}, {}
    for bid, binary in binaries['rust-binaries'].items():
        if binary['kind'] not in {'lib', 'proc-macro'}:
            continue
        matches = [row for row in artifacts if row['executable'] == binary['binary-path']]
        assert len(matches) == 1
        row = matches[0]
        assert row['package_id'] == binary['package-id'] and row['target']['name'] == binary['binary-name']
        package = packages[row['package_id']]
        optimization = '1' if package == 'crabber-core' else '0'
        assert row['profile'] == {'debug_assertions': True, 'debuginfo': 0, 'opt_level': optimization, 'overflow_checks': True, 'test': True}
        executable = owned(Path(binary['binary-path']))
        dependency = owned(executable.with_suffix('.d'))
        dependency_text = dependency.read_text(errors='replace')
        source = Path(row['target']['src_path'])
        assert str(executable) in dependency_text and (str(source) in dependency_text or str(source.relative_to(ROOT)) in dependency_text), 'cached executable dependency source differs from493'
        suffix = executable.name.rsplit('-', 1)[1]
        marker = owned(ROOT / 'target/debug/.fingerprint' / (package + '-' + suffix) / ('test-lib-' + binary['binary-name'] + '.json'))
        fingerprint = read(marker)
        assert sorted(json.loads(fingerprint['features'])) == sorted(row['features'])
        fingerprint.pop('local', None)
        fingerprints[bid] = fingerprint
        hashes[bid] = hashlib.sha256(executable.read_bytes()).hexdigest()
        libraries[bid] = {'binary': binary, 'package_name': package, 'profile': row['profile']}
    assert len(libraries) == 29
    subset = [row for row in full if row[0] in libraries]
    assert len(subset) == 2364 and sum(not row[2] for row in subset) == 2363
    assert digest(subset) == '73b2a7bc4076701ddae7e662272a439ef309f7a9e7ed431f229fb9eb15b9392a'
    baseline = {'revision': REVISION, 'approved_rebuild_fix': REBUILD_FIX, 'artifact': ARTIFACT, 'artifact_sha256': DIGEST, 'cache_id': 9468484999, 'cache_key': CACHE, 'full_total': len(full), 'full_nonignored': 3800, 'total': len(subset), 'nonignored': 2363, 'inventory_sha256': digest(subset), 'libraries': libraries, 'fingerprints': fingerprints, 'cached_binary_sha256': hashes, 'fingerprint_provenance': 'observed from exact matched cache, not separately retained in original artifact'}
    (ROOT / 'target/test-performance/library-quarter-baseline.json').write_text(json.dumps(baseline, indent=2) + '\n')
    (ROOT / 'target/test-performance/cache-restore.json').write_text(json.dumps({'restored_key': CACHE, 'exact_hit': True, 'cache_state': 'matched-normal493'}, indent=2) + '\n')


if __name__ == '__main__':
    {'download': download, 'prepare': prepare}[sys.argv[1]]()
