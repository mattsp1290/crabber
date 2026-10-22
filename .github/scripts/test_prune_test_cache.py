#!/usr/bin/env python3
"""Exercise provenance refusal and executable-only cache pruning."""
import importlib.util
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('prune', Path(__file__).with_name('prune-test-cache.py'))
prune = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prune)


class CachePruneTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.run = self.root / 'target/test-performance/ci-current'
        self.manifest = self.root / 'target/test-performance/complete-current'
        self.run.mkdir(parents=True)
        self.manifest.mkdir()
        self.debug = self.root / 'target/debug'
        (self.debug / 'deps').mkdir(parents=True)
        (self.debug / '.fingerprint').mkdir()
        self.package = {'id': 'workspace-package', 'name': 'example-package',
                        'targets': [{'name': 'contract', 'kind': ['test'], 'test': True}]}
        self.kept = self.executable('1111111111111111')
        self.old = self.executable('2222222222222222')
        self.write(self.manifest / 'cargo-metadata.json', {
            'workspace_root': str(self.root), 'target_directory': str(self.root / 'target'),
            'workspace_members': [self.package['id']], 'packages': [self.package]})
        self.write(self.manifest / 'binaries.json', {'rust-binaries': {'contract': {
            'package-id': self.package['id'], 'binary-name': 'contract', 'binary-path': str(self.kept)}}})
        self.phases = {
            'status': 'failed', 'coverage_scope': 'complete',
            'source': {'revision': 'revision', 'tracked_source_clean': True},
            'fixture_metrics': str(self.manifest / 'fixtures.jsonl'),
            'gates': [
                {'id': 'compile', 'status': 'passed', 'argv': ['cargo', 'nextest', 'list', '--workspace', '--list-type', 'binaries-only']},
                {'id': 'list', 'status': 'passed', 'argv': ['cargo', 'nextest', 'list', '--cargo-metadata', str(self.manifest / 'cargo-metadata.json'), '--binaries-metadata', str(self.manifest / 'binaries.json')]},
                {'id': 'required-coverage-and-features', 'status': 'passed'}]}
        self.report = {'status': 'failed', 'source_revision': 'revision', 'end_revision': 'revision',
                       'source_clean': True, 'end_clean': True, 'ended_unix_seconds': 123,
                       'phases_report': str(self.run / 'check-phases.json'),
                       'command': ['cargo', 'xtask', 'check', '--timings', str(self.run / 'check-phases.json')]}
        self.persist()

    def write(self, path, value):
        path.write_text(json.dumps(value))

    def persist(self):
        self.write(self.run / 'check.json', self.report)
        self.write(self.run / 'check-phases.json', self.phases)

    def executable(self, suffix, name='contract', package='example-package'):
        fingerprint = self.debug / '.fingerprint' / (package + '-' + suffix)
        fingerprint.mkdir()
        self.write(fingerprint / ('test-integration-test-' + name + '.json'),
                   {'rustc': 1, 'target': 1, 'profile': 1, 'deps': []})
        path = self.debug / 'deps' / (name + '-' + suffix)
        path.write_bytes(b'\x7fELF executable fixture')
        path.chmod(0o755)
        return path

    def selected(self):
        target, packages, keep = prune.inventory(self.root, 'revision', self.run)
        return prune.candidates(target, packages, keep)

    def partial_mode(self):
        self.package['targets'][0]['src_path'] = str(self.root / 'tests/contract.rs')
        metadata = prune.read_json(self.manifest / 'cargo-metadata.json')
        metadata['packages'] = [self.package]
        metadata['resolve'] = {'nodes': [{'id': self.package['id'], 'features': []}]}
        self.write(self.manifest / 'cargo-metadata.json', metadata)
        for path in [self.kept, self.old]:
            elf = bytearray(64)
            elf[:6] = b'\x7fELF\x02\x01'
            elf[18:20] = (62).to_bytes(2, 'little')
            path.write_bytes(elf)
            suffix = path.name.rsplit('-', 1)[1]
            self.write(self.debug / '.fingerprint' / ('example-package-' + suffix) /
                       'test-integration-test-contract.json', {
                           'rustc': 1, 'target': 2, 'profile': 3, 'path': 4,
                           'features': '[]', 'declared_features': '[]', 'rustflags': [],
                           'config': 5, 'compile_kind': 0, 'deps': [], 'local': []})
        self.phases['source']['rustc'] = 'rustc pinned\nhost: x86_64-unknown-linux-gnu\n'
        compile_gate = self.phases['gates'][0]
        compile_gate.update(status='cancelled', features='xtask/postgres-check', argv=[
            'cargo', 'nextest', 'list', '--workspace', '--features', 'xtask/postgres-check',
            '--list-type', 'binaries-only', '--message-format', 'json',
            '--cargo-message-format', 'json-render-diagnostics'],
            native_artifacts=str(self.manifest / 'native-artifacts.jsonl'))
        self.phases['gates'] += [
            {'status': 'passed', 'argv': ['cargo', 'metadata', '--format-version', '1',
                                        '--features', 'xtask/postgres-check']},
            {'status': 'passed', 'argv': ['cargo', 'nextest', '--version']}]
        self.report['profile'] = {'CARGO_BUILD_JOBS': '4', 'CARGO_INCREMENTAL': '0',
                                  'CARGO_PROFILE_DEV_DEBUG': '0', 'CARGO_PROFILE_TEST_DEBUG': '0'}
        self.header = {'kind': 'header', 'version': 1, 'workspace_root': str(self.root),
                       'source': self.phases['source'], 'argv': compile_gate['argv'],
                       'metadata_sha256': hashlib.sha256((self.manifest / 'cargo-metadata.json').read_bytes()).hexdigest(),
                       'profile': dict(self.report['profile'], CARGO_BUILD_TARGET=None,
                                       CARGO_BUILD_BUILD_DIR=None)}
        self.artifact = {'kind': 'artifact', 'package_id': self.package['id'],
                         'target': {k: self.package['targets'][0][k] for k in ['name', 'kind', 'src_path']},
                         'profile': {'opt_level': '0', 'debuginfo': 0, 'debug_assertions': True,
                                     'overflow_checks': True, 'test': True},
                         'features': [], 'executable': str(self.kept), 'fresh': True}
        self.footer = {'kind': 'reader-finished', 'invalid': False, 'truncated_tail': False}
        self.persist()
        self.partial_journal()

    def partial_journal(self, records=None):
        records = records if records is not None else [self.artifact]
        (self.manifest / 'native-artifacts.jsonl').write_text(
            ''.join(json.dumps(row) + '\n' for row in [self.header, *records, self.footer]))

    def partial_selected(self):
        return prune.partial_candidates(self.root, 'revision', self.run, {})

    def test_partial_build_prunes_only_observed_unit_and_keeps_unknowns(self):
        self.partial_mode()
        unknown = self.executable('3333333333333333', name='unbuilt')
        self.package['targets'].append({'name': 'unbuilt', 'kind': ['test'], 'test': True,
                                       'src_path': str(self.root / 'tests/unbuilt.rs')})
        metadata = prune.read_json(self.manifest / 'cargo-metadata.json')
        metadata['packages'] = [self.package]
        self.write(self.manifest / 'cargo-metadata.json', metadata)
        self.header['metadata_sha256'] = hashlib.sha256((self.manifest / 'cargo-metadata.json').read_bytes()).hexdigest()
        self.partial_journal()
        self.assertEqual([p for p, _ in self.partial_selected()], [self.old])
        self.assertTrue(unknown.exists())
        self.assertTrue(self.kept.exists())
        # A dependency-generation change is not a second required feature or
        # profile: the completed current unit supersedes only its old context.
        marker = self.debug / '.fingerprint/example-package-2222222222222222/test-integration-test-contract.json'
        value = prune.read_json(marker)
        value['deps'] = ['older dependency generation']
        self.write(marker, value)
        self.assertEqual([p for p, _ in self.partial_selected()], [self.old])

    def test_partial_build_keeps_other_fingerprint_contexts_and_normal_aliases(self):
        self.partial_mode()
        marker = self.debug / '.fingerprint/example-package-2222222222222222/test-integration-test-contract.json'
        value = prune.read_json(marker)
        for key, different in [('features', '["other"]'), ('profile', 99), ('rustflags', ['--cfg=other']), ('path', 99)]:
            changed = dict(value, **{key: different})
            self.write(marker, changed)
            self.assertEqual(self.partial_selected(), [])
        self.write(marker, value)
        os.link(self.old, self.debug / 'normal-cli')
        self.assertEqual(self.partial_selected(), [])

    def test_partial_build_rejects_stale_provenance_and_failed_reader(self):
        self.partial_mode()
        for key, invalid in [('metadata_sha256', 'stale'), ('workspace_root', '/other')]:
            valid = self.header[key]
            self.header[key] = invalid
            self.partial_journal()
            with self.assertRaises(ValueError):
                self.partial_selected()
            self.header[key] = valid
        self.footer['invalid'] = True
        self.partial_journal()
        with self.assertRaises(ValueError):
            self.partial_selected()
        self.footer['invalid'] = False
        self.partial_journal()
        journal = self.manifest / 'native-artifacts.jsonl'
        journal.write_text(journal.read_text().rsplit('\n', 2)[0] + '\n')
        with self.assertRaises(ValueError):
            self.partial_selected()
        self.assertTrue(self.old.exists())

    def test_partial_build_keeps_entire_ambiguous_pair(self):
        self.partial_mode()
        conflicting = dict(self.artifact, features=['another-context'])
        self.partial_journal([self.artifact, conflicting])
        with self.assertRaises(ValueError):
            self.partial_selected()
        self.assertTrue(self.old.exists())
        self.assertTrue(self.kept.exists())

    def test_partial_build_does_not_follow_current_fingerprint_directory_symlink(self):
        self.partial_mode()
        fingerprint = self.debug / '.fingerprint/example-package-1111111111111111'
        detached = self.debug / 'detached-fingerprint'
        fingerprint.rename(detached)
        fingerprint.symlink_to(detached, target_is_directory=True)
        with self.assertRaises(ValueError):
            self.partial_selected()
        self.assertTrue(self.old.exists())

    def test_partial_build_requires_completed_artifact_not_truncated_record(self):
        self.partial_mode()
        self.footer['truncated_tail'] = True
        self.partial_journal()
        self.assertEqual([p for p, _ in self.partial_selected()], [self.old])
        self.partial_journal([])
        with self.assertRaises(ValueError):
            self.partial_selected()
        self.assertTrue(self.old.exists())

    def test_keeps_current_and_removes_only_proven_obsolete_executable(self):
        library = self.debug / 'deps/libcontract.rlib'
        library.write_bytes(b'library')
        removed_target = self.executable('3333333333333333', 'removed')
        registry = self.executable('4444444444444444', package='registry-package')
        selected = self.selected()
        self.assertEqual([p for p, _ in selected], [self.old])
        for path, _ in selected:
            path.unlink()
        for path in [self.kept, library, removed_target, registry]:
            self.assertTrue(path.exists())
        self.assertTrue((self.debug / '.fingerprint/example-package-2222222222222222/test-integration-test-contract.json').exists())

    def test_does_not_prune_normal_binary_fingerprint_or_alias(self):
        marker = self.debug / '.fingerprint/example-package-2222222222222222/bin-contract.json'
        self.write(marker, {})
        self.assertEqual(self.selected(), [])
        marker.unlink()
        os.link(self.old, self.debug / 'normal-cli')
        self.assertEqual(self.selected(), [])

    def test_skip_incomplete_discovery_even_with_same_revision_metadata(self):
        self.phases['gates'][0]['status'] = 'cancelled'
        self.persist()
        with self.assertRaises(ValueError):
            self.selected()
        self.assertTrue(self.old.exists())

    def test_rejects_different_manifest_run_and_wrong_target(self):
        self.phases['fixture_metrics'] = str(self.manifest.with_name('complete-stale') / 'fixtures.jsonl')
        self.persist()
        with self.assertRaises(ValueError):
            self.selected()
        self.phases['fixture_metrics'] = str(self.manifest / 'fixtures.jsonl')
        self.persist()
        metadata = prune.read_json(self.manifest / 'cargo-metadata.json')
        metadata['target_directory'] = '/another/target'
        self.write(self.manifest / 'cargo-metadata.json', metadata)
        with self.assertRaises(ValueError):
            self.selected()

    def test_rejects_dirty_or_different_revision_and_missing_kept_binary(self):
        self.report['end_revision'] = 'other'
        self.persist()
        with self.assertRaises(ValueError):
            self.selected()
        self.report['end_revision'] = 'revision'
        self.report['end_clean'] = False
        self.persist()
        with self.assertRaises(ValueError):
            self.selected()
        self.report['end_clean'] = True
        self.persist()
        self.kept.unlink()
        with self.assertRaises(ValueError):
            self.selected()

    def test_keeps_symlinks_and_non_executables(self):
        self.old.chmod(0o644)
        self.assertEqual(self.selected(), [])
        self.old.unlink()
        self.old.symlink_to(self.kept)
        self.assertEqual(self.selected(), [])

    def test_refuses_running_qualification(self):
        directory = self.run / 'qualification'
        directory.mkdir()
        self.write(directory / 'summary.json', {'status': 'running', 'revision': 'revision'})
        with self.assertRaises(ValueError):
            self.selected()

    def test_records_exact_inventory_inputs_and_partial_removals(self):
        audit = {}
        prune.inventory(self.root, 'revision', self.run, audit)
        self.assertEqual(audit['retained_paths'], [str(self.kept.relative_to(self.root))])
        self.assertEqual(len(audit['inputs']), 3)
        self.assertTrue(all(len(h) == 64 for h in audit['inputs'].values()))
        result = {'status': 'applied', 'removed_bytes': 0, 'removed_executables': []}
        original = Path.unlink

        def fail_second(path):
            if path == self.kept:
                raise PermissionError('simulated maintenance failure')
            return original(path)

        with patch.object(Path, 'unlink', fail_second):
            prune.remove_obsolete(result, [(self.old, self.old.stat().st_size), (self.kept, 100)])
        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['removed_executables'], [str(self.old)])
        self.assertGreater(result['removed_bytes'], 0)
        self.assertTrue(self.kept.exists())


if __name__ == '__main__':
    unittest.main()
