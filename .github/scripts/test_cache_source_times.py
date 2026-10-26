"""Source freshness must preserve unchanged inputs and invalidate changed ones."""
import importlib.util
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('source_times', Path(__file__).with_name('cache-source-times.py'))
cache = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cache)


class SourceTimesTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.git('init', '-q')
        self.source = self.root / 'lib.rs'
        self.source.write_text('pub fn value() -> u8 { 1 }\n')
        self.git('add', 'lib.rs')
        self.report = self.root / '.git/source-times.json'

    def git(self, *args):
        subprocess.run(['git', *args], cwd=self.root, check=True, capture_output=True)

    def test_identical_checkout_reuses_time_but_changed_blob_does_not(self):
        original = self.source.stat().st_mtime_ns
        self.assertEqual(cache.capture(self.root, self.report), 1)
        os.utime(self.source, ns=(original + 10**9, original + 10**9))
        self.assertEqual(cache.restore(self.root, self.report), 1)
        self.assertEqual(self.source.stat().st_mtime_ns, original)
        self.source.write_text('pub fn value() -> u8 { 2 }\n')
        self.git('add', 'lib.rs')
        changed = self.source.stat().st_mtime_ns
        self.assertEqual(cache.restore(self.root, self.report), 0)
        self.assertGreater(self.source.stat().st_mtime_ns, changed)

    def test_dirty_input_is_refused(self):
        cache.capture(self.root, self.report)
        self.source.write_text('changed without updating the index\n')
        with self.assertRaises(subprocess.CalledProcessError):
            cache.restore(self.root, self.report)

    def test_mode_change_and_symlink_do_not_reuse_times(self):
        cache.capture(self.root, self.report)
        self.source.chmod(0o755)
        self.git('add', 'lib.rs')
        self.assertEqual(cache.restore(self.root, self.report), 0)
        self.source.unlink()
        self.source.symlink_to('missing-target')
        self.git('add', 'lib.rs')
        self.assertEqual(cache.restore(self.root, self.report), 0)

    def newer_restored_artifact(self):
        invoked = self.root / 'target/debug/.fingerprint/crate-test/invoked.timestamp'
        invoked.parent.mkdir(parents=True)
        invoked.write_text('')
        future = time.time_ns() + 20 * 10**9
        os.utime(invoked, ns=(future, future))
        return future

    def test_overlapping_snapshot_cannot_hide_a_changed_input(self):
        cache.capture(self.root, self.report)
        self.source.write_text('pub fn value() -> u8 { 2 }\n')
        self.git('add', 'lib.rs')
        # Another job can build/cache the prior blob after our checkout.
        artifact_time = self.newer_restored_artifact()
        self.assertEqual(cache.restore(self.root, self.report), 0)
        self.assertGreater(self.source.stat().st_mtime_ns, artifact_time)

    def test_unknown_snapshot_invalidates_workspace_inputs(self):
        artifact_time = self.newer_restored_artifact()
        self.assertEqual(cache.restore(self.root, self.report), 0)
        self.assertGreater(self.source.stat().st_mtime_ns, artifact_time)

    def committed_source(self):
        self.git('-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.test',
                 '-c', 'commit.gpgsign=false',
                 'commit', '-qm', 'source fixture')

    def test_optional_report_records_the_input_and_actual_same_pass_decisions(self):
        self.committed_source()
        cache.capture(self.root, self.report)
        raw = self.report.read_bytes()
        original = self.source.stat().st_mtime_ns
        os.utime(self.source, ns=(original + 10**9, original + 10**9))
        destination = self.root / '.git/restore-report.json'

        self.assertEqual(cache.restore(self.root, self.report, destination), 1)

        result = json.loads(destination.read_text())
        self.assertEqual(result['input_sha256'], hashlib.sha256(raw).hexdigest())
        self.assertEqual((result['reused_files'], result['invalidated_files']), (1, 0))
        self.assertEqual(result['status'], 'completed')
        self.assertTrue(result['tracked_source_clean'])
        self.assertEqual(result['current_files']['lib.rs']['mode'], '100644')
        self.assertEqual(self.source.stat().st_mtime_ns, original)

    def test_report_binds_a_changed_blob_and_the_restored_invocation_bound(self):
        self.committed_source()
        cache.capture(self.root, self.report)
        previous = json.loads(self.report.read_text())['files']['lib.rs']['blob']
        self.source.write_text('pub fn value() -> u8 { 2 }\n')
        self.git('add', 'lib.rs')
        self.committed_source()
        artifact_time = self.newer_restored_artifact()
        destination = self.root / '.git/restore-report.json'

        self.assertEqual(cache.restore(self.root, self.report, destination), 0)

        result = json.loads(destination.read_text())
        self.assertEqual((result['reused_files'], result['invalidated_files']), (0, 1))
        self.assertNotEqual(result['current_files']['lib.rs']['blob'], previous)
        self.assertEqual(result['max_invocation_ns'], artifact_time)
        self.assertGreater(result['changed_time_ns'], artifact_time)
        self.assertEqual(self.source.stat().st_mtime_ns, result['changed_time_ns'])

    def test_missing_and_foreign_inputs_report_zero_reuse(self):
        self.committed_source()
        for name, saved in [('missing', None), ('foreign', {
                'version': 2, 'workspace': '/different-checkout', 'files': {}})]:
            if saved is not None:
                self.report.write_text(json.dumps(saved))
            destination = self.root / '.git' / (name + '.json')
            self.assertEqual(cache.restore(self.root, self.report, destination), 0)
            result = json.loads(destination.read_text())
            self.assertEqual((result['reused_files'], result['invalidated_files']), (0, 1))
            self.assertEqual(result['input_sha256'],
                             hashlib.sha256(self.report.read_bytes()).hexdigest() if saved else None)

    def test_existing_report_is_rejected_before_timestamps_change(self):
        destination = self.root / '.git/restore-report.json'
        destination.write_text('previous report\n')
        before = self.source.stat().st_mtime_ns
        with self.assertRaises(FileExistsError):
            cache.restore(self.root, self.report, destination)
        self.assertEqual(self.source.stat().st_mtime_ns, before)
        self.assertEqual(destination.read_text(), 'previous report\n')

    def test_restore_does_not_admit_utf16_input(self):
        self.report.write_bytes('{"version": 2, "workspace": "/other", "files": {}}'.encode('utf-16'))
        before = self.source.stat().st_mtime_ns
        with self.assertRaises(UnicodeDecodeError):
            cache.restore(self.root, self.report)
        self.assertEqual(self.source.stat().st_mtime_ns, before)


if __name__ == '__main__':
    unittest.main()
