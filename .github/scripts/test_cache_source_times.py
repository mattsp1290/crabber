"""Source freshness must preserve unchanged inputs and invalidate changed ones."""
import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import tempfile
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
        self.assertEqual(self.source.stat().st_mtime_ns, changed)

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


if __name__ == '__main__':
    unittest.main()
