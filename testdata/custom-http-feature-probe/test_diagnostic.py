"""Failure classification must not turn an infrastructure failure green."""
import importlib.util
from pathlib import Path
import unittest
import sys
sys.dont_write_bytecode = True

spec = importlib.util.spec_from_file_location("diagnostic", Path(__file__).with_name("check-diagnostic.py"))
diagnostic = importlib.util.module_from_spec(spec)
spec.loader.exec_module(diagnostic)


class Rejections(unittest.TestCase):
    def test_only_completed_expected_root_error_counts(self):
        source = Path("probe/main.rs").resolve()
        error = {"reason": "compiler-message", "target": {"src_path": str(source)},
                 "message": {"level": "error", "code": {"code": "E0599"}}}
        terminal = {"reason": "build-finished", "success": False}
        self.assertTrue(diagnostic.rejected([error, terminal], source, "E0599"))
        self.assertFalse(diagnostic.rejected([error], source, "E0599"))
        self.assertFalse(diagnostic.rejected([terminal], source, "E0599"))
        self.assertFalse(diagnostic.rejected([error, terminal], source, "E0277"))
        dependency = {**error, "target": {"src_path": "dependency/lib.rs"}}
        self.assertFalse(diagnostic.rejected([dependency, terminal], source, "E0599"))
        self.assertFalse(diagnostic.rejected([error, dependency, terminal], source, "E0599"))


if __name__ == "__main__":
    unittest.main()
