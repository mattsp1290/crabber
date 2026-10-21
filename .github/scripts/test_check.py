"""Focused ownership and failure tests for the outer check deadline."""
import importlib.util
import json
import os
from pathlib import Path
import signal
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

sys.dont_write_bytecode = True
MODULE = Path(__file__).with_name('check.py')
spec = importlib.util.spec_from_file_location('complete_check', MODULE)
check = importlib.util.module_from_spec(spec)
spec.loader.exec_module(check)
ROOT = Path.cwd()


class DeadlineTests(unittest.TestCase):
    def setUp(self):
        (ROOT / 'target').mkdir(exist_ok=True)
        self.directory = tempfile.TemporaryDirectory(dir=ROOT / 'target')
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)

    def run_child(self, code):
        report = self.root / 'report.json'
        result = check.supervise([sys.executable, '-I', '-S', '-B', '-c', code], ROOT, report,
                                 budget=2, reserve=.4)
        return result, json.loads(report.read_text())

    def test_success_and_failure_propagate(self):
        code, report = self.run_child('pass')
        self.assertEqual(code, 0)
        self.assertEqual(report['status'], 'passed')
        (self.root / 'report.json').unlink()
        code, report = self.run_child('raise SystemExit(7)')
        self.assertEqual(code, 7)
        self.assertEqual(report['status'], 'failed')

    def test_deadline_before_xtask_reaps_escaped_compiler_helper(self):
        marker = self.root / 'compiler-helper.pid'
        program = ("import pathlib,subprocess,sys,time; "
                   "p=subprocess.Popen([sys.executable,'-I','-S','-B','-c','import time;time.sleep(60)'],start_new_session=True); "
                   f"pathlib.Path({str(marker)!r}).write_text(str(p.pid)); time.sleep(60)")
        code, report = self.run_child(program)
        self.assertEqual(code, 124)
        self.assertEqual(report['reason'], 'budget-expired')
        self.assertLess(report['wall_seconds'], 2)
        self.assertFalse(Path('/proc', marker.read_text()).exists())

    def test_cancelled_zero_exit_child_still_fails(self):
        marker = self.root / 'ready'
        report = self.root / 'cancel.json'
        child = ("import pathlib,signal,sys,time; signal.signal(signal.SIGTERM,lambda *_:sys.exit(0)); "
                 f"pathlib.Path({str(marker)!r}).write_text('ready');time.sleep(60)")
        program = ("import importlib.util,pathlib; "
                   f"s=importlib.util.spec_from_file_location('check',{str(MODULE.resolve())!r}); "
                   "m=importlib.util.module_from_spec(s);s.loader.exec_module(m); "
                   f"raise SystemExit(m.supervise({[sys.executable, '-I', '-S', '-B', '-c', child]!r},pathlib.Path({str(ROOT)!r}),pathlib.Path({str(report)!r}),budget=4,reserve=.4))")
        process = subprocess.Popen([sys.executable, '-I', '-S', '-B', '-c', program])
        try:
            until = time.monotonic() + 3
            while not marker.exists():
                if process.poll() is not None or time.monotonic() >= until:
                    self.fail('child did not acknowledge readiness')
                time.sleep(.02)
            process.send_signal(signal.SIGTERM)
            self.assertEqual(process.wait(timeout=3), 143)
            result = json.loads(report.read_text())
            self.assertEqual(result['status'], 'failed')
            self.assertEqual(result['child_exit_code'], 0)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()

    def test_exit_recollects_a_helper_forked_after_the_last_snapshot(self):
        ready = self.root / 'parent-ready'
        go = self.root / 'fork-now'
        helper = self.root / 'late-helper'
        program = '\n'.join([
            'import os,pathlib,subprocess,sys,time',
            f'pathlib.Path({str(ready)!r}).write_text(str(os.getpid()))',
            f'while not pathlib.Path({str(go)!r}).exists(): time.sleep(.005)',
            "p=subprocess.Popen([sys.executable,'-I','-S','-B','-c','import time;time.sleep(60)'],start_new_session=True)",
            f'pathlib.Path({str(helper)!r}).write_text(str(p.pid))',
            'os._exit(0)',
        ])
        collect = check.collect
        injected = False

        def snapshot_then_fork(owned):
            nonlocal injected
            tree = collect(owned)
            if ready.exists() and not injected:
                injected = True
                go.write_text('go')
                until = time.monotonic() + 2
                while time.monotonic() < until:
                    parent = check.identity(int(ready.read_text()))
                    if helper.exists() and parent is not None and parent[2] == 'Z':
                        break
                    time.sleep(.005)
                else:
                    self.fail('parent did not fork and exit at the controlled boundary')
            return tree

        try:
            report = self.root / 'late-fork.json'
            with mock.patch.object(check, 'collect', side_effect=snapshot_then_fork):
                code = check.supervise([sys.executable, '-I', '-S', '-B', '-c', program],
                                       ROOT, report, budget=4, reserve=1)
            self.assertEqual(code, 1)
            self.assertEqual(json.loads(report.read_text())['reason'], 'leaked-descendants')
            self.assertFalse(Path('/proc', helper.read_text()).exists())
        finally:
            if helper.exists():
                pid = int(helper.read_text())
                row = check.identity(pid)
                if row is not None:
                    check.send(pid, row[1], signal.SIGKILL)
                    try:
                        os.waitpid(pid, 0)
                    except ChildProcessError:
                        pass

    def test_deadline_during_child_test_allows_supervisor_cleanup(self):
        # An executable named xtask exercises the launcher's cooperative
        # handoff rather than its pre-compiler kill-all path. The running child
        # acknowledges cancellation before its parent writes the cleanup marker.
        supervisor = self.root / 'xtask'
        shutil.copyfile(Path(sys.executable).resolve(), supervisor)
        supervisor.chmod(0o700)
        ready = self.root / 'test-ready'
        stopped = self.root / 'test-stopped'
        cleaned = self.root / 'fixture-cleaned'
        child = (
            'import pathlib,signal,sys,time; '
            f'signal.signal(signal.SIGTERM,lambda *_:(pathlib.Path({str(stopped)!r}).write_text("stopped"),sys.exit(0))); '
            f'pathlib.Path({str(ready)!r}).write_text("ready"); time.sleep(60)'
        )
        program = '\n'.join([
            'import pathlib,signal,subprocess,sys,time',
            f'p=subprocess.Popen({[sys.executable, '-I', '-S', '-B', '-c', child]!r})',
            'def cleanup(*_):',
            '    p.terminate()',
            '    p.wait(timeout=.5)',
            f'    assert pathlib.Path({str(stopped)!r}).exists()',
            f'    pathlib.Path({str(cleaned)!r}).write_text("cleaned")',
            '    raise SystemExit(0)',
            'signal.signal(signal.SIGTERM,cleanup)',
            'time.sleep(60)',
        ])
        report = self.root / 'live-test.json'
        code = check.supervise([str(supervisor), '-I', '-S', '-B', '-c', program],
                               ROOT, report, budget=3, reserve=1)
        result = json.loads(report.read_text())
        self.assertEqual(code, 124)
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['reason'], 'budget-expired')
        self.assertEqual(result['child_exit_code'], 0)
        self.assertTrue(ready.exists())
        self.assertTrue(cleaned.exists())
        self.assertLess(result['wall_seconds'], 3)


if __name__ == '__main__':
    unittest.main()
