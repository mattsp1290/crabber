"""Focused ownership and failure tests for the outer check deadline."""
import contextlib
import importlib.util
import io
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
spec = importlib.util.spec_from_file_location('qualify_check', MODULE.with_name('qualify-check.py'))
qualify = importlib.util.module_from_spec(spec)
spec.loader.exec_module(qualify)
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

    def test_report_preserves_package_profiles_and_caller_overrides(self):
        manifest = self.root / 'Cargo.toml'
        manifest.write_text('[profile.test.package.example]\nopt-level=1\ndebug-assertions=true\noverflow-checks=true\n')
        env = dict(os.environ, CARGO_PROFILE_TEST_CODEGEN_UNITS='8')
        report_path = self.root / 'profiles.json'
        code = check.supervise([sys.executable, '-I', '-S', '-B', '-c', 'pass'],
                               self.root, report_path, budget=2, reserve=.4, env=env)
        self.assertEqual(code, 0)
        report = json.loads(report_path.read_text())
        self.assertEqual(report['profile']['CARGO_PROFILE_TEST_CODEGEN_UNITS'], '8')
        self.assertEqual(report['cargo_profiles']['test']['package']['example'],
                         {'opt-level': 1, 'debug-assertions': True, 'overflow-checks': True})
        manifest.write_text('invalid = [')
        self.assertEqual(check.cargo_profiles(self.root), {'unavailable': 'TOMLDecodeError'})

    def test_qualification_rejects_new_changed_and_removed_profile_overrides(self):
        previous = {'CARGO_BUILD_JOBS': '8', 'CARGO_INCREMENTAL': '1',
                    'CARGO_PROFILE_DEV_DEBUG': '0', 'CARGO_PROFILE_TEST_DEBUG': '0'}
        current = dict(previous, CARGO_PROFILE_TEST_OPT_LEVEL='1')
        self.assertEqual(qualify.profile_differences(previous, current),
                         ['CARGO_PROFILE_TEST_OPT_LEVEL'])
        self.assertEqual(qualify.profile_differences(current, previous),
                         ['CARGO_PROFILE_TEST_OPT_LEVEL'])
        current = dict(previous, CARGO_BUILD_JOBS='4')
        self.assertEqual(qualify.profile_differences(previous, current), ['CARGO_BUILD_JOBS'])

    def test_qualification_keeps_launcher_debug_defaults(self):
        previous = {'CARGO_BUILD_JOBS': '8', 'CARGO_INCREMENTAL': '1',
                    'CARGO_PROFILE_DEV_DEBUG': '0', 'CARGO_PROFILE_TEST_DEBUG': '0'}
        self.assertEqual(qualify.profile_differences(previous,
                         {'CARGO_BUILD_JOBS': '8', 'CARGO_INCREMENTAL': '1'}), [])

    def test_main_requires_stable_source_for_success(self):
        cases = [('strict', 600, None, 0), ('bootstrap', 4800, None, 0),
                 ('dirty', 4800, 'dirty', 0), ('revision', 4800, 'revision', 0),
                 ('initially-dirty', 4800, 'initially-dirty', 0),
                 ('failed-child', 4800, 'dirty', 7),
                 ('partial', 600, None, 0), ('partial-report-only', 4800, None, 0),
                 ('partial-wrong-scope', 600, None, 0),
                 ('partial-claims-qualified', 600, None, 0),
                 ('partial-not-drained', 600, None, 0)]
        for name, budget, change, child_code in cases:
            with self.subTest(name=name):
                repo = self.root / name
                repo.mkdir()

                def git(*args):
                    return subprocess.check_output(['git', *args], cwd=repo,
                                                   text=True, stderr=subprocess.DEVNULL).strip()

                git('init', '--quiet')
                tracked = repo / 'tracked'
                tracked.write_text('original\n')
                git('add', 'tracked')
                commit = ['-c', 'user.name=Policy test', '-c', 'user.email=policy@example.invalid',
                          '-c', 'commit.gpgsign=false', 'commit', '--quiet']
                git(*commit, '-m', 'initial')
                if change == 'initially-dirty':
                    tracked.write_text('changed\n')
                report = self.root / (name + '.json')

                def completed_child(command, cwd, path, **kwargs):
                    self.assertEqual(cwd, repo)
                    self.assertEqual(kwargs['budget'], budget)
                    value = {'status': 'passed' if child_code == 0 else 'failed',
                             'reason': None if child_code == 0 else 'child-failed',
                             'source_revision': git('rev-parse', 'HEAD'),
                             'source_clean': not git('status', '--porcelain')}
                    if change == 'dirty':
                        tracked.write_text('changed\n')
                    elif change == 'initially-dirty':
                        tracked.write_text('original\n')
                    elif change == 'revision':
                        git(*commit, '--allow-empty', '-m', 'changed head')
                    path.write_text(json.dumps(value))
                    partial = name.startswith('partial')
                    self.assertEqual(command[3:5] if partial else command[3:4],
                                     ['--part', 'quality-contracts'] if partial else ['--timings'])
                    scope = ('quality-contracts' if partial and name != 'partial-wrong-scope'
                             else 'complete')
                    Path(command[-1]).write_text(json.dumps({'coverage_scope': scope,
                                                            'qualified': name == 'partial-claims-qualified',
                                                            'processes_drained': name != 'partial-not-drained',
                                                            'status': 'passed'}))
                    return child_code

                argv = ['check.py', '--report', str(report), '--cache-state', 'unchanged']
                partial = name.startswith('partial')
                if partial:
                    argv += ['--part', 'quality-contracts']
                if budget != 600:
                    argv += ['--budget-seconds', str(budget)]
                with mock.patch.object(check, '__file__', str(repo / '.github/scripts/check.py')), \
                        mock.patch.object(check, 'supervise', side_effect=completed_child), \
                        mock.patch.object(sys, 'argv', argv), contextlib.redirect_stdout(io.StringIO()):
                    code = check.main()
                value = json.loads(report.read_text())
                bad_partial = name in {'partial-wrong-scope', 'partial-claims-qualified', 'partial-not-drained'}
                expected = child_code or (1 if change or bad_partial else 0)
                self.assertEqual(code, expected)
                self.assertEqual(value['status'], 'failed' if expected else 'passed')
                self.assertEqual(value['timing_target_met'], expected == 0 and not partial)
                self.assertEqual(value['qualified'], expected == 0 and budget == 600 and not partial)
                if change:
                    self.assertEqual(value['reason'], 'child-failed' if child_code else 'source-changed-or-dirty')

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

    def test_unlinked_supervisor_retains_cooperative_cleanup(self):
        # The full schedule rebuilds xtask while its original inode is running.
        # Linux then exposes /proc/PID/exe as "xtask (deleted)". Only the parent
        # may initiate cancellation; signalling its child directly is observable.
        supervisor = self.root / 'xtask'
        shutil.copyfile(Path(sys.executable).resolve(), supervisor)
        supervisor.chmod(0o700)
        ready = self.root / 'test-ready'
        stopped = self.root / 'test-stopped'
        unexpected = self.root / 'unexpected-termination'
        cleaned = self.root / 'fixture-cleaned'
        child = '\n'.join([
            'import pathlib,signal,sys,time',
            'def stop(*_):',
            f'    pathlib.Path({str(stopped)!r}).write_text("stopped")',
            '    time.sleep(.2)',
            '    raise SystemExit(0)',
            'signal.signal(signal.SIGUSR1,stop)',
            f'signal.signal(signal.SIGTERM,lambda *_:pathlib.Path({str(unexpected)!r}).write_text("unexpected"))',
            f'pathlib.Path({str(supervisor)!r}).unlink()',
            f'pathlib.Path({str(ready)!r}).write_text("ready")',
            'time.sleep(60)',
        ])
        program = '\n'.join([
            'import pathlib,signal,subprocess,sys,time',
            f'p=subprocess.Popen({[sys.executable, '-I', '-S', '-B', '-c', child]!r})',
            'def cleanup(*_):',
            '    p.send_signal(signal.SIGUSR1)',
            '    p.wait(timeout=.5)',
            f'    assert pathlib.Path({str(stopped)!r}).exists()',
            f'    assert not pathlib.Path({str(unexpected)!r}).exists()',
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
