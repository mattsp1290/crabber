"""Focused ownership and failure tests for the outer check deadline."""
import contextlib
import hashlib
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
spec = importlib.util.spec_from_file_location('prepare_rebuild', MODULE.with_name('prepare-rebuild.py'))
rebuild = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rebuild)
ROOT = Path.cwd()


class RebuildInventoryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name) / 'checkout'
        self.root.mkdir()
        (self.root / 'Cargo.toml').write_text('[workspace]\n')
        self.source = self.root / 'crates/crabber-session/src/lib.rs'
        self.source.parent.mkdir(parents=True)
        self.source.write_text('')

    def cached_session(self, target):
        debug = target / 'debug'
        affected = debug / '.fingerprint/crabber-session-0123456789abcdef/lib-crabber_session.json'
        dependent = debug / '.fingerprint/crabber-runtime-123456789abcdef0/lib-crabber_runtime.json'
        dependency = debug / '.fingerprint/serde-23456789abcdef01/lib-serde.json'
        for path, deps in [(affected, []), (dependent, [[0, 'crabber_session', False, 0]]),
                           (dependency, [])]:
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps({'deps': deps}))
        depfile = debug / 'deps/crabber_session-0123456789abcdef.d'
        depfile.parent.mkdir()
        depfile.write_text(f'{depfile}: {self.source}\n')
        return affected, dependent, dependency

    def test_main_cache_without_compiler_marker_and_discovered_probe_are_retained(self):
        main = self.root / 'target'
        main_files = self.cached_session(main)
        probe = main / 'local-probes/api/target'
        probe_files = self.cached_session(probe)
        (probe / '.rustc_info.json').write_text('{}')
        manifest = probe.parent / 'source/Cargo.toml'
        manifest.parent.mkdir()
        manifest.write_text('[package]\nname="probe"\nversion="0.1.0"\n')
        self.assertFalse((main / '.rustc_info.json').exists())

        rows = rebuild.inventory(self.root, main, 'crabber-session')

        self.assertEqual({row['target'] for row in rows}, {str(main), str(probe)})
        by_target = {row['target']: row for row in rows}
        for target, files, expected_manifest in [(main, main_files, self.root / 'Cargo.toml'),
                                                 (probe, probe_files, manifest)]:
            affected, dependent, dependency = files
            row = by_target[str(target)]
            self.assertEqual(row['owners'], [{'manifest': str(expected_manifest)}])
            self.assertEqual(set(row['affected_before']), {str(affected)})
            self.assertEqual(set(row['dependents_before']), {str(dependent)})
            self.assertEqual(set(row['dependency_before']), {str(dependency)})
            self.assertTrue(all(path.exists() for path in files))

    def test_known_main_must_be_owned_without_symlinks(self):
        foreign = Path(self.directory.name) / 'foreign'
        foreign.mkdir()
        with self.assertRaisesRegex(ValueError, 'outside this checkout'):
            rebuild.inventory(self.root, foreign, 'crabber-session')
        actual = self.root / 'target'
        actual.mkdir()
        alias = self.root / 'linked-target'
        alias.symlink_to(actual, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'must not traverse a symlink'):
            rebuild.inventory(self.root, alias, 'crabber-session')

    def test_package_cleanup_covers_native_and_cross_target_artifacts(self):
        main = self.root / 'target'
        triple = 'aarch64-unknown-linux-musl'
        cached = {None: self.cached_session(main), triple: self.cached_session(main / triple)}
        dependencies = {files[2]: [files[2].stat().st_size, files[2].stat().st_mtime_ns]
                        for files in cached.values()}
        selected = []

        def clean(command, *, cwd, env):
            self.assertEqual(command[:2], ['cargo', 'clean'])
            self.assertEqual(cwd, self.root)
            self.assertEqual(env['CARGO_BUILD_BUILD_DIR'], str(main))
            self.assertEqual(command[command.index('--target-dir') + 1], str(main))
            self.assertEqual(command[-2:], ['-p', 'crabber-session'])
            target = command[command.index('--target') + 1] if '--target' in command else None
            selected.append(target)
            shutil.rmtree(cached[target][0].parent)
            return subprocess.CompletedProcess(command, 0)

        report = self.root / 'rebuild.json'
        argv = ['prepare-rebuild.py', 'crabber-session', '--report', str(report)]
        with mock.patch.object(rebuild, '__file__', str(self.root / '.github/scripts/prepare-rebuild.py')), \
                mock.patch.object(rebuild.subprocess, 'check_output', return_value='revision'), \
                mock.patch.object(rebuild.subprocess, 'run', side_effect=clean), \
                mock.patch.dict(os.environ, {'CARGO_TARGET_DIR': str(main)}), \
                mock.patch.object(sys, 'argv', argv), contextlib.redirect_stdout(io.StringIO()):
            rebuild.main()

        self.assertEqual(selected, [None, triple])
        value = json.loads(report.read_text())
        self.assertEqual(value['status'], 'prepared')
        self.assertEqual(set(value['targets'][0]['affected_removed']),
                         {str(files[0]) for files in cached.values()})
        self.assertEqual(rebuild.current_package(main, 'crabber-session', self.source.parent), [])
        for path, previous in dependencies.items():
            self.assertEqual([path.stat().st_size, path.stat().st_mtime_ns], previous)
        alias = main / 'aliased-target'
        alias.symlink_to(self.root / 'foreign-target', target_is_directory=True)
        unsafe = alias / 'debug/.fingerprint/crabber-session-0123456789abcdef/lib-crabber_session.json'
        with self.assertRaisesRegex(ValueError, 'must not traverse a symlink'):
            rebuild.clean_target_arguments(main, {str(unsafe): [1, 2]})


class SharedProbeInventoryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name).resolve()
        self.cache = self.root / 'target/local-probes/checkout'
        self.target = self.cache / 'shared-target'
        self.target.mkdir(parents=True)
        self.package_source = self.root / 'crates/crabber-session/src/lib.rs'
        self.package_source.parent.mkdir(parents=True)
        self.package_source.write_text('')
        self.marker = {'version': 1, 'checkout': str(self.root), 'owners': {}}
        self.receipts = {}
        for name, affected in [('left', True), ('right', False)]:
            source = self.cache / name / 'source'
            (source / 'src').mkdir(parents=True)
            manifest = source / 'Cargo.toml'
            manifest.write_text('[package]\nname="' + name + '"\nversion="0.1.0"\n')
            (source / 'src/main.rs').write_text('fn main(){}')
            entry = {'manifest': name + '/source/Cargo.toml', 'gate': 'local-probe-' + name,
                     'receipt': name + '/compiler.json'}
            self.marker['owners'][name] = entry
            artifacts = [self.artifact(name, source / 'src/main.rs')]
            if affected:
                artifacts.append(self.artifact('crabber_session', self.package_source))
            receipt = {'version': 1, 'manifest': str(manifest), 'target': str(self.target),
                       'gate': entry['gate'], 'manifest_sha256': hashlib.sha256(manifest.read_bytes()).hexdigest(),
                       'completed_unix_seconds': 10, 'artifacts': artifacts}
            self.receipts[name] = receipt
            self.write_receipt(name)
        (self.cache / 'owners.json').write_text(json.dumps(self.marker))
        self.row = {'target': str(self.target), 'owners': rebuild.owners_for(self.root, self.target, self.root / 'target')}

    def artifact(self, name, source):
        output = self.target / 'debug/deps' / (name + '.rmeta')
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text('metadata')
        return {'package_id': name, 'target': {'name': name, 'kind': ['lib'], 'src_path': str(source)},
                'features': [], 'filenames': [str(output)], 'fresh': True,
                'profile': {'opt_level': '0', 'debuginfo': 0, 'debug_assertions': True,
                            'overflow_checks': True, 'test': False}}

    def write_receipt(self, name):
        (self.cache / name / 'compiler.json').write_text(json.dumps(self.receipts[name]))

    def passing_phases(self):
        gates = []
        for owner in self.row['owners']:
            gates.append({'id': owner['gate'], 'status': 'passed', 'exit_code': 0,
                          'working_directory': str(Path(owner['manifest']).parent.relative_to(self.root)),
                          'argv': ['cargo', 'check', '--all-targets', '--target-dir', str(self.target),
                                   '--message-format', 'json-render-diagnostics'],
                          'kind': 'captured-command', 'environment_names': ['CARGO_BUILD_BUILD_DIR']})
        return {'coverage_scope': 'complete', 'status': 'passed', 'exit_code': 0,
                'source': {'revision': 'revision'}, 'started_unix_seconds': 19,
                'wall_seconds': 2, 'gates': gates}

    def finish_compilation(self):
        for name, receipt in self.receipts.items():
            receipt['completed_unix_seconds'] = 20
            for artifact in receipt['artifacts']:
                artifact['fresh'] = name != 'left'
            self.write_receipt(name)

    def test_each_affected_owner_rebuilds_without_requiring_unaffected_owner_work(self):
        self.finish_compilation()
        phases = self.passing_phases()
        rebuild.verify_consumer_gates(self.root, self.row, {'source_revision': 'revision'}, phases, 'crabber-session')
        left = self.receipts['left']
        left['artifacts'][0]['fresh'] = True
        self.write_receipt('left')
        with self.assertRaisesRegex(ValueError, 'affected consumer root'):
            rebuild.verify_consumer_gates(self.root, self.row, {'source_revision': 'revision'}, phases, 'crabber-session')

    def test_prepared_owner_snapshot_cannot_lose_a_gate_or_follow_a_regenerated_marker(self):
        self.finish_compilation()
        phases = self.passing_phases()
        self.marker['owners'].pop('right')
        (self.cache / 'owners.json').write_text(json.dumps(self.marker))
        rebuild.verify_consumer_gates(self.root, self.row, {'source_revision': 'revision'}, phases, 'crabber-session')
        for row, report in [(dict(self.row, owners=self.row['owners'][:1]), phases),
                            (self.row, dict(phases, gates=phases['gates'][:1]))]:
            with self.assertRaisesRegex(ValueError, 'gates differ'):
                rebuild.verify_consumer_gates(self.root, row, {'source_revision': 'revision'}, report, 'crabber-session')

    def test_gate_routing_and_owner_paths_fail_closed(self):
        self.finish_compilation()
        for field, value in [('argv', ['cargo', 'check', '--all-targets']),
                             ('working_directory', 'elsewhere'), ('environment_names', [])]:
            phases = self.passing_phases()
            phases['gates'][0][field] = value
            with self.assertRaisesRegex(ValueError, 'prepared manifest/cache'):
                rebuild.verify_consumer_gates(self.root, self.row, {'source_revision': 'revision'}, phases, 'crabber-session')
        self.marker['owners']['left']['manifest'] = '../Cargo.toml'
        (self.cache / 'owners.json').write_text(json.dumps(self.marker))
        with self.assertRaisesRegex(ValueError, 'owner identity'):
            rebuild.owners_for(self.root, self.target, self.root / 'target')

    def test_missing_variant_stale_receipt_and_source_substitution_are_rejected(self):
        self.finish_compilation()
        self.receipts['left']['artifacts'][1]['fresh'] = True
        self.write_receipt('left')
        with self.assertRaisesRegex(ValueError, 'shared compiler variant'):
            rebuild.verify_consumer_compilation(self.root, self.row, 'crabber-session', 19, 22)
        self.finish_compilation()
        self.receipts['left']['completed_unix_seconds'] = 10
        self.write_receipt('left')
        with self.assertRaisesRegex(ValueError, 'not produced'):
            rebuild.verify_consumer_compilation(self.root, self.row, 'crabber-session', 19, 22)
        self.finish_compilation()
        self.receipts['left']['manifest_sha256'] = 'wrong'
        self.write_receipt('left')
        with self.assertRaisesRegex(ValueError, 'does not match'):
            rebuild.verify_consumer_compilation(self.root, self.row, 'crabber-session', 19, 22)

    def test_main_cannot_downgrade_shared_targets_by_removing_owner_fields(self):
        phases_path = self.root / 'phases.json'
        phases_path.write_text(json.dumps(self.passing_phases()))
        check_path = self.root / 'check.json'
        check_path.write_text(json.dumps({'status': 'passed', 'source_revision': 'revision',
                                         'started_unix_seconds': 19,
                                         'phases_report': str(phases_path)}))
        report_path = self.root / 'rebuild.json'
        for owners in [[], [{'manifest': self.row['owners'][0]['manifest']}]]:
            row = dict(self.row, owners=owners, dependents_before={}, dependency_before={})
            report_path.write_text(json.dumps({'version': 3, 'status': 'prepared',
                                              'revision': 'revision', 'package': 'crabber-session',
                                              'prepared_unix_seconds': 18, 'targets': [row]}))
            argv = ['prepare-rebuild.py', 'crabber-session', '--report', str(report_path),
                    '--verify-check', str(check_path)]
            with self.subTest(owners=owners), \
                    mock.patch.object(rebuild, '__file__', str(self.root / '.github/scripts/prepare-rebuild.py')), \
                    mock.patch.object(rebuild.subprocess, 'check_output', return_value='revision'), \
                    mock.patch.object(rebuild, 'current_package', return_value=['affected']), \
                    mock.patch.object(rebuild, 'stamps', return_value={'affected': [1, 2]}), \
                    mock.patch.object(rebuild, 'dependent_stamps', return_value={'dependent': [1, 2]}), \
                    mock.patch.dict(os.environ, {'CARGO_TARGET_DIR': str(self.root / 'target')}), \
                    mock.patch.object(sys, 'argv', argv), contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(ValueError, 'shared consumer owners'):
                    rebuild.main()

    def test_main_cannot_omit_the_shared_target_from_a_nonempty_report(self):
        main_target = self.root / 'target'
        conventional = {'target': str(main_target),
                        'owners': [{'manifest': str(self.root / 'Cargo.toml')}],
                        'dependents_before': {}, 'dependency_before': {}}
        phases_path = self.root / 'phases.json'
        phases_path.write_text(json.dumps(self.passing_phases()))
        check_path = self.root / 'check.json'
        check_path.write_text(json.dumps({'status': 'passed', 'source_revision': 'revision',
                                         'started_unix_seconds': 19,
                                         'phases_report': str(phases_path)}))
        report_path = self.root / 'rebuild.json'
        report_path.write_text(json.dumps({'version': 3, 'status': 'prepared',
                                          'revision': 'revision', 'package': 'crabber-session',
                                          'prepared_unix_seconds': 18,
                                          'targets': [conventional]}))
        argv = ['prepare-rebuild.py', 'crabber-session', '--report', str(report_path),
                '--verify-check', str(check_path)]
        with mock.patch.object(rebuild, '__file__', str(self.root / '.github/scripts/prepare-rebuild.py')), \
                mock.patch.object(rebuild.subprocess, 'check_output', return_value='revision'), \
                mock.patch.object(rebuild, 'current_package', return_value=['affected']), \
                mock.patch.object(rebuild, 'stamps', return_value={'affected': [1, 2]}), \
                mock.patch.object(rebuild, 'dependent_stamps', return_value={'dependent': [1, 2]}), \
                mock.patch.object(rebuild, 'inventory', return_value=[conventional, self.row]), \
                mock.patch.dict(os.environ, {'CARGO_TARGET_DIR': str(main_target)}), \
                mock.patch.object(sys, 'argv', argv), contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(ValueError, 'retained target inventory'):
                rebuild.main()

    def test_owner_marker_discovers_shared_cache_without_rustc_info(self):
        self.assertFalse((self.target / '.rustc_info.json').exists())
        with mock.patch.object(rebuild, 'current_package', return_value=['affected']), \
                mock.patch.object(rebuild, 'stamps', return_value={'affected': [1, 2]}), \
                mock.patch.object(rebuild, 'dependent_stamps', return_value={'dependent': [1, 2]}):
            rows = rebuild.inventory(self.root, self.root / 'target', 'crabber-session')
        self.assertEqual({row['target'] for row in rows},
                         {str(self.root / 'target'), str(self.target)})
        shared = next(row for row in rows if row['target'] == str(self.target))
        self.assertEqual({owner['gate'] for owner in shared['owners']},
                         {'local-probe-left', 'local-probe-right'})
        (self.cache / 'owners.json').unlink()
        with mock.patch.object(rebuild, 'current_package', return_value=['affected']), \
                mock.patch.object(rebuild, 'stamps', return_value={'affected': [1, 2]}), \
                mock.patch.object(rebuild, 'dependent_stamps', return_value={'dependent': [1, 2]}):
            with self.assertRaises(FileNotFoundError):
                rebuild.inventory(self.root, self.root / 'target', 'crabber-session')


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
                 ('partial-not-drained', 600, None, 0),
                 ('artifacts', 600, None, 0), ('artifacts-report-only', 4800, None, 0),
                 ('artifacts-awaiting-cleanup', 600, None, 0),
                 ('artifacts-not-drained', 600, None, 0),
                 ('artifacts-claims-qualified', 600, None, 0),
                 ('artifacts-without-owner', 600, None, 0)]
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
                snapshot = repo / '.github/native-workers.json'
                snapshot.parent.mkdir()
                snapshot.write_bytes((MODULE.parents[1] / 'native-workers.json').read_bytes())
                git('add', 'tracked', '.github/native-workers.json')
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
                    partial = name.startswith(('partial', 'artifacts'))
                    selected_part = 'quality-artifacts' if name.startswith('artifacts') else 'quality-contracts'
                    self.assertEqual(command[3:5] if partial else command[3:4],
                                     ['--part', selected_part] if partial else ['--timings'])
                    scope = (selected_part if partial and name != 'partial-wrong-scope'
                             else 'complete')
                    Path(command[-1]).write_text(json.dumps({'coverage_scope': scope,
                                                            'qualified': name in {'partial-claims-qualified', 'artifacts-claims-qualified'},
                                                            'processes_drained': name not in {'partial-not-drained', 'artifacts-not-drained'},
                                                            'full_native_suite_executed': False,
                                                            'owned_postgres_run': name.startswith('artifacts') and name != 'artifacts-without-owner',
                                                            'status': 'awaiting-owned-cleanup' if name == 'artifacts-awaiting-cleanup' else 'passed'}))
                    return child_code

                argv = ['check.py', '--report', str(report), '--cache-state', 'unchanged']
                partial = name.startswith(('partial', 'artifacts'))
                selected_part = 'quality-artifacts' if name.startswith('artifacts') else 'quality-contracts'
                if partial:
                    argv += ['--part', selected_part]
                if budget != 600:
                    argv += ['--budget-seconds', str(budget)]
                with mock.patch.object(check, '__file__', str(repo / '.github/scripts/check.py')), \
                        mock.patch.object(check, 'supervise', side_effect=completed_child), \
                        mock.patch.object(sys, 'argv', argv), contextlib.redirect_stdout(io.StringIO()):
                    code = check.main()
                value = json.loads(report.read_text())
                bad_partial = name in {'partial-wrong-scope', 'partial-claims-qualified', 'partial-not-drained',
                                'artifacts-awaiting-cleanup', 'artifacts-not-drained',
                                'artifacts-claims-qualified', 'artifacts-without-owner'}
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


def load_tests(loader, tests, pattern):
    spec = importlib.util.spec_from_file_location('test_native_ci', MODULE.with_name('test_native_ci.py'))
    native_tests = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(native_tests)
    tests.addTests(loader.loadTestsFromModule(native_tests))
    return tests


if __name__ == '__main__':
    unittest.main()
