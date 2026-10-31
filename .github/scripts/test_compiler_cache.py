"""Compiler cache ownership, input admission and snapshot lease contracts."""
import importlib.util
import contextlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest import mock

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('compiler_cache', Path(__file__).with_name('compiler-cache.py'))
cache = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cache)
spec = importlib.util.spec_from_file_location('cache_check', Path(__file__).with_name('check.py'))
check = importlib.util.module_from_spec(spec)
spec.loader.exec_module(check)
spec = importlib.util.spec_from_file_location('qualify_compiler_cache', Path(__file__).with_name('qualify-compiler-cache.py'))
qualify = importlib.util.module_from_spec(spec)
spec.loader.exec_module(qualify)
ROOT = Path(__file__).resolve().parents[2]


class CacheTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.base = self.root / 'snapshots'
        self.namespace = 'a' * 64
        self.store = cache.SnapshotStore(self.base, self.namespace)
        self.source = self.root / 'source'
        self.source.mkdir()
        (self.source / 'object').write_bytes(b'finalized compiler object')

    def fake_config(self):
        executable = self.root / 'sccache'
        executable.write_text('#!' + sys.executable + '\n' + '''
import json, os, pathlib, signal, socket, sys, time
if '--show-stats' in sys.argv:
    print(json.dumps({'stats': {'cache_hits': {'counts': {'Rust': 1}}}}))
elif len(sys.argv) > 1:
    print(json.dumps({'environment': dict(os.environ), 'pid': os.getpid(), 'group': os.getpgrp()}))
    sys.exit(7)
else:
    signal.signal(signal.SIGUSR1, lambda *_: os._exit(0))
    pathlib.Path(os.environ['SCCACHE_DIR'], 'server-env.json').write_text(json.dumps(dict(os.environ)))
    server = socket.socket(socket.AF_UNIX)
    server.bind(os.environ['SCCACHE_SERVER_UDS'])
    server.listen()
    while True:
        time.sleep(.05)
''')
        executable.chmod(0o755)
        return SimpleNamespace(executable=executable, binary_sha256=cache.digest(executable),
                               wrapper=ROOT / '.github/scripts/compiler-cache-wrapper',
                               wrapper_sha256=cache.digest(ROOT / '.github/scripts/compiler-cache-wrapper'),
                               base=self.base, namespace=self.namespace)

    def test_off_and_invalid_mode_are_closed_variants(self):
        self.assertIsNone(cache.configuration(self.root, {}))
        self.assertIsNone(cache.configuration(self.root, {'CRABBER_COMPILER_CACHE': 'off', 'CARGO_INCREMENTAL': '1'}))
        for mode in ['', 'true', 'remote']:
            with self.assertRaisesRegex(ValueError, 'off or local'):
                cache.configuration(self.root, {'CRABBER_COMPILER_CACHE': mode})
        for env, direct in [({'CARGO_INCREMENTAL': '1'}, False), ({}, True)]:
            with self.assertRaisesRegex(ValueError, 'CARGO_INCREMENTAL=0'):
                cache.configuration(self.root, {'CRABBER_COMPILER_CACHE': 'local', **env}, direct=direct)

    def test_ambient_cache_and_wrappers_are_removed(self):
        env = {'SCCACHE_START_SERVER': '1', 'SCCACHE_ERROR_LOG': 'secret', 'SCCACHE_BUCKET': 'foreign',
               'RUSTC_WRAPPER': 'foreign', 'RUSTC_WORKSPACE_WRAPPER': 'foreign', 'CARGO_BUILD_JOBS': '4'}
        self.assertEqual(cache.clean_environment(env), {'CARGO_BUILD_JOBS': '4'})

    def test_compiler_closed_allowlist_preserves_declared_build_inputs_only(self):
        descriptor = {'config': '/private/config.toml', 'socket': '/private/socket', 'store': '/private/store'}
        incoming = {'PATH': '/bin', 'OUT_DIR': '/build/out', 'CRABBER_GIT_SHA': 'a' * 40,
                    'CARGO_PKG_VERSION': '0.1.0', 'CLIPPY_ARGS': '-D__CLIPPY_HACKERY__warnings',
                    'CLIPPY_CONF_DIR': '/private/lint-policy',
                    'CRABBER_POSTGRES_URL': 'SYNTHETIC_FIXTURE_SECRET',
                    'POSTGRES_URL': 'SYNTHETIC_FIXTURE_SECRET', 'BUILD_LEAK_SENTINEL': 'SYNTHETIC_SECRET',
                    'RUSTC_BOOTSTRAP': '1',
                    'SCCACHE_START_SERVER': '1', 'SCCACHE_ERROR_LOG': 'foreign', 'SCCACHE_BUCKET': 'foreign'}
        result = cache.compiler_environment(descriptor, incoming)
        self.assertEqual(result['OUT_DIR'], incoming['OUT_DIR'])
        self.assertEqual(result['CRABBER_GIT_SHA'], incoming['CRABBER_GIT_SHA'])
        self.assertEqual(result['CARGO_PKG_VERSION'], incoming['CARGO_PKG_VERSION'])
        self.assertEqual(result['CLIPPY_ARGS'], incoming['CLIPPY_ARGS'])
        self.assertEqual(result['CLIPPY_CONF_DIR'], incoming['CLIPPY_CONF_DIR'])
        self.assertFalse(any('SYNTHETIC' in value for value in result.values()))
        for key in ['SCCACHE_START_SERVER', 'SCCACHE_ERROR_LOG', 'SCCACHE_BUCKET']:
            self.assertNotIn(key, result)
        self.assertNotIn('RUSTC_BOOTSTRAP', result)

    def test_version_and_compiler_identity_probes_receive_only_closed_inputs(self):
        version_env, compiler_env = self.root / 'version-env.json', self.root / 'compiler-env.json'
        executable = self.root / 'verified-tool'
        executable.write_text('#!' + sys.executable + '\nimport json,os,pathlib\n'
                              + f'pathlib.Path({str(version_env)!r}).write_text(json.dumps(dict(os.environ)))\n'
                              + 'print("sccache 0.19.0")\n')
        executable.chmod(0o755)
        pin = json.loads((ROOT / '.config/test-tools.json').read_text())['sccache']
        target = next(iter(pin['archives']))
        cache.write_json(executable.with_name(executable.name + '.pin.json'), {
            'version': pin['version'], 'target': target, 'archive_sha256': pin['archives'][target],
            'binary_sha256': cache.digest(executable)})
        compiler = self.root / 'rustc'
        compiler.write_text('#!' + sys.executable + '\nimport json,os,pathlib\n'
                            + f'pathlib.Path({str(compiler_env)!r}).write_text(json.dumps(dict(os.environ)))\n'
                            + 'print("rustc fixture")\n')
        compiler.chmod(0o755)
        incoming = {**os.environ, 'POSTGRES_URL': 'SYNTHETIC_FIXTURE_SECRET', 'DD_API_KEY': 'SYNTHETIC_KEY',
                    'BUILD_LEAK_SENTINEL': 'SYNTHETIC_UNKNOWN_SECRET', 'CRABBER_SCCACHE_PATH': str(executable),
                    'CRABBER_COMPILER_CACHE_DIR': str(self.base), 'RUSTC': str(compiler)}
        with mock.patch.dict(os.environ, incoming, clear=True), \
                mock.patch.object(cache.subprocess, 'check_output', wraps=cache.subprocess.check_output) as probes:
            cache.LocalConfig(ROOT, incoming)
        forwarded = [call.kwargs['env'] for call in probes.call_args_list if 'env' in call.kwargs]
        self.assertEqual(len(forwarded), 2)
        self.assertTrue(all(set(environment) <= cache.COMPILER_INPUTS for environment in forwarded))
        for path in [version_env, compiler_env]:
            observed = json.loads(path.read_text())
            self.assertEqual(observed['RUSTC'], str(compiler))
            for name in ['POSTGRES_URL', 'DD_API_KEY', 'BUILD_LEAK_SENTINEL']:
                self.assertNotIn(name, observed)

    def test_pin_verification_rejects_output_variants_links_and_writable_files(self):
        executable = self.root / 'pin-tool'
        executable.write_bytes(b'pinned tool fixture')
        executable.chmod(0o755)
        pin = json.loads((ROOT / '.config/test-tools.json').read_text())['sccache']
        target = next(iter(pin['archives']))
        receipt = executable.with_name(executable.name + '.pin.json')
        cache.write_json(receipt, {'version': pin['version'], 'target': target,
            'archive_sha256': pin['archives'][target], 'binary_sha256': cache.digest(executable)})
        with mock.patch.object(cache.subprocess, 'check_output', return_value=b'sccache 0.19.0\n'):
            self.assertEqual(cache.verify_executable(executable), cache.digest(executable))
            for candidate in [executable, receipt]:
                original = candidate.stat().st_mode & 0o777
                candidate.chmod(original | 0o020)
                with self.assertRaisesRegex(ValueError, 'group/other-writable'):
                    cache.verify_executable(executable)
                candidate.chmod(original)
                link = self.root / 'hardlink'
                os.link(candidate, link)
                with self.assertRaisesRegex(ValueError, 'single-linked'):
                    cache.verify_executable(executable)
                link.unlink()
            alias = self.root / 'alias'
            alias.symlink_to(self.root, target_is_directory=True)
            with self.assertRaisesRegex(ValueError, 'symlinks'):
                cache.verify_executable(alias / executable.name)
        for version in [b'sccache 0.19.0', b' sccache 0.19.0\n', b'sccache 0.19.0\r\n', b'sccache 0.19.0\n\n']:
            with mock.patch.object(cache.subprocess, 'check_output', return_value=version), self.assertRaises(ValueError):
                cache.verify_executable(executable)

    def test_statistics_export_only_fixed_numeric_counters(self):
        result = cache.statistics({'stats': {'compile_requests': 4, 'cache_hits': {'counts': {'Rust': 2, '/private/source': 6}},
            'not_cached': {'crate-type': 1, '/private/unexpected-source': 3},
            'dist_compiles': {'private-scheduler.example': 8}, 'source': '/private/root',
            'multi_level': {'secret': 'SYNTHETIC_SECRET'}}})
        self.assertEqual(result['compile_requests'], 4)
        self.assertEqual(result['cache_hits'], {'counts': {'Rust': 2}})
        self.assertEqual(result['not_cached'], {'crate-type': 1, 'incremental': 0, 'other': 3})
        self.assertNotIn('private', json.dumps(result))
        self.assertNotIn('SYNTHETIC', json.dumps(result))
        for invalid in [{'compile_requests': True}, {'compile_fails': -1},
                        {'cache_hits': {'counts': {'Rust': '/private/root'}}}, {'not_cached': {'incremental': 'error'}}]:
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                cache.statistics(invalid)

    def test_ci_setup_identity_check_and_publication_use_the_final_tool_path(self):
        action = (ROOT / '.github/actions/check-setup/action.yml').read_text()
        identity_position = action.index('compiler-cache.py ci-identity')
        exports = list(re.finditer(r'echo "\$GITHUB_WORKSPACE/([^"\n]+)" >> "\$GITHUB_PATH"', action))
        self.assertEqual(len(exports), 2)
        self.assertTrue(all(match.start() < identity_position for match in exports))
        paths = [str(ROOT / match[1]) for match in reversed(exports)]
        before = {**os.environ, 'CRABBER_COMPILER_CACHE': 'local', 'CARGO_INCREMENTAL': '0',
                  'CRABBER_COMPILER_CACHE_DIR': str(self.base),
                  'CRABBER_SCCACHE_PATH': str(self.fake_config().executable),
                  'CARGO_PROFILE_DEV_DEBUG': '0', 'CARGO_PROFILE_TEST_DEBUG': '0',
                  'CARGO_PROFILE_DEV_STRIP': 'symbols', 'CARGO_PROFILE_TEST_STRIP': 'symbols',
                  'GITHUB_REPOSITORY': 'mattsp1290/crabber', 'GITHUB_EVENT_NAME': 'push',
                  'GITHUB_REF': 'refs/heads/main', 'GITHUB_SHA': 'a' * 40,
                  'GITHUB_RUN_ID': '123', 'GITHUB_RUN_ATTEMPT': '1'}
        effective = {**before, 'PATH': os.pathsep.join([*paths, before['PATH']])}
        outputs = []
        def probe(command, **kwargs):
            if command[0] == 'git':
                return '' if command[1] == 'status' else ('canonical-repository' if command[1] == 'config' else 'a' * 40)
            outputs.append(kwargs['env'])
            return 'rustc fixture\nhost: x86_64-unknown-linux-gnu\n'
        with mock.patch.object(cache, 'verify_executable', return_value='b' * 64), \
                mock.patch.object(cache.subprocess, 'check_output', side_effect=probe):
            old_identity = cache.ci_identity(before)
            setup = cache.ci_identity(effective)
            actual = cache.configuration(ROOT, effective)
            self.assertNotEqual(old_identity['path'], setup['path'])
            self.assertEqual(setup['path'], str(actual.base / actual.namespace))
            store = cache.SnapshotStore(actual.base, actual.namespace)
            published = store.publish(self.source, 'passed', actual.producer)
            reports = self.root / 'reports'
            reports.mkdir()
            cache.write_json(reports / 'check.json', {'status': 'passed', 'compiler_cache': {
                'mode': 'local', 'namespace': actual.namespace, 'published': published,
                'state': 'closed', 'cleanup_verified': True}})
            self.assertTrue(cache.publication_eligible(reports, effective))
        self.assertEqual([observed['PATH'] for observed in outputs[1:]], [effective['PATH']] * 3)

    @unittest.skipUnless(Path('/proc/self/stat').exists(), 'Linux owner descriptor')
    def test_wrapper_exec_preserves_group_exit_and_scrubs_actual_compiler_environment(self):
        session = cache.CompilerCacheSession(self.fake_config())
        try:
            session.start()
            env = session.clients({**os.environ, 'BUILD_LEAK_SENTINEL': 'SYNTHETIC_SECRET',
                                   'POSTGRES_URL': 'SYNTHETIC_FIXTURE_SECRET', 'OUT_DIR': '/declared/build'})
            child = subprocess.Popen([str(session.config.wrapper), 'fake-rustc'], env=env,
                                     stdout=subprocess.PIPE, text=True, start_new_session=True)
            output, _ = child.communicate(timeout=5)
            evidence = json.loads(output)
            self.assertEqual(child.returncode, 7)
            self.assertEqual(evidence['pid'], child.pid)
            self.assertEqual(evidence['group'], child.pid)
            self.assertEqual(evidence['environment']['OUT_DIR'], '/declared/build')
            self.assertNotIn('BUILD_LEAK_SENTINEL', evidence['environment'])
            self.assertNotIn('POSTGRES_URL', evidence['environment'])
        finally:
            session.close('failed', publish=False)

    def test_copy_is_independent_and_failed_check_is_not_pass_credit(self):
        name = self.store.publish(self.source, 'failed')
        destination = self.root / 'imported'
        receipt = self.store.restore(destination)
        self.assertEqual(receipt['snapshot'], name)
        self.assertEqual(receipt['check_status'], 'failed')
        (destination / 'object').write_bytes(b'mutated')
        self.assertEqual((self.store.snapshots / name / 'store/object').read_bytes(), b'finalized compiler object')

    def test_absent_namespace_is_a_miss(self):
        self.store.publish(self.source, 'passed')
        other = cache.SnapshotStore(self.base, 'b' * 64)
        self.assertIsNone(other.restore(self.root / 'other'))

    def test_corruption_is_quarantined_and_not_retried(self):
        name = self.store.publish(self.source, 'passed')
        (self.store.snapshots / name / 'store/object').write_bytes(b'poison')
        with self.assertRaisesRegex(ValueError, 'quarantined'):
            self.store.restore(self.root / 'imported')
        self.assertFalse((self.store.snapshots / name).exists())
        self.assertIsNone(self.store.restore(self.root / 'empty'))

    def test_symlinks_and_oversize_store_are_rejected(self):
        (self.source / 'link').symlink_to(self.source / 'object')
        with self.assertRaisesRegex(ValueError, 'regular file'):
            self.store.publish(self.source, 'passed')
        (self.source / 'link').unlink()
        with mock.patch.object(cache, 'STORE_LIMIT', 1), self.assertRaisesRegex(ValueError, 'bound'):
            self.store.publish(self.source, 'passed')
        alias = self.root / 'alias'
        alias.symlink_to(self.base)
        with self.assertRaisesRegex(ValueError, 'symlinks'):
            cache.SnapshotStore(alias, self.namespace)

    def test_eviction_keeps_the_newest_closed_snapshot(self):
        first = self.store.publish(self.source, 'passed')
        with mock.patch.object(cache, 'SNAPSHOT_LIMIT', 30):
            second = self.store.publish(self.source, 'failed')
        self.assertFalse((self.store.snapshots / first).exists())
        self.assertTrue((self.store.snapshots / second).exists())

    def test_eviction_rejects_intermediate_symlink_without_deleting_unrelated_data(self):
        sibling = self.base / ('b' * 64)
        sibling.mkdir(mode=0o700)
        unrelated = self.root / 'unrelated'
        keep = unrelated / 'keep'
        keep.mkdir(parents=True)
        sentinel = keep / 'sentinel'
        sentinel.write_bytes(b'unrelated data must survive')
        (keep / 'manifest.json').write_text(json.dumps({'created_ns': 0, 'bytes': 100}))
        (sibling / 'snapshots').symlink_to(unrelated, target_is_directory=True)
        with mock.patch.object(cache, 'SNAPSHOT_LIMIT', 10), self.assertRaisesRegex(ValueError, 'ordinary directory'):
            self.store.publish(self.source, 'passed')
        self.assertEqual(sentinel.read_bytes(), b'unrelated data must survive')
        self.assertTrue((keep / 'manifest.json').is_file())

    def test_eviction_requires_complete_matching_manifest_and_actual_contents(self):
        first = self.store.publish(self.source, 'passed')
        manifest_path = self.store.snapshots / first / 'manifest.json'
        original = json.loads(manifest_path.read_text())
        for changed in [{'created_ns': 0, 'bytes': 100}, {**original, 'schema': 2},
                        {**original, 'namespace': 'b' * 64}, {**original, 'snapshot': '0' * 32},
                        {**original, 'bytes': original['bytes'] + 1}, {**original, 'entries': {}}]:
            with self.subTest(changed=changed):
                manifest_path.write_text(json.dumps(changed))
                with mock.patch.object(cache, 'SNAPSHOT_LIMIT', 10), self.assertRaises(ValueError):
                    self.store.publish(self.source, 'passed')
                self.assertEqual((self.store.snapshots / first / 'store/object').read_bytes(), b'finalized compiler object')
        manifest_path.write_text(json.dumps(original))

    def test_eviction_requires_snapshot_id_before_deleting_an_ordinary_directory(self):
        keep = self.store.snapshots / 'unrelated'
        keep.mkdir()
        (keep / 'sentinel').write_bytes(b'keep')
        with mock.patch.object(cache, 'SNAPSHOT_LIMIT', 10), self.assertRaisesRegex(ValueError, 'identity'):
            self.store.publish(self.source, 'passed')
        self.assertEqual((keep / 'sentinel').read_bytes(), b'keep')

    def test_import_shared_lease_blocks_publication_and_eviction(self):
        first = self.store.publish(self.source, 'passed')
        copying, release, published = threading.Event(), threading.Event(), threading.Event()
        copytree = shutil.copytree
        errors = []
        def pause(source, destination, **kwargs):
            if Path(destination) == self.root / 'imported':
                copying.set()
                self.assertTrue(release.wait(3))
            return copytree(source, destination, **kwargs)
        def restore():
            try:
                self.store.restore(self.root / 'imported')
            except BaseException as error:
                errors.append(error)
        def publish():
            try:
                self.store.publish(self.source, 'passed')
                published.set()
            except BaseException as error:
                errors.append(error)
        with mock.patch.object(cache.shutil, 'copytree', side_effect=pause), mock.patch.object(cache, 'SNAPSHOT_LIMIT', 30):
            reader = threading.Thread(target=restore)
            writer = threading.Thread(target=publish)
            reader.start()
            self.assertTrue(copying.wait(2))
            writer.start()
            self.assertFalse(published.wait(.15))
            self.assertTrue((self.store.snapshots / first).exists())
            release.set()
            reader.join(3)
            writer.join(3)
        self.assertEqual(errors, [])
        self.assertTrue(published.is_set())
        self.assertEqual((self.root / 'imported/object').read_bytes(), b'finalized compiler object')

    def test_maintenance_wait_is_bounded(self):
        with self.store.lease(exclusive=True):
            with self.assertRaisesRegex(ValueError, 'timed out'), self.store.lease(timeout=.05):
                pass

    def test_only_canonical_main_push_can_publish(self):
        canonical = {'GITHUB_REPOSITORY': 'mattsp1290/crabber', 'GITHUB_EVENT_NAME': 'push',
                     'GITHUB_REF': 'refs/heads/main'}
        self.assertTrue(cache.trusted_writer(canonical))
        for changed in [{'GITHUB_EVENT_NAME': 'pull_request'}, {'GITHUB_EVENT_NAME': 'pull_request_target'},
                        {'GITHUB_EVENT_NAME': 'workflow_dispatch'}, {'GITHUB_REF': 'refs/heads/feature'},
                        {'GITHUB_REPOSITORY': 'fork/crabber'}]:
            self.assertFalse(cache.trusted_writer({**canonical, **changed}))

    def test_deadline_failure_does_not_quarantine_valid_snapshot(self):
        name = self.store.publish(self.source, 'passed')
        self.store.deadline = time.monotonic() - 1
        with self.assertRaises(TimeoutError):
            self.store.restore(self.root / 'deadline')
        self.assertTrue((self.store.snapshots / name).is_dir())

    def test_archive_checksum_rejection_precedes_extraction(self):
        from io import BytesIO
        with mock.patch.object(cache.urllib.request, 'urlopen', return_value=BytesIO(b'invalid archive')):
            with self.assertRaisesRegex(ValueError, 'checksum mismatch'):
                cache.install(self.root / 'tools')
        self.assertFalse((self.root / 'tools/sccache').exists())

    def test_foreground_owner_has_no_credentials_and_independent_sessions(self):
        first, second = [cache.CompilerCacheSession(self.fake_config()) for _ in range(2)]
        try:
            with mock.patch.dict(os.environ, {'DD_API_KEY': 'SYNTHETIC_SECRET', 'SCCACHE_ERROR_LOG': 'foreign'}):
                first.start()
                second.start()
            environment = json.loads((first.store / 'server-env.json').read_text())
            self.assertNotIn('DD_API_KEY', environment)
            self.assertNotIn('SCCACHE_ERROR_LOG', environment)
            clients = first.clients({'SCCACHE_START_SERVER': '1', 'CARGO_BUILD_JOBS': '4'})
            self.assertNotIn('SCCACHE_START_SERVER', clients)
            self.assertEqual(clients['SCCACHE_CLIENT_SIDE'], '1')
            self.assertEqual(clients['CARGO_INCREMENTAL'], '0')
            self.assertTrue(first.close('failed'))
            self.assertTrue(first.report['cleanup_verified'])
            self.assertTrue(second.healthy())
            self.assertTrue(second.close('passed'))
        finally:
            for session in [first, second]:
                if session.server is not None and session.server.poll() is None:
                    session.close('failed', publish=False)

    def test_original_server_death_is_terminal_and_cannot_publish(self):
        session = cache.CompilerCacheSession(self.fake_config())
        session.start()
        session.server.kill()
        session.server.wait()
        self.assertFalse(session.healthy())
        self.assertFalse(session.close('passed'))
        self.assertIsNone(session.report['published'])

    def test_original_exit_at_first_shutdown_poll_cannot_publish_even_with_zero_status(self):
        for number, expected in [(signal.SIGKILL, -signal.SIGKILL), (signal.SIGUSR1, 0)]:
            with self.subTest(expected_exit=expected):
                session = cache.CompilerCacheSession(self.fake_config())
                session.start()
                original_poll = session.server.poll
                injected = False
                def exit_before_shutdown():
                    nonlocal injected
                    if session.state == 'closing' and not injected:
                        injected = True
                        os.kill(session.server.pid, number)
                        session.server.wait(timeout=2)
                    return original_poll()
                with mock.patch.object(session.server, 'poll', side_effect=exit_before_shutdown):
                    self.assertFalse(session.close('passed'))
                self.assertTrue(injected)
                self.assertEqual(session.server.returncode, expected)
                self.assertIsNone(session.report['published'])
                self.assertEqual(session.report['state'], 'failed')
                self.assertTrue(session.report['cleanup_verified'])

    @unittest.skipUnless(Path('/proc/self/stat').exists(), 'Linux measured supervisor')
    def test_local_profile_records_incremental_zero_before_cargo_without_caller_setting(self):
        actual = self.root / 'child-incremental'
        env = {name: value for name, value in os.environ.items() if name != 'CARGO_INCREMENTAL'}
        command = [sys.executable, '-I', '-S', '-c',
                   f'import os,pathlib; pathlib.Path({str(actual)!r}).write_text(os.environ["CARGO_INCREMENTAL"])']
        with mock.patch.object(check.compiler_cache, 'configuration', return_value=self.fake_config()):
            self.assertEqual(check.supervise(command, ROOT, self.root / 'profile.json', budget=5, reserve=1, env=env), 0)
        result = json.loads((self.root / 'profile.json').read_text())
        self.assertEqual(actual.read_text(), '0')
        self.assertEqual(result['profile']['CARGO_INCREMENTAL'], '0')

    @unittest.skipUnless(os.environ.get('CRABBER_TEST_REAL_SCCACHE'), 'explicit pinned real-tool probe')
    def test_real_rust_library_hit_survives_closed_snapshot_and_fresh_target(self):
        env = {**os.environ, 'CRABBER_COMPILER_CACHE': 'local', 'CARGO_INCREMENTAL': '0',
               'CRABBER_COMPILER_CACHE_DIR': str(self.base),
               'CRABBER_SCCACHE_PATH': os.environ.get('CRABBER_TEST_REAL_SCCACHE', '')}
        config = cache.configuration(ROOT, env)
        rustc = subprocess.check_output(['rustup', 'which', 'rustc'], text=True).strip()
        source = self.root / 'lib.rs'
        source.write_text('pub fn answer() -> u32 { 42 }\n')
        application = self.root / 'main.rs'
        application.write_text('fn main() { println!("{}", cache_fixture::answer()); }\n')
        results = []
        for number in range(3):
            if number == 2:
                source.write_text('pub fn answer() -> u32 { 43 }\n')
            session = cache.CompilerCacheSession(config)
            try:
                session.start()
                target = self.root / f'fresh-target-{number}'
                target.mkdir()
                clients = session.clients(env)
                subprocess.run([str(config.executable), rustc, str(source), '--crate-type', 'rlib',
                                '--crate-name', 'cache_fixture', '--emit=dep-info,link', '--out-dir', str(target)],
                               env=clients, check=True, timeout=30, capture_output=True)
                executable = target / 'app'
                subprocess.run([rustc, str(application), '--extern', f'cache_fixture={target}/libcache_fixture.rlib',
                                '-o', str(executable)], env=cache.clean_environment(env), check=True,
                               timeout=30, capture_output=True)
                results.append(subprocess.check_output([str(executable)], text=True).strip())
                self.assertTrue(session.close('passed'))
                self.assertEqual(session.server.returncode, -signal.SIGTERM)
                if number == 1:
                    self.assertGreater(session.report['stats']['cache_hits']['counts'].get('Rust', 0), 0)
            finally:
                if session.server is not None and session.server.poll() is None:
                    session.close('failed', publish=False)
        self.assertEqual(results, ['42', '42', '43'])
        uncached = self.root / 'uncached'
        uncached.mkdir()
        subprocess.run([rustc, str(source), '--crate-type', 'rlib', '--crate-name', 'cache_fixture',
                        '--out-dir', str(uncached)], check=True, timeout=30, capture_output=True)
        subprocess.run([rustc, str(application), '--extern', f'cache_fixture={uncached}/libcache_fixture.rlib',
                        '-o', str(uncached / 'app')], check=True, timeout=30, capture_output=True)
        self.assertEqual(subprocess.check_output([str(uncached / 'app')], text=True).strip(), results[-1])

    @unittest.skipUnless(Path('/proc/self/stat').exists() and os.environ.get('CRABBER_TEST_REAL_SCCACHE'),
                         'explicit pinned Clippy/wrapper probe on Linux')
    def test_real_clippy_preserves_denied_warnings_and_external_lint_configuration(self):
        fixture = self.root / 'clippy-inputs'
        (fixture / 'src').mkdir(parents=True)
        (fixture / 'Cargo.toml').write_text(
            '[workspace]\n[package]\nname="clippy-input-fixture"\nversion="0.1.0"\nedition="2024"\n')
        source = fixture / 'src/lib.rs'
        warning = '#![warn(clippy::needless_return)]\npub fn answer() -> u32 { return 42; }\n'
        source.write_text(warning)
        for relative in ['.github/scripts/compiler-cache.py', '.github/scripts/compiler-cache-wrapper',
                         '.config/test-tools.json', 'rust-toolchain.toml']:
            destination = fixture / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / relative, destination)
        env = cache.clean_environment(dict(os.environ))
        for name in ['CLIPPY_ARGS', 'CLIPPY_CONF_DIR']:
            env.pop(name, None)
        env.update(CARGO_INCREMENTAL='0', CRABBER_SCCACHE_PATH=os.environ['CRABBER_TEST_REAL_SCCACHE'],
                   CRABBER_COMPILER_CACHE_DIR=str(self.base))
        subprocess.run(['cargo', 'generate-lockfile', '--offline'], cwd=fixture, env=env,
                       check=True, capture_output=True, timeout=10)
        for arguments in [['init', '-q'], ['remote', 'add', 'origin', 'compiler-clippy-fixture'], ['add', '.'],
                          ['-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', 'commit', '-qm', 'fixture']]:
            subprocess.run(['git', *arguments], cwd=fixture, check=True, capture_output=True, timeout=10)
        configuration = self.root / 'clippy-configuration'
        configuration.mkdir()
        (configuration / 'clippy.toml').write_text('disallowed-names = ["forbidden"]\n')
        cases = [
            ('allowed-warning', warning, {}, True, False),
            ('warning', warning, {}, False, True),
            ('configuration', 'pub fn answer(forbidden: u32) -> u32 { forbidden }\n',
             {'CLIPPY_CONF_DIR': str(configuration)}, False, True),
            ('clean', 'pub fn answer(value: u32) -> u32 { value }\n', {}, True, True),
        ]
        seed = None
        for name, code, extra, success, deny in cases:
            source.write_text(code)
            for mode in ['off', 'local']:
                with self.subTest(case=name, mode=mode):
                    command = ['cargo', 'clippy', '--offline', '--locked', '--target-dir',
                               str(self.root / f'{name}-{mode}')]
                    if deny:
                        command += ['--', '-D', 'warnings']
                    report = self.root / f'{name}-{mode}.json'
                    result = check.supervise(command, fixture, report, budget=30, reserve=5,
                                             env={**env, **extra, 'CRABBER_COMPILER_CACHE': mode})
                    record = json.loads(report.read_text())
                    expected = 0 if success else 101
                    self.assertEqual(result, expected, f'Clippy {name}, cache {mode}, exit {result}')
                    self.assertIsNone(record['reason'])
                    self.assertEqual(record['child_exit_code'], expected)
                    if mode == 'local':
                        local = record['compiler_cache']
                        self.assertTrue(local['cleanup_verified'])
                        if name == 'allowed-warning':
                            self.assertTrue(record['source_clean'])
                            seed = local['published']
                            self.assertIsInstance(seed, str)
                        elif name == 'warning':
                            self.assertTrue(record['source_clean'])
                            self.assertEqual(local['restored']['snapshot'], seed)
                            self.assertEqual(local['restored']['check_status'], 'passed')

    @unittest.skipUnless(Path('/proc/self/stat').exists() and os.environ.get('CRABBER_TEST_REAL_SCCACHE'),
                         'explicit pinned Cargo/wrapper probe on Linux')
    def test_real_cargo_wrapper_preserves_macro_directory_build_env_flags_features_profile_and_source(self):
        fixture = self.root / 'cargo-inputs'
        def write(relative, source):
            path = fixture / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(source)
        write('Cargo.toml', '''[workspace]
members = ["dependency", "plain", "macro-input"]
resolver = "2"
[package]
name = "input-fixture"
version = "0.1.0"
edition = "2024"
[features]
alternate = ["dependency/alternate"]
[dependencies]
dependency = { path = "dependency" }
plain = { path = "plain" }
''')
        write('src/main.rs', 'fn main(){println!("{}:{}",dependency::value(),plain::answer());}\n')
        write('plain/Cargo.toml', '[package]\nname="plain"\nversion="0.1.0"\nedition="2024"\n')
        write('plain/src/lib.rs', 'pub fn answer()->u32{42}\n')
        write('dependency/Cargo.toml', '''[package]
name = "dependency"
version = "0.1.0"
edition = "2024"
[features]
alternate = []
[dependencies]
macro-input = { path = "../macro-input" }
''')
        write('dependency/build.rs', '''fn main() {
println!("cargo:rerun-if-changed=build-input");
println!("cargo:rustc-env=CRABBER_COMPILED_SHA={}",std::fs::read_to_string("build-input").unwrap());
println!("cargo:rerun-if-env-changed=INPUT_VALUE");
println!("cargo:rustc-env=CRABBER_BUILD_SHA={}",std::env::var("INPUT_VALUE").unwrap());
println!("cargo:rerun-if-changed=macro-inputs");
}''')
        write('dependency/src/lib.rs', '''pub fn value()->String {
format!("{}:{}:{}:{}:{}:{}:{}",include_str!("../source-input"),env!("CRABBER_COMPILED_SHA"),
env!("CRABBER_BUILD_SHA"),macro_input::files!(),cfg!(feature="alternate"),cfg!(cache_flag),cfg!(debug_assertions))
}''')
        write('dependency/source-input', 'source-a')
        write('dependency/build-input', 'build-a')
        write('dependency/macro-inputs/a', 'macro-a')
        write('macro-input/Cargo.toml', '[package]\nname="macro-input"\nversion="0.1.0"\nedition="2024"\n[lib]\nproc-macro=true\n')
        write('macro-input/src/lib.rs', '''extern crate proc_macro;
use proc_macro::TokenStream;
#[proc_macro] pub fn files(_:TokenStream)->TokenStream {
let root=std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap()).join("macro-inputs");
let mut paths=std::fs::read_dir(root).unwrap().map(|entry|entry.unwrap().path()).collect::<Vec<_>>(); paths.sort();
let tokens=paths.iter().map(|path|format!("include_str!({:?})",path)).collect::<Vec<_>>().join(",");
format!("concat!({tokens})").parse().unwrap()
}''')
        for relative in ['.github/scripts/compiler-cache.py', '.github/scripts/compiler-cache-wrapper',
                         '.config/test-tools.json', 'rust-toolchain.toml']:
            destination = fixture / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / relative, destination)
        toolchain = json.loads((ROOT / '.config/test-tools.json').read_text())['rust']['version']
        prepared = {**os.environ, 'RUSTUP_TOOLCHAIN': toolchain, 'RUSTUP_AUTO_INSTALL': '0'}
        subprocess.run(['cargo', 'generate-lockfile', '--offline'], cwd=fixture, env=prepared,
                       check=True, capture_output=True, timeout=10)
        for arguments in [['init', '-q'], ['remote', 'add', 'origin', 'compiler-input-fixture'], ['add', '.'],
                          ['-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', 'commit', '-qm', 'fixture']]:
            subprocess.run(['git', *arguments], cwd=fixture, check=True, capture_output=True, timeout=10)
        reports = self.root / 'cargo-input-reports'
        reports.mkdir()
        for step in range(10):
            if step == 2:
                write('dependency/source-input', 'source-b')
            elif step == 3:
                write('dependency/build-input', 'build-b')
            elif step == 4:
                write('dependency/macro-inputs/a', 'macro-b')
            elif step == 5:
                write('dependency/macro-inputs/added', 'new-macro-file')
            env = {name: value for name, value in prepared.items() if name not in {'RUSTFLAGS', 'CARGO_ENCODED_RUSTFLAGS'}}
            env.update(CARGO_INCREMENTAL='0', CRABBER_SCCACHE_PATH=os.environ['CRABBER_TEST_REAL_SCCACHE'],
                       CRABBER_COMPILER_CACHE_DIR=str(self.base), INPUT_VALUE='env-b' if step >= 9 else 'env-a',
                       RUSTFLAGS='--check-cfg=cfg(cache_flag)' + (' --cfg cache_flag' if step >= 7 else ''))
            outputs = []
            for mode in ['local', 'off']:
                target = self.root / f'cargo-{step}-{mode}'
                command = ['cargo', 'build', '--offline', '--locked', '--target-dir', str(target)]
                if step >= 6:
                    command += ['--features', 'alternate']
                if step >= 8:
                    command += ['--release']
                report = reports / f'{step}-{mode}.json'
                code = check.supervise(command, fixture, report, budget=60, reserve=5,
                                       env={**env, 'CRABBER_COMPILER_CACHE': mode})
                self.assertEqual(code, 0, f'Cargo input step {step}, mode {mode}')
                executable = target / ('release' if step >= 8 else 'debug') / 'input-fixture'
                outputs.append(subprocess.check_output([str(executable)], text=True, timeout=5))
                if step == 1 and mode == 'local':
                    stats = json.loads(report.read_text())['compiler_cache']['stats']
                    self.assertGreater(stats['cache_hits']['counts']['Rust'], 0)
            self.assertEqual(outputs[0], outputs[1], f'Cargo input step {step}')
            if step >= 5:
                self.assertIn('new-macro-file', outputs[0])
            if step >= 9:
                self.assertIn('env-b', outputs[0])

    @unittest.skipUnless(Path('/proc/self/stat').exists(), 'Linux ownership supervisor')
    def test_dead_server_cannot_admit_successful_client(self):
        command = [sys.executable, '-I', '-S', '-c',
                   'import json,os,signal; d=json.load(open(os.environ["CRABBER_COMPILER_CACHE_SESSION"])); os.kill(d["server_pid"],signal.SIGKILL)']
        with mock.patch.object(check.compiler_cache, 'configuration', return_value=self.fake_config()):
            code = check.supervise(command, ROOT, self.root / 'check.json', budget=5, reserve=1)
        result = json.loads((self.root / 'check.json').read_text())
        self.assertNotEqual(code, 0)
        self.assertEqual(result['reason'], 'compiler-cache-server-failed')
        self.assertIsNone(result['compiler_cache']['published'])

    @unittest.skipUnless(Path('/proc/self/stat').exists(), 'Linux ownership supervisor')
    def test_timeout_drains_client_compiler_group_and_server(self):
        marker = self.root / 'late-output'
        command = [sys.executable, '-I', '-S', '-c',
                   f'import subprocess,sys,time; subprocess.Popen([sys.executable,"-c",{("import time,pathlib; time.sleep(3); pathlib.Path(" + repr(str(marker)) + ").write_text(\"late\")")!r}]); time.sleep(30)']
        with mock.patch.object(check.compiler_cache, 'configuration', return_value=self.fake_config()):
            code = check.supervise(command, ROOT, self.root / 'timeout.json', budget=2, reserve=.5)
        result = json.loads((self.root / 'timeout.json').read_text())
        self.assertEqual(code, 124)
        self.assertTrue(result['compiler_cache']['cleanup_verified'])
        self.assertFalse(marker.exists())


class QualificationProtocolTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.checkout, self.output = self.root / 'checkout', self.root / 'reports'
        self.checkout.mkdir()
        self.output.mkdir()
        self.calls = []
        self.resources = {'cargo_build_jobs': 4, 'cpu_affinity': [0, 1, 2, 3], 'cgroup_limits': {}}
        resources = mock.patch.object(qualify, 'qualification_resources', return_value=self.resources)
        resources.start()
        self.addCleanup(resources.stop)

    def run_conditions(self, failures=None, trials=1):
        failures = failures or {}
        def execute(command, cwd, env, log):
            role = log.parent.name.split('-', 1)[1]
            pair = int(log.parent.name.split('-', 1)[0])
            self.calls.append((role, command, env.copy()))
            self.assertEqual(cwd, self.checkout)
            self.assertNotIn('--part', command)
            self.assertNotIn('--lane', command)
            self.assertEqual(command[command.index('--budget-seconds') + 1], '600')
            self.assertEqual(env['CARGO_INCREMENTAL'], '0')
            target = Path(env['CARGO_TARGET_DIR'])
            self.assertEqual(env['CARGO_BUILD_BUILD_DIR'], str(target))
            self.assertFalse(any(target.iterdir()))
            (target / 'compiled-marker').write_text(role)
            fixture = self.checkout / 'fixtures/wasm/target'
            self.assertFalse(fixture.exists())
            fixture.mkdir(parents=True)
            (fixture / 'compiled-marker').write_text(role)
            fault = failures.get(role)
            status = 'failed' if fault in {'timeout', 'failure', 'server-failed'} else 'passed'
            local = {'version': 1, 'mode': 'off'} if role == 'off' else {
                'version': 1, 'mode': 'local', 'namespace': 'd' * 64,
                'restored': {'snapshot': f'{pair * 2:032x}', 'check_status': 'failed' if failures.get('empty') == 'timeout' else 'passed'} if role == 'warm' else None,
                'published': f'{pair * 2 + (role == "warm"):032x}', 'state': 'closed', 'cleanup_verified': True,
                'stats': {'cache_hits': {'counts': {'Rust': 0 if fault == 'no-hits' else 1}}}}
            if fault == 'server-failed':
                local.update(state='failed', published=None, cleanup_verified=False)
            if fault == 'wrong-seed':
                local['restored']['snapshot'] = '0' * 32
            result = {'status': status, 'reason': 'budget-expired' if fault == 'timeout' else None,
                      'coverage_scope': 'partial' if fault == 'partial' else 'complete', 'budget_seconds': 600,
                      'budget_breached': fault == 'timeout', 'timing_target_met': status == 'passed',
                      'source_revision': 'a' * 40, 'end_revision': 'a' * 40,
                      'source_clean': fault != 'dirty', 'end_clean': True,
                      'wall_seconds': 600 if fault == 'timeout' else .001,
                      'cpu_count': 4, 'cpu_affinity_count': 4, 'physical_memory_bytes': 8 * 1024 ** 3,
                      'cgroup_limits': {}, 'cargo_profiles': {},
                      'profile': {'CARGO_INCREMENTAL': '0', 'CARGO_BUILD_JOBS': '4'}, 'compiler_cache': local}
            changed = {'context-cpu-count': ('cpu_count', 8),
                       'context-affinity': ('cpu_affinity_count', 2),
                       'context-memory': ('physical_memory_bytes', 4 * 1024 ** 3),
                       'context-limits': ('cgroup_limits', {'memory.max': '4294967296'}),
                       'context-profile': ('profile', {'CARGO_INCREMENTAL': '0', 'CARGO_BUILD_JOBS': '4',
                                                       'CARGO_PROFILE_TEST_OPT_LEVEL': '1'}),
                       'context-cargo-profiles': ('cargo_profiles', {'test': {'opt-level': 1}})}
            if fault in changed:
                field, value = changed[fault]
                result[field] = value
            cache.write_json(Path(command[command.index('--report') + 1]), result)
            return 124 if fault == 'timeout' else (1 if status == 'failed' else 0)
        with mock.patch.object(qualify, 'run_check', side_effect=execute), contextlib.redirect_stdout(io.StringIO()):
            return qualify.conditions(self.checkout, self.output, trials, {'CARGO_BUILD_JOBS': '4'}, 'a' * 40, self.resources)

    def test_pilot_uses_full_schedule_fresh_targets_and_explicitly_never_qualifies(self):
        result = self.run_conditions()
        self.assertEqual(result['status'], 'passed')
        self.assertTrue(result['pilot'])
        self.assertFalse(result['qualified'])
        self.assertEqual([role for role, _, _ in self.calls], ['off', 'empty', 'warm'])
        self.assertEqual(len({env['CARGO_TARGET_DIR'] for _, _, env in self.calls}), 3)
        self.assertEqual([env['CRABBER_COMPILER_CACHE'] for _, _, env in self.calls], ['off', 'local', 'local'])
        self.assertEqual((self.output / '01-off/wasm-target/compiled-marker').read_text(), 'off')
        self.assertEqual((self.output / '01-empty/wasm-target/compiled-marker').read_text(), 'empty')

    def test_bounded_failed_off_and_seed_allow_exactly_one_planned_warm(self):
        result = self.run_conditions({'off': 'timeout', 'empty': 'timeout'})
        self.assertEqual(result['status'], 'passed')
        self.assertEqual([role for role, _, _ in self.calls], ['off', 'empty', 'warm'])
        self.assertEqual(sum(case['exit_code'] == 124 for case in result['cases']), 2)
        self.assertFalse(result['qualified'])

    def test_first_warm_failure_stops_all_future_trials(self):
        result = self.run_conditions({'warm': 'failure'}, trials=5)
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(len(self.calls), 3)
        self.assertFalse(result['qualified'])

    def test_pairs_alternate_the_off_control_before_and_after_seed_warm(self):
        result = self.run_conditions(trials=2)
        self.assertEqual(result['status'], 'passed')
        self.assertEqual([role for role, _, _ in self.calls], ['off', 'empty', 'warm', 'empty', 'warm', 'off'])
        self.assertEqual(result['pair_orders'], [{'pair': 1, 'roles': ['off', 'empty', 'warm']},
                                                {'pair': 2, 'roles': ['empty', 'warm', 'off']}])

    def test_missing_report_and_exception_preserve_the_failed_attempt_census(self):
        for outcome in [7, RuntimeError('injected runner failure')]:
            with self.subTest(outcome=type(outcome).__name__), tempfile.TemporaryDirectory(dir=self.root) as temporary:
                output = Path(temporary)
                effect = outcome if isinstance(outcome, Exception) else None
                with mock.patch.object(qualify, 'run_check', return_value=outcome, side_effect=effect), \
                        contextlib.redirect_stdout(io.StringIO()):
                    result = qualify.conditions(self.checkout, output, 5, {'CARGO_BUILD_JOBS': '4'}, 'a' * 40, self.resources)
                self.assertEqual(result['status'], 'failed')
                self.assertEqual(len(result['cases']), 1)
                self.assertEqual(result['cases'][0]['role'], 'off')
                self.assertEqual(result['cases'][0]['exit_code'], None if effect else 7)
                self.assertIn('missing-or-invalid-report', result['cases'][0]['reason'])
                self.assertIsNone(result['cases'][0]['measurement'])
                shutil.rmtree(self.checkout / 'target')

    def test_cancellation_is_recorded_as_a_failed_attempt_with_the_original_signal(self):
        with mock.patch.object(qualify, 'run_check', side_effect=qualify.Cancelled(signal.SIGTERM)), \
                contextlib.redirect_stdout(io.StringIO()):
            result = qualify.conditions(self.checkout, self.output, 5, {'CARGO_BUILD_JOBS': '4'}, 'a' * 40, self.resources)
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(len(result['cases']), 1)
        self.assertEqual(result['cases'][0]['exit_code'], 143)
        self.assertEqual(result['cases'][0]['interrupted_signal'], signal.SIGTERM)

    def test_preexisting_wasm_target_is_preserved_and_the_refused_attempt_is_recorded(self):
        fixture = self.checkout / 'fixtures/wasm/target'
        fixture.mkdir(parents=True)
        (fixture / 'sentinel').write_text('keep')
        result = self.run_conditions()
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(len(self.calls), 0)
        self.assertEqual(len(result['cases']), 1)
        self.assertIsNone(result['cases'][0]['exit_code'])
        self.assertEqual((fixture / 'sentinel').read_text(), 'keep')

    def test_unverified_seed_cleanup_blocks_warm_instead_of_retrying(self):
        result = self.run_conditions({'empty': 'server-failed'})
        self.assertEqual(result['status'], 'failed')
        self.assertEqual([role for role, _, _ in self.calls], ['off', 'empty'])

    def test_missing_real_library_hit_blocks_performance_success(self):
        result = self.run_conditions({'warm': 'no-hits'})
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(len(self.calls), 3)

    def test_foreign_seed_cannot_supply_warm_credit(self):
        result = self.run_conditions({'warm': 'wrong-seed'})
        self.assertEqual(result['status'], 'failed')

    def test_partial_or_dirty_source_is_never_an_off_control(self):
        for fault in ['partial', 'dirty', 'failure']:
            with self.subTest(fault=fault), tempfile.TemporaryDirectory(dir=self.root) as temporary:
                output = Path(temporary)
                with mock.patch.object(self, 'output', output):
                    result = self.run_conditions({'off': fault})
                self.assertEqual(result['status'], 'failed')
                self.assertEqual(result['cases'][-1]['role'], 'off')
                self.calls.clear()
                shutil.rmtree(self.checkout / 'target')
                shutil.rmtree(self.checkout / 'fixtures')

    def test_static_resource_and_profile_changes_stop_and_preserve_the_case(self):
        for fault in ['context-cpu-count', 'context-affinity', 'context-memory', 'context-limits',
                      'context-profile', 'context-cargo-profiles']:
            with self.subTest(fault=fault), tempfile.TemporaryDirectory(dir=self.root) as temporary:
                with mock.patch.object(self, 'output', Path(temporary)):
                    result = self.run_conditions({'empty': fault}, trials=5)
                self.assertEqual(result['status'], 'failed')
                self.assertEqual([case['role'] for case in result['cases']], ['off', 'empty'])
                self.assertEqual(result['cases'][-1]['reason'], 'resource-context-changed')
                self.assertEqual(result['resources'], self.resources)
                self.calls.clear()
                shutil.rmtree(self.checkout / 'target')
                shutil.rmtree(self.checkout / 'fixtures')

    def test_live_affinity_change_stops_after_the_recorded_attempt(self):
        changed = {**self.resources, 'cpu_affinity': [4, 5, 6, 7]}
        with mock.patch.object(qualify, 'qualification_resources', return_value=changed):
            result = self.run_conditions(trials=5)
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(len(result['cases']), 1)
        self.assertEqual(result['cases'][0]['observed_resources'], changed)
        self.assertEqual(result['cases'][0]['reason'], 'resource-context-changed')

    def test_default_is_five_trials_in_a_new_detached_worktree(self):
        output = self.root / 'new-experiment'
        def git(command, **_kwargs):
            return '' if command[1] == 'status' else 'a' * 40
        with mock.patch.object(qualify.sys, 'platform', 'linux'), \
                mock.patch.object(qualify.subprocess, 'check_output', side_effect=git), \
                mock.patch.object(qualify.cache, 'verify_executable'), \
                mock.patch.object(qualify.subprocess, 'run') as create, \
                mock.patch.object(qualify, 'conditions', return_value={'status': 'passed'}) as run, \
                mock.patch.dict(os.environ, {'CARGO_BUILD_JOBS': '4'}), \
                mock.patch.object(sys, 'argv', ['qualify-compiler-cache.py', '--output', str(output), '--offline']), \
                mock.patch.object(qualify.signal, 'signal'), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(qualify.main(), 0)
        self.assertEqual(run.call_args.args[2], 5)
        self.assertEqual(create.call_args.args[0][1:4], ['worktree', 'add', '--detach'])
        self.assertEqual(run.call_args.args[0], output / 'checkout')
        self.assertEqual(run.call_args.args[5], self.resources)


class QualificationResourceTests(unittest.TestCase):
    def test_explicit_positive_cargo_job_limit_is_required(self):
        for value in [None, '', '0', '-1', 'auto', '4.0', ' 4', '４']:
            with self.subTest(value=value):
                env = {} if value is None else {'CARGO_BUILD_JOBS': value}
                with self.assertRaisesRegex(ValueError, 'explicit positive CARGO_BUILD_JOBS'):
                    qualify.qualification_resources(env)

    def test_spare_cpu_capacity_is_refused_and_a_matching_affinity_is_recorded(self):
        with mock.patch.object(qualify.os, 'sched_getaffinity', return_value={0, 1, 2, 3}, create=True):
            with self.assertRaisesRegex(ValueError, 'CPU affinity must not exceed'):
                qualify.qualification_resources({'CARGO_BUILD_JOBS': '2'})
            resources = qualify.qualification_resources({'CARGO_BUILD_JOBS': '4'})
        self.assertEqual(resources['cargo_build_jobs'], 4)
        self.assertEqual(resources['cpu_affinity'], [0, 1, 2, 3])
        self.assertIsInstance(resources['cgroup_limits'], dict)

    def test_capacity_refusal_precedes_tool_probes_and_worktree_creation(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / 'never-created'
            with mock.patch.object(qualify.sys, 'platform', 'linux'), \
                    mock.patch.object(qualify.subprocess, 'check_output', side_effect=['', 'a' * 40]), \
                    mock.patch.dict(os.environ, {'CARGO_BUILD_JOBS': '2'}), \
                    mock.patch.object(qualify.os, 'sched_getaffinity', return_value={0, 1, 2, 3}, create=True), \
                    mock.patch.object(qualify.cache, 'verify_executable') as verify, \
                    mock.patch.object(qualify.subprocess, 'run') as create, \
                    mock.patch.object(sys, 'argv', ['qualify-compiler-cache.py', '--output', str(output), '--offline']):
                with self.assertRaisesRegex(ValueError, 'CPU affinity must not exceed'):
                    qualify.main()
            verify.assert_not_called()
            create.assert_not_called()
            self.assertFalse(output.exists())


if __name__ == '__main__':
    unittest.main()
