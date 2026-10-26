"""Credential-free failure controls for the hosted Native transport boundary."""
import base64
import copy
import importlib.util
import io
import json
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
import zipfile

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('native_ci', Path(__file__).with_name('native-ci.py'))
native = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = native
spec.loader.exec_module(native)


class NativeTransportTests(unittest.TestCase):
    def test_planner_cli_leaves_its_checkout_unchanged(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            checkout = directory / 'checkout'
            scripts = checkout / '.github/scripts'
            scripts.mkdir(parents=True)
            for name in ['native-ci.py', 'cache-use.py']:
                (scripts / name).write_bytes(Path(__file__).with_name(name).read_bytes())
            (checkout / '.github/native-workers.json').write_text(json.dumps(native.slots()))
            before = {str(path.relative_to(checkout)): path.read_bytes()
                      for path in checkout.rglob('*') if path.is_file()}
            result = subprocess.run(
                [sys.executable, '-I', '-S', str(scripts / 'native-ci.py'),
                 'plan', '--mode', 'x64', '--source', 'a' * 40],
                env=native.os.environ | {'GITHUB_OUTPUT': str(directory / 'outputs')},
                capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(
                {str(path.relative_to(checkout)): path.read_bytes()
                 for path in checkout.rglob('*') if path.is_file()}, before)

    def setUp(self):
        self.identity = native.Identity('owner/repo', 42, 2, 'a' * 40)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.repository = {'id': 7, 'full_name': 'owner/repo'}
        self.workflow = {'id': 9, 'path': native.WORKFLOW}
        self.run = {'id': 42, 'run_attempt': 2, 'head_sha': 'a' * 40, 'event': 'workflow_dispatch',
                    'workflow_id': 9, 'repository': {'id': 7}, 'head_repository': {'id': 7},
                    'path': native.WORKFLOW}
        self.artifact = {'id': 11, 'name': self.identity.artifact_name('x64', 'native-library-1'),
                         'expired': False, 'size_in_bytes': 123, 'digest': 'sha256:' + 'b' * 64,
                         'workflow_run': {'id': 42, 'repository_id': 7, 'head_repository_id': 7,
                                          'head_sha': 'a' * 40}}
        self.job = {'id': 12, 'name': native.worker_name('x64', 'native-library-1'),
                    'run_id': 42, 'run_attempt': 2, 'head_sha': 'a' * 40,
                    'labels': ['ubuntu-latest'], 'status': 'completed', 'conclusion': 'success',
                    'completed_at': '2026-10-26T00:02:00Z', 'steps': [{
                        'name': native.CHECK_STEP, 'status': 'completed', 'conclusion': 'success',
                        'started_at': '2026-10-26T00:01:00Z', 'completed_at': '2026-10-26T00:01:30Z'}]}

    def test_context_uses_canonical_repository_route_and_rejects_sibling_paths(self):
        prefix = '/repos/owner/repo'
        routes = {prefix: self.repository,
                  prefix + '/actions/workflows/ci.yml': self.workflow,
                  prefix + '/actions/runs/42/attempts/2': self.run,
                  prefix + '/actions/runs/42/attempts/2/jobs?per_page=100&page=1':
                      {'jobs': [self.job], 'total_count': 1}}

        def respond(request, timeout):
            path = request.full_url.removeprefix('https://api.github.com')
            self.assertIn(path, routes, 'unrecognized GitHub API route')
            return io.BytesIO(json.dumps(routes[path]).encode())

        with mock.patch.dict(native.os.environ, {'GH_TOKEN': 'credential-free-control'}):
            api = native.GitHub(self.identity, self.root)
        with mock.patch.object(api.opener, 'open', side_effect=respond) as opened:
            self.assertEqual(api.context(), (self.repository, self.workflow, self.run, [self.job]))
            self.assertEqual(opened.call_count, 4)
            for endpoint in ['/repos/owner/repository', '/repos/owner/repo-other/jobs']:
                with self.assertRaises(ValueError):
                    api.request(endpoint)
            self.assertEqual(opened.call_count, 4)

    def test_run_context_rejects_foreign_identity(self):
        native.validate_context(self.identity, self.repository, self.workflow, self.run)
        for key, value in [('id', 43), ('run_attempt', 1), ('head_sha', 'c' * 40),
                           ('workflow_id', 10), ('event', 'push'), ('path', 'other.yml'),
                           ('head_repository', {'id': 8}), ('repository', {'id': 8})]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                native.validate_context(self.identity, self.repository, self.workflow, dict(self.run, **{key: value}))

    def test_attempt_jobs_require_success_and_exact_context(self):
        native.admit_job(self.identity, [self.job], self.job['name'], 'x64')
        for key, value in [('run_attempt', 1), ('run_id', 41), ('head_sha', 'b' * 40),
                           ('status', 'in_progress'), ('conclusion', 'skipped'), ('conclusion', 'failure'),
                           ('labels', ['ubuntu-24.04-arm']), ('completed_at', None)]:
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                native.admit_job(self.identity, [dict(self.job, **{key: value})], self.job['name'], 'x64')
        for jobs in [[], [self.job, self.job]]:
            with self.assertRaises(ValueError):
                native.admit_job(self.identity, jobs, self.job['name'], 'x64')

    def test_artifacts_reject_missing_duplicate_foreign_and_prior_attempt_name(self):
        name = self.artifact['name']
        native.admit_artifact(self.identity, 7, [self.artifact], name)
        for candidates in [[], [self.artifact, self.artifact],
                           [dict(self.artifact, name=name.replace('-42-2-', '-42-1-'))]]:
            with self.assertRaises(ValueError):
                native.admit_artifact(self.identity, 7, candidates, name)
        for key, value in [('expired', True), ('digest', ''), ('digest', 'md5:' + 'b' * 32)]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                native.admit_artifact(self.identity, 7, [dict(self.artifact, **{key: value})], name)
        for key, value in [('id', 41), ('head_sha', 'c' * 40), ('repository_id', 8), ('head_repository_id', 8)]:
            changed = dict(self.artifact, workflow_run=dict(self.artifact['workflow_run'], **{key: value}))
            with self.subTest(key=key), self.assertRaises(ValueError):
                native.admit_artifact(self.identity, 7, [changed], name)
        self.assertIn('source-trusted correlation', native.CORRELATION)
        self.assertNotIn('run_attempt', self.artifact)

    def test_server_digest_is_checked_over_raw_archive_bytes(self):
        raw = b'server archive representation'
        artifact = dict(self.artifact, size_in_bytes=len(raw), digest='sha256:' + native.sha256(raw))
        native.verify_archive(raw, artifact)
        for changed in [raw + b'x', raw[:-1]]:
            with self.assertRaises(ValueError):
                native.verify_archive(changed, artifact)
        with self.assertRaises(ValueError):
            native.verify_archive(raw, dict(artifact, size_in_bytes=len(raw) + 1))

    def archive(self, entries):
        path = self.root / 'archive.zip'
        with zipfile.ZipFile(path, 'w') as archive:
            for name, value in entries:
                archive.writestr(name, value)
        return path

    def test_safe_archive_relocates_and_rejects_reused_destination(self):
        path = self.archive([('bundle/reports/check.json', '{}'), ('ci-envelope.json', '{}')])
        destination = self.root / 'relocated'
        native.extract_archive(path, destination)
        self.assertEqual((destination / 'bundle/reports/check.json').read_text(), '{}')
        with self.assertRaises(ValueError):
            native.extract_archive(path, destination)

    def test_unsafe_zip_members_and_file_directory_overlap_are_rejected(self):
        for name in ['../escape', '/absolute', 'a/../escape', 'a/./file', 'a//file', 'a\\file', 'C:/file']:
            with self.subTest(name=name), self.assertRaises(ValueError):
                native.extract_archive(self.archive([(name, 'x')]), self.root / 'output')
        for entries in [[('a', 'x'), ('a/b', 'x')], [('a', 'x'), ('a', 'y')]]:
            with self.assertRaises(ValueError):
                native.extract_archive(self.archive(entries), self.root / 'output')

    def test_symlinks_special_files_and_expansion_limits_are_rejected(self):
        for kind in [stat.S_IFLNK, stat.S_IFIFO, stat.S_IFSOCK]:
            entry = zipfile.ZipInfo('link')
            entry.create_system = 3
            entry.external_attr = (kind | 0o777) << 16
            with self.assertRaises(ValueError):
                native.extract_archive(self.archive([(entry, 'target')]), self.root / 'output')
        path = self.archive([('regular', 'abc')])
        with mock.patch.object(native, 'MAX_EXPANDED', 2), self.assertRaises(ValueError):
            native.extract_archive(path, self.root / 'output')

    def test_encrypted_archive_is_rejected_before_extraction(self):
        path = self.archive([('regular', 'value')])
        raw = bytearray(path.read_bytes())
        for signature, offset in [(b'PK\x03\x04', 6), (b'PK\x01\x02', 8)]:
            start = raw.index(signature)
            raw[start + offset] |= 1
        path.write_bytes(raw)
        with self.assertRaises(ValueError):
            native.extract_archive(path, self.root / 'output')
        self.assertFalse((self.root / 'output').exists())

    def test_redirect_does_not_forward_credentials(self):
        request = native.urllib.request.Request('https://api.github.com/archive',
                                                headers={'Authorization': 'Bearer fixture'})
        redirected = native.SafeRedirect().redirect_request(request, None, 302, 'Found', {}, 'https://blob.example/file')
        self.assertFalse(redirected.has_header('Authorization'))
        with self.assertRaises(ValueError):
            native.SafeRedirect().redirect_request(request, None, 302, 'Found', {}, 'http://blob.example/file')

    def worker_fixture(self, result='disabled', slot='native-library-1', directory=None):
        directory = directory or self.root
        bundle = directory / 'bundle'
        (bundle / 'reports').mkdir(parents=True, exist_ok=True)
        compiler = b'rustc fixture\nhost: x86_64-unknown-linux-gnu\nrelease: fixture\n'
        index = {'source': {'revision': self.identity.source, 'tree': 'b' * 40,
                            'rustc': compiler.decode().strip()},
                 'recorded_roots': {'workspace': '/original/worker'}}
        (bundle / 'bundle.json').write_text(json.dumps(index))
        wrapper = {'coverage_scope': slot, 'profile': native.PROFILE,
                   'source_revision': self.identity.source, 'end_revision': self.identity.source,
                   'source_clean': True, 'end_clean': True, 'cache_state': 'unclassified',
                   'status': 'passed', 'platform': ['Linux', 'host', 'release', 'version', 'x86_64']}
        (bundle / 'reports/check.json').write_text(json.dumps(wrapper))
        compiler_hash = native.sha256(compiler)
        base = 'rust-Linux-X64-' + compiler_hash + '-dev0-test0-strip-symbols-inc0-combined-v2-'
        graph = base + 'c' * 64 + '-'
        context = {'repository': self.identity.repository, 'run': self.identity.run,
                   'attempt': self.identity.attempt, 'source': self.identity.source,
                   'tree': index['source']['tree'], 'workspace': index['recorded_roots']['workspace'],
                   'runner_os': 'Linux', 'runner_arch': 'X64', 'profile': native.PROFILE.copy(),
                   'compiler_stdout_base64': base64.b64encode(compiler).decode(),
                   'compiler_sha256': compiler_hash,
                   'keys': {'base': base, 'graph': graph, 'graph_hash': 'c' * 64,
                            'primary': graph + self.identity.source + '-42-2'}}
        disabled = result == 'disabled'
        receipt = {'version': 1, 'status': 'finalized', 'context': context,
                   'request': 'off' if disabled else 'restore-compatible', 'result': result,
                   'action': {'outcome': 'skipped' if disabled else 'success',
                              'matched_key': context['keys']['primary'] if result == 'restored' else ''},
                   'inputs': {'marker': {'kind': 'not-consumed' if disabled else 'absent'},
                              'source_times': {'kind': 'not-consumed' if disabled else 'absent'}},
                   'marker': {'kind': 'not-consumed' if disabled else 'absent'},
                   'refresh': {'kind': 'not-run'} if disabled else {
                       'version': 1, 'status': 'completed', 'workspace': context['workspace'],
                       'source': context['source'], 'tree': context['tree'], 'tracked_source_clean': True,
                       'input_sha256': None,
                       'current_files': {'src/lib.rs': {'blob': 'e' * 40, 'mode': '100644'}},
                       'reused_files': 0, 'invalidated_files': 1,
                       'changed_time_ns': 101, 'max_invocation_ns': 100},
                   'cache_state': 'unclassified', 'producer_association': native.cache_use.ASSOCIATION}
        envelope = {'identity': native.asdict(self.identity), 'arch': 'x64', 'runner_arch': 'X64',
                    'machine': 'x86_64', 'slot': slot, 'job': 'native-worker',
                    'job_name': native.worker_name('x64', slot), 'cache_state': 'unclassified',
                    'cache_use': receipt, 'profile': native.PROFILE, 'source_clean': True,
                    'workflow_sha': self.identity.source, 'workflow_ref': 'owner/repo/' + native.WORKFLOW + '@refs/heads/topic',
                    'rustc': index['source']['rustc'],
                    'bundle_index_sha256': native.sha256((bundle / 'bundle.json').read_bytes())}
        native.save(directory / 'ci-envelope.json', envelope)
        return bundle, wrapper, envelope

    def test_worker_context_profile_architecture_and_index_are_bound(self):
        slot = 'native-library-1'
        bundle, wrapper, envelope = self.worker_fixture()
        native.admit_envelope(self.identity, 'x64', slot, self.root, 'off')
        for key, value in [('arch', 'arm64'), ('machine', 'aarch64'), ('source_clean', False),
                           ('profile', {}), ('compiler_cache_restored', True), ('workflow_sha', 'c' * 40),
                           ('bundle_index_sha256', '0' * 64), ('rustc', 'another compiler')]:
            native.save(self.root / 'ci-envelope.json', dict(envelope, **{key: value}))
            with self.subTest(key=key), self.assertRaises(ValueError):
                native.admit_envelope(self.identity, 'x64', slot, self.root, 'off')
        native.save(self.root / 'ci-envelope.json', envelope)
        for key, value in [('profile', {}), ('source_clean', False), ('end_revision', 'c' * 40)]:
            native.save(bundle / 'reports/check.json', dict(wrapper, **{key: value}))
            with self.subTest(key=key), self.assertRaises(ValueError):
                native.admit_envelope(self.identity, 'x64', slot, self.root, 'off')

    def test_cache_use_binds_recorded_context_and_preserves_markerless_hits(self):
        slot = 'native-library-1'
        for result, request in [('disabled', 'off'), ('miss', 'restore-compatible'), ('restored', 'restore-compatible')]:
            _, _, envelope = self.worker_fixture(result)
            actual = native.admit_envelope(self.identity, 'x64', slot, self.root, request)
            self.assertEqual(actual['cache_use']['result'], result)
            self.assertEqual(actual['cache_use']['marker']['kind'], 'not-consumed' if result == 'disabled' else 'absent')
            with self.assertRaises(ValueError):
                native.admit_envelope(self.identity, 'x64', slot, self.root,
                                      'off' if request == 'restore-compatible' else 'restore-compatible')
        _, _, envelope = self.worker_fixture()
        for key, value in [('repository', 'foreign/repo'), ('run', 43), ('attempt', 3),
                           ('tree', 'd' * 40), ('workspace', '/foreign/worker')]:
            changed = copy.deepcopy(envelope)
            changed['cache_use']['context'][key] = value
            if key in ['run', 'attempt']:
                context = changed['cache_use']['context']
                context['keys']['primary'] = context['keys']['graph'] + context['source'] + f"-{context['run']}-{context['attempt']}"
            native.save(self.root / 'ci-envelope.json', changed)
            with self.subTest(key=key), self.assertRaises(ValueError):
                native.admit_envelope(self.identity, 'x64', slot, self.root, 'off')
        changed = copy.deepcopy(envelope)
        changed['cache_use']['context']['source'] = 'f' * 40
        changed['cache_use']['context']['keys']['primary'] = changed['cache_use']['context']['keys']['graph'] + 'f' * 40 + '-42-2'
        native.save(self.root / 'ci-envelope.json', changed)
        with self.assertRaises(ValueError):
            native.admit_envelope(self.identity, 'x64', slot, self.root, 'off')

    def test_missing_legacy_pending_and_unknown_cache_use_are_rejected(self):
        slot = 'native-library-1'
        _, _, envelope = self.worker_fixture()
        changed = copy.deepcopy(envelope)
        del changed['cache_use']
        for legacy in [False, True]:
            native.save(self.root / 'ci-envelope.json', changed | {'compiler_cache_restored': legacy})
            with self.assertRaises(ValueError):
                native.admit_envelope(self.identity, 'x64', slot, self.root, 'off')
        native.save(self.root / 'ci-envelope.json', changed)
        with self.assertRaises(ValueError):
            native.admit_envelope(self.identity, 'x64', slot, self.root, 'off')
        for key, value in [('status', 'pending'), ('request', 'unknown'), ('unexpected', True)]:
            changed = copy.deepcopy(envelope)
            changed['cache_use'][key] = value
            native.save(self.root / 'ci-envelope.json', changed)
            with self.subTest(key=key), self.assertRaises(ValueError):
                native.admit_envelope(self.identity, 'x64', slot, self.root, 'off')

    def test_all_slots_require_the_collectors_cache_request(self):
        for mixed in [False, True]:
            for position, slot in enumerate(native.slots()):
                directory = self.root / (str(mixed) + slot)
                self.worker_fixture('disabled' if mixed and position == 7 else 'restored', slot, directory)
                if mixed and position == 7:
                    with self.assertRaises(ValueError):
                        native.admit_envelope(self.identity, 'x64', slot, directory, 'restore-compatible')
                else:
                    native.admit_envelope(self.identity, 'x64', slot, directory, 'restore-compatible')

    def test_server_timestamps_accept_documented_forms_without_shortening_bounds(self):
        valid = [
            ('2026-10-26T00:01:00Z', '2026-10-26T00:01:00+00:00'),
            ('2026-10-26T00:01:00.125Z', '2026-10-26T00:01:00.125000+00:00'),
            ('2026-10-26T01:01:00+01:00', '2026-10-26T00:01:00+00:00'),
            ('2020-01-20T09:42:40.000-08:00', '2020-01-20T17:42:40+00:00'),
        ]
        for value, expected in valid:
            with self.subTest(value=value):
                actual = native.server_time(value)
                self.assertEqual(actual.isoformat(), expected)
                self.assertIs(actual.tzinfo, native.timezone.utc)
        invalid = [None, 1, True, '2026-10-26T00:01:00', '2026-10-26 00:01:00Z',
                   '2026-10-26T00:01:00+00:60', '2026-10-26T00:01:00+25:00',
                   '2026-02-30T00:01:00Z', '2026-10-26T24:01:00Z', '0000-01-01T00:00:00Z',
                   '0001-01-01T00:00:00+01:00']
        for value in invalid:
            with self.subTest(value=value), self.assertRaises(ValueError):
                native.server_time(value)
        worker = copy.deepcopy(self.job)
        worker['steps'][0]['started_at'] = '2026-10-26T01:01:00.125+01:00'
        collector = dict(self.job, completed_at='2026-10-26T00:06:00.875Z')
        timing = native.outer_timing([worker], collector)
        self.assertEqual(timing['earliest_worker_check_started_at'], '2026-10-26T00:01:00.125000Z')
        self.assertEqual(timing['server_span_seconds'], 300.75)
        self.assertEqual(timing['conservative_upper_bound_seconds'], 301.75)
        self.assertEqual(timing['timestamp_resolution_seconds'], 1)
        worker['steps'][0]['completed_at'] = '2026-10-26T00:01:00.124Z'
        with self.assertRaises(ValueError):
            native.outer_timing([worker], collector)

    def test_terminal_outer_span_includes_stagger_and_collector_poststeps(self):
        delayed = copy.deepcopy(self.job)
        delayed['steps'][0]['started_at'] = '2026-10-26T00:03:00Z'
        delayed['steps'][0]['completed_at'] = '2026-10-26T00:04:00Z'
        delayed['completed_at'] = '2026-10-26T00:04:30Z'
        collector = dict(self.job, completed_at='2026-10-26T00:06:00Z')
        timing = native.outer_timing([delayed, self.job], collector)
        self.assertEqual(timing['server_span_seconds'], 300)
        self.assertEqual(timing['conservative_upper_bound_seconds'], 301)
        self.assertEqual(timing['timestamp_resolution_seconds'], 1)
        collector['completed_at'] = '2026-10-26T00:07:00Z'
        self.assertEqual(native.outer_timing([self.job, delayed], collector)['server_span_seconds'], 360)
        for changed in [dict(collector, completed_at=None), dict(collector, status='in_progress'),
                        dict(collector, completed_at='2026-10-26T00:00:00Z')]:
            with self.assertRaises(ValueError):
                native.outer_timing([self.job], changed)
        for steps in [[], self.job['steps'] * 2]:
            with self.assertRaises(ValueError):
                native.outer_timing([dict(self.job, steps=steps)], collector)

    def test_plan_is_opt_in_and_rejects_qualification_conflicts(self):
        self.assertEqual(native.plan('off', False, '', 'a' * 40)['workers']['include'], [])
        both = native.plan('both', False, '', 'a' * 40)
        self.assertEqual(len(both['workers']['include']), 16)
        self.assertEqual(len(both['collectors']['include']), 2)
        for mode in native.CACHE_MODES:
            planned = native.plan('both', False, '', 'a' * 40, mode)
            self.assertTrue(all(entry['compiler_cache'] == mode for group in planned.values() for entry in group['include']))
        self.assertTrue(all(entry['compiler_cache'] == 'off' for group in both.values() for entry in group['include']))
        with self.assertRaises(ValueError):
            native.plan('x64', False, '', 'a' * 40, 'unknown')
        for qualify, revision in [(True, ''), (False, 'a' * 40)]:
            with self.assertRaises(ValueError):
                native.plan('x64', qualify, revision, 'a' * 40)

    def test_native_cannot_be_promoted_to_full(self):
        result = {'native_coverage_validated': True, 'complete_suite_qualified': False, 'full_check_executed': False}
        native.admit_result(result)
        for key in result:
            with self.assertRaises(ValueError):
                native.admit_result(dict(result, **{key: not result[key]}))


if __name__ == '__main__':
    unittest.main()
