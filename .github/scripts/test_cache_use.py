"""Credential-free controls for setup-owned cache-use facts."""
import base64
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location('cache_use', Path(__file__).with_name('cache-use.py'))
cache = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cache)


class CacheUseTests(unittest.TestCase):
    def setUp(self):
        self.compiler = b'rustc fixture\nhost: x86_64-unknown-linux-gnu\nrelease: fixture\n'
        self.ctx = {'repository': 'local/control', 'run': 1, 'attempt': 1,
                    'source': 'a' * 40, 'tree': 'b' * 40, 'workspace': '/checkout',
                    'runner_os': 'Linux', 'runner_arch': 'X64', 'profile': cache.PROFILE.copy(),
                    'compiler_stdout_base64': base64.b64encode(self.compiler).decode(),
                    'compiler_sha256': hashlib.sha256(self.compiler).hexdigest()}
        base = 'rust-Linux-X64-' + self.ctx['compiler_sha256'] + '-dev0-test0-strip-symbols-inc0-combined-v2-'
        graph = base + 'c' * 64 + '-'
        self.ctx['keys'] = {'base': base, 'graph': graph, 'graph_hash': 'c' * 64,
                            'primary': graph + 'a' * 40 + '-1-1'}

    def input(self, value):
        text = json.dumps(value)
        return {'kind': 'captured', 'raw_utf8': text,
                'sha256': hashlib.sha256(text.encode()).hexdigest()}

    def marker(self, complete=True):
        return self.input({'revision': 'd' * 40, 'complete': complete, 'check': {
            'version': 1, 'status': 'passed' if complete else 'failed', 'child_exit_code': 0 if complete else 1,
            'source_revision': 'd' * 40, 'end_revision': 'd' * 40,
            'source_clean': True, 'end_clean': True, 'coverage_scope': 'complete', 'budget_breached': False}})

    def receipt(self, result='disabled', saved=None, prior=None):
        disabled = result == 'disabled'
        inputs = {'marker': {'kind': 'not-consumed'}, 'source_times': {'kind': 'not-consumed'}} if disabled else {
            'marker': prior or {'kind': 'absent'}, 'source_times': saved or {'kind': 'absent'}}
        refresh = {'kind': 'not-run'} if disabled else {
            'version': 1, 'status': 'completed', 'workspace': self.ctx['workspace'],
            'source': self.ctx['source'], 'tree': self.ctx['tree'], 'tracked_source_clean': True,
            'input_sha256': saved['sha256'] if saved else None,
            'current_files': {'lib.rs': {'blob': 'e' * 40, 'mode': '100644'}},
            'reused_files': 0, 'invalidated_files': 1, 'changed_time_ns': 101, 'max_invocation_ns': 100}
        return {'version': 1, 'status': 'finalized', 'context': copy.deepcopy(self.ctx),
                'request': 'off' if disabled else 'restore-compatible', 'result': result,
                'action': {'outcome': 'skipped' if disabled else 'success',
                           'matched_key': self.ctx['keys']['primary'] if result == 'restored' else ''},
                'inputs': inputs, 'marker': {'kind': 'not-consumed'} if disabled else cache.marker(inputs['marker']),
                'refresh': refresh, 'cache_state': 'unclassified', 'producer_association': cache.ASSOCIATION}

    def reject(self, value):
        with self.assertRaises(ValueError):
            cache.admit(value)

    def test_closed_outcomes_preserve_hit_without_marker(self):
        for result in ['disabled', 'miss', 'restored']:
            value = self.receipt(result)
            self.assertEqual(cache.admit(value)['result'], result)
        self.assertEqual(cache.admit(self.receipt('restored'))['marker'], {'kind': 'absent'})

    def test_request_action_and_matched_key_cannot_disagree(self):
        for outcome, key in [('success', ''), ('skipped', self.ctx['keys']['primary'])]:
            value = self.receipt();value['action'] = {'outcome': outcome, 'matched_key': key};self.reject(value)
        for result in ['miss', 'restored']:
            value = self.receipt(result);value['action']['outcome'] = 'failure';self.reject(value)
            value = self.receipt(result);value['action']['matched_key'] = '' if result == 'restored' else self.ctx['keys']['primary'];self.reject(value)
        value = self.receipt('miss');value['request'] = 'off';self.reject(value)
        with self.assertRaises(ValueError):
            cache.admit(self.receipt('restored'), requested='off')

    def test_raw_compiler_bytes_bind_the_actual_key_and_bundle_text(self):
        value = self.receipt('restored')
        cache.admit(value, compiler_text=self.compiler.decode().strip())
        with self.assertRaises(ValueError):
            cache.admit(value, compiler_text='different compiler')
        value['context']['compiler_stdout_base64'] = base64.b64encode(self.compiler.rstrip()).decode()
        self.reject(value)
        value = self.receipt('restored');value['context']['compiler_sha256'] = 'f' * 64;self.reject(value)

    def test_context_profile_architecture_and_key_namespace_are_required(self):
        for key, item in [('runner_arch', 'ARM64'), ('runner_os', 'other'), ('source', 'not-a-sha')]:
            value = self.receipt('restored');value['context'][key] = item;self.reject(value)
        value = self.receipt('restored');value['context']['profile']['CARGO_INCREMENTAL'] = '1';self.reject(value)
        value = self.receipt('restored');value['action']['matched_key'] = 'foreign-key';self.reject(value)
        value = self.receipt('restored');value['context']['keys']['graph_hash'] = 'f' * 64;self.reject(value)
        with self.assertRaises(ValueError):
            cache.admit(self.receipt(), {'source': 'f' * 40})

    def test_complete_and_noncomplete_marker_claims_remain_distinct(self):
        for complete, kind in [(True, 'complete'), (False, 'non-complete')]:
            value = self.receipt('restored', prior=self.marker(complete))
            self.assertEqual(cache.admit(value)['marker']['kind'], kind)
        value = self.receipt('restored', prior=self.marker());value['marker']['revision'] = 'e' * 40;self.reject(value)
        prior = self.marker();parsed = json.loads(prior['raw_utf8']);parsed['check']['status'] = 'failed'
        with self.assertRaises(ValueError):
            cache.marker(self.input(parsed))
        parsed['check']['unexpected_environment'] = 'not exported'
        with self.assertRaises(ValueError):
            cache.marker(self.input(parsed))

    def test_complete_marker_rejects_rehashed_interruption_claims(self):
        for fields in [{'reason': 'interrupted'}, {'interrupted_signal': 2},
                       {'reason': 'interrupted', 'interrupted_signal': 2}]:
            with self.subTest(fields=fields):
                parsed = json.loads(self.marker()['raw_utf8'])
                parsed['check'].update(fields)
                with self.assertRaisesRegex(ValueError, 'contradictory complete marker'):
                    cache.marker(self.input(parsed))
        parsed = json.loads(self.marker()['raw_utf8'])
        parsed['check'].update(reason=None, interrupted_signal=None)
        self.assertEqual(cache.marker(self.input(parsed))['kind'], 'complete')

    def test_input_digests_and_json_are_not_silently_discarded(self):
        value = self.receipt('restored', prior=self.marker());value['inputs']['marker']['sha256'] = 'f' * 64;self.reject(value)
        malformed = {'kind': 'captured', 'raw_utf8': '{', 'sha256': hashlib.sha256(b'{').hexdigest()}
        with self.assertRaises(ValueError):
            cache.marker(malformed)
        with self.assertRaises(ValueError):
            cache.source_manifest(self.input({'version': 2, 'workspace': '/checkout', 'files': {'../escape': {}}}))

    def test_same_blob_reuse_and_changed_or_foreign_input_counts_are_checked(self):
        source = {'version': 2, 'workspace': '/checkout', 'files': {
            'lib.rs': {'blob': 'e' * 40, 'mode': '100644', 'mtime_ns': 1}}}
        saved = self.input(source);value = self.receipt('restored', saved)
        value['refresh'].update(reused_files=1, invalidated_files=0)
        cache.admit(value)
        value['refresh']['reused_files'] = 0;self.reject(value)
        for key, item in [('workspace', '/foreign'), ('version', 1)]:
            changed = copy.deepcopy(source);changed[key] = item
            cache.admit(self.receipt('restored', self.input(changed)))
        changed = copy.deepcopy(source);changed['files']['lib.rs']['blob'] = 'f' * 40
        cache.admit(self.receipt('restored', self.input(changed)))

    def test_refresh_requires_complete_current_source_and_safe_time_bound(self):
        for key, item in [('status', 'running'), ('source', 'f' * 40), ('tracked_source_clean', False),
                          ('input_sha256', 'f' * 64), ('changed_time_ns', 100), ('invalidated_files', 0)]:
            value = self.receipt('miss');value['refresh'][key] = item;self.reject(value)
        value = self.receipt();value['status'] = 'pending';self.reject(value)

    def test_capture_precedes_marker_consumption_and_finalization_is_exclusive(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary);directory = root / 'state';directory.mkdir()
            self.ctx['workspace'] = str(root)
            pending = {'phase': 'requested', 'context': self.ctx, 'request': 'restore-compatible'}
            cache.save(directory / 'pending.json', pending)
            path = root / 'target/test-tools/complete-cache.json';path.parent.mkdir(parents=True)
            raw = self.marker()['raw_utf8'];path.write_text(raw)
            cache.capture_restored(root, directory, 'success', self.ctx['keys']['primary'])
            path.unlink()
            captured = json.loads((directory / 'pending.json').read_text())['inputs']['marker']
            self.assertEqual(captured['raw_utf8'], raw)
            refresh = self.receipt('restored')['refresh'];cache.save(directory / 'source-refresh.json', refresh)
            environment = cache.PROFILE | {'GITHUB_REPOSITORY': self.ctx['repository'],
                'GITHUB_RUN_ID': '1', 'GITHUB_RUN_ATTEMPT': '1', 'GITHUB_SHA': self.ctx['source'],
                'RUNNER_OS': 'Linux', 'RUNNER_ARCH': 'X64'}
            with mock.patch.dict(cache.os.environ, environment | {'GITHUB_RUN_ID': '2'}, clear=True), mock.patch.object(
                    cache.subprocess, 'check_output', side_effect=[self.ctx['tree'], self.ctx['source'], '']):
                with self.assertRaises(ValueError):
                    cache.finish(root, directory)
            self.assertFalse((directory / 'receipt.json').exists())
            with mock.patch.dict(cache.os.environ, environment, clear=True), mock.patch.object(
                    cache.subprocess, 'check_output', side_effect=[self.ctx['tree'], self.ctx['source'], '']):
                result = cache.finish(root, directory)
            self.assertEqual(result['marker']['kind'], 'complete')
            with mock.patch.dict(cache.os.environ, environment, clear=True), mock.patch.object(
                    cache.subprocess, 'check_output', side_effect=[self.ctx['tree'], self.ctx['source'], '']):
                with self.assertRaises(FileExistsError):
                    cache.finish(root, directory)

    def test_capture_refuses_symlinked_or_oversized_input(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary);target = root / 'data';target.write_text('x' * 20)
            link = root / 'link';link.symlink_to(target)
            with self.assertRaises(ValueError):
                cache.capture(link, 100)
            with self.assertRaises(ValueError):
                cache.capture(target, 10)

    def test_compiler_result_schema_is_exact_and_never_native_credit(self):
        cache.compiler_result({'version': 1, 'mode': 'off'}, off_only=True)
        for value in [{'version': 2, 'mode': 'off'}, {'version': 1, 'mode': 'off', 'extra': True},
                      {'version': 1, 'mode': 'local'}, {'version': 1, 'mode': 'remote'}]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                cache.compiler_result(value, off_only=True)


if __name__ == '__main__':
    unittest.main()
