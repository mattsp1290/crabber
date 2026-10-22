"""Explicitly partial, single 2-vs-4 PostgreSQL slot comparison on CI hardware."""
import importlib.util
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import time
import tomllib

sys.dont_write_bytecode = True

SOURCE = '79ee1041af01c1defc7e9daec2e9cbfca379e087'
FILTER = 'package(crabber-runtime) & test(/postgres/)'
ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / 'target/test-performance/pg-slot-comparison'


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, ROOT / '.github/scripts' / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def features():
    source = (ROOT / 'xtask/src/complete.rs').read_text()
    body = re.search(r'const FEATURES: &str = concat!\((.*?)\);', source, re.S).group(1)
    return ''.join(json.loads(s) for s in re.findall(r'"[^"\n]*"', body))


def compile_metadata():
    subprocess.run(['cargo', 'build', '-p', 'xtask', '--features', 'postgres-check'], check=True)
    with (OUT / 'binaries.json').open('w') as output:
        subprocess.run(['cargo', 'nextest', 'list', '--workspace', '--features', features(),
                        '--list-type', 'binaries-only', '--message-format', 'json'], stdout=output, check=True)
    with (OUT / 'cargo-metadata.json').open('w') as output:
        subprocess.run(['cargo', 'metadata', '--format-version', '1', '--features', features()], stdout=output, check=True)


def reuse():
    return ['--cargo-metadata', str(OUT / 'cargo-metadata.json'),
            '--binaries-metadata', str(OUT / 'binaries.json')]


def fixture_summary(path, slots):
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    created = {r['identity'] for r in rows if r['phase'] == 'created'}
    finished = {r['identity'] for r in rows if r['phase'] == 'finished'}
    assert created == finished and len(created) == 43, 'not every selected history finished'
    active = set()
    peak = 0
    for row in sorted(rows, key=lambda r: r['unix_nanos']):
        if row['phase'] == 'created':
            active.add(row['identity'])
        elif row['phase'] == 'finished':
            active.remove(row['identity'])
        peak = max(peak, len(active))
    assert peak == slots, (peak, slots)
    for phase in ['run_cleanup', 'server_cleanup', 'gate_passed']:
        assert sum(r['phase'] == phase for r in rows) == 1, phase
    return {'finished_histories': len(finished), 'peak_live_histories': peak}


def main():
    if sys.argv[1:] == ['--compile']:
        compile_metadata()
        return 0
    assert not sys.argv[1:]
    assert os.environ.get('CI') == 'true' and os.cpu_count() == 4, 'comparison requires declared four-CPU CI runner'
    assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT, text=True).strip()
    changed = subprocess.check_output(['git', 'diff', '--name-only', SOURCE, 'HEAD'], cwd=ROOT, text=True).splitlines()
    assert set(changed) <= {'.github/workflows/ci.yml', '.github/scripts/compare-postgres-slots.py', '.github/scripts/compare-postgres-monitor.py'}, changed
    OUT.mkdir(parents=True, exist_ok=False)
    check = load('check', 'check.py')
    monitor = load('monitor', 'compare-postgres-monitor.py')
    started = time.monotonic()
    summary = {'scope': 'partial Runtime PostgreSQL family resource comparison, never complete-check qualification',
               'source_revision': SOURCE, 'harness_revision': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
               'filter': FILTER, 'global_test_threads': 4, 'status': 'running', 'cases': []}
    summary['ci_elapsed_before_common_build_seconds'] = time.time() - float(os.environ['CHECK_STARTED'])
    def save():
        summary['comparison_wall_seconds'] = time.monotonic() - started
        (OUT / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    def budget():
        # Leave a whole-job margin around setup, reporting and monitor joins.
        # Each partial phase remains capped at 600s with the normal 20s reserve.
        return min(600., 1900. - (time.time() - float(os.environ['CHECK_STARTED'])))
    save()
    code = check.supervise([sys.executable, '-I', '-S', str(Path(__file__).resolve()), '--compile'], ROOT, OUT / 'common-build.json', budget=budget())
    if code:
        summary['status'] = 'failed-common-build'; save(); return code
    real_cargo = shutil.which('cargo')
    wrapper = OUT / 'wrapper'
    wrapper.mkdir()
    program = ('import os,sys\na=sys.argv[1:]\n'
               'if a[:2]==["nextest","run"]:\n'
               ' a=["nextest","run"]+' + repr(reuse()) + '+["--profile","complete-ci","--config-file",os.environ["CRABBER_SLOT_CONFIG"],"--test-threads","4","--retries","0","--no-fail-fast","-E",' + repr(FILTER) + ']\n'
               'os.execv(' + repr(real_cargo) + ',[' + repr(real_cargo) + ']+a)\n')
    (wrapper / 'cargo.py').write_text(program)
    (wrapper / 'cargo').write_text('#!/bin/sh\nexec ' + shlex.quote(sys.executable) + ' -I -S ' + shlex.quote(str(wrapper / 'cargo.py')) + ' "$@"\n')
    (wrapper / 'cargo').chmod(0o700)
    original = (ROOT / '.config/nextest.toml').read_text()
    old = tomllib.loads(original)
    for slots in [2, 4]:
        config = OUT / f'slots-{slots}.toml'
        config.write_text(original.replace('postgres-ci = { max-threads = 2 }', 'postgres-ci = { max-threads = ' + str(slots) + ' }', 1))
        parsed = tomllib.loads(config.read_text())
        assert parsed['test-groups']['postgres-ci']['max-threads'] == slots
        parsed['test-groups']['postgres-ci']['max-threads'] = 2
        assert parsed == old, 'comparison changed another scheduler setting'
        selected = subprocess.check_output([real_cargo, 'nextest', 'list'] + reuse() + ['--profile', 'complete-ci', '--config-file', str(config), '-E', FILTER, '--message-format', 'json'], text=True)
        (OUT / f'slots-{slots}-tests.json').write_text(selected)
        cases = [(key, name) for key, suite in json.loads(selected)['rust-suites'].items() for name, test in suite['testcases'].items() if test['filter-match']['status'] == 'matches']
        cases = json.loads(json.dumps(cases))
        assert len(cases) == 43 and all(key == 'crabber-runtime' and 'postgres' in name for key, name in cases)
        if summary['cases']:
            assert cases == summary['cases'][0]['selected_tests']
        log = OUT / f'slots-{slots}.log'
        log.touch()
        sampler = monitor.Sampler(log)
        env = dict(os.environ, PATH=str(wrapper) + ':' + os.environ['PATH'], CRABBER_SLOT_CONFIG=str(config))
        oldout, olderr = os.dup(1), os.dup(2)
        code = 1
        try:
            sampler.thread.start()
            with log.open('w') as output:
                os.dup2(output.fileno(), 1); os.dup2(output.fileno(), 2)
                code = check.supervise([str(ROOT / 'target/debug/xtask'), 'check-postgres', '--package', 'crabber', '--test', 'typed_output', '--runner', 'nextest', '--filter', 'partial-resource-comparison', '--test-threads', '4', '--fixture-metrics', str(OUT / f'slots-{slots}-fixtures.jsonl')], ROOT, OUT / f'slots-{slots}.json', env=env, budget=budget())
        finally:
            os.dup2(oldout, 1); os.dup2(olderr, 2); os.close(oldout); os.close(olderr)
            try:
                resources = sampler.finish()
                resources['status'] = 'passed'
            except Exception as error:
                resources = {'status': 'failed', 'error_type': type(error).__name__,
                             'samples': sampler.samples, 'sample_errors': sampler.errors}
            try:
                measurement = json.loads((OUT / f'slots-{slots}.json').read_text())
            except (OSError, ValueError) as error:
                measurement = {'status': 'unavailable', 'error_type': type(error).__name__}
            record = {'slots': slots, 'selected_tests': cases, 'test_exit_code': code,
                      'measurement': measurement, 'server_resources': resources}
            summary['cases'].append(record)
            save()
        if code or resources['status'] != 'passed':
            summary['status'] = 'failed-case'; save(); return code or 1
        native = re.search(r'Summary \[\s*([\d.]+)s\] 43 tests run: 43 passed', log.read_text())
        assert native, 'partial native family did not complete'
        record['native_wall_seconds'] = float(native[1])
        record['fixtures'] = fixture_summary(OUT / f'slots-{slots}-fixtures.jsonl', slots)
        save()
    summary['status'] = 'passed-partial-comparison'
    save()
    return 0


def run():
    # The common-build subprocess does not own its parent's summary.
    if sys.argv[1:] == ['--compile']:
        return main()
    try:
        return main()
    finally:
        # Assertion, metadata or monitor exceptions still produce terminal
        # evidence. Preserve the original exception/exit status unchanged.
        path = OUT / 'summary.json'
        if path.exists():
            summary = json.loads(path.read_text())
            if summary['status'] == 'running':
                summary['status'] = 'failed-exception'
                path.write_text(json.dumps(summary, indent=2) + '\n')


if __name__ == '__main__':
    raise SystemExit(run())
