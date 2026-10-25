"""Measure the unchanged base revision's PostgreSQL CI commands in one server."""
import argparse
import datetime
import json
import os
from pathlib import Path
import shlex
import signal
import subprocess
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--output', type=Path, required=True)
args = parser.parse_args()
root = Path.cwd()
out = args.output.resolve()
out.mkdir(parents=True, exist_ok=False)
job = (root / '.github/workflows/ci.yml').read_text().split('  postgres:', 1)[1]
commands = [shlex.split(line.split('run: ', 1)[1]) for line in job.splitlines()
            if 'run: cargo ' in line]
assert len(commands) == 24
owner = f'{os.getpid()}-{time.time_ns()}'
image = 'postgres:14@sha256:c2427de38f998489d36de7ca3553db2134872c400f2b08be4b824e5c50e4d619'
report = {'status': 'preparing', 'started_utc': datetime.datetime.now(datetime.UTC).isoformat(),
          'image_reference': image, 'gates': [{'argv': c, 'status': 'pending'} for c in commands]}

def save():
    temporary = out / 'summary.tmp'
    temporary.write_text(json.dumps(report, indent=2) + '\n')
    temporary.replace(out / 'summary.json')

def capture(command):
    return subprocess.check_output(command, text=True).strip()

def interrupted(signum, _frame):
    raise InterruptedError(signum)

signal.signal(signal.SIGTERM, interrupted)
signal.signal(signal.SIGINT, interrupted)
container = None
child = None
started = time.monotonic()
code = 1
save()
try:
    container = capture(['docker', 'run', '--detach', '--publish', '127.0.0.1::5432',
                         '--env', 'POSTGRES_HOST_AUTH_METHOD=trust',
                         '--label', f'crabber.test-runner={owner}', image,
                         '-c', 'fsync=on', '-c', 'synchronous_commit=on',
                         '-c', 'full_page_writes=on'])
    print(f'managed PostgreSQL container={container} owner={owner}', flush=True)
    report['container_id'] = container
    report['image_id'] = capture(['docker', 'inspect', '--format', '{{.Image}}', container])
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        ready = subprocess.run(['docker', 'exec', container, 'psql', '-h', '127.0.0.1',
                                '-U', 'postgres', '-Atc',
                                "SELECT current_setting('server_version'),current_setting('fsync'),"
                                "current_setting('synchronous_commit'),current_setting('full_page_writes')"],
                               capture_output=True, text=True)
        if ready.returncode == 0:
            settings = ready.stdout.strip().split('|')
            assert settings[0].startswith('14.') and settings[1:] == ['on', 'on', 'on']
            report['server_settings'] = settings
            break
        time.sleep(.2)
    else:
        raise RuntimeError('baseline PostgreSQL did not become ready')
    report['server_setup_seconds'] = time.monotonic() - started
    port = capture(['docker', 'port', container, '5432/tcp']).rsplit(':', 1)[1]
    endpoint = f'postgres://postgres@127.0.0.1:{port}/postgres'
    env = dict(os.environ)
    for key in list(env):
        if key.startswith(('CRABBER_TEST_', 'OTEL_EXPORTER_OTLP_')) or key in {
            'ANTHROPIC_API_KEY', 'OPENAI_API_KEY', 'OPENCODE_GO_API_KEY',
            'DD_API_KEY', 'DD_APP_KEY', 'CRABBER_TRACE_LIVE', 'CRABBER_OBS_VERIFY_MARKER',
        }:
            env.pop(key)
    env.update(CRABBER_REQUIRE_POSTGRES='1', CRABBER_TEST_POSTGRES_URL=endpoint,
               CRABBER_POSTGRES_URL=endpoint)
    report['status'] = 'running'
    save()
    for index, gate in enumerate(report['gates']):
        gate['status'] = 'running'
        phase = time.monotonic()
        save()
        print(f'baseline command {index + 1}/24: {shlex.join(gate["argv"])}', flush=True)
        with (out / f'{index:02}-raw.log').open('w') as log:
            child = subprocess.Popen(gate['argv'], stdout=log, stderr=subprocess.STDOUT,
                                     env=env, start_new_session=True)
            code = child.wait()
            child = None
        gate.update(status='passed' if code == 0 else 'failed', exit_code=code,
                    wall_seconds=time.monotonic() - phase)
        save()
        if code:
            break
    else:
        code = 0
except InterruptedError as error:
    code = 128 + int(error.args[0])
except Exception as error:
    report['error_type'] = type(error).__name__
    code = 1
finally:
    if child is not None and child.poll() is None:
        os.killpg(child.pid, signal.SIGTERM)
        try:
            child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(child.pid, signal.SIGKILL)
            child.wait()
    if container is not None:
        cleanup_started = time.monotonic()
        assert capture(['docker', 'inspect', '--format', '{{index .Config.Labels "crabber.test-runner"}}', container]) == owner
        cleanup = subprocess.run(['docker', 'rm', '--force', container], capture_output=True, text=True)
        report['container_cleanup_exit_code'] = cleanup.returncode
        report['server_cleanup_seconds'] = time.monotonic() - cleanup_started
        if cleanup.returncode:
            code = 1
    for gate in report['gates']:
        if gate['status'] == 'pending':
            gate['status'] = 'cancelled'
        elif gate['status'] == 'running':
            gate['status'] = 'failed'
    report.update(status='passed' if code == 0 else 'failed', exit_code=code,
                  wall_seconds=time.monotonic() - started)
    save()
raise SystemExit(code)
