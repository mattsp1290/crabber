#!/usr/bin/env python3
"""Owned local compiler snapshots. Compiler objects never grant check credit."""
import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
import tomllib
import urllib.request
import uuid

sys.dont_write_bytecode = True

SCHEMA = 1
STORE_LIMIT = 10 * 1024 ** 3
SNAPSHOT_LIMIT = 20 * 1024 ** 3
RECOVERY = 'CRABBER_COMPILER_CACHE=off python3 -I -S .github/scripts/check.py'
ROOT = Path(__file__).resolve().parents[2]
DESCRIPTOR_FIELDS = {'schema', 'owner_pid', 'owner_start', 'server_pid', 'server_start',
                     'executable', 'executable_sha256', 'wrapper', 'wrapper_sha256',
                     'socket', 'store', 'config', 'namespace'}
COMPILER_INPUTS = frozenset('''PATH HOME CARGO_HOME RUSTUP_HOME RUSTUP_TOOLCHAIN RUSTC
LD_LIBRARY_PATH DYLD_LIBRARY_PATH DYLD_FALLBACK_LIBRARY_PATH LIBRARY_PATH TMPDIR TMP TEMP LANG LC_ALL LC_CTYPE
TZ SDKROOT MACOSX_DEPLOYMENT_TARGET CARGO CARGO_MANIFEST_DIR CARGO_MANIFEST_PATH CARGO_MANIFEST_LINKS
CARGO_CRATE_NAME CARGO_BIN_NAME CARGO_PRIMARY_PACKAGE CARGO_TARGET_TMPDIR OUT_DIR OPT_LEVEL DEBUG PROFILE HOST
TARGET NUM_JOBS CARGO_INCREMENTAL RUSTFLAGS CARGO_ENCODED_RUSTFLAGS CARGO_PKG_NAME CARGO_PKG_VERSION
CARGO_PKG_VERSION_MAJOR CARGO_PKG_VERSION_MINOR CARGO_PKG_VERSION_PATCH CARGO_PKG_VERSION_PRE CARGO_PKG_AUTHORS
CARGO_PKG_DESCRIPTION CARGO_PKG_HOMEPAGE CARGO_PKG_REPOSITORY CARGO_PKG_LICENSE CARGO_PKG_LICENSE_FILE
CARGO_PKG_RUST_VERSION CARGO_PKG_README CRABBER_COMPILED_SHA CRABBER_BUILD_SHA CRABBER_BUILD_ROOT CRABBER_GIT_SHA
CARGO_BIN_EXE_codex-login CARGO_BIN_EXE_minimal-embed CARGO_BIN_EXE_native-extension CRUNCHY_LIB_SUFFIX WBG_VERSION
SCHEMA_FILE_HASH DEBUG_OUTPUT_DIR CFG_CARGO_FEATURES CFG_OPT_LEVEL CFG_TARGET_FEATURES ISLE_DIR CARGO_CFG_TARGET_FEATURE'''.split())
STAT_COUNTERS = frozenset('''compile_requests requests_unsupported_compiler requests_not_compile requests_not_cacheable
requests_executed cache_timeouts cache_read_errors non_cacheable_compilations forced_recaches cache_write_errors
cache_writes compilations compile_fails'''.split())
STAT_LANGUAGE_COUNTERS = frozenset({'cache_errors', 'cache_hits', 'cache_misses'})
STAT_REASONS = frozenset({'crate-type', 'incremental'})


def require(value, message):
    if not value:
        raise ValueError(message)


def within(deadline):
    if deadline is not None and time.monotonic() >= deadline:
        raise TimeoutError('compiler cache exceeded the existing check deadline')


def digest(path, deadline=None):
    result = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            within(deadline)
            result.update(block)
    return result.hexdigest()


def write_json(path, value):
    temporary = path.with_name('.' + path.name + '-' + uuid.uuid4().hex)
    temporary.write_text(json.dumps(value, sort_keys=True) + '\n')
    temporary.chmod(0o600)
    temporary.replace(path)


def regular(path):
    require(stat.S_ISREG(path.lstat().st_mode), 'expected a regular file')


def owned_directory(path):
    mode = path.lstat()
    require(stat.S_ISDIR(mode.st_mode) and mode.st_uid == os.getuid(),
            'compiler snapshot directory must be an owned ordinary directory')


def private_directory(path):
    require(path.is_absolute(), 'compiler cache directory must be absolute')
    for part in [*reversed(path.parents), path]:
        require(not part.is_symlink(), 'compiler cache path must not contain symlinks')
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    mode = path.stat()
    require(mode.st_uid == os.getuid() and stat.S_IMODE(mode.st_mode) == 0o700,
            'compiler cache directory must be owned by this user with mode 0700')


def clean_environment(env):
    return {name: value for name, value in env.items()
            if not name.startswith('SCCACHE_') and name not in {
                'RUSTC_WRAPPER', 'RUSTC_WORKSPACE_WRAPPER', 'CRABBER_COMPILER_CACHE_SESSION'}}


def probe_environment(env):
    return {name: value for name, value in env.items() if name in COMPILER_INPUTS}


def pinned_file(path, executable=False):
    require(path.is_absolute(), 'sccache installation paths must be absolute')
    for component in [*path.parents, path]:
        require(not component.is_symlink(), 'sccache installation path must not contain symlinks')
    regular(path)
    mode = path.stat()
    require(mode.st_uid == os.getuid() and mode.st_nlink == 1 and not mode.st_mode & 0o022,
            'sccache installation must be owned, single-linked and not group/other-writable')
    require(not executable or mode.st_mode & 0o100, 'sccache binary must be executable by its owner')


def statistics(raw):
    require(isinstance(raw, dict), 'invalid sccache statistics')
    raw = raw.get('stats', raw)
    require(isinstance(raw, dict), 'invalid sccache counters')
    def counter(value):
        require(type(value) is int and value >= 0, 'invalid numeric sccache counter')
        return value
    result = {name: counter(raw.get(name, 0)) for name in STAT_COUNTERS}
    for name in STAT_LANGUAGE_COUNTERS:
        language = raw.get(name, {})
        require(isinstance(language, dict) and isinstance(language.get('counts', {}), dict), 'invalid Rust cache counters')
        result[name] = {'counts': {'Rust': counter(language.get('counts', {}).get('Rust', 0))}}
    reasons = raw.get('not_cached', {})
    require(isinstance(reasons, dict), 'invalid non-cacheable counters')
    result['not_cached'] = {name: counter(reasons.get(name, 0)) for name in STAT_REASONS}
    result['not_cached']['other'] = sum(counter(value) for name, value in reasons.items() if name not in STAT_REASONS)
    return result


def verify_executable(path, root=ROOT, env=None):
    pinned_file(path, executable=True)
    receipt = path.with_name(path.name + '.pin.json')
    pinned_file(receipt)
    pin = json.loads((root / '.config/test-tools.json').read_text())['sccache']
    proof = json.loads(receipt.read_text())
    require(set(proof) == {'version', 'target', 'archive_sha256', 'binary_sha256'}, 'invalid sccache installation receipt')
    require(proof['version'] == pin['version'] and pin['archives'].get(proof['target']) == proof['archive_sha256']
            and digest(path) == proof['binary_sha256'], 'sccache installation digest mismatch; reinstall the pinned tool')
    version = subprocess.check_output([str(path), '--version'],
                                     env=probe_environment(os.environ if env is None else env), timeout=5)
    require(version == ('sccache ' + pin['version'] + '\n').encode(), 'sccache version output must match the exact pinned line')
    return proof['binary_sha256']


def install(destination, root=ROOT):
    pin = json.loads((root / '.config/test-tools.json').read_text())['sccache']
    machine = {'arm64': 'aarch64', 'aarch64': 'aarch64', 'x86_64': 'x86_64'}.get(platform.machine())
    suffix = {'Linux': 'unknown-linux-musl', 'Darwin': 'apple-darwin'}.get(platform.system())
    target = f'{machine}-{suffix}'
    require(target in pin['archives'], 'sccache supports Linux/macOS x86_64/aarch64 only')
    archive = f'sccache-v{pin["version"]}-{target}'
    url = f'https://github.com/mozilla/sccache/releases/download/v{pin["version"]}/{archive}.tar.gz'
    with urllib.request.urlopen(url, timeout=60) as response:
        data = response.read(64 * 1024 * 1024 + 1)
    require(hashlib.sha256(data).hexdigest() == pin['archives'][target], 'sccache archive checksum mismatch')
    with tarfile.open(fileobj=io.BytesIO(data), mode='r:gz') as bundle:
        member = bundle.getmember(archive + '/sccache')
        require(member.isfile() and member.size <= 128 * 1024 * 1024, 'invalid sccache archive executable')
        binary = bundle.extractfile(member).read()
    destination.mkdir(parents=True, exist_ok=True)
    executable = destination.resolve() / 'sccache'
    temporary = executable.with_name('.sccache-' + uuid.uuid4().hex)
    temporary.write_bytes(binary)
    temporary.chmod(0o755)
    temporary.replace(executable)
    write_json(executable.with_name('sccache.pin.json'), {
        'version': pin['version'], 'target': target, 'archive_sha256': pin['archives'][target],
        'binary_sha256': digest(executable)})
    verify_executable(executable, root)
    return executable


class LocalConfig:
    def __init__(self, root, env):
        self.producer = {'source_revision': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip(),
                         'source_clean': not subprocess.check_output(['git', 'status', '--porcelain'], cwd=root, text=True).strip()}
        self.wrapper = root / '.github/scripts/compiler-cache-wrapper'
        self.wrapper_sha256 = digest(self.wrapper)
        self.executable = Path(env.get('CRABBER_SCCACHE_PATH', str(root / 'target/test-tools/sccache')))
        self.binary_sha256 = verify_executable(self.executable, root, env)
        self.executable = self.executable.resolve()
        default = (Path.home() / 'Library/Caches' if platform.system() == 'Darwin'
                   else Path(env.get('XDG_CACHE_HOME', str(Path.home() / '.cache'))))
        self.base = Path(env.get('CRABBER_COMPILER_CACHE_DIR', str(default / 'crabber/compiler-cache')))
        private_directory(self.base)
        compiler = subprocess.check_output([env.get('RUSTC', 'rustc'), '-Vv'], cwd=root,
                                          env=probe_environment(env), text=True, timeout=10)
        repository = subprocess.check_output(['git', 'config', '--get', 'remote.origin.url'], cwd=root, text=True).strip()
        identity = {'schema': SCHEMA, 'repository': repository, 'sccache': self.binary_sha256,
                    'wrapper': self.wrapper_sha256, 'policy': digest(Path(__file__)),
                    'compiler_environment_sha256': hashlib.sha256(json.dumps(
                        {**{k: v for k, v in env.items() if k in COMPILER_INPUTS}, 'CARGO_INCREMENTAL': '0'},
                        sort_keys=True).encode()).hexdigest(),
                    'compiler': compiler, 'platform': [platform.system(), platform.machine()],
                    'profiles': tomllib.loads((root / 'Cargo.toml').read_text()).get('profile', {}),
                    'cargo_config': (root / '.cargo/config.toml').read_text() if (root / '.cargo/config.toml').exists() else '',
                    'configuration': {k: v for k, v in env.items() if k.startswith('CARGO_PROFILE_')
                                      or k in {'RUSTFLAGS', 'CARGO_ENCODED_RUSTFLAGS', 'CARGO_BUILD_TARGET',
                                               'RUSTC', 'RUSTUP_TOOLCHAIN', 'CARGO_BUILD_RUSTFLAGS'}
                                      or (k.startswith('CARGO_TARGET_') and k.endswith(('_RUSTFLAGS', '_LINKER')))}}
        self.namespace = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()


def configuration(root, env, direct=False):
    mode = env.get('CRABBER_COMPILER_CACHE', 'off')
    require(mode in {'off', 'local'}, 'CRABBER_COMPILER_CACHE must be off or local')
    if mode == 'off':
        return None
    require(env.get('CARGO_INCREMENTAL') in ({'0'} if direct else {None, '0'}),
            'local compiler cache requires CARGO_INCREMENTAL=0 before outer Cargo')
    return LocalConfig(root, env)


def files(directory, deadline=None):
    owned_directory(directory)
    result = {}
    total = 0
    for parent, directories, names in os.walk(directory, followlinks=False):
        within(deadline)
        for name in directories:
            owned_directory(Path(parent) / name)
        for name in names:
            path = Path(parent) / name
            regular(path)
            require(path.stat().st_uid == os.getuid(), 'compiler snapshot file must be owned by this user')
            total += path.stat().st_size
            require(total <= STORE_LIMIT, 'snapshot exceeds 10-GiB namespace bound')
            result[path.relative_to(directory).as_posix()] = {'size': path.stat().st_size, 'sha256': digest(path, deadline)}
    return result, total


def closed_snapshot(directory, namespace, deadline=None):
    require(re.fullmatch('[0-9a-f]{64}', namespace) and re.fullmatch('[0-9a-f]{32}', directory.name),
            'invalid closed snapshot identity')
    owned_directory(directory)
    require({path.name for path in directory.iterdir()} == {'manifest.json', 'store'},
            'unexpected closed snapshot contents')
    manifest_path = directory / 'manifest.json'
    regular(manifest_path)
    require(manifest_path.stat().st_uid == os.getuid(), 'snapshot manifest must be owned by this user')
    manifest = json.loads(manifest_path.read_text())
    require(isinstance(manifest, dict) and set(manifest) == {
        'schema', 'namespace', 'snapshot', 'check_status', 'producer', 'entries', 'bytes', 'created_ns'},
        'invalid snapshot manifest fields')
    require(type(manifest['schema']) is int and manifest['schema'] == SCHEMA
            and manifest['namespace'] == namespace and manifest['snapshot'] == directory.name
            and manifest['check_status'] in ('passed', 'failed'), 'incompatible snapshot')
    require(type(manifest['bytes']) is int and 0 <= manifest['bytes'] <= STORE_LIMIT
            and type(manifest['created_ns']) is int and manifest['created_ns'] >= 0,
            'invalid snapshot size or creation time')
    producer = manifest['producer']
    require(producer is None or (isinstance(producer, dict)
            and set(producer) == {'source_revision', 'source_clean'}
            and isinstance(producer['source_revision'], str) and re.fullmatch('[0-9a-f]{40}', producer['source_revision'])
            and type(producer['source_clean']) is bool), 'invalid snapshot producer')
    entries, size = files(directory / 'store', deadline)
    require(entries == manifest['entries'] and size == manifest['bytes'], 'corrupt snapshot entries')
    return manifest


def copy_store(source, destination, deadline=None):
    def copy_file(source, destination):
        with open(source, 'rb') as incoming, open(destination, 'xb') as outgoing:
            for block in iter(lambda: incoming.read(1024 * 1024), b''):
                within(deadline)
                outgoing.write(block)
        return destination
    within(deadline)
    shutil.copytree(source, destination, dirs_exist_ok=True, copy_function=copy_file)
    within(deadline)


class SnapshotStore:
    def __init__(self, base, namespace, deadline=None):
        self.base, self.namespace = base, namespace
        self.deadline = deadline
        private_directory(base)
        self.directory = base / namespace
        private_directory(self.directory)
        self.snapshots = self.directory / 'snapshots'
        private_directory(self.snapshots)

    @contextmanager
    def lease(self, exclusive=False, timeout=5):
        private_directory(self.base)
        lock = self.base / 'maintenance.lock'
        fd = os.open(lock, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        started = time.monotonic()
        try:
            require(stat.S_ISREG(os.fstat(fd).st_mode), 'maintenance lock must be a regular file')
            while True:
                within(self.deadline)
                try:
                    fcntl.flock(fd, (fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH) | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    require(time.monotonic() - started < timeout, 'compiler cache maintenance lease timed out')
                    time.sleep(.02)
            yield
        finally:
            os.close(fd)

    def validate_directories(self):
        owned_directory(self.directory)
        owned_directory(self.snapshots)

    def restore(self, destination):
        bad = None
        try:
            with self.lease():
                self.validate_directories()
                latest = self.directory / 'latest.json'
                if not latest.exists():
                    return None
                regular(latest)
                pointer = json.loads(latest.read_text())
                require(set(pointer) == {'snapshot'} and re.fullmatch('[0-9a-f]{32}', pointer['snapshot']), 'invalid snapshot pointer')
                bad = self.snapshots / pointer['snapshot']
                if not bad.exists():
                    return None
                manifest = closed_snapshot(bad, self.namespace, self.deadline)
                copy_store(bad / 'store', destination, self.deadline)
                return {'snapshot': bad.name, 'check_status': manifest['check_status'], 'producer': manifest['producer'], 'bytes': manifest['bytes']}
        except TimeoutError:
            raise
        except (ValueError, OSError, KeyError, TypeError) as error:
            if bad is not None:
                with self.lease(exclusive=True):
                    self.validate_directories()
                    if bad.exists() or bad.is_symlink():
                        bad.rename(self.directory / ('quarantined-' + bad.name + '-' + uuid.uuid4().hex))
                    latest = self.directory / 'latest.json'
                    if latest.exists() and json.loads(latest.read_text()).get('snapshot') == bad.name:
                        latest.unlink()
            raise ValueError('compiler snapshot rejected and quarantined; run a separate off or empty invocation') from error

    def publish(self, source, status, producer=None):
        require(status in {'passed', 'failed'}, 'snapshot needs a finalized check status')
        entries, size = files(source, self.deadline)
        name = uuid.uuid4().hex
        with self.lease(exclusive=True):
            self.validate_directories()
            temporary = self.directory / ('.publishing-' + name)
            temporary.mkdir(mode=0o700)
            try:
                copy_store(source, temporary / 'store', self.deadline)
                write_json(temporary / 'manifest.json', {'schema': SCHEMA, 'namespace': self.namespace,
                    'snapshot': name, 'check_status': status, 'producer': producer, 'entries': entries, 'bytes': size,
                    'created_ns': time.time_ns()})
                within(self.deadline)
                temporary.rename(self.snapshots / name)
                write_json(self.directory / 'latest.json', {'snapshot': name})
                self.evict()
            finally:
                if temporary.exists():
                    shutil.rmtree(temporary)
        return name

    def evict(self):
        snapshots = []
        for namespace in self.base.iterdir():
            within(self.deadline)
            if not re.fullmatch('[0-9a-f]{64}', namespace.name):
                continue
            owned_directory(namespace)
            owned_directory(namespace / 'snapshots')
            for directory in (namespace / 'snapshots').iterdir():
                manifest = closed_snapshot(directory, namespace.name, self.deadline)
                snapshots.append((manifest['created_ns'], manifest['bytes'], directory))
        total = sum(size for _, size, _ in snapshots)
        for _, size, directory in sorted(snapshots):
            within(self.deadline)
            if total <= SNAPSHOT_LIMIT:
                break
            owned_directory(directory.parent.parent)
            owned_directory(directory.parent)
            manifest = closed_snapshot(directory, directory.parent.parent.name, self.deadline)
            require(manifest['bytes'] == size, 'snapshot changed during eviction')
            shutil.rmtree(directory)
            total -= size


def process_start(pid):
    try:
        data = Path(f'/proc/{pid}/stat').read_text()
        return int(data[data.rfind(')') + 2:].split()[19])
    except (OSError, ValueError, IndexError):
        return None


def process_parent(pid):
    try:
        data = Path(f'/proc/{pid}/stat').read_text()
        return int(data[data.rfind(')') + 2:].split()[1])
    except (OSError, ValueError, IndexError):
        return None


class CompilerCacheSession:
    def __init__(self, config, work_deadline=None, deadline=None):
        self.config = config
        self.state = 'starting'
        self.server = None
        self.work = Path(tempfile.mkdtemp(prefix='cc-', dir='/tmp'))
        self.work.chmod(0o700)
        self.store = self.work / 'store'
        self.store.mkdir(mode=0o700)
        self.snapshots = SnapshotStore(config.base, config.namespace, work_deadline)
        self.deadline = deadline
        self.report = {'version': SCHEMA, 'mode': 'local', 'namespace': config.namespace,
                       'restored': None, 'published': None, 'state': self.state,
                       'stats': None, 'cleanup_verified': False, 'error': None,
                       'restore_seconds': 0., 'startup_seconds': 0., 'cleanup_seconds': 0.}
        self.socket = self.work / 's'
        self.conf = self.work / 'config.toml'
        self.conf.write_text('[cache.disk]\n'
                             + 'dir = ' + json.dumps(str(self.store)) + '\nsize = 10737418240\n')
        self.conf.chmod(0o600)
        self.environment = {'PATH': os.environ.get('PATH', '/usr/bin:/bin'), 'HOME': str(self.work),
                            'SCCACHE_CONF': str(self.conf), 'SCCACHE_CACHED_CONF': str(self.work / 'cached-config'),
                            'SCCACHE_DIR': str(self.store), 'SCCACHE_CACHE_SIZE': str(STORE_LIMIT),
                            'SCCACHE_SERVER_UDS': str(self.socket), 'SCCACHE_NO_DAEMON': '1',
                            'SCCACHE_CLIENT_SIDE': '1', 'SCCACHE_IDLE_TIMEOUT': '0'}

    def start(self):
        started = time.monotonic()
        self.report['restored'] = self.snapshots.restore(self.store)
        self.report['restore_seconds'] = time.monotonic() - started
        self.log = (self.work / 'server.log').open('wb')
        server_env = {**self.environment, 'SCCACHE_START_SERVER': '1'}
        self.server = subprocess.Popen([str(self.config.executable)], env=server_env,
                                       stdout=self.log, stderr=self.log, start_new_session=True)
        self.server_start = process_start(self.server.pid)
        while not self.socket.exists():
            if self.server.poll() is not None or time.monotonic() - started > 5 + self.report['restore_seconds']:
                self.fail('foreground storage server did not become ready')
                raise ValueError(self.report['error'])
            time.sleep(.02)
        self.state = 'healthy'
        self.report['state'] = self.state
        self.descriptor = self.work / 'session.json'
        write_json(self.descriptor, {'schema': SCHEMA, 'owner_pid': os.getpid(), 'owner_start': process_start(os.getpid()),
                    'server_pid': self.server.pid, 'server_start': self.server_start,
                    'executable': str(self.config.executable), 'executable_sha256': self.config.binary_sha256,
                    'wrapper': str(self.config.wrapper), 'wrapper_sha256': self.config.wrapper_sha256,
                    'socket': str(self.socket), 'store': str(self.store), 'config': str(self.conf),
                    'namespace': self.config.namespace})
        self.report['startup_seconds'] = time.monotonic() - started - self.report['restore_seconds']

    def fail(self, message):
        if self.state != 'failed':
            self.state = 'failed'
            self.report.update(state='failed', error=message)

    def healthy(self):
        if self.state == 'healthy' and self.server.poll() is not None:
            self.fail('original foreground storage server exited unexpectedly')
        return self.state == 'healthy'

    def clients(self, env):
        require(self.healthy(), 'compiler cache owner is not healthy')
        return {**clean_environment(env), **{k: v for k, v in self.environment.items() if k.startswith('SCCACHE_')},
                'RUSTC_WRAPPER': str(self.config.wrapper), 'CARGO_INCREMENTAL': '0',
                'CRABBER_COMPILER_CACHE_SESSION': str(self.descriptor)}

    def close(self, status, publish=True, verify_cleanup=None):
        started = time.monotonic()
        self.snapshots.deadline = self.deadline
        healthy = self.healthy() if self.state == 'healthy' else False
        try:
            if healthy:
                try:
                    output = subprocess.check_output([str(self.config.executable), '--show-stats', '--stats-format=json'],
                              env=self.environment, timeout=2, text=True)
                    self.report['stats'] = statistics(json.loads(output))
                    require(self.healthy(), 'original server died during stats collection')
                except (OSError, ValueError, subprocess.SubprocessError) as error:
                    self.fail('compiler cache statistics failed: ' + type(error).__name__)
            if self.server is not None:
                if self.state == 'healthy':
                    self.state = 'closing'
                before_shutdown = self.server.poll()
                if before_shutdown is not None:
                    self.fail('original foreground storage server exited before requested shutdown')
                else:
                    os.killpg(self.server.pid, signal.SIGTERM)
                try:
                    exit_code = self.server.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    self.fail('foreground storage server required forced shutdown')
                    os.killpg(self.server.pid, signal.SIGKILL)
                    exit_code = self.server.wait(timeout=2)
                if before_shutdown is None and exit_code != -signal.SIGTERM:
                    self.fail('original foreground storage server did not exit from the requested SIGTERM')
                try:
                    os.killpg(self.server.pid, 0)
                    self.fail('storage server descendants survived shutdown')
                except ProcessLookupError:
                    self.report['cleanup_verified'] = True
            if verify_cleanup is not None and not verify_cleanup():
                self.report['cleanup_verified'] = False
                self.fail('owned cache clients or server descendants survived shutdown')
            if self.state != 'failed' and self.report['cleanup_verified'] and publish:
                self.report['published'] = self.snapshots.publish(self.store, status, getattr(self.config, 'producer', None))
            if self.state != 'failed':
                self.state = 'closed'
        except (OSError, ValueError, subprocess.SubprocessError) as error:
            self.fail('compiler cache close failed: ' + type(error).__name__)
        finally:
            self.report.update(state=self.state, cleanup_seconds=time.monotonic() - started)
            if hasattr(self, 'log'):
                self.log.close()
            if self.report['cleanup_verified'] or self.server is None:
                shutil.rmtree(self.work)
        return self.state != 'failed'


def validate_descriptor(path):
    regular(path)
    require(path.stat().st_uid == os.getuid() and stat.S_IMODE(path.stat().st_mode) == 0o600, 'invalid session permissions')
    data = json.loads(path.read_text())
    require(set(data) == DESCRIPTOR_FIELDS and data['schema'] == SCHEMA, 'invalid compiler session descriptor')
    require(process_start(data['owner_pid']) == data['owner_start'] and data['owner_start'] is not None
            and process_start(data['server_pid']) == data['server_start'] and data['server_start'] is not None,
            'compiler cache owner lease expired')
    ancestor = os.getppid()
    visited = set()
    while ancestor not in {None, 0, data['owner_pid']} and ancestor not in visited:
        visited.add(ancestor)
        ancestor = process_parent(ancestor)
    require(ancestor == data['owner_pid'] and process_parent(data['server_pid']) == data['owner_pid'],
            'compiler session owner must be an ancestor and own the original server')
    require(digest(Path(data['executable'])) == data['executable_sha256'], 'compiler session executable changed')
    require(digest(Path(data['wrapper'])) == data['wrapper_sha256'], 'compiler session wrapper changed')
    require(Path(data['socket']).is_socket(), 'compiler session socket missing')
    return data


def compiler_environment(descriptor, env):
    result = {name: value for name, value in env.items() if name in COMPILER_INPUTS}
    result.update(SCCACHE_CONF=descriptor['config'], SCCACHE_CACHED_CONF=str(Path(descriptor['config']).with_name('cached-config')),
                  SCCACHE_SERVER_UDS=descriptor['socket'], SCCACHE_DIR=descriptor['store'],
                  SCCACHE_CACHE_SIZE=str(STORE_LIMIT), SCCACHE_NO_DAEMON='1', SCCACHE_CLIENT_SIDE='1',
                  SCCACHE_IDLE_TIMEOUT='0', CARGO_INCREMENTAL='0')
    return result


def trusted_writer(environment):
    return (environment.get('GITHUB_REPOSITORY') == 'mattsp1290/crabber'
            and environment.get('GITHUB_EVENT_NAME') == 'push'
            and environment.get('GITHUB_REF') == 'refs/heads/main')


def ci_identity(env):
    config = configuration(ROOT, env)
    require(config is not None, 'CI compiler-result identity requires local mode')
    snapshots = SnapshotStore(config.base, config.namespace)
    graph = hashlib.sha256((ROOT / 'Cargo.lock').read_bytes()).hexdigest()
    prefix = f'crabber-compiler-results-v{SCHEMA}-{config.namespace}-'
    key = prefix + graph + '-' + '-'.join(env[name] for name in ['GITHUB_SHA', 'GITHUB_RUN_ID', 'GITHUB_RUN_ATTEMPT'])
    return {'prefix': prefix, 'key': key, 'path': str(snapshots.directory)}


def publication_eligible(directory, env):
    if not trusted_writer(env):
        return False
    config = configuration(ROOT, env)
    require(config is not None, 'CI publication requires local mode')
    snapshots = SnapshotStore(config.base, config.namespace)
    with snapshots.lease():
        snapshots.validate_directories()
        pointer = snapshots.directory / 'latest.json'
        if not pointer.exists():
            return False
        regular(pointer)
        value = json.loads(pointer.read_text())
        require(isinstance(value, dict) and set(value) == {'snapshot'} and isinstance(value['snapshot'], str)
                and re.fullmatch('[0-9a-f]{32}', value['snapshot']), 'invalid CI snapshot pointer')
        current = value['snapshot']
        manifest = closed_snapshot(snapshots.snapshots / current, config.namespace)
        for path in directory.rglob('*.json'):
            report = json.loads(path.read_text())
            local = report.get('compiler_cache', {})
            if (report.get('status') in {'passed', 'failed'} and local.get('mode') == 'local'
                    and local.get('namespace') == config.namespace and local.get('published') == current
                    and local.get('state') == 'closed' and local.get('cleanup_verified') is True
                    and manifest['check_status'] == report['status']):
                return True
    return False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    setup = sub.add_parser('install')
    setup.add_argument('directory', type=Path)
    sub.add_parser('ci-identity')
    publication = sub.add_parser('ci-publication')
    publication.add_argument('directory', type=Path)
    run = sub.add_parser('run-direct')
    run.add_argument('arguments', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.command == 'install':
        print(install(args.directory))
        return 0
    if args.command == 'ci-identity':
        for name, value in ci_identity(os.environ).items():
            print(f'{name}={value}')
        return 0
    if args.command == 'ci-publication':
        print('eligible=' + str(publication_eligible(args.directory, os.environ)).lower())
        return 0
    require(os.environ.get('CRABBER_COMPILER_CACHE') == 'local' and os.environ.get('CARGO_INCREMENTAL') == '0',
            'direct compiler cache requires CRABBER_COMPILER_CACHE=local CARGO_INCREMENTAL=0')
    arguments = args.arguments[1:] if args.arguments[:1] == ['--'] else args.arguments
    require(arguments, 'direct session requires a command')
    spec = importlib.util.spec_from_file_location('check', Path(__file__).with_name('check.py'))
    check = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(check)
    report = ROOT / 'target/test-performance' / f'direct-compiler-{time.time_ns()}.json'
    print('Direct xtask outer Cargo compilation preceded this cache session.', flush=True)
    return check.supervise(arguments, ROOT, report)


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        raise SystemExit(f'compiler cache: {error}. Recovery: {RECOVERY}')
