"""Sample only the comparison's owned Docker container, without host children.

Docker Engine API: https://docs.docker.com/reference/api/engine/
Socket requests avoid introducing sibling commands under the process supervisor.
"""
import http.client
import json
import re
import socket
import threading
import time


class DockerError(Exception):
    def __init__(self, status):
        self.status = status


class Docker:
    def __init__(self):
        self.version = None

    def request(self, method, path, body=None):
        if self.version is None:
            self.version = json.loads(self.raw_request('GET', '/version'))['ApiVersion']
            if tuple(map(int, self.version.split('.'))) < (1, 41):
                raise RuntimeError('Docker API 1.41 or newer required')
        return self.raw_request(method, '/v' + self.version + path, body)

    def raw_request(self, method, path, body=None):
        connection = http.client.HTTPConnection('localhost', timeout=2)
        connection.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        connection.sock.settimeout(2)
        try:
            connection.sock.connect('/var/run/docker.sock')
            connection.request(method, path,
                               json.dumps(body) if body is not None else None,
                               {'Content-Type': 'application/json'})
            response = connection.getresponse()
            data = response.read()
            if response.status >= 400:
                raise DockerError(response.status)
            return data
        finally:
            connection.close()

    def connections(self, container):
        query = ("SELECT count(*) FROM pg_stat_activity "
                 "WHERE backend_type='client backend' AND pid<>pg_backend_pid()")
        command = 'exec psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc ' + "\"" + query + "\""
        created = json.loads(self.request('POST', f'/containers/{container}/exec', {
            'AttachStdout': True, 'AttachStderr': True, 'Tty': False,
            'Env': ['PGCONNECT_TIMEOUT=1', 'PGOPTIONS=-c statement_timeout=1000'],
            'Cmd': ['sh', '-c', command]}))
        stream = self.request('POST', f'/exec/{created["Id"]}/start', {'Detach': False, 'Tty': False})
        stdout = bytearray()
        while stream:
            if len(stream) < 8:
                raise ValueError('incomplete Docker stream header')
            size = int.from_bytes(stream[4:8], 'big')
            if len(stream) < 8 + size:
                raise ValueError('incomplete Docker stream frame')
            if stream[0] == 1:
                stdout.extend(stream[8:8 + size])
            stream = stream[8 + size:]
        return int(stdout.strip())


class Sampler:
    def __init__(self, log):
        self.log = log
        self.stop = threading.Event()
        self.samples = []
        self.errors = []
        self.container = None
        self.thread = threading.Thread(target=self.run, daemon=True)

    def run(self):
        docker = Docker()
        while not self.stop.is_set():
            try:
                if self.container is None and self.log.exists():
                    match = re.search(r'managed PostgreSQL container=([a-f0-9]{64})', self.log.read_text())
                    if match:
                        self.container = match[1]
                if self.container:
                    stats = json.loads(docker.request('GET', f'/containers/{self.container}/stats?stream=false&one-shot=true'))
                    self.samples.append({
                        'unix_seconds': time.time(),
                        'container_cpu_nanoseconds': stats['cpu_stats']['cpu_usage']['total_usage'],
                        'container_memory_bytes': stats['memory_stats']['usage'],
                        'client_connections': docker.connections(self.container)})
            except (OSError, ValueError, KeyError, DockerError, http.client.HTTPException) as error:
                # Container startup/removal can race a sample. Never log bodies,
                # connection URLs, server messages or environment values.
                self.errors.append(type(error).__name__)
            self.stop.wait(1)

    def finish(self):
        self.stop.set()
        self.thread.join(10)
        if self.thread.is_alive() or not self.samples or not self.container:
            raise RuntimeError('owned PostgreSQL resource sampling incomplete')
        try:
            Docker().request('GET', f'/containers/{self.container}/json')
        except DockerError as error:
            if error.status != 404:
                raise
        else:
            raise RuntimeError('owned comparison container still exists')
        return {'container_absent': True, 'samples': self.samples,
                'sample_errors': self.errors,
                'peak_container_memory_bytes': max(s['container_memory_bytes'] for s in self.samples),
                'peak_client_connections': max(s['client_connections'] for s in self.samples),
                'sampled_container_cpu_seconds': (self.samples[-1]['container_cpu_nanoseconds'] - self.samples[0]['container_cpu_nanoseconds']) / 1e9,
                'sampling_scope': 'one-second samples of the owned container; CPU delta excludes startup before first sample and cleanup after last sample; connections exclude the sampler connection'}
