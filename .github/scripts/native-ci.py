#!/usr/bin/env python3
"""Source-bound hosted Native diagnostics. Full qualification remains separate."""

import argparse
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
import time
import urllib.error
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = '.github/workflows/ci.yml'
ARCHES = {'x64': ('ubuntu-latest', 'X64', 'x86_64'),
          'arm64': ('ubuntu-24.04-arm', 'ARM64', 'aarch64')}
CHECK_STEP = 'Run Native check after setup'
PROFILE = {'CARGO_BUILD_JOBS': '4', 'CARGO_INCREMENTAL': '0',
           'CARGO_PROFILE_DEV_DEBUG': '0', 'CARGO_PROFILE_TEST_DEBUG': '0',
           'CARGO_PROFILE_DEV_STRIP': 'symbols', 'CARGO_PROFILE_TEST_STRIP': 'symbols'}
CORRELATION = ('Artifact metadata authenticates repository/run/source and archive bytes. '
               'Artifact-to-attempt/job association is source-trusted correlation. '
               'The artifact API authenticates neither attempt nor uploading job ID.')
MAX_ARCHIVE = 128 * 1024 * 1024
MAX_EXPANDED = 512 * 1024 * 1024


def require(condition, message):
    if not condition:
        raise ValueError(message)


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + '\n')


def load(path):
    return json.loads(path.read_text())


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def slots():
    names = load(ROOT / '.github/native-workers.json')
    require(isinstance(names, list) and len(names) == 8 and len(set(names)) == 8
            and all(isinstance(name, str) and re.fullmatch(r'native-(library|remaining)-[1-4]', name)
                    for name in names), 'invalid committed Native scheduling snapshot')
    return names


@dataclass(frozen=True)
class Identity:
    repository: str
    run: int
    attempt: int
    source: str

    def __post_init__(self):
        require(re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', self.repository), 'invalid repository')
        require(type(self.run) is int and type(self.attempt) is int
                and self.run > 0 and self.attempt > 0, 'invalid run or attempt')
        require(re.fullmatch(r'[0-9a-f]{40}', self.source), 'source must be an exact SHA')

    @classmethod
    def environment(cls):
        return cls(os.environ['GITHUB_REPOSITORY'], int(os.environ['GITHUB_RUN_ID']),
                   int(os.environ['GITHUB_RUN_ATTEMPT']), os.environ['GITHUB_SHA'])

    def artifact_name(self, arch, slot=None):
        stem = 'native-worker' if slot else 'native-collection'
        suffix = f'-{slot}' if slot else ''
        return f'{stem}-{self.source}-{arch}-{self.run}-{self.attempt}{suffix}'


def worker_name(arch, slot):
    return f'Native worker ({arch}, {slot})'


def collector_name(arch):
    return f'Native collect ({arch})'


def plan(mode, qualify, qualification_revision, source):
    require(re.fullmatch(r'[0-9a-f]{40}', source), 'planner source must be an exact SHA')
    require(mode in {'off', 'x64', 'arm64', 'both'}, 'unknown Native diagnostic choice')
    require(not (mode != 'off' and (qualify or qualification_revision == source)),
            'qualification and Native diagnostic cannot run together')
    selected = list(ARCHES) if mode == 'both' else ([] if mode == 'off' else [mode])
    return {'workers': {'include': [{'arch': arch, 'runner': ARCHES[arch][0], 'slot': slot}
                                   for arch in selected for slot in slots()]},
            'collectors': {'include': [{'arch': arch, 'runner': ARCHES[arch][0]}
                                      for arch in selected]}}


class SafeRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        require(newurl.startswith('https://'), 'artifact redirect requires HTTPS')
        redirected = super().redirect_request(req, fp, code, msg, headers, newurl)
        if redirected is not None:
            redirected.remove_header('Authorization')
        return redirected


class GitHub:
    def __init__(self, identity, destination):
        self.identity = identity
        self.destination = destination
        self.provenance = []
        self.token = os.environ['GH_TOKEN']
        self.opener = urllib.request.build_opener(SafeRedirect())

    def request(self, endpoint, limit=16 * 1024 * 1024):
        prefix = f'/repos/{self.identity.repository}'
        require(endpoint == prefix or endpoint.startswith(prefix + '/'), 'unexpected API endpoint')
        request = urllib.request.Request('https://api.github.com' + endpoint, headers={
            'Authorization': 'Bearer ' + self.token,
            'Accept': 'application/vnd.github+json', 'X-GitHub-Api-Version': '2022-11-28',
            'User-Agent': 'crabber-native-diagnostic'})
        with self.opener.open(request, timeout=120) as response:
            data = response.read(limit + 1)
        require(len(data) <= limit, 'API response exceeds diagnostic limit')
        return data

    def json(self, endpoint):
        value = json.loads(self.request(endpoint))
        self.provenance.append({'endpoint': endpoint, 'fetched_at': datetime.now(timezone.utc).isoformat(),
                                'response': value})
        return value

    def pages(self, endpoint, key):
        rows = []
        for page in range(1, 101):
            separator = '&' if '?' in endpoint else '?'
            value = self.json(f'{endpoint}{separator}per_page=100&page={page}')
            batch = value[key]
            require(isinstance(batch, list), 'invalid paginated response')
            rows.extend(batch)
            if len(batch) < 100:
                require(len(rows) == value['total_count'], 'incomplete paginated inventory')
                return rows
        raise ValueError('API pagination exceeds diagnostic limit')

    def context(self):
        prefix = f'/repos/{self.identity.repository}'
        repository = self.json(prefix)
        workflow = self.json(prefix + '/actions/workflows/ci.yml')
        run = self.json(prefix + f'/actions/runs/{self.identity.run}/attempts/{self.identity.attempt}')
        validate_context(self.identity, repository, workflow, run)
        jobs = self.pages(prefix + f'/actions/runs/{self.identity.run}/attempts/{self.identity.attempt}/jobs', 'jobs')
        return repository, workflow, run, jobs

    def artifacts(self):
        return self.pages(f'/repos/{self.identity.repository}/actions/runs/{self.identity.run}/artifacts', 'artifacts')

    def archive(self, artifact, destination):
        raw = self.request(f'/repos/{self.identity.repository}/actions/artifacts/{artifact["id"]}/zip', MAX_ARCHIVE)
        verify_archive(raw, artifact)
        destination.write_bytes(raw)
        return raw

    def preserve(self):
        save(self.destination / 'server-provenance.json', self.provenance)


def validate_context(identity, repository, workflow, run):
    require(repository['full_name'] == identity.repository, 'wrong repository')
    require(workflow['path'] == WORKFLOW and workflow['id'] == run['workflow_id'], 'wrong workflow')
    require(run['id'] == identity.run and run['run_attempt'] == identity.attempt, 'wrong run attempt')
    require(run['head_sha'] == identity.source and run['event'] == 'workflow_dispatch', 'wrong run source or event')
    require(run['repository']['id'] == repository['id']
            and run['head_repository']['id'] == repository['id'], 'foreign repository source')
    require(run['path'].split('@')[0] == WORKFLOW, 'wrong run workflow path')


def admit_job(identity, jobs, name, arch, terminal=True):
    matches = [job for job in jobs if job['name'] == name]
    require(len(matches) == 1, 'missing or duplicate required job ' + name)
    job = matches[0]
    require(job['run_id'] == identity.run and job['run_attempt'] == identity.attempt
            and job['head_sha'] == identity.source, 'wrong job context')
    require(ARCHES[arch][0] in job['labels'], 'wrong runner label')
    if terminal:
        require(job['status'] == 'completed' and job['conclusion'] == 'success' and job['completed_at'],
                'required job did not succeed')
    return job


def admit_artifact(identity, repository_id, artifacts, name):
    matches = [artifact for artifact in artifacts if artifact['name'] == name]
    require(len(matches) == 1, 'missing or duplicate required artifact ' + name)
    artifact = matches[0]
    run = artifact['workflow_run']
    require(not artifact['expired'] and run['id'] == identity.run
            and run['head_sha'] == identity.source and run['repository_id'] == repository_id
            and run['head_repository_id'] == repository_id, 'wrong artifact context')
    require(re.fullmatch(r'sha256:[0-9a-f]{64}', artifact.get('digest', '')), 'missing or unsupported server digest')
    require(type(artifact['id']) is int and type(artifact['size_in_bytes']) is int
            and 0 < artifact['size_in_bytes'] <= MAX_ARCHIVE, 'invalid artifact identity or size')
    return artifact


def verify_archive(raw, artifact):
    require(artifact.get('digest') == 'sha256:' + sha256(raw), 'server archive digest mismatch')
    require(len(raw) == artifact['size_in_bytes'], 'server archive size mismatch')


def extract_archive(raw_path, destination):
    require(not destination.exists(), 'extraction destination already exists')
    with zipfile.ZipFile(raw_path) as archive:
        entries = archive.infolist()
        require(0 < len(entries) <= 20000, 'invalid ZIP entry count')
        names, files, total = set(), set(), 0
        for entry in entries:
            name = entry.filename
            parts = name.rstrip('/').split('/')
            require(name and '\\' not in name and '\x00' not in name and ':' not in name
                    and not name.startswith('/') and all(part not in {'', '.', '..'} for part in parts),
                    'unsafe ZIP path')
            normalized = '/'.join(parts)
            require(normalized not in names, 'duplicate ZIP path')
            names.add(normalized)
            mode = entry.external_attr >> 16
            kind = stat.S_IFMT(mode)
            require(not entry.flag_bits & 1 and entry.compress_type in {zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED}
                    and kind in {0, stat.S_IFREG, stat.S_IFDIR}
                    and (kind != stat.S_IFDIR or entry.is_dir())
                    and (not entry.is_dir() or kind != stat.S_IFREG), 'unsupported ZIP entry')
            require(entry.file_size >= 0 and entry.compress_size >= 0, 'invalid ZIP sizes')
            total += entry.file_size
            require(total <= MAX_EXPANDED, 'ZIP expansion exceeds diagnostic limit')
            if not entry.is_dir():
                files.add(normalized)
        require(not any('/'.join(PurePosixPath(name).parts[:i]) in files
                        for name in names for i in range(1, len(PurePosixPath(name).parts))),
                'overlapping ZIP paths')
        destination.mkdir(parents=True)
        for entry in entries:
            path = destination.joinpath(*entry.filename.rstrip('/').split('/'))
            if entry.is_dir():
                path.mkdir(parents=True, exist_ok=True)
            else:
                path.parent.mkdir(parents=True, exist_ok=True)
                with archive.open(entry) as source, path.open('xb') as target:
                    shutil.copyfileobj(source, target)


def admit_envelope(identity, arch, slot, directory):
    envelope = load(directory / 'ci-envelope.json')
    require(envelope['identity'] == asdict(identity) and envelope['arch'] == arch
            and envelope['runner_arch'] == ARCHES[arch][1]
            and envelope['machine'] == ARCHES[arch][2]
            and envelope['slot'] == slot and envelope['job'] == 'native-worker'
            and envelope['job_name'] == worker_name(arch, slot), 'wrong worker envelope context')
    require(envelope['cache_state'] == 'unclassified' and envelope['compiler_cache_restored'] is False
            and envelope['profile'] == PROFILE and envelope['source_clean'] is True,
            'wrong worker profile, cache policy, or dirty source')
    require(envelope['workflow_sha'] == identity.source
            and envelope['workflow_ref'].startswith(identity.repository + '/' + WORKFLOW + '@'),
            'wrong worker workflow context')
    bundle = directory / 'bundle'
    require(envelope['rustc'] == load(bundle / 'bundle.json')['source']['rustc'],
            'worker toolchain envelope mismatch')
    require(envelope['bundle_index_sha256'] == sha256((bundle / 'bundle.json').read_bytes()),
            'worker bundle index digest mismatch')
    require(sorted(path.name for path in directory.iterdir()) == ['bundle', 'ci-envelope.json'],
            'unexpected worker archive layout')
    wrapper = load(bundle / 'reports/check.json')
    require(wrapper['coverage_scope'] == slot and wrapper['profile'] == PROFILE
            and wrapper['source_revision'] == identity.source and wrapper['end_revision'] == identity.source
            and wrapper['source_clean'] is True and wrapper['end_clean'] is True
            and wrapper['cache_state'] == 'unclassified' and wrapper['status'] == 'passed',
            'worker report context mismatch')
    require(ARCHES[arch][2] == wrapper['platform'][4], 'worker report architecture mismatch')
    return envelope


def worker(args):
    identity = Identity.environment()
    require(args.slot in slots(), 'unknown worker slot')
    report = load(args.report)
    phase = load(Path(report['phases_report']))
    metrics = Path(phase['fixture_metrics'])
    require(metrics.is_absolute() and metrics.name == 'fixtures.jsonl', 'invalid finalized fixture data path')
    require(report['coverage_scope'] == args.slot and report['status'] == 'passed'
            and report['profile'] == PROFILE and report['cache_state'] == 'unclassified'
            and report['source_revision'] == identity.source and report['end_revision'] == identity.source
            and report['source_clean'] is True and report['end_clean'] is True, 'invalid worker report')
    args.output.mkdir(parents=True, exist_ok=False)
    subprocess.run(['cargo', 'xtask', 'native-bundle', '--report', str(args.report),
                    '--data-dir', str(metrics.parent), '--output', str(args.output / 'bundle')],
                   cwd=ROOT, check=True)
    envelope = {'version': 1, 'identity': asdict(identity), 'arch': args.arch, 'slot': args.slot,
                'runner_arch': os.environ['RUNNER_ARCH'], 'machine': os.uname().machine,
                'workflow_sha': os.environ['GITHUB_WORKFLOW_SHA'],
                'workflow_ref': os.environ['GITHUB_WORKFLOW_REF'],
                'rustc': load(args.output / 'bundle/bundle.json')['source']['rustc'],
                'job': os.environ['GITHUB_JOB'], 'job_name': worker_name(args.arch, args.slot),
                'profile': report['profile'], 'cache_state': 'unclassified',
                'compiler_cache_restored': False, 'source_clean': True,
                'bundle_index_sha256': sha256((args.output / 'bundle/bundle.json').read_bytes()),
                'wrapper_wall_seconds': report['wall_seconds'],
                'wrapper_cpu_seconds': {key: value for key, value in report.items() if 'cpu' in key},
                'setup_seconds_before_check': args.setup_seconds,
                'artifact_job_association': CORRELATION}
    save(args.output / 'ci-envelope.json', envelope)
    admit_envelope(identity, args.arch, args.slot, args.output)


def server_time(value):
    require(isinstance(value, str) and re.fullmatch(
        r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)', value),
        'missing timezone-aware server timestamp')
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00')).astimezone(timezone.utc)
    except OverflowError:
        raise ValueError('server timestamp out of range') from None


def outer_timing(workers, collector):
    starts = []
    for worker_job in workers:
        steps = [step for step in worker_job['steps'] if step['name'] == CHECK_STEP]
        require(len(steps) == 1 and steps[0]['status'] == 'completed'
                and steps[0]['conclusion'] == 'success', 'missing successful worker check step')
        start = server_time(steps[0]['started_at'])
        step_end = server_time(steps[0]['completed_at'])
        job_end = server_time(worker_job['completed_at'])
        require(start <= step_end <= job_end, 'invalid worker step or job interval')
        starts.append(start)
    require(starts and collector['status'] == 'completed' and collector['conclusion'] == 'success',
            'collector has not successfully completed')
    start, end = min(starts), server_time(collector['completed_at'])
    require(all(end >= server_time(job['completed_at']) for job in workers),
            'collector completion precedes a required worker job')
    seconds = (end - start).total_seconds()
    return {'earliest_worker_check_started_at': start.isoformat().replace('+00:00', 'Z'),
            'collector_job_completed_at': collector['completed_at'], 'server_span_seconds': seconds,
            'timestamp_resolution_seconds': 1, 'conservative_upper_bound_seconds': seconds + 1,
            'scope': 'earliest required worker check after setup through collector job completion',
            'includes': 'stagger, later queue/setup, compilation, cleanup, transfers, collector, publication, poststeps',
            'flat_matrix_both_architecture_wait_included': True,
            'complete_suite_qualified': False, 'full_check_executed': False}


def collect(args):
    identity = Identity.environment()
    require(subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
            == identity.source, 'collector checkout differs from source')
    require(not subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT, text=True).strip(),
            'collector source is dirty')
    args.output.mkdir(parents=True, exist_ok=False)
    api = GitHub(identity, args.output)
    started = time.monotonic()
    try:
        repository, workflow, run, jobs = api.context()
        artifacts = api.artifacts()
        records = []
        bundle_root = args.output / 'bundles'
        bundle_root.mkdir()
        for slot in slots():
            job = admit_job(identity, jobs, worker_name(args.arch, slot), args.arch)
            artifact = admit_artifact(identity, repository['id'], artifacts, identity.artifact_name(args.arch, slot))
            require(artifact['id'] not in {record['artifact']['id'] for record in records},
                    'duplicate worker artifact ID')
            archive = args.output / 'archives' / f'{slot}.zip'
            archive.parent.mkdir(exist_ok=True)
            api.archive(artifact, archive)
            extracted = args.output / 'workers' / slot
            extract_archive(archive, extracted)
            envelope = admit_envelope(identity, args.arch, slot, extracted)
            shutil.copytree(extracted / 'bundle', bundle_root / slot)
            records.append({'slot': slot, 'job_id': job['id'], 'artifact': artifact,
                            'archive': str(archive.relative_to(args.output)), 'envelope': envelope})
        downloaded = time.monotonic()
        subprocess.run(['cargo', 'xtask', 'native-matrix', '--check', '.github/native-workers.json'],
                       cwd=ROOT, check=True)
        subprocess.run(['cargo', 'xtask', 'native-reduce', '--source', identity.source,
                        '--bundles', str(bundle_root), '--output', str(args.output / 'native-result.json')],
                       cwd=ROOT, check=True)
        result = load(args.output / 'native-result.json')
        admit_result(result)
        require(not subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT, text=True).strip(),
                'collector source changed during reduction')
        save(args.output / 'collection.json', {
            'version': 1, 'identity': asdict(identity), 'arch': args.arch,
            'repository_id': repository['id'], 'workflow_id': workflow['id'],
            'workers': records, 'artifact_job_association': CORRELATION,
            'native_coverage_validated': True, 'complete_suite_qualified': False, 'full_check_executed': False,
            'cache_state': 'unclassified', 'timing_status': 'provisional; obtain receipt after collector completion',
            'stages_seconds': {'authenticated_download_and_extraction': downloaded - started,
                               'collector_compilation_and_reduction': time.monotonic() - downloaded}})
    finally:
        api.preserve()


def admit_result(result):
    require(result['native_coverage_validated'] is True
            and result['complete_suite_qualified'] is False and result['full_check_executed'] is False,
            'Native result changed assurance scope')


def receipt(args):
    identity = Identity(args.repository, args.run, args.attempt, args.source)
    require(subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
            == identity.source, 'receipt checkout differs from source')
    require(not subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT, text=True).strip(),
            'receipt source is dirty')
    args.output.mkdir(parents=True, exist_ok=False)
    api = GitHub(identity, args.output)
    try:
        repository, workflow, run, jobs = api.context()
        collector = admit_job(identity, jobs, collector_name(args.arch), args.arch)
        workers = [admit_job(identity, jobs, worker_name(args.arch, slot), args.arch) for slot in slots()]
        artifacts = api.artifacts()
        artifact = admit_artifact(identity, repository['id'], artifacts, identity.artifact_name(args.arch))
        archive = args.output / 'collection.zip'
        api.archive(artifact, archive)
        directory = args.output / 'collection'
        extract_archive(archive, directory)
        collection = load(directory / 'collection.json')
        require(collection['identity'] == asdict(identity) and collection['arch'] == args.arch
                and collection['repository_id'] == repository['id'] and collection['workflow_id'] == workflow['id'],
                'collection context mismatch')
        records = collection['workers']
        require([record['slot'] for record in records] == slots(), 'wrong collection slots')
        require(len({record['artifact']['id'] for record in records}) == len(records),
                'duplicate collection worker artifact ID')
        for record, job in zip(records, workers, strict=True):
            slot = record['slot']
            fresh = admit_artifact(identity, repository['id'], artifacts, identity.artifact_name(args.arch, slot))
            require(record['job_id'] == job['id'] and all(record['artifact'][key] == fresh[key]
                    for key in ['id', 'name', 'size_in_bytes', 'digest', 'workflow_run']), 'worker provenance changed')
            require(record['archive'] == f'archives/{slot}.zip', 'unexpected archived worker path')
            verify_archive((directory / record['archive']).read_bytes(), fresh)
            admit_envelope(identity, args.arch, slot, directory / 'workers' / slot)
        admit_result(load(directory / 'native-result.json'))
        save(args.output / 'receipt.json', {'version': 1, 'identity': asdict(identity), 'arch': args.arch,
             'collector_job_id': collector['id'], 'collector_artifact': artifact,
             'timing': outer_timing(workers, collector), 'artifact_job_association': CORRELATION,
             'native_coverage_validated': True, 'complete_suite_qualified': False, 'full_check_executed': False,
             'cache_state': 'unclassified', 'receipt_retrieval_excluded_from_span': True,
             'semantic_validation': 'source-trusted successful collector invoked existing Native reducer; offline replay available'})
    finally:
        api.preserve()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    planning = commands.add_parser('plan')
    planning.add_argument('--mode', choices=['off', 'x64', 'arm64', 'both'], default='off')
    planning.add_argument('--qualify', action='store_true')
    planning.add_argument('--qualification-revision', default='')
    planning.add_argument('--source', required=True)
    for command in ['worker', 'collect', 'receipt']:
        child = commands.add_parser(command)
        child.add_argument('--arch', choices=ARCHES, required=True)
        child.add_argument('--output', type=Path, required=True)
        if command == 'worker':
            child.add_argument('--slot', required=True)
            child.add_argument('--report', type=Path, required=True)
            child.add_argument('--setup-seconds', type=float, required=True)
        if command == 'receipt':
            child.add_argument('--repository', required=True)
            child.add_argument('--run', type=int, required=True)
            child.add_argument('--attempt', type=int, required=True)
            child.add_argument('--source', required=True)
    args = parser.parse_args()
    try:
        if args.command == 'plan':
            value = plan(args.mode, args.qualify, args.qualification_revision, args.source)
            with open(os.environ['GITHUB_OUTPUT'], 'a') as output:
                for key, matrix in value.items():
                    print(key + '=' + json.dumps(matrix, separators=(',', ':')), file=output)
        else:
            {'worker': worker, 'collect': collect, 'receipt': receipt}[args.command](args)
    except (ValueError, KeyError, OSError, subprocess.CalledProcessError, zipfile.BadZipFile):
        # API exception strings can contain signed archive URLs. Keep them out of logs.
        parser.exit(1, 'Native diagnostic rejected; inspect preserved sanitized provenance and source-bound inputs.\n')


if __name__ == '__main__':
    main()
