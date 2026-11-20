#!/usr/bin/env python3
"""Native verification runner. Build with scoped credentials; run isolated tests without job identity."""
import ctypes
import json
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time

BEHAVIORS = ('service_free', 'installer', 'conformance', 'runtime')
PREFIX = 'github.com/colony-2/pgjobdb/'
INTEGRATION = PREFIX + 'pkg/pgjobdb/runtime/integration'
COMMAND = PREFIX + 'cmd/jobdb'
REQUIRED_CONSUMER = {'TestConsumerConstructorCapabilities', 'TestConsumerHTTPRetiresAttachment',
                     'TestConsumerCapabilities', 'TestConsumerReadOnlyHistory',
                     'TestConsumerSignedLeaseAuthority', 'TestConsumerNamespacedAuthority'}
REQUIRED_COMMAND = {'TestPostgresConsumerGoalsCommands'}
TASK_DELEGATION_PACKAGE = INTEGRATION + '/internal/jobdbtest'
REQUIRED_TASK_DELEGATION = {'TestPostgresTaskDelegation'}


def select_packages(listing, behavior):
    if len(listing) != len(set(listing)):
        raise ValueError('duplicate package enumeration')
    required = {COMMAND, INTEGRATION, PREFIX + 'pkg/pgjobdb/installer',
                INTEGRATION + '/internal/jobdbtest',
                INTEGRATION + '/internal/jobdbtest/engineconformance',
                INTEGRATION + '/internal/jobdbtest/usageparity', INTEGRATION + '/workflow'}
    if not required.issubset(listing):
        raise ValueError('missing required packages: ' + ', '.join(sorted(required - set(listing))))
    def selected(pkg):
        name = pkg.removeprefix(PREFIX)
        if behavior == 'installer': return name == 'pkg/pgjobdb/installer'
        if behavior == 'conformance': return pkg == INTEGRATION
        if behavior == 'runtime': return pkg == COMMAND or pkg.startswith(INTEGRATION)
        return not (name == 'pkg/pgjobdb/installer' or pkg.startswith(INTEGRATION))
    packages = [p for p in listing if selected(p)]
    if not packages: raise ValueError('empty behavior selection')
    return packages


def select_tests(package, names, behavior):
    tests = [n for n in names if n.startswith('Test')]
    if not tests or len(tests) != len(set(tests)):
        raise ValueError('empty or duplicate test enumeration: ' + package)
    if package == TASK_DELEGATION_PACKAGE and not REQUIRED_TASK_DELEGATION.issubset(tests):
        raise ValueError('missing PostgreSQL task delegation tests')
    if package == COMMAND:
        if not REQUIRED_COMMAND.issubset(tests): raise ValueError('missing PostgreSQL command tests')
        database = [n for n in tests if n.startswith('TestPostgresConsumerGoals')]
        return database if behavior == 'runtime' else [n for n in tests if n not in database]
    if package == INTEGRATION:
        if not REQUIRED_CONSUMER.issubset(tests): raise ValueError('missing focused consumer tests')
        conformance = [n for n in tests if 'Conformance' in n]
        if not conformance: raise ValueError('empty conformance test selection')
        return conformance if behavior == 'conformance' else [n for n in tests if n not in conformance]
    return tests


def validate_outcomes(events, chosen):
    outcomes = {e['Test']: e['Action'] for e in events
                if 'Test' in e and e.get('Action') in ('pass', 'fail', 'skip')}
    missing = set(chosen) - set(outcomes)
    if missing: raise ValueError('missing required test outcomes: ' + ', '.join(sorted(missing)))
    for test in chosen:
        if outcomes[test] != 'pass': raise ValueError('required test did not pass: ' + test + ': ' + outcomes[test])
    for name, action in outcomes.items():
        # Preserve upstream's stricter native acceptance: every selected child
        # must execute, including inherited conformance and availability cases.
        if action in ('fail', 'skip'):
            raise ValueError('required test did not pass: ' + name + ': ' + action)
    return outcomes

def enable_subreaper():
    # PostgreSQL daemonizes into a new session. Adopt detached descendants so
    # timeout/cancellation cleanup also reaches them without privileged tooling.
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:  # PR_SET_CHILD_SUBREAPER, unprivileged on Linux
        raise OSError(ctypes.get_errno(), 'cannot enable native child cleanup')


def cleanup_children():
    def children():
        found = []
        for path in Path('/proc').glob('[0-9]*/stat'):
            try:
                fields = path.read_text().rsplit(')', 1)[1].split()
                if int(fields[1]) == os.getpid(): found.append(int(path.parent.name))
            except (FileNotFoundError, ProcessLookupError):
                pass
        return found
    # Detached grandchildren become our children after their launcher exits.
    deadline = time.monotonic() + 3
    while True:
        ids = children()
        if not ids: return
        for pid in ids:
            try: os.kill(pid, signal.SIGTERM if time.monotonic() < deadline else signal.SIGKILL)
            except ProcessLookupError: pass
        for pid in ids:
            try: os.waitpid(pid, os.WNOHANG)
            except ChildProcessError: pass
        if time.monotonic() > deadline + 3:
            raise RuntimeError('native child cleanup did not complete')
        time.sleep(0.05)


def execute(argv, log, **kwargs):
    log.write('$ ' + json.dumps(argv) + '\n'); log.flush()
    # Timeout is a failure; terminate the entire child process group, including PostgreSQL.
    enable_subreaper()
    p = subprocess.Popen(argv, stdout=kwargs.pop('stdout', log), stderr=log, start_new_session=True, **kwargs)
    def stop(signum, frame):
        os.killpg(p.pid, signal.SIGTERM)
        try: p.wait(timeout=2)
        except subprocess.TimeoutExpired:
            os.killpg(p.pid, signal.SIGKILL)
            p.wait()
        raise SystemExit(128 + signum)
    old = {s: signal.signal(s, stop) for s in (signal.SIGTERM, signal.SIGINT)}
    try:
        try:
            return p.wait(timeout=1800)
        except subprocess.TimeoutExpired:
            os.killpg(p.pid, signal.SIGKILL)
            p.wait()
            raise RuntimeError('command timed out after 1800 seconds')
    finally:
        pending = sys.exception()
        try:
            cleanup_children()
        except Exception:
            if isinstance(pending, SystemExit): raise pending
            raise
        finally:
            for s, handler in old.items():
                signal.signal(s, handler)


def test_outcomes(output):
    """Count named Go test outcomes; required native checks may not pass via skips."""
    names = {kind: re.findall(r'^\s*--- ' + marker + r': (\S+)', output, re.MULTILINE)
             for kind, marker in (('passed', 'PASS'), ('failed', 'FAIL'), ('skipped', 'SKIP'))}
    return {**{kind: len(tests) for kind, tests in names.items()},
            'failed_names': names['failed'], 'skipped_names': names['skipped']}


def run(behavior):
    out = Path(os.environ['OUTBOX']); out.mkdir(parents=True, exist_ok=True)
    root = Path(os.environ.get('ROOT', '.')).resolve()
    result = {'behavior': behavior, 'status': 'failed', 'commands': [], 'packages': []}
    with (out / (behavior + '.log')).open('w') as log:
        try:
            if platform.system() != 'Linux' or platform.machine() not in ('aarch64', 'arm64'):
                raise RuntimeError('verification requires native Linux arm64')
            with tempfile.TemporaryDirectory(prefix='pgjobdb-verify-') as tmp:
                scratch = Path(tmp)
                # Non-root binaries need traversal access to their own isolated temporary tree.
                scratch.chmod(0o755)
                for f in ('go.mod', 'go.sum'):
                    shutil.copyfile(root / f, scratch / f)
                buildenv = dict(os.environ, GOPRIVATE='github.com/colony-2/*',
                                GOCACHE=str(scratch / 'cache'), GOMODCACHE=str(scratch / 'modules'),
                                GOTMPDIR=str(scratch), CGO_ENABLED='0', GOTOOLCHAIN='local',
                                GOWORK='off', GOFLAGS='')
                goflags = ['-modfile=' + str(scratch / 'go.mod'), '-tags=integration']
                listcmd = ['go', 'list', *goflags, './...']
                result['commands'].append(listcmd)
                with (scratch / 'packages.txt').open('w') as listing_file:
                    if execute(listcmd, log, cwd=root, env=buildenv, stdout=listing_file):
                        raise RuntimeError('Go package enumeration prerequisite failed')
                listing = (scratch / 'packages.txt').read_text().splitlines()
                packages = select_packages(listing, behavior)
                result['packages'] = packages
                # Allow only runtime prerequisites, never live C2J/Pulse/JobDB identities or credentials.
                uid = 65534 if os.geteuid() == 0 else os.geteuid()
                gid = 65534 if os.geteuid() == 0 else os.getegid()
                testhome = scratch / 'test-home'; testhome.mkdir(); os.chown(testhome, uid, gid)
                testenv = {k: v for k, v in os.environ.items() if k in ('PATH', 'SSL_CERT_FILE', 'SSL_CERT_DIR', 'LANG', 'LC_ALL')}
                # initdb derives database encoding from locale. A worker's C/POSIX
                # locale would create SQL_ASCII databases and break Unicode JSON.
                testenv.update(HOME=str(testhome), TMPDIR=str(testhome), XDG_CACHE_HOME=str(testhome / 'cache'),
                               LANG='C.UTF-8', LC_ALL='C.UTF-8', GOTOOLCHAIN='local', GOWORK='off', GOFLAGS='')
                # Package bindings may be worker launchers that cannot run as
                # the database UID. Use the resolved, already installed Go tool.
                goroot = subprocess.check_output(['go', 'env', 'GOROOT'], cwd=root, env=buildenv,
                                                text=True, timeout=30).strip()
                testenv['PATH'] = str(Path(goroot) / 'bin') + os.pathsep + testenv.get('PATH', '')
                result['test_uid'] = uid
                result['test_locale'] = 'C.UTF-8'
                drop = {'user': uid, 'group': gid, 'extra_groups': []} if os.geteuid() == 0 else {}
                failed = False
                if behavior == 'service_free':
                    # The managed checkout's ancestors may be private to root.
                    # Copy source, never credentials, for unprivileged hook tests.
                    hookroot = scratch / 'hook-source'
                    hookroot.mkdir()
                    for directory in ('.c2', 'cmd', 'pkg'):
                        shutil.copytree(root / directory, hookroot / directory,
                                        ignore=shutil.ignore_patterns('__pycache__'))
                    for name in ('go.mod', 'go.sum'):
                        shutil.copyfile(root / name, hookroot / name)
                    hooktests = ['python3', str(hookroot / '.c2/recipes/hooks/build/tests/test_verify.py')]
                    result['commands'].append(hooktests)
                    if execute(hooktests, log, cwd=testhome, env=testenv, **drop):
                        failed = True
                result['tests'] = {}
                for i, package in enumerate(packages):
                    binary = scratch / ('test-' + str(i))
                    cmd = ['go', 'test', *goflags, '-c', '-o', str(binary), package]
                    result['commands'].append(cmd)
                    if execute(cmd, log, cwd=root, env=buildenv):
                        log.write('Not run: ' + package + ' tests; compilation prerequisite failed.\n')
                        failed = True; continue
                    if not binary.exists():
                        log.write('No test files: ' + package + '\n'); continue
                    listcmd = [str(binary), '-test.list=.']
                    result['commands'].append(listcmd)
                    with (scratch / 'test-names.txt').open('w') as names_file:
                        if execute(listcmd, log, env=testenv, cwd=testhome, stdout=names_file, **drop):
                            raise RuntimeError('test enumeration prerequisite failed')
                    names = (scratch / 'test-names.txt').read_text().splitlines()
                    chosen = select_tests(package, names, behavior)
                    if not chosen: raise RuntimeError('empty test selection: ' + package)
                    expr = '^(' + '|'.join(chosen) + ')$'
                    cmd = [str(binary), '-test.v=test2json', '-test.count=1', '-test.timeout=25m', '-test.run=' + expr]
                    result['commands'].append(cmd)
                    with (scratch / 'test-output.txt').open('w') as output:
                        code = execute(cmd, log, cwd=testhome, env=testenv, stdout=output, **drop)
                    raw = (scratch / 'test-output.txt').read_text()
                    log.write(raw); log.flush()
                    # Use Go's parser, including nested/skipped outcomes, rather
                    # than inferring acceptance from the binary's exit code.
                    jsoncmd = ['go', 'tool', 'test2json', '-p', package]
                    result['commands'].append(jsoncmd)
                    with (scratch / 'test-output.txt').open() as source, (scratch / 'events.json').open('w') as eventsfile:
                        if execute(jsoncmd, log, cwd=root, env=buildenv, stdin=source, stdout=eventsfile):
                            raise RuntimeError('test2json prerequisite failed')
                    events = [json.loads(line) for line in (scratch / 'events.json').read_text().splitlines()]
                    try:
                        result['tests'][package] = validate_outcomes(events, chosen)
                    except ValueError as exc:
                        log.write(str(exc) + '\n')
                        result['tests'][package] = {'acceptance_error': str(exc)}
                        failed = True
                    if code: failed = True
                log.flush()
                outcomes = test_outcomes((out / (behavior + '.log')).read_text().replace('\x16', ''))
                result['outcomes'] = outcomes
                result['status'] = 'failed' if failed or outcomes['failed'] or outcomes['skipped'] else 'passed'
        except Exception as exc:
            result['status'] = 'failed'
            log.write(type(exc).__name__ + ': ' + str(exc) + '\n')
            result['error'] = str(exc)
            log.write('Dependent tests not run after prerequisite or launch failure.\n')
    (out / (behavior + '.json')).write_text(json.dumps(result, indent=2) + '\n')
    print(result['status'], end='')


def aggregate():
    inbox, out = Path(os.environ['INBOX']), Path(os.environ['OUTBOX'])
    records = []
    for b in BEHAVIORS:
        record = json.loads((inbox / (b + '.json')).read_text())
        if record.get('behavior') != b or record.get('status') not in ('passed', 'failed'):
            raise ValueError('malformed behavior evidence: ' + b)
        if not (inbox / (b + '.log')).stat().st_size:
            raise ValueError('missing or empty behavior log: ' + b)
        records.append(record)
    status = 'passed' if all(r['status'] == 'passed' for r in records) else 'failed'
    report = '# Native verification\n\n' + '\n'.join('* ' + r['behavior'] + ': ' + r['status'] for r in records)
    report += '\n\nHeld: Docker container-smoke/Postgres/MinIO topology; amd64/macOS matrix; release, signing and publication.\n'
    (out / 'verification.md').write_text(report)
    metadata = {'platform': platform.platform(), 'machine': platform.machine(), 'python': sys.version,
                'go': subprocess.check_output(['go', 'version'], text=True, timeout=30).strip(),
                'candidate': os.environ.get('CANDIDATE'), 'base': os.environ.get('BASE')}
    (out / 'environment.json').write_text(json.dumps(metadata, indent=2) + '\n')
    print(status, end='')

if __name__ == '__main__':
    if sys.argv[1] == 'aggregate': aggregate()
    elif sys.argv[1] in BEHAVIORS: run(sys.argv[1])
    else: raise ValueError('unknown behavior')
