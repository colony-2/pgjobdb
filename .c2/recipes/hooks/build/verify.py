#!/usr/bin/env python3
"""Native verification runner. Build with scoped credentials; run isolated tests without job identity."""
import ctypes
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import subprocess
import sys
import tempfile
import time

BEHAVIORS = ('service_free', 'installer', 'conformance', 'runtime')
PREFIX = 'github.com/colony-2/pgjobdb/'

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
                                GOTMPDIR=str(scratch), CGO_ENABLED='0', GOTOOLCHAIN='local')
                goflags = ['-modfile=' + str(scratch / 'go.mod'), '-tags=integration']
                listcmd = ['go', 'list', *goflags, './...']
                result['commands'].append(listcmd)
                with (scratch / 'packages.txt').open('w') as listing_file:
                    if execute(listcmd, log, cwd=root, env=buildenv, stdout=listing_file):
                        raise RuntimeError('Go package enumeration prerequisite failed')
                listing = (scratch / 'packages.txt').read_text().splitlines()
                def selected(pkg):
                    name = pkg.removeprefix(PREFIX)
                    if behavior == 'installer': return name == 'pkg/pgjobdb/installer'
                    if behavior == 'conformance': return name == 'pkg/pgjobdb/runtime/integration'
                    if behavior == 'runtime': return name.startswith('pkg/pgjobdb/runtime/integration')
                    return not (name == 'pkg/pgjobdb/installer' or name.startswith('pkg/pgjobdb/runtime/integration'))
                packages = [p for p in listing if selected(p)]
                if not packages: raise RuntimeError('empty behavior selection')
                result['packages'] = packages
                # Allow only runtime prerequisites, never live C2J/Pulse/JobDB identities or credentials.
                uid = 65534 if os.geteuid() == 0 else os.geteuid()
                gid = 65534 if os.geteuid() == 0 else os.getegid()
                testhome = scratch / 'test-home'; testhome.mkdir(); os.chown(testhome, uid, gid)
                testenv = {k: v for k, v in os.environ.items() if k in ('PATH', 'SSL_CERT_FILE', 'SSL_CERT_DIR', 'LANG', 'LC_ALL')}
                # initdb derives database encoding from locale. A worker's C/POSIX
                # locale would create SQL_ASCII databases and break Unicode JSON.
                testenv.update(HOME=str(testhome), TMPDIR=str(testhome), XDG_CACHE_HOME=str(testhome / 'cache'),
                               LANG='C.UTF-8', LC_ALL='C.UTF-8')
                result['test_uid'] = uid
                result['test_locale'] = 'C.UTF-8'
                drop = {'user': uid, 'group': gid, 'extra_groups': []} if os.geteuid() == 0 else {}
                failed = False
                for i, package in enumerate(packages):
                    binary = scratch / ('test-' + str(i))
                    cmd = ['go', 'test', *goflags, '-c', '-o', str(binary), package]
                    result['commands'].append(cmd)
                    if execute(cmd, log, cwd=root, env=buildenv):
                        log.write('Not run: ' + package + ' tests; compilation prerequisite failed.\n')
                        failed = True; continue
                    if not binary.exists():
                        log.write('No test files: ' + package + '\n'); continue
                    expr = '.'
                    if package == PREFIX + 'pkg/pgjobdb/runtime/integration':
                        # Go RE2 has no negative lookahead; enumerate names for the disjoint runtime selection.
                        listcmd = [str(binary), '-test.list=.']
                        result['commands'].append(listcmd)
                        with (scratch / 'test-names.txt').open('w') as names_file:
                            if execute(listcmd, log, env=testenv, cwd=testhome, stdout=names_file, **drop):
                                raise RuntimeError('test enumeration prerequisite failed')
                        names = (scratch / 'test-names.txt').read_text().splitlines()
                        conf = [n for n in names if 'Conformance' in n]
                        chosen = conf if behavior == 'conformance' else [n for n in names if n.startswith('Test') and n not in conf]
                        if not chosen: raise RuntimeError('empty runtime test selection')
                        expr = '^(' + '|'.join(chosen) + ')$'
                    cmd = [str(binary), '-test.v', '-test.count=1', '-test.timeout=25m', '-test.run=' + expr]
                    result['commands'].append(cmd)
                    if execute(cmd, log, cwd=testhome, env=testenv, **drop): failed = True
                result['status'] = 'failed' if failed else 'passed'
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
