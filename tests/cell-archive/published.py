#!/usr/bin/env python3
"""Verify and exercise the exact published pgjobdb module in an external module."""
import argparse
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import tempfile
import time
from urllib.parse import urlsplit


PG = 'github.com/colony-2/pgjobdb'
JOBDB = 'github.com/colony-2/jobdb'
JOBDB_VERSION = 'v0.0.99-0.20261126061858-c55cbf2da623'
JOBDB_COMMIT = 'c55cbf2da62318fecc7024ff2ba5cffd1a641ed0'
LAUNCHER = '/pkg/pgjobdb/runtime/integration/cmd/cellarchivefixture'


def verify(commit, out):
    if os.geteuid() == 0:
        raise RuntimeError('run as a non-root user for embedded PostgreSQL')
    if not re.fullmatch('[0-9a-f]{40}', commit):
        raise ValueError('provide the full published pgjobdb commit')
    out.mkdir(parents=True, exist_ok=False)
    with tempfile.TemporaryDirectory(prefix='pgjobdb-cell-archive-published-') as temporary, (out / 'commands.log').open('w') as log:
        scratch = Path(temporary)
        consumer = scratch / 'consumer'
        consumer.mkdir()
        env = dict(os.environ, GOWORK='off', GOFLAGS='', GOTOOLCHAIN='local', GOCACHE=str(scratch / 'cache'),
                   GOMODCACHE=str(scratch / 'modules'), GOPRIVATE='github.com/colony-2/*',
                   GOPROXY='https://proxy.golang.org,direct', CGO_ENABLED='0')
        fixture_env = {name: env[name] for name in ('PATH', 'SSL_CERT_FILE', 'SSL_CERT_DIR') if name in env}
        fixture_env.update(HOME=str(scratch), TMPDIR=str(scratch), LANG='C.UTF-8', LC_ALL='C.UTF-8')

        def run(*command):
            print(json.dumps(command), file=log, flush=True)
            return subprocess.check_output(command, cwd=consumer, env=env, text=True, stderr=log, timeout=600)

        def download(path, revision):
            result = json.loads(run('go', 'mod', 'download', '-json', f'{path}@{revision}'))
            if result['Path'] != path or result.get('Origin', {}).get('Hash') != revision:
                raise RuntimeError(f'published origin mismatch: {path}@{revision}')
            if not result.get('Sum') or not result.get('GoModSum'):
                raise RuntimeError(f'missing published checksums for {path}')
            return result

        run('go', 'mod', 'init', 'example.test/cell-archive-consumer')
        published = download(PG, commit)
        dep = download(JOBDB, JOBDB_COMMIT)
        if dep['Version'] != JOBDB_VERSION:
            raise RuntimeError('JobDB resolved to the wrong version')
        version = published['Version']
        source = Path(published['Dir'])
        harness = source / 'tests/cell-archive'
        (consumer / 'main.go').write_bytes((harness / 'consumer.go.txt').read_bytes())
        run('go', 'get', PG + LAUNCHER + '@' + version, PG + '/pkg/pgjobdb/runtime@' + version)
        expected = {PG: version, JOBDB: JOBDB_VERSION}
        selected = {}
        with (out / 'selected-modules.txt').open('w') as modules:
            for line in run('go', 'list', '-m', 'all').splitlines():
                modules.write(line + '\n')
                if '=>' in line:
                    raise RuntimeError('replacement in published consumer graph: ' + line)
                parts = line.split()
                if parts[0] in expected:
                    selected[parts[0]] = parts[1]
        if selected != expected:
            raise RuntimeError(f'unexpected consumer selections: {selected!r}')
        launcher, probe, info = scratch / 'launcher', scratch / 'probe', scratch / 'fixture.json'
        run('go', 'build', '-mod=readonly', '-trimpath', '-o', str(launcher), PG + LAUNCHER)
        run('go', 'build', '-mod=readonly', '-trimpath', '-o', str(probe), '.')
        for binary in (launcher, probe):
            linked = run('go', 'version', '-m', str(binary))
            (out / (binary.name + '-linked.txt')).write_text(linked)
            for result in (published, dep):
                row = '\tdep\t' + '\t'.join(result[key] for key in ('Path', 'Version', 'Sum'))
                if row not in linked:
                    raise RuntimeError(f'{binary.name} did not link the expected {result["Path"]}')

        command = [str(launcher), '-info', str(info)]
        print(json.dumps(command), file=log, flush=True)
        with (out / 'launcher.log').open('w') as launcher_log:
            process = subprocess.Popen(command, cwd=scratch, env=fixture_env, stdout=launcher_log, stderr=subprocess.STDOUT)
            try:
                deadline = time.monotonic() + 90
                while not info.is_file():
                    if process.poll() is not None or time.monotonic() > deadline:
                        raise RuntimeError('fixture did not become ready; see launcher.log')
                    time.sleep(.1)
                descriptor = json.loads(info.read_text())
                with (out / 'smoke.log').open('w') as smoke:
                    for check in ([str(probe), str(info), str(scratch / 'probe-blobs')],
                                  ['python3', str(harness / 'smoke.py'), str(info)]):
                        print(json.dumps(check), file=log, flush=True)
                        subprocess.run(check, cwd=scratch, env=fixture_env, stdout=smoke, stderr=smoke, check=True, timeout=90)
            finally:
                if process.poll() is None:
                    process.send_signal(signal.SIGTERM)
                    process.wait(timeout=30)
        if process.returncode or info.exists():
            raise RuntimeError('fixture did not shut down and remove its descriptor')
        for key in ('baseURL', 'dsn'):
            endpoint = urlsplit(descriptor[key])
            try:
                connection = socket.create_connection((endpoint.hostname, endpoint.port), timeout=3)
            except ConnectionRefusedError:
                continue
            connection.close()
            raise RuntimeError(f'fixture {key} still accepts connections after shutdown')
        (out / 'consumer.go.txt').write_bytes((consumer / 'main.go').read_bytes())
        for name in ('go.mod', 'go.sum'):
            (out / name).write_bytes((consumer / name).read_bytes())
        report = {result['Path']: {key: result[key] for key in ('Version', 'Sum', 'GoModSum', 'Origin')}
                  for result in (published, dep)}
        report.update(result='passed', checks=['external consumer runtime open and scoped list', 'tenant and project HTTP smoke', 'fixture shutdown and descriptor cleanup'])
        (out / 'receipt.json').write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--commit', required=True)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    verify(args.commit, args.out.resolve())
