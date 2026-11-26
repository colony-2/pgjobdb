#!/usr/bin/env python3
"""Build a test helper against the authentic, published format-14 source."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


def main(output):
    path = 'github.com/colony-2/pgjobdb'
    revision = '794a8970e279c7b6fb8b64b4a3479c11aa428805'
    env = dict(os.environ, GOWORK='off', GOFLAGS='')
    module = json.loads(subprocess.check_output(['go', 'mod', 'download', '-json', path + '@' + revision], env=env, timeout=120))
    if module.get('Origin', {}).get('Hash') != revision or module['Sum'] != 'h1:g5a69uV2qn08Jc5CYfb7xtW7160hsVAjNOAHPROFVCE=':
        raise RuntimeError('unexpected historical module identity')
    print(f"Historical fixture source: {path} {module['Version']} {revision} {module['Sum']}", flush=True)
    with tempfile.TemporaryDirectory() as scratch:
        source = Path(scratch) / 'source'
        shutil.copytree(module['Dir'], source)
        for item in source.rglob('*'):
            item.chmod(item.stat().st_mode | 0o200)
        shutil.copyfile(Path(__file__).with_name('format14_fixture_test.go'), source / 'pkg/pgjobdb/installer/format14_fixture_test.go')
        subprocess.run(['go', 'test', '-mod=readonly', '-c', '-o', str(output), './pkg/pgjobdb/installer'], cwd=source, env=env, check=True)


if __name__ == '__main__':
    main(Path(sys.argv[1]).resolve())
