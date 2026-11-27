#!/usr/bin/env bash
set -euo pipefail

commit=${1:?provide the full published pgjobdb commit SHA}
[[ $commit =~ ^[0-9a-f]{40}$ ]] || { echo 'Expected a full commit SHA' >&2; exit 2; }
: "${PGJOBDB_VERIFY_DSN:?provide a new, empty, disposable PostgreSQL database}"
root=$(cd -- "$(dirname -- "$0")" && pwd)
work=$(mktemp -d)
trap 'rm -rf -- "$work"' EXIT
export GOWORK=off GOFLAGS= GOMODCACHE="$work/modules" GOCACHE="$work/cache"
export PGJOBDB_VERIFY_BLOBS="blobfs://$work/blobs"
jobdb=v0.0.99-0.20261126174656-857ac1ada983

read -r tip _ < <(git ls-remote https://github.com/colony-2/pgjobdb.git refs/heads/main)
[[ $tip == "$commit" ]] || { echo "GitHub main is $tip, expected $commit" >&2; exit 1; }
cd "$work"
go mod init example.invalid/published-pgjobdb-lease-activity
sed '1,2d' "$root/testdata/published-lease-activity/main.go" >main.go
go get "github.com/colony-2/pgjobdb@$commit"
go mod tidy
go list -m -json github.com/colony-2/pgjobdb github.com/colony-2/jobdb >selected.json
pgversion=$(go list -m -f '{{.Version}}' github.com/colony-2/pgjobdb)
go mod download -json "github.com/colony-2/pgjobdb@$pgversion" >postgres-origin.json
go mod download -json "github.com/colony-2/jobdb@$jobdb" >jobdb-origin.json
python3 - "$commit" "$pgversion" "$jobdb" <<'PY'
import json
import pathlib
import sys

commit, pgversion, jobversion = sys.argv[1:]
text = pathlib.Path('selected.json').read_text()
decoder = json.JSONDecoder()
for path, version in [('pgjobdb', pgversion), ('jobdb', jobversion)]:
    module, offset = decoder.raw_decode(text)
    assert module['Path'] == 'github.com/colony-2/' + path, module
    assert module['Version'] == version and not module.get('Replace'), module
    text = text[offset:].lstrip()
assert not text
for path, expected in [('postgres', commit), ('jobdb', '857ac1ada983ed5ed3ab06ae01bb8b6d95b5c538')]:
    origin = json.loads(pathlib.Path(f'{path}-origin.json').read_text())['Origin']
    assert origin['Hash'] == expected, origin
PY
go run -mod=readonly ./main.go
printf 'pgjobdb commit: %s\npgjobdb module: %s\njobdb module: %s\n' "$commit" "$pgversion" "$jobdb"
