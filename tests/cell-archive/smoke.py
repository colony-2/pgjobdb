#!/usr/bin/env python3
"""Exercise both real HTTP route families against the disposable fixture."""
import json
from pathlib import Path
import sys
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def smoke(info):
    def call(path, status=200, *, method='GET', token='fixture-writer', etag=None):
        headers = {} if token is None else {'Authorization': 'Bearer ' + token}
        if etag is not None:
            headers['If-Match'] = etag
        try:
            response = urlopen(Request(info['baseURL'] + path, headers=headers, method=method), timeout=10)
        except HTTPError as error:
            response = error
        with response:
            raw = response.read()
            if response.status != status:
                raise RuntimeError(f'{method} {path}: got {response.status}, wanted {status}: {raw!r}')
            return json.loads(raw) if raw else None, response.headers.get('ETag')

    def strong(cell, etag):
        if not cell['revision'] or etag != f'"{cell["revision"]}"':
            raise RuntimeError(f'expected one matching strong ETag, got {etag!r}')

    call('/readyz', 204, token=None)
    seeded = info['seeded']
    for family in ('tenants', 'projects'):
        base = f'/v1/{family}/{info["project"]}/cells'
        for archived in (None, 'true', 'false'):
            query = {'pageSize': 1}
            if archived is not None:
                query['archived'] = archived
            observed = []
            for _ in range(len(seeded)):
                page, _ = call(base + '?' + urlencode(query))
                if len(page['cells']) != 1:
                    raise RuntimeError('expected one visible cell on every page')
                observed.append(page['cells'][0]['id'])
                if not page.get('nextPageToken'):
                    break
                query['pageToken'] = page['nextPageToken']
                if len(observed) == 1:
                    call(base + '?' + urlencode(query), 400, token='fixture-reader')
                    changed = dict(query, archived='false' if archived == 'true' else 'true')
                    call(base + '?' + urlencode(changed), 400)
            expected = sorted(cell['id'] for cell in seeded if cell['state'] == 'ACTIVE' and cell['id'] not in info['hiddenCellIds']
                              and (archived is None or bool(cell.get('archivedAt')) == (archived == 'true')))
            if observed != expected or len(observed) < 2:
                raise RuntimeError(f'{family} {archived}: saw {observed!r}, expected {expected!r}')
        target = base + '/' + seeded[0]['id']
        before, etag = call(target)
        strong(before, etag)
        for token, status in [('fixture-reader', 403), ('fixture-wrong-tenant', 403), ('fixture-outside-scope', 404), (None, 403)]:
            call(target + '/archive', status, token=token, method='POST', etag=etag)
        if call(target) != (before, etag):
            raise RuntimeError(f'{family}: denied mutation changed the cell')
        call(target + '/archive', 428, method='POST')
        marked, marked_tag = call(target + '/archive', method='POST', etag=etag)
        strong(marked, marked_tag)
        if not marked.get('archivedAt') or marked_tag == etag or call(target) != (marked, marked_tag):
            raise RuntimeError(f'{family}: archive did not persist with a new revision')
        call(target + '/unarchive', 412, method='POST', etag=etag)
        cleared, cleared_tag = call(target + '/unarchive', method='POST', etag=marked_tag)
        strong(cleared, cleared_tag)
        if cleared.get('archivedAt') or cleared_tag == marked_tag or call(target) != (cleared, cleared_tag):
            raise RuntimeError(f'{family}: unarchive did not persist with a new revision')
        print(f'{family}: scoped 403/404, 428/412, pagination and cursor binding, strong archive/unarchive passed')


if __name__ == '__main__':
    smoke(json.loads(Path(sys.argv[1]).read_text()))
