#!/usr/bin/env python3
"""Acquire exact published modules into empty caches, inspect origins, then test.

Requires explicit publication identity/checksums. No checkout, replace, workspace,
local proxy, vendor directory or prewarmed Go cache supplies either tested module.
"""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

PG = 'github.com/colony-2/pgjobdb'
JOB = 'github.com/colony-2/jobdb'
JOB_VERSION = 'v0.0.99-0.20261122051819-86f7ba9bc009'
JOB_HASH = '86f7ba9bc009ca5dc851a7e8a0057c63d788bc8b'
JOB_SUM = 'h1:LzkQjvmEG/kmRtrIuOjCQBp9EU1iSPySD3reXRgQrRg='
JOB_MODSUM = 'h1:45c4NbbKzZt8uRHgWUktOoGhsTkO8ogELlfxdqXJ4Ek='


def check_download(receipt, path, version, commit, checksum, modsum):
    if receipt.get('Error'):
        raise ValueError('required publication unavailable: '+str(receipt['Error']))
    if receipt.get('Path') != path or receipt.get('Version') != version:
        raise ValueError('download identity mismatch: '+path)
    if receipt.get('Sum') != checksum or receipt.get('GoModSum') != modsum:
        raise ValueError('download checksum mismatch: '+path)
    origin = receipt.get('Origin', {})
    if origin.get('VCS') != 'git' or origin.get('URL', '').removesuffix('.git') != 'https://'+path or origin.get('Hash') != commit:
        raise ValueError('download origin mismatch: '+path)


def check_modules(modules, expected):
    if any(m.get('Replace') for m in modules):
        raise ValueError('replacement in dependency graph')
    selected = {m['Path']: m for m in modules}
    for path, (version, checksum) in expected.items():
        m = selected.get(path, {})
        if (m.get('Version'), m.get('Sum')) != (version, checksum):
            raise ValueError('selected module identity mismatch: '+path)


def read_objects(raw):
    decoder = json.JSONDecoder()
    result = []
    while raw.strip():
        value, end = decoder.raw_decode(raw.lstrip())
        result.append(value)
        raw = raw.lstrip()[end:]
    return result


def check_behavior(raw, exit_code):
    failures=re.findall(r'^\s*--- (?:FAIL|SKIP): (\S+)',raw,re.M)
    required={'TestPublishedGoals','TestPublishedGoals/direct','TestPublishedGoals/default','TestPublishedGoals/signed','TestPublishedGoals/migrated'}
    for name in ('direct','default','signed'):
        required.add('TestPublishedGoals/'+name+'/admission')
        required.add('TestPublishedGoals/'+name+'/protected')
        required.add('TestPublishedGoals/migrated/'+name)
    required.update({'TestPublishedProject'+name for name in (
        'FreshInstallationAndReopen','CatalogReceiptsAndDeletion','AdministrationAndFirstUse',
        'GuardsAndBorrowedTransactions','HistoricalAdoptionPreservesResources',
        'InstallationRollbackAndInventoryFailures','DamagedGuardsAndResolution',
        'LargeNumericCatalogMetadata','OpaqueCatalogStrings','AdmissionProjectionIntegrity','InstallationConflictsAndExternalInventory',
        'WriterExclusionAndUnknownParticipants','SQLNoopAdmissionAndCopy',
        'SealSurvivesDownstreamFailureAndRelaxedCommit','RefusesIncompleteSharedSchema',
        'RejectsUncommittedSeal','PartialAndFutureInstallations','CommittedSealSurvivesVacuum','CommittedCatalogNewerThanWriter','ConcurrentRetainedCreationAndDeletion','Commands')})
    passed=set(re.findall(r'^\s*--- PASS: (\S+)',raw,re.M))
    if exit_code or failures or not required.issubset(passed):
        raise RuntimeError(f'published acceptance failed, exit={exit_code}, failed/skipped={failures}, missing={sorted(required-passed)}')


def prepare_harness(source, destination):
    """Copy consumer-only fixtures; production is always linked from the module graph."""
    destination.mkdir(parents=True, exist_ok=True)
    root = source.parents[1]
    shutil.copyfile(source/'goal_publication_test.go', destination/'goal_publication_test.go')
    helper = root/'pkg/pgjobdb/runtime/integration/internal/jobdbtest'
    for name in ('namespaced_runtime', 'namespaced_capabilities'):
        raw = (helper/(name+'.go')).read_text().replace('package jobdbtest', 'package publishedgoals_test', 1)
        (destination/(name+'_test.go')).write_text('//go:build published_goals\n\n'+raw)
    raw = (root/'pkg/pgjobdb/projects/projects_test.go').read_text().replace('//go:build integration', '//go:build published_goals', 1).replace('package projects_test', 'package publishedgoals_test', 1).replace('func TestProject', 'func TestPublishedProject')
    (destination/'project_publication_test.go').write_text(raw)
    raw = (root/'cmd/jobdb/projects_integration_test.go').read_text().replace('//go:build integration', '//go:build published_goals', 1).replace('package main', 'package publishedgoals_test', 1).replace('"github.com/colony-2/pgjobdb/internal/projectfixture"', '').replace('projectfixture.Participants','participants').replace('func TestPostgresProjectCommands','func TestPublishedProjectCommands')
    (destination/'project_commands_test.go').write_text(raw)


def run(args):
    if not re.fullmatch(r'[0-9a-f]{40}', args.commit):
        raise ValueError('exact native published commit required')
    if not args.version.startswith('v') or not all(v.startswith('h1:') for v in (args.sum, args.gomodsum)):
        raise ValueError('exact normally published version and both checksums required')
    out = Path(args.out).resolve()
    out.mkdir(parents=True, exist_ok=True)
    (out/'acceptance.json').unlink(missing_ok=True)
    # Keep the actual harness and receipts with logs; caches remain disposable.
    source = Path(__file__).resolve().parent
    prepare_harness(source, out)
    shutil.copytree(source/'cmd', out/'cmd', dirs_exist_ok=True)
    history=out/'history';history.mkdir(exist_ok=True)
    shutil.copyfile(source.parents[1]/'pkg/pgjobdb/installer/testdata/format11.sql',history/'format11.sql')
    with tempfile.TemporaryDirectory(prefix='pgjobdb-published-') as directory:
        scratch = Path(directory)
        scratch.chmod(0o755)
        consumer = scratch/'consumer'
        shutil.copytree(out/'cmd', consumer/'cmd')
        shutil.copytree(history,consumer/'history')
        for path in out.glob('*_test.go'):
            shutil.copyfile(path, consumer/path.name)
        (consumer/'go.mod').write_text(f'module example.test/pgjobdb-published-acceptance\n\ngo 1.25.5\n\nrequire (\n {PG} {args.version}\n {JOB} {JOB_VERSION}\n github.com/fergusstrange/embedded-postgres v1.32.0\n github.com/lib/pq v1.10.9\n)\n')
        env = dict(os.environ, GOWORK='off', GOFLAGS='', GOTOOLCHAIN='local',
                   GOPROXY='direct', GOPRIVATE='github.com/colony-2/*',
                   GOCACHE=str(scratch/'build-cache'), GOMODCACHE=str(scratch/'modules'))
        with (out/'commands.log').open('w') as log:
            def command(argv, name=None):
                log.write('$ '+json.dumps(argv)+'\n'); log.flush()
                done = subprocess.run(argv, cwd=consumer, env=env, stdout=subprocess.PIPE, stderr=log, text=True, timeout=1800)
                if name:
                    (out/name).write_text(done.stdout)
                else:
                    log.write(done.stdout)
                log.write(f'exit={done.returncode}\n');log.flush()
                if done.returncode:
                    raise RuntimeError('required command failed: '+json.dumps(argv))
                return done.stdout
            for path, version, commit, checksum, modsum, name in (
                (PG,args.version,args.commit,args.sum,args.gomodsum,'pgjobdb-download.json'),
                (JOB,JOB_VERSION,JOB_HASH,JOB_SUM,JOB_MODSUM,'jobdb-download.json')):
                raw=command(['go','mod','download','-json',path+'@'+version],name)
                check_download(json.loads(raw),path,version,commit,checksum,modsum)
            command(['go','mod','tidy'])
            modules=read_objects(command(['go','list','-m','-json','all'],'selected-modules.json'))
            expected={PG:(args.version,args.sum),JOB:(JOB_VERSION,JOB_SUM)}
            check_modules(modules,expected)
            command(['go','build','-tags=published_goals','-o',str(scratch/'identity'),'./cmd/identity'])
            identity=json.loads(command([str(scratch/'identity')],'linked-identity.json'))
            check_modules(identity.get('Deps', []),expected)
            command(['go','build','-o',str(scratch/'pgjobdb-cli'),PG+'/cmd/jobdb'])
            command(['go','test','-tags=published_goals','-c','-o',str(scratch/'accept.test'),'.'])
            shutil.copyfile(consumer/'go.mod',out/'go.mod')
            shutil.copyfile(consumer/'go.sum',out/'go.sum')
            # initdb runs without elevated identity. Do not forward runtime
            # credentials, external job identity or module-acquisition secrets.
            uid=65534 if os.geteuid()==0 else os.geteuid()
            gid=65534 if os.geteuid()==0 else os.getegid()
            home=scratch/'test-home';home.mkdir();os.chown(home,uid,gid)
            testenv={k:v for k,v in env.items() if k in ('PATH','SSL_CERT_FILE','SSL_CERT_DIR')}
            testenv.update(HOME=str(home),TMPDIR=str(home),LANG='C.UTF-8',LC_ALL='C.UTF-8',GOALS_HISTORY_FIXTURE=str(consumer/'history/format11.sql'),PGJOBDB_TEST_CLI=str(scratch/'pgjobdb-cli'))
            drop={'user':uid,'group':gid,'extra_groups':[]} if os.geteuid()==0 else {}
            with (out/'behavior.log').open('w') as behavior:
                done=subprocess.run([str(scratch/'accept.test'),'-test.v','-test.timeout=20m'],cwd=home,env=testenv,stdout=behavior,**drop,stderr=behavior,timeout=1230)
            raw=(out/'behavior.log').read_text()
            check_behavior(raw,done.returncode)
            (out/'acceptance.json').write_text(json.dumps({'passed':True,'pgjobdb':{'version':args.version,'commit':args.commit,'sum':args.sum,'goModSum':args.gomodsum},'jobdb':{'version':JOB_VERSION,'commit':JOB_HASH,'sum':JOB_SUM,'goModSum':JOB_MODSUM},'behaviorExit':done.returncode},indent=2)+'\n')


if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('version','commit','sum','gomodsum','out'):
        parser.add_argument('--'+name,required=True)
    run(parser.parse_args())
