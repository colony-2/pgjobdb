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
PROJECTS = PREFIX + 'pkg/pgjobdb/projects'
REQUIRED_PROJECTS = {'TestProjectFreshInstallationAndReopen', 'TestProjectCatalogReceiptsAndDeletion',
 'TestProjectAdministrationAndFirstUse', 'TestProjectGuardsAndBorrowedTransactions',
 'TestProjectHistoricalAdoptionPreservesResources', 'TestProjectInstallationRollbackAndInventoryFailures',
 'TestProjectDamagedGuardsAndResolution', 'TestProjectLargeNumericCatalogMetadata', 'TestProjectOpaqueCatalogStrings', 'TestProjectAdmissionProjectionIntegrity',
 'TestProjectInstallationConflictsAndExternalInventory', 'TestProjectWriterExclusionAndUnknownParticipants',
 'TestProjectConcurrentRetainedCreationAndDeletion', 'TestProjectCommittedSealSurvivesVacuum', 'TestProjectCommittedCatalogNewerThanWriter', 'TestProjectPartialAndFutureInstallations', 'TestProjectRefusesIncompleteSharedSchema', 'TestProjectRejectsUncommittedSeal',
 'TestProjectSQLNoopAdmissionAndCopy', 'TestProjectSealSurvivesDownstreamFailureAndRelaxedCommit',
 'TestProjectSchedulerClockDrift'}
REQUIRED_PROJECT_RUNTIME = {'TestProjectIndependentSharedWriters', 'TestProjectNativeWriterAdmission',
 'TestProjectReadDoesNotSeal', 'TestProjectAdmissionBeforeSecretProvider', 'TestProjectHTTPAdministrationAndReceiptReplay', 'TestProjectHTTPLeaseRenewal', 'TestPostgresTaskAlternates', 'TestPostgresTaskAlternateDelayedReschedule'}
REQUIRED_CONSUMER = {'TestConsumerConstructorCapabilities', 'TestConsumerHTTPRetiresAttachment',
                     'TestConsumerCapabilities', 'TestConsumerReadOnlyHistory',
                     'TestConsumerSignedLeaseAuthority', 'TestConsumerNamespacedAuthority',
                     'TestTenantConfiguration'}
REQUIRED_GOALS = {'TestRetainedGoalPublicConformance', 'TestRetainedGoalIndependentCommitOrders',
                  'TestRetainedGoalSimultaneousIdleAdmission', 'TestRetainedGoalPublicationRollbackAndReopen',
                  'TestRetainedGoalHiddenAndCancellationAdmission', 'TestRetainedGoalFailureCurrentAuthority',
                  'TestRetainedGoalLostHTTPFailureResponse', 'TestRetainedGoalDiscoveryAndRetirement',
                  'TestRetainedGoalProtectedHistoryGuards', 'TestRetainedGoalConcurrentInitialHistory',
                  'TestPostgresSelectedConcurrentRequests', 'TestPostgresInitialHistoryRollback',
                  'TestLastLeaseUpdatedAtHTTP', 'TestAppendActivitySurvivesChapterWriteFailure',
                  'TestStartOrAppendConformance'}
REQUIRED_INSTALLER = {'TestGoalConversionHistoricalFormats', 'TestGoalConversionRollback',
                      'TestGoalConversionAuthoritativeReport', 'TestGoalConversionRejectsCorruption',
                      'TestGoalConversionLostCommitAcknowledgement', 'TestGoalConversionStartupReport',
                      'TestGoalConversionOriginTie', 'TestRetainedGoalVerifyContract', 'TestRetainedGoalRejectsDamagedMirror',
                      'TestRetainedGoalProtectionDefinitions', 'TestGoalConversionRejectsAlteredProtections',
                      'TestLastLeaseUpdatedAtMutations', 'TestLastLeaseUpdatedAtRejectsUnsupported',
                      'TestLastLeaseUpdatedAtUpgrade', 'TestLeaseDefinitionDriftRejected', 'TestStatusViewDriftRejected'}
REQUIRED_ARCHIVE_RUNTIME = {'TestPostgresCellArchiveConformance', 'TestPostgresCellArchiveHTTP',
                            'TestPostgresCellArchiveVisibilityAndReopen'}
REQUIRED_ARCHIVE_INSTALLER = {'TestPostgresCellArchiveMigration', 'TestPostgresCellArchiveMigrationRollback', 'TestLastLeaseUpdatedAtFormat15'}
ARCHIVE_LAYOUTS = ('plain', 'direct_project', 'migrated_project')
ARCHIVE_HTTP_CASES = ('strong_preconditions', 'revision_race_and_noop', 'retirement',
                      'scoped_authorization', 'pagination_and_cursor_binding')
REQUIRED_ARCHIVE_CHILDREN = {parent: ARCHIVE_LAYOUTS for parent in REQUIRED_ARCHIVE_INSTALLER}
REQUIRED_ARCHIVE_CHILDREN['TestPostgresCellArchiveHTTP'] = tuple(
    family + '/' + case for family in ('tenants', 'projects') for case in ARCHIVE_HTTP_CASES)
REQUIRED_LEASE_CHILDREN = {
    'TestLeaseDefinitionDriftRejected': ('15/strict-renewal', '15/fixed-clock', '16/strict-renewal', '16/fixed-clock'),
    'TestStatusViewDriftRejected': ('9', '13', '14', '15', '16'),
}
REQUIRED_RESOURCE = {'TestResourceBackoffConformance', 'TestResourceBackoffPostgresBoundaries', 'TestResourceBackoffHTTPPostgres', 'TestResourceBackoffPostgresProcessRestart', 'TestResourceBackoffInstalledProtections', 'TestResourceBackoffDefaultAssignments', 'TestResourceBackoffWorkflowPostgres'}
REQUIRED_RESOURCE_CHILDREN = {'B13b_guarded_clear', 'B05_B10_atomic_validation', 'B06a_late_duplicate', 'B17_retained_after_cancel', 'B11a_zero_credit_deadline_job_target', 'B07_competing_transitions', 'B09_append_gate_B13a_parent_not_refilled', 'B04_tenant_job_and_attempt_isolation', 'B13a_B13c_ordinary_reset_and_renew', 'B07a_single_reclaim', 'B05a_transaction_rollback', 'B11a_zero_credit_deadline_job_poll', 'B08_reopen', 'B11_zero_credit_engine_timeout_external_true', 'B09b_deadline_keeps_resource_gate', 'B04_B04a_retained_sequences', 'B13_countdown_boundary', 'B11a_zero_credit_deadline_task_poll', 'B15a_S02_snapshot', 'B03f_policy_binding', 'B11_zero_credit_engine_timeout_external_false', 'B09_task_completion_gate', 'B11a_zero_credit_deadline_task_target', 'B09_B09b_independent_gate', 'B07_barrier', 'B11a_ordinary_completion_after_due', 'B07a_task_poll_target_completion_append_reclaim', 'B09a_ordinary_join_alternative', 'B06_exact_receipt'}
REQUIRED_RESOURCE_INSTALLER = {'TestPostgresResourceBackoffMigrationFormat13', 'TestPostgresResourceBackoffMigrationRollback', 'TestPostgresResourceBackoffMigrationPreservesProjects', 'TestPostgresResourceBackoffMigrationDefaultSerialization', 'TestPostgresResourceBackoffMigrationRejectsInvalidCrashData'}
REQUIRED_FORMAT14_REGRESSIONS = {
    'TestRetainedGoalProtectionDefinitions': ('project_search_path',),
    'TestTenantConfiguration': ('reopen_and_connection_ownership',),
    'TestResourceBackoffHTTPPostgres': ('H08_pre_eagain_client',),
    'TestResourceBackoffWorkflowPostgres': (
        'acknowledged_local', 'acknowledged_external', 'failed_before_commit_unselected', 'lost_after_commit',
        'cancel_resumed_wait',
    ),
    'TestPostgresResourceBackoffMigrationRejectsInvalidCrashData': (
        'missing_threshold', 'null_threshold', 'zero_threshold', 'negative_threshold',
        'negative_active_count', 'negative_archived_count',
    ),
}
REQUIRED_COMMAND = {'TestPostgresConsumerGoalsCommands', 'TestPostgresProjectCommands', 'TestPostgresResourceBackoffMigrationCommands'}
TASK_DELEGATION_PACKAGE = INTEGRATION + '/internal/jobdbtest'
REQUIRED_TASK_DELEGATION = {'TestPostgresTaskDelegation'}
ENGINE_CONFORMANCE_PACKAGE = INTEGRATION + '/internal/jobdbtest/engineconformance'
REQUIRED_RETRY_HISTORY = {'TestGetJobRunRetryRepresentationOnDirectRuntime',
                          'TestGetJobRunRecordedRetryBackoffOnDirectRuntime'}


def select_packages(listing, behavior):
    if len(listing) != len(set(listing)):
        raise ValueError('duplicate package enumeration')
    required = {COMMAND, INTEGRATION, PROJECTS, PREFIX + 'pkg/pgjobdb/installer',
                INTEGRATION + '/internal/jobdbtest',
                INTEGRATION + '/internal/jobdbtest/engineconformance',
                INTEGRATION + '/internal/jobdbtest/usageparity', INTEGRATION + '/workflow'}
    if not required.issubset(listing):
        raise ValueError('missing required packages: ' + ', '.join(sorted(required - set(listing))))
    def selected(pkg):
        name = pkg.removeprefix(PREFIX)
        if behavior == 'installer': return name in ('pkg/pgjobdb/installer', 'pkg/pgjobdb/projects')
        if behavior == 'conformance': return pkg == INTEGRATION
        if behavior == 'runtime': return pkg == COMMAND or pkg.startswith(INTEGRATION)
        return not (name in ('pkg/pgjobdb/installer', 'pkg/pgjobdb/projects') or pkg.startswith(INTEGRATION))
    packages = [p for p in listing if selected(p)]
    if not packages: raise ValueError('empty behavior selection')
    return packages


def select_tests(package, names, behavior):
    tests = [n for n in names if n.startswith('Test')]
    if not tests or len(tests) != len(set(tests)):
        raise ValueError('empty or duplicate test enumeration: ' + package)
    if package == TASK_DELEGATION_PACKAGE and not REQUIRED_TASK_DELEGATION.issubset(tests):
        raise ValueError('missing PostgreSQL task delegation tests')
    if package == ENGINE_CONFORMANCE_PACKAGE and not REQUIRED_RETRY_HISTORY.issubset(tests):
        raise ValueError('missing PostgreSQL retry history tests')
    if package == PREFIX + 'pkg/pgjobdb/installer' and not (REQUIRED_INSTALLER | REQUIRED_RESOURCE_INSTALLER | REQUIRED_ARCHIVE_INSTALLER).issubset(tests):
        raise ValueError('missing required historical conversion/report tests')
    if package == PROJECTS and not REQUIRED_PROJECTS.issubset(tests):
        raise ValueError('missing required PostgreSQL project tests')
    if package == COMMAND:
        if not REQUIRED_COMMAND.issubset(tests): raise ValueError('missing PostgreSQL command tests')
        database = [n for n in tests if n.startswith(('TestPostgresConsumerGoals', 'TestPostgresProject', 'TestPostgresResourceBackoffMigration'))]
        return database if behavior == 'runtime' else [n for n in tests if n not in database]
    if package == INTEGRATION:
        if not (REQUIRED_CONSUMER | REQUIRED_GOALS | REQUIRED_PROJECT_RUNTIME | REQUIRED_RESOURCE | REQUIRED_ARCHIVE_RUNTIME).issubset(tests): raise ValueError('missing focused consumer tests')
        conformance = [n for n in tests if 'Conformance' in n]
        if not conformance: raise ValueError('empty conformance test selection')
        return conformance if behavior == 'conformance' else [n for n in tests if n not in conformance]
    return tests


def validate_outcomes(events, chosen):
    outcomes = {e['Test']: e['Action'] for e in events
                if 'Test' in e and e.get('Action') in ('pass', 'fail', 'skip')}
    if 'TestResourceBackoffConformance' in chosen:
        expected = {'TestResourceBackoffConformance/' + name for name in REQUIRED_RESOURCE_CHILDREN}
        missing_children = expected - set(outcomes)
        if missing_children:
            raise ValueError('missing required resource cases: ' + ', '.join(sorted(missing_children)))
    required_regressions = {parent + '/' + child for parent, children in (REQUIRED_FORMAT14_REGRESSIONS | REQUIRED_ARCHIVE_CHILDREN | REQUIRED_LEASE_CHILDREN).items()
                            if parent in chosen for child in children}
    missing = (set(chosen) | required_regressions) - set(outcomes)
    if missing: raise ValueError('missing required test outcomes: ' + ', '.join(sorted(missing)))
    for name, action in outcomes.items():
        if action != 'pass':
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

    class Cancelled(Exception):
        pass

    cancellation = None
    cleaning = False

    def stop(signum, frame):
        nonlocal cancellation
        if cancellation is not None:
            return
        cancellation = signum
        if not cleaning:
            # Unwind before waiting again: the interrupted wait may hold
            # Popen's waitpid lock in this same thread.
            raise Cancelled

    old = {}
    try:
        try:
            for signum in (signal.SIGTERM, signal.SIGINT):
                old[signum] = signal.getsignal(signum)
                signal.signal(signum, stop)
            return p.wait(timeout=1800)
        except Cancelled:
            try:
                os.killpg(p.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                p.wait(timeout=2)
            except subprocess.TimeoutExpired:
                os.killpg(p.pid, signal.SIGKILL)
                p.wait()
        except subprocess.TimeoutExpired:
            os.killpg(p.pid, signal.SIGKILL)
            p.wait()
            raise RuntimeError('command timed out after 1800 seconds')
    finally:
        cleaning = True
        try:
            cleanup_children()
        except Exception:
            if cancellation is None:
                raise
        finally:
            for s, handler in old.items():
                signal.signal(s, handler)
        if cancellation is not None:
            raise SystemExit(128 + cancellation) from None


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
            if platform.system() != 'Linux' or platform.machine() not in ('aarch64', 'arm64', 'x86_64', 'amd64'):
                raise RuntimeError('verification requires native Linux arm64 or amd64')
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
                goflags = ['-mod=readonly', '-modfile=' + str(scratch / 'go.mod'), '-tags=integration']
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
                    for directory in ('.c2', 'cmd', 'pkg', 'internal'):
                        if not (root / directory).exists(): continue
                        shutil.copytree(root / directory, hookroot / directory,
                                        ignore=shutil.ignore_patterns('__pycache__'))
                    if (root/'tests/published-goals').exists():
                        shutil.copytree(root/'tests/published-goals', hookroot/'tests/published-goals', ignore=shutil.ignore_patterns('__pycache__'))
                    for name in ('go.mod', 'go.sum'):
                        shutil.copyfile(root / name, hookroot / name)
                    hooktests = ['python3', str(hookroot / '.c2/recipes/hooks/build/tests/test_verify.py')]
                    result['commands'].append(hooktests)
                    if execute(hooktests, log, cwd=testhome, env=testenv, **drop):
                        failed = True
                    if (hookroot/'tests/published-goals/test_accept.py').exists():
                        checker_tests=['python3','-B','-m','unittest','discover','-s',str(hookroot/'tests/published-goals'),'-p','test_*.py']
                        result['commands'].append(checker_tests)
                        if execute(checker_tests,log,cwd=testhome,env=testenv,**drop): failed=True
                result['tests'] = {}
                result['test_exit_codes'] = {}
                if behavior == 'installer':
                    for version in (14, 15):
                        historical = scratch / f'format{version}-fixture'
                        cmd = [sys.executable, str(root / 'pkg/pgjobdb/installer/testdata/build_format14_fixture.py'), str(historical), str(version)]
                        result['commands'].append(cmd)
                        if execute(cmd, log, cwd=root, env=buildenv) or not historical.is_file():
                            raise RuntimeError(f'format-{version} fixture builder compilation prerequisite failed')
                        testenv[f'PGJOBDB_TEST_FORMAT{version}_FIXTURE'] = str(historical)
                    producer = scratch / 'format13'
                    cmd = ['go', 'build', '-mod=readonly', '-o', str(producer), '.']
                    result['commands'].append(cmd)
                    if execute(cmd, log, cwd=root / 'scripts/testdata/pgjobdb-format13', env=buildenv) or not producer.is_file():
                        raise RuntimeError('format-13 fixture builder compilation prerequisite failed')
                    testenv['PGJOBDB_TEST_FORMAT13'] = str(producer)
                if behavior == 'runtime' and COMMAND in packages:
                    cli = scratch / 'pgjobdb-cli'
                    cmd = ['go', 'build', *goflags, '-o', str(cli), './cmd/jobdb']
                    result['commands'].append(cmd)
                    if execute(cmd, log, cwd=root, env=buildenv):
                        raise RuntimeError('production CLI compilation prerequisite failed')
                    testenv['PGJOBDB_TEST_CLI'] = str(cli)
                if behavior == 'runtime' and INTEGRATION in packages:
                    legacy = scratch / 'legacy-client'
                    cmd = ['go', 'build', '-mod=readonly', '-o', str(legacy), '.']
                    result['commands'].append(cmd)
                    legacy_source = root / 'scripts/testdata/pgjobdb-legacy-client'
                    if execute(cmd, log, cwd=legacy_source, env=buildenv) or not legacy.is_file():
                        raise RuntimeError('pre-EAGAIN client compilation prerequisite failed')
                    testenv['PGJOBDB_TEST_LEGACY_CLIENT'] = str(legacy)
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
                    result['tests'][package] = {}
                    result['test_exit_codes'][package] = []
                    # Bound each binary invocation independently. A large package
                    # must not spend its entire timeout before its final tests.
                    for offset in range(0, len(chosen), 20):
                        batch = chosen[offset:offset + 20]
                        expr = '^(' + '|'.join(batch) + ')$'
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
                            result['tests'][package].update(validate_outcomes(events, batch))
                            if code != 0:
                                raise ValueError('unexpected test binary exit: ' + str(code))
                        except ValueError as exc:
                            log.write(str(exc) + '\n')
                            result['tests'][package].setdefault('acceptance_errors', []).append(str(exc))
                            failed = True
                        result['test_exit_codes'][package].append(code)
                        log.write('test binary exit=' + str(code) + '\n')
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
    root = Path(os.environ.get('ROOT', '.')).resolve()
    env = dict(os.environ, GOWORK='off', GOFLAGS='', GOTOOLCHAIN='local')
    module = 'github.com/colony-2/jobdb'
    selected = json.loads(subprocess.check_output(['go', 'list', '-mod=readonly', '-m', '-json', module], cwd=root, env=env, timeout=120))
    identity = json.loads(subprocess.check_output(['go', 'mod', 'download', '-json', module + '@' + selected['Version']], cwd=root, env=env, timeout=120))
    jobdb = {'version': identity['Version'], 'revision': identity.get('Origin', {}).get('Hash'),
             'sum': identity['Sum'], 'gomodsum': identity['GoModSum']}
    if selected.get('Replace') or jobdb['version'] != 'v0.0.99-0.20261126174656-857ac1ada983' or jobdb['revision'] != '857ac1ada983ed5ed3ab06ae01bb8b6d95b5c538':
        raise ValueError('unexpected JobDB module identity: ' + str(jobdb))
    metadata = {'platform': platform.platform(), 'machine': platform.machine(), 'python': sys.version,
                'go': subprocess.check_output(['go', 'version'], text=True, timeout=30).strip(),
                'candidate': os.environ.get('CANDIDATE'), 'base': os.environ.get('BASE'), 'jobdb': jobdb}
    (out / 'environment.json').write_text(json.dumps(metadata, indent=2) + '\n')
    report += f"\n\nCandidate: `{metadata['candidate']}`; base: `{metadata['base']}`."
    report += f"\n\nJobDB: `{jobdb['version']}` (`{jobdb['revision']}`)."
    report += '\n\nHeld: Docker container-smoke/Postgres/MinIO topology; other architecture/macOS runs; release, signing and publication.\n'
    (out / 'verification.md').write_text(report)
    print(status, end='')

if __name__ == '__main__':
    if sys.argv[1] == 'aggregate': aggregate()
    elif sys.argv[1] in BEHAVIORS: run(sys.argv[1])
    else: raise ValueError('unknown behavior')
