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
 'TestProjectSQLNoopAdmissionAndCopy', 'TestProjectSealSurvivesDownstreamFailureAndRelaxedCommit'}
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
                  'TestRetainedGoalProtectedHistoryGuards', 'TestRetainedGoalConcurrentInitialHistory'}
REQUIRED_INSTALLER = {'TestGoalConversionHistoricalFormats', 'TestGoalConversionRollback',
                      'TestGoalConversionAuthoritativeReport', 'TestGoalConversionRejectsCorruption',
                      'TestGoalConversionLostCommitAcknowledgement', 'TestGoalConversionStartupReport',
                      'TestGoalConversionOriginTie', 'TestRetainedGoalVerifyContract', 'TestRetainedGoalRejectsDamagedMirror',
                      'TestRetainedGoalProtectionDefinitions', 'TestGoalConversionRejectsAlteredProtections'}
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
# Current human direction on cancelled owner consultation WTYBvQMGp87NeKRx1GATPEIBFXD.
# Keep the diagnosed raw failures; never retry or skip them. This supersedes the
# previous delivery's exception for 05238600e743; it does not authorize other bugs.
INITIAL_HISTORY_DIRECTION = "we don't want to address this bug at this time. please ignore it."
INITIAL_HISTORY_OWNER_JOB = 'WTYBvQMGp87NeKRx1GATPEIBFXD'
INITIAL_HISTORY_VERSION = 'v0.0.99-0.20261122051819-86f7ba9bc009'
INITIAL_HISTORY_ERROR = 'chapter not found: chapterstore: resource not found: chapterstore: chapter not found'
INITIAL_HISTORY_HTTP_DIAGNOSTIC = 'initial history HTTP response: status=404 error=' + INITIAL_HISTORY_ERROR
INITIAL_HISTORY_SITES = {
    'TestRetainedGoalConcurrentInitialHistory': 'selected_history_concurrency_test.go:100',
    'TestPostgresPrimaryGoalSelection/direct': 'runtimetest/primary_goal.go:86',
    'TestPostgresPrimaryGoalSelection/remote': 'runtimetest/primary_goal.go:86',
    'TestPostgresSelectedConcurrentRequests': 'primary_goal_selection_test.go:86',
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
    if package == PREFIX + 'pkg/pgjobdb/installer' and not (REQUIRED_INSTALLER | REQUIRED_RESOURCE_INSTALLER).issubset(tests):
        raise ValueError('missing required historical conversion/report tests')
    if package == PROJECTS and not REQUIRED_PROJECTS.issubset(tests):
        raise ValueError('missing required PostgreSQL project tests')
    if package == COMMAND:
        if not REQUIRED_COMMAND.issubset(tests): raise ValueError('missing PostgreSQL command tests')
        database = [n for n in tests if n.startswith(('TestPostgresConsumerGoals', 'TestPostgresProject', 'TestPostgresResourceBackoffMigration'))]
        return database if behavior == 'runtime' else [n for n in tests if n not in database]
    if package == INTEGRATION:
        if not (REQUIRED_CONSUMER | REQUIRED_GOALS | REQUIRED_PROJECT_RUNTIME | REQUIRED_RESOURCE).issubset(tests): raise ValueError('missing focused consumer tests')
        conformance = [n for n in tests if 'Conformance' in n]
        if not conformance: raise ValueError('empty conformance test selection')
        return conformance if behavior == 'conformance' else [n for n in tests if n not in conformance]
    return tests


def allowed_history_diagnostics(output, name, site=None):
    """Go assertions/cleanup errors have file:line prefixes, unlike PG output.

    Permit only the diagnosed testify header and the three exact barrier logs.
    In particular, counting testify Error blocks alone misses testing.T.Errorf.
    Parent tests have no authorized Go diagnostics of their own.
    """
    barrier_logs = (
        r'selected_history_concurrency_test[.]go:74: first runtime pid [0-9]+ blocked inside initial chapter INSERT',
        r'selected_history_concurrency_test[.]go:89: second runtime pid [0-9]+: early result=.+, waiting=(?:true|false)',
        r'selected_history_concurrency_test[.]go:98: after barrier release: first=<nil>, equivalent=' + re.escape(INITIAL_HISTORY_ERROR),
    ) if name == 'TestRetainedGoalConcurrentInitialHistory' else ()
    for line in output.splitlines():
        if not re.match(r'\s*\S+[.](?:go|s):[0-9]+:', line):
            continue
        text = line.strip()
        if site and text == site.rsplit('/', 1)[-1] + ':':
            continue  # testify emits an empty source header before Error Trace
        if any(re.fullmatch(pattern, text) for pattern in barrier_logs):
            continue
        if name == 'TestPostgresPrimaryGoalSelection/remote' and re.fullmatch(r'initial_history_http_test[.]go:22: ' + re.escape(INITIAL_HISTORY_HTTP_DIAGNOSTIC), text):
            continue
        return False
    return not re.search(r'(?i)panic:|fatal error:|cleanup[^\n]*(?:fail|error)', output)


def accepted_initial_history_failures(events, package, pinned):
    """Recognize only the diagnosed assertion, never a whole-test exemption."""
    if package != INTEGRATION or not pinned:
        return set()
    failed = {e['Test'] for e in events if e.get('Action') == 'fail' and 'Test' in e}
    accepted = set()
    for name, site in INITIAL_HISTORY_SITES.items():
        if name not in failed:
            continue
        output = ''.join(e.get('Output', '') for e in events if e.get('Test') == name)
        # A second assertion (even alongside the known message), missing trace,
        # startup error, panic or altered failure is outside this authorization.
        errors = re.findall(r'\bError:\s*(.*?)(?=\s+Test:|\Z)', output, re.S)
        expected = 'Received unexpected error: ' + INITIAL_HISTORY_ERROR
        if name == 'TestPostgresPrimaryGoalSelection/remote' and INITIAL_HISTORY_HTTP_DIAGNOSTIC in output:
            expected = 'Received unexpected error: primary goal selection is unsupported'
        if site in output and len(errors) == 1 and ' '.join(errors[0].split()) == expected and allowed_history_diagnostics(output, name, site):
            accepted.add(name)
    parent = 'TestPostgresPrimaryGoalSelection'
    children = {name for name in failed if name.startswith(parent + '/')}
    if parent in failed and children and children.issubset(accepted):
        own_output = ''.join(e.get('Output', '') for e in events if e.get('Test') == parent)
        if not re.search(r'\bError:', own_output) and allowed_history_diagnostics(own_output, parent):
            accepted.add(parent)
    return accepted


def validate_outcomes(events, chosen, accepted=()):
    outcomes = {e['Test']: e['Action'] for e in events
                if 'Test' in e and e.get('Action') in ('pass', 'fail', 'skip')}
    if 'TestResourceBackoffConformance' in chosen:
        expected = {'TestResourceBackoffConformance/' + name for name in REQUIRED_RESOURCE_CHILDREN}
        missing_children = expected - set(outcomes)
        if missing_children:
            raise ValueError('missing required resource cases: ' + ', '.join(sorted(missing_children)))
    required_regressions = {parent + '/' + child for parent, children in REQUIRED_FORMAT14_REGRESSIONS.items()
                            if parent in chosen for child in children}
    missing = (set(chosen) | required_regressions) - set(outcomes)
    if missing: raise ValueError('missing required test outcomes: ' + ', '.join(sorted(missing)))
    for test in chosen:
        if outcomes[test] != 'pass' and not (outcomes[test] == 'fail' and test in accepted): raise ValueError('required test did not pass: ' + test + ': ' + outcomes[test])
    for name, action in outcomes.items():
        # Every selected child still executes; only the diagnosed, explicitly
        # accepted assertion may retain a failed outcome without blocking.
        if action == 'skip' or (action == 'fail' and name not in accepted):
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
                result['accepted_failures'] = {}
                modtext = (root / 'go.mod').read_text()
                pinned = bool(re.search(r'^\s*github\.com/colony-2/jobdb\s+' + re.escape(INITIAL_HISTORY_VERSION) + r'\s*$', modtext, re.M)) and not re.search(r'^\s*replace\b', modtext, re.M)
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
                            accepted = accepted_initial_history_failures(events, package, pinned)
                            result['tests'][package].update(validate_outcomes(events, batch, accepted))
                            if code != (1 if accepted else 0):
                                raise ValueError('unexpected test binary exit: ' + str(code))
                            if accepted:
                                result['accepted_failures'][package] = sorted(set(result['accepted_failures'].get(package, [])) | accepted)
                                result['accepted_failure_direction'] = {'owner_job': INITIAL_HISTORY_OWNER_JOB, 'message': INITIAL_HISTORY_DIRECTION, 'dependency': INITIAL_HISTORY_VERSION}
                                log.write('Accepted initial-history failures under human direction: ' + json.dumps(sorted(accepted)) + '\n')
                        except ValueError as exc:
                            log.write(str(exc) + '\n')
                            result['tests'][package].setdefault('acceptance_errors', []).append(str(exc))
                            failed = True
                        result['test_exit_codes'][package].append(code)
                        log.write('test binary exit=' + str(code) + '\n')
                log.flush()
                outcomes = test_outcomes((out / (behavior + '.log')).read_text().replace('\x16', ''))
                result['outcomes'] = outcomes
                accepted_names = {name for names in result['accepted_failures'].values() for name in names}
                unaccepted = set(outcomes['failed_names']) - accepted_names
                result['status'] = 'failed' if failed or unaccepted or outcomes['skipped'] else 'passed'
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
    for record in records:
        for package, names in record.get('accepted_failures', {}).items():
            report += '\n\nAccepted initial-history failures (raw test outcomes remain failed): ' + package + ': ' + ', '.join(names)
            report += '\nHuman direction on ' + INITIAL_HISTORY_OWNER_JOB + ': “' + INITIAL_HISTORY_DIRECTION + '”'
    report += '\n\nHeld: Docker container-smoke/Postgres/MinIO topology; other architecture/macOS runs; release, signing and publication.\n'
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
