"""Bounded evidence/failure tests, plus generation of current-candidate loader fixtures."""
import importlib.util
import json
import os
import re
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
RUNNER = HERE.parent / 'verify.py'
spec = importlib.util.spec_from_file_location('verify', RUNNER)
verify = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verify)

PACKAGES = [verify.PROJECTS, verify.COMMAND, verify.INTEGRATION, verify.PREFIX+'pkg/pgjobdb/installer',
            verify.INTEGRATION+'/internal/jobdbtest', verify.INTEGRATION+'/internal/jobdbtest/engineconformance',
            verify.INTEGRATION+'/internal/jobdbtest/usageparity', verify.INTEGRATION+'/workflow']

class EvidenceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        for b in verify.BEHAVIORS:
            (self.root / (b + '.json')).write_text(json.dumps({'behavior': b, 'status': 'passed'}))
            (self.root / (b + '.log')).write_text('test evidence\n')
        self.env = patch.dict(os.environ, INBOX=str(self.root), OUTBOX=str(self.root), CANDIDATE='fixture', BASE='base')
        self.env.start(); self.addCleanup(self.env.stop)

    def test_success_and_forced_check_failure(self):
        verify.aggregate()
        self.assertIn('service_free: passed', (self.root / 'verification.md').read_text())
        (self.root / 'installer.json').write_text(json.dumps({'behavior': 'installer', 'status': 'failed'}))
        verify.aggregate()
        self.assertIn('installer: failed', (self.root / 'verification.md').read_text())

    def test_missing_and_malformed_evidence_fail(self):
        for content in ('invalid json', '{"behavior":"installer","status":"skipped"}'):
            (self.root / 'installer.json').write_text(content)
            with self.assertRaises((ValueError, json.JSONDecodeError)): verify.aggregate()
        (self.root / 'runtime.log').unlink()
        (self.root / 'installer.json').write_text(json.dumps({'behavior': 'installer', 'status': 'passed'}))
        with self.assertRaises(FileNotFoundError): verify.aggregate()

    def test_empty_log_fails(self):
        (self.root / 'installer.log').write_text('')
        with self.assertRaises(ValueError): verify.aggregate()

    def test_native_tests_override_worker_locale(self):
        source = self.root / 'source'; source.mkdir()
        (source / 'go.mod').write_text('module example.test/fixture')
        (source / 'go.sum').write_text('')
        for directory in ('.c2', 'cmd', 'pkg'): (source/directory).mkdir()
        observed = []
        def command(argv, log, **kwargs):
            if argv[:2] == ['go', 'list']:
                kwargs['stdout'].write('\n'.join(PACKAGES)+'\n')
            elif argv[:2] == ['go', 'test']:
                Path(argv[argv.index('-o') + 1]).touch()
            elif argv[:3] == ['go', 'tool', 'test2json']:
                kwargs['stdout'].write(json.dumps({'Test':'TestLocal', 'Action':'pass'})+'\n')
            elif '-test.list=.' in argv:
                kwargs['stdout'].write('TestLocal\nTestPostgresConsumerGoalsCommands\nTestPostgresProjectCommands\n')
            elif argv[0] != 'python3':
                observed.append(kwargs['env'])
            return 0
        with patch.dict(os.environ, ROOT=str(source), LANG='C', LC_ALL='C'), patch.object(verify, 'execute', side_effect=command):
            verify.run('service_free')
        self.assertEqual(len(observed), 1)
        self.assertEqual(observed[0]['LANG'], 'C.UTF-8')
        self.assertEqual(observed[0]['LC_ALL'], 'C.UTF-8')
        result = json.loads((self.root / 'service_free.json').read_text())
        self.assertEqual(result['status'], 'passed')
        self.assertEqual(result['test_locale'], 'C.UTF-8')

    def test_named_test_outcomes(self):
        output = "=== RUN   TestA\n--- PASS: TestA (0.01s)\n    --- SKIP: TestB/leaf (0.00s)\n--- FAIL: TestC (0.02s)\n"
        self.assertEqual(verify.test_outcomes(output), {
            'passed': 1, 'failed': 1, 'skipped': 1,
            'failed_names': ['TestC'], 'skipped_names': ['TestB/leaf']})

    def test_build_ignores_workspace_and_inherited_flags_and_skip_fails(self):
        source = self.root / 'source'; source.mkdir()
        (source / 'go.mod').write_text('module example.test/fixture')
        (source / 'go.sum').write_text('')
        for directory in ('.c2', 'cmd', 'pkg'): (source/directory).mkdir()
        def command(argv, log, **kwargs):
            if argv[0] == 'go':
                self.assertEqual(kwargs['env']['GOWORK'], 'off')
                self.assertEqual(kwargs['env']['GOFLAGS'], '')
                if argv[1] == 'list': kwargs['stdout'].write('\n'.join(PACKAGES)+'\n')
                elif argv[1] == 'test': Path(argv[argv.index('-o') + 1]).touch()
                elif argv[1:3] == ['tool', 'test2json']:
                    kwargs['stdout'].write(json.dumps({'Test':'TestRequired', 'Action':'skip'})+'\n')
            elif '-test.list=.' in argv:
                kwargs['stdout'].write('TestRequired\nTestPostgresConsumerGoalsCommands\nTestPostgresProjectCommands\n')
            elif argv[0] != 'python3':
                kwargs['stdout'].write('--- SKIP: TestRequired (0.00s)\n')
            return 0
        with patch.dict(os.environ, ROOT=str(source), GOWORK='/foreign/go.work', GOFLAGS='-overlay=/foreign/overlay.json'), patch.object(verify, 'execute', side_effect=command):
            verify.run('service_free')
        result = json.loads((self.root / 'service_free.json').read_text())
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['outcomes']['skipped_names'], ['TestRequired'])

    def test_scratch_cleanup_failure_prevents_pass(self):
        root = self.root / 'source'; root.mkdir()
        (root / 'go.mod').write_text('module example.test/fixture')
        (root / 'go.sum').write_text('')
        scratch = self.root / 'scratch'; scratch.mkdir()
        class BrokenCleanup:
            def __enter__(self): return str(scratch)
            def __exit__(self, *args): raise OSError('forced scratch cleanup failure')
        def command(argv, log, **kwargs):
            if argv[1] == 'list': kwargs['stdout'].write('\n'.join(PACKAGES)+'\n')
            return 0
        with patch.dict(os.environ, ROOT=str(root)), patch.object(verify.tempfile, 'TemporaryDirectory', return_value=BrokenCleanup()), patch.object(verify, 'execute', side_effect=command):
            verify.run('service_free')
        record = json.loads((self.root / 'service_free.json').read_text())
        self.assertEqual(record['status'], 'failed')
        self.assertIn('scratch cleanup failure', record['error'])

    def test_detached_daemon_is_stopped_and_reaped(self):
        marker = self.root / 'daemon.pid'
        program = "import os,time; p=os.fork(); os._exit(0) if p else None; os.setsid(); open(%r,'w').write(str(os.getpid())); time.sleep(30)" % str(marker)
        with (self.root / 'daemon.log').open('w') as log:
            self.assertEqual(verify.execute([sys.executable, '-c', program], log), 0)
        if marker.exists() and marker.read_text().strip():
            with self.assertRaises(ProcessLookupError): os.kill(int(marker.read_text()), 0)
        with self.assertRaises(ChildProcessError): os.waitpid(-1, os.WNOHANG)

    def test_report_write_failure_prevents_pass(self):
        with patch.object(Path, 'write_text', side_effect=OSError('forced evidence write failure')):
            with self.assertRaisesRegex(OSError, 'forced evidence'):
                verify.aggregate()

    def test_cancellation_propagates(self):
        script = "import importlib.util; s=importlib.util.spec_from_file_location('v',%r); v=importlib.util.module_from_spec(s); s.loader.exec_module(v); f=open(%r,'w'); v.execute([%r,'-c','import time; time.sleep(30)'],f)" % (str(RUNNER), str(self.root / 'cancel.log'), sys.executable)
        p = subprocess.Popen([sys.executable, '-B', '-c', script])
        import time
        import signal
        try:
            for _ in range(100):
                if (self.root / 'cancel.log').exists() and (self.root / 'cancel.log').stat().st_size:
                    time.sleep(0.1)
                    break
                time.sleep(0.02)
            p.send_signal(signal.SIGTERM)
            self.assertEqual(p.wait(timeout=5), 143)
        finally:
            if p.poll() is None:
                p.kill(); p.wait()

    def test_missing_tool_records_failure(self):
        with patch.dict(os.environ, PATH='', ROOT=str(HERE.parents[4])):
            verify.run('installer')
        result = json.loads((self.root / 'installer.json').read_text())
        self.assertEqual(result['status'], 'failed')
        self.assertIn('No such file', result['error'])

    def test_launch_error_fails(self):
        with (self.root / 'launch.log').open('w') as log:
            with self.assertRaises(FileNotFoundError): verify.execute(['/nonexistent/verify-command'], log)

    def test_timeout_kills_process_group(self):
        with (self.root / 'timeout.log').open('w') as log:
            p = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'], start_new_session=True)
            with patch.object(verify.subprocess, 'Popen', return_value=p), patch.object(p, 'wait', side_effect=[subprocess.TimeoutExpired('fixture', 1800), 0]):
                with self.assertRaisesRegex(RuntimeError, 'timed out'): verify.execute(['fixture'], log)
            p.wait()



class SelectionTests(unittest.TestCase):
    def test_complete_disjoint_partitions_and_nested_packages(self):
        names = ['TestLocal', 'TestConsumerGoalsRejectBeforeHTTP', 'TestPostgresConsumerGoalsCommands', 'TestPostgresProjectCommands']
        free = verify.select_tests(verify.COMMAND, names, 'service_free')
        runtime = verify.select_tests(verify.COMMAND, names, 'runtime')
        self.assertEqual(set(free) | set(runtime), set(names))
        self.assertFalse(set(free) & set(runtime))
        names = ['TestPostgresRuntimeConformance', 'TestWorkflowRuntimeConformance',
                 'TestStartOrAppendConformance', 'TestOther'] + sorted(verify.REQUIRED_CONSUMER | verify.REQUIRED_GOALS | verify.REQUIRED_PROJECT_RUNTIME)
        conf = verify.select_tests(verify.INTEGRATION, names, 'conformance')
        runtime = verify.select_tests(verify.INTEGRATION, names, 'runtime')
        self.assertEqual(set(conf) | set(runtime), set(names))
        self.assertFalse(set(conf) & set(runtime))
        selected = verify.select_packages(PACKAGES + [verify.INTEGRATION+'/future/nested'], 'runtime')
        self.assertEqual(set(selected), set(PACKAGES)-{verify.PROJECTS, verify.PREFIX+'pkg/pgjobdb/installer'} | {verify.INTEGRATION+'/future/nested'})
        self.assertIn(verify.COMMAND, verify.select_packages(PACKAGES, 'service_free'))

    def test_required_goal_and_conversion_outcomes(self):
        for package, required in ((verify.INTEGRATION, verify.REQUIRED_CONSUMER | verify.REQUIRED_GOALS | verify.REQUIRED_PROJECT_RUNTIME),
                                  (verify.PREFIX+'pkg/pgjobdb/installer', verify.REQUIRED_INSTALLER),
                                  (verify.PROJECTS, verify.REQUIRED_PROJECTS),
                                  (verify.ENGINE_CONFORMANCE_PACKAGE, verify.REQUIRED_RETRY_HISTORY)):
            for omitted in required:
                with self.assertRaises(ValueError): verify.select_tests(package, sorted(required-{omitted}), 'runtime')
            names = sorted(required)
            for action in ('fail', 'skip'):
                events = [{'Test': n, 'Action': action if n == names[0] else 'pass'} for n in names]
                with self.assertRaises(ValueError): verify.validate_outcomes(events, names)
            with self.assertRaises(ValueError): verify.validate_outcomes([], names)

    def test_retry_history_selection(self):
        names = sorted(verify.REQUIRED_RETRY_HISTORY) + ['TestOther']
        self.assertEqual(verify.select_tests(verify.ENGINE_CONFORMANCE_PACKAGE, names, 'runtime'), names)
        with self.assertRaisesRegex(ValueError, 'retry history tests'):
            verify.select_tests(verify.ENGINE_CONFORMANCE_PACKAGE, ['TestOther'], 'runtime')

    def test_missing_focused_tests_or_packages_fail(self):
        with self.assertRaisesRegex(ValueError, 'command tests'):
            verify.select_tests(verify.COMMAND, ['TestLocal'], 'runtime')
        with self.assertRaisesRegex(ValueError, 'consumer tests'):
            verify.select_tests(verify.INTEGRATION, ['TestConformance'], 'runtime')
        with self.assertRaisesRegex(ValueError, 'task delegation tests'):
            verify.select_tests(verify.TASK_DELEGATION_PACKAGE, ['TestLocal'], 'runtime')
        with self.assertRaisesRegex(ValueError, 'missing required packages'):
            verify.select_packages(PACKAGES[:-1], 'runtime')
        with self.assertRaises(ValueError): verify.select_tests('fixture', [], 'runtime')

    def test_required_database_skips_fail_even_after_zero_exit(self):
        for name in ('TestPostgresConsumerGoalsCommands', 'TestConsumerHTTPRetiresAttachment', 'TestInstaller', 'TestPostgresTaskDelegation'):
            for action in ('skip', 'fail'):
                with self.assertRaisesRegex(ValueError, 'did not pass'):
                    verify.validate_outcomes([{'Test':name, 'Action':action}], [name])
        with self.assertRaisesRegex(ValueError, 'did not pass'):
            verify.validate_outcomes([{'Test':'TestConsumerHTTPRetiresAttachment','Action':'pass'},
                                      {'Test':'TestConsumerHTTPRetiresAttachment/database','Action':'skip'}],
                                     ['TestConsumerHTTPRetiresAttachment'])
        with self.assertRaisesRegex(ValueError, 'missing required test outcomes'):
            verify.validate_outcomes([], ['TestDatabase'])
        with self.assertRaisesRegex(ValueError, 'did not pass'):
            verify.validate_outcomes([{'Test':'TestPostgresTaskDelegation','Action':'pass'},
                                      {'Test':'TestPostgresTaskDelegation/signed/harness-engine','Action':'skip'}],
                                     ['TestPostgresTaskDelegation'])
        for name in ('TestConformance/optional', 'TestSubmissionAvailability/nested'):
            with self.assertRaisesRegex(ValueError, 'did not pass'):
                verify.validate_outcomes([{'Test':name.split('/')[0], 'Action':'pass'},
                                          {'Test':name, 'Action':'skip'}],
                                         [name.split('/')[0]])

    def history_events(self, leaf='TestRetainedGoalConcurrentInitialHistory', message=None):
        message = verify.INITIAL_HISTORY_ERROR if message is None else message
        output = ('Error Trace: /source/' + verify.INITIAL_HISTORY_SITES[leaf] +
                  '\nError: Received unexpected error:\n' + message + '\nTest: ' + leaf + '\n')
        events = [{'Test': leaf, 'Action': 'output', 'Output': output}, {'Test': leaf, 'Action': 'fail'}]
        if '/' in leaf:
            events.append({'Test': leaf.split('/')[0], 'Action': 'fail'})
        return events

    def test_human_exception_preserves_failed_outcomes(self):
        for leaf in verify.INITIAL_HISTORY_SITES:
            events = self.history_events(leaf)
            accepted = verify.accepted_initial_history_failures(events, verify.INTEGRATION, True)
            expected = {leaf} | ({leaf.split('/')[0]} if '/' in leaf else set())
            self.assertEqual(accepted, expected)
            outcomes = verify.validate_outcomes(events, [leaf.split('/')[0]], accepted)
            self.assertEqual(outcomes[leaf], 'fail')
            with self.assertRaises(ValueError): verify.validate_outcomes(events, [leaf.split('/')[0]])

    def test_history_deferral_does_not_accept_task_alternate_failure(self):
        events = self.history_events()
        alternate = 'TestPostgresTaskAlternates'
        leaf = alternate + '/TestTaskAlternateLifecycle/remote-postgres/fallback'
        events += [{'Test': leaf, 'Action': 'output', 'Output': 'Error: Should be true\nTest: ' + leaf + '\n'}]
        events += [{'Test': name, 'Action': 'fail'} for name in (leaf, alternate + '/TestTaskAlternateLifecycle', alternate)]
        accepted = verify.accepted_initial_history_failures(events, verify.INTEGRATION, True)
        self.assertEqual(accepted, {'TestRetainedGoalConcurrentInitialHistory'})
        with self.assertRaisesRegex(ValueError, 'TestPostgresTaskAlternates'):
            verify.validate_outcomes(events, ['TestRetainedGoalConcurrentInitialHistory', alternate], accepted)

    def test_local_history_sites_match_current_source_assertions(self):
        assertions = {
            'TestPostgresSelectedConcurrentRequests': 'require.NoError(t, r.err)',
            'TestRetainedGoalConcurrentInitialHistory': 'require.NoError(t, y.err, "equivalent submission must await complete initial history")',
        }
        root = HERE.parents[4] / 'pkg/pgjobdb/runtime/integration'
        for name, assertion in assertions.items():
            with self.subTest(name=name):
                filename, number = verify.INITIAL_HISTORY_SITES[name].rsplit(':', 1)
                lines = (root / filename).read_text().splitlines()
                start = next(i for i, line in enumerate(lines) if line.startswith('func ' + name + '('))
                end = next((i for i in range(start + 1, len(lines)) if lines[i].startswith('func ')), len(lines))
                actual = [i + 1 for i in range(start, end) if lines[i].strip() == assertion]
                self.assertEqual(actual, [int(number)], 'accepted failure site must track its exact source assertion')

    def test_current_human_direction_is_bound_to_cancelled_consultation(self):
        self.assertEqual(verify.INITIAL_HISTORY_OWNER_JOB, 'WTYBvQMGp87NeKRx1GATPEIBFXD')
        self.assertEqual(verify.INITIAL_HISTORY_DIRECTION, "we don't want to address this bug at this time. please ignore it.")
        self.assertEqual(verify.INITIAL_HISTORY_VERSION, 'v0.0.99-0.20261122051819-86f7ba9bc009')

    def test_remote_history_symptom_requires_exact_captured_response(self):
        leaf = 'TestPostgresPrimaryGoalSelection/remote'
        events = self.history_events(leaf, 'primary goal selection is unsupported')
        self.assertFalse(verify.accepted_initial_history_failures(events, verify.INTEGRATION, True))
        diagnostic = '    initial_history_http_test.go:22: ' + verify.INITIAL_HISTORY_HTTP_DIAGNOSTIC + '\n'
        events[0]['Output'] = diagnostic + events[0]['Output']
        self.assertEqual(verify.accepted_initial_history_failures(events, verify.INTEGRATION, True), {leaf, leaf.split('/')[0]})
        for wrong in (diagnostic.replace('status=404', 'status=501'), diagnostic.replace('chapter not found', 'route not found'), diagnostic.replace(':22:', ':24:')):
            changed = [dict(e) for e in events]
            changed[0]['Output'] = events[0]['Output'].replace(diagnostic, wrong)
            self.assertFalse(verify.accepted_initial_history_failures(changed, verify.INTEGRATION, True))
        events[0]['Output'] += '    initial_history_http_test.go:22: unrelated assertion\n'
        self.assertFalse(verify.accepted_initial_history_failures(events, verify.INTEGRATION, True))

    def test_human_exception_rejects_other_signature_package_or_dependency(self):
        events = self.history_events()
        self.assertFalse(verify.accepted_initial_history_failures(events, verify.COMMAND, True))
        self.assertFalse(verify.accepted_initial_history_failures(events, verify.INTEGRATION, False))
        for message in ('database startup failed', 'context deadline exceeded', 'chapter not found',
                        verify.INITIAL_HISTORY_ERROR + '\npanic: unrelated failure'):
            self.assertFalse(verify.accepted_initial_history_failures(self.history_events(message=message), verify.INTEGRATION, True))
        events[0]['Output'] = events[0]['Output'].replace('selected_history_concurrency_test.go:100', 'another_test.go:100')
        self.assertFalse(verify.accepted_initial_history_failures(events, verify.INTEGRATION, True))
        events = self.history_events()
        for e in events: e['Test'] = 'TestUnrelated'
        self.assertFalse(verify.accepted_initial_history_failures(events, verify.INTEGRATION, True))

    def test_selected_concurrent_exception_requires_exact_assertion(self):
        leaf = 'TestPostgresSelectedConcurrentRequests'
        events = self.history_events(leaf)
        events[0]['Output'] = '    primary_goal_selection_test.go:86: \n' + events[0]['Output']
        self.assertEqual(verify.accepted_initial_history_failures(events, verify.INTEGRATION, True), {leaf})
        for label, output in (
            ('other site', events[0]['Output'].replace(':86', ':88')),
            ('stale site', events[0]['Output'].replace(':86', ':84')),
            ('other error', events[0]['Output'].replace(verify.INITIAL_HISTORY_ERROR, 'context deadline exceeded')),
            ('second testify assertion', events[0]['Output'] + 'Error: unrelated assertion\nTest: ' + leaf + '\n'),
            ('plain Go assertion', events[0]['Output'] + '    primary_goal_selection_test.go:88: handles differ\n'),
            ('cleanup', events[0]['Output'] + '    testsupport_test.go:59: cleanup failed\n'),
        ):
            with self.subTest(label=label):
                changed = [dict(e) for e in events]
                changed[0]['Output'] = output
                accepted = verify.accepted_initial_history_failures(changed, verify.INTEGRATION, True)
                self.assertFalse(accepted)
                with self.assertRaises(ValueError): verify.validate_outcomes(changed, [leaf], accepted)
        self.assertFalse(verify.accepted_initial_history_failures(events, verify.INTEGRATION, False))
        self.assertFalse(verify.accepted_initial_history_failures(events, verify.COMMAND, True))

    def test_human_exception_cannot_hide_extra_assertion_skip_or_child(self):
        events = self.history_events()
        events.insert(1, {'Test': events[0]['Test'], 'Action': 'output', 'Output': 'Error: unrelated assertion\nTest: fixture\n'})
        self.assertFalse(verify.accepted_initial_history_failures(events, verify.INTEGRATION, True))
        events = self.history_events('TestPostgresPrimaryGoalSelection/direct')
        events.append({'Test': 'TestPostgresPrimaryGoalSelection/another', 'Action': 'fail'})
        accepted = verify.accepted_initial_history_failures(events, verify.INTEGRATION, True)
        self.assertNotIn('TestPostgresPrimaryGoalSelection', accepted)
        with self.assertRaises(ValueError): verify.validate_outcomes(events, ['TestPostgresPrimaryGoalSelection'], accepted)
        events = self.history_events()
        events[-1]['Action'] = 'skip'
        accepted = verify.accepted_initial_history_failures(events, verify.INTEGRATION, True)
        with self.assertRaises(ValueError): verify.validate_outcomes(events, [events[-1]['Test']], accepted)

    def test_human_exception_rejects_plain_go_assertions_and_cleanup(self):
        for leaf in verify.INITIAL_HISTORY_SITES:
            parent = leaf.split('/')[0]
            for name in {leaf, parent}:
                for diagnostic in (
                    '    unrelated_test.go:123: unrelated invariant failed\n',
                    '    unrelated_test.go:124: \n        plain Go assertion\n',
                    '    testsupport_test.go:59: cleanup failed: database still running\n',
                    '    selected_history_concurrency_test.go:100: unexpected additional assertion\n',
                    'cleanup failed: forced process cleanup error\n',
                ):
                    with self.subTest(leaf=leaf, name=name, diagnostic=diagnostic):
                        events = self.history_events(leaf)
                        events.insert(1, {'Test': name, 'Action': 'output', 'Output': diagnostic})
                        accepted = verify.accepted_initial_history_failures(events, verify.INTEGRATION, True)
                        self.assertNotIn(name, accepted)
                        with self.assertRaises(ValueError):
                            verify.validate_outcomes(events, [parent], accepted)

    def test_human_exception_allows_only_exact_barrier_logs(self):
        name = 'TestRetainedGoalConcurrentInitialHistory'
        lines = ('    selected_history_concurrency_test.go:74: first runtime pid 123 blocked inside initial chapter INSERT\n'
                 '    selected_history_concurrency_test.go:89: second runtime pid 124: early result=&{{}}, waiting=false\n'
                 '    selected_history_concurrency_test.go:98: after barrier release: first=<nil>, equivalent=' + verify.INITIAL_HISTORY_ERROR + '\n'
                 '    selected_history_concurrency_test.go:100: \n')
        for extra, expected in (('', {name}), ('    selected_history_concurrency_test.go:74: unrelated assertion\n', set())):
            events = self.history_events(name)
            events.insert(0, {'Test': name, 'Action': 'output', 'Output': lines + extra})
            self.assertEqual(verify.accepted_initial_history_failures(events, verify.INTEGRATION, True), expected)

    def test_runner_retains_raw_failure_and_only_accepts_expected_exit(self):
        for code, version, expected in ((1, verify.INITIAL_HISTORY_VERSION, 'passed'),
                                         (2, verify.INITIAL_HISTORY_VERSION, 'failed'),
                                         (0, verify.INITIAL_HISTORY_VERSION, 'failed'),
                                         (1, 'v0.0.100', 'failed'),
                                         (1, 'v0.0.99-0.20261121034814-05238600e743', 'failed')):
            with self.subTest(code=code, version=version), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                (root/'go.mod').write_text('module fixture\nrequire (\n github.com/colony-2/jobdb ' + version + '\n)\n')
                (root/'go.sum').write_text('')
                leaf = 'TestRetainedGoalConcurrentInitialHistory'
                events = self.history_events(leaf)
                def command(argv, log, **kwargs):
                    if argv[:2] == ['go', 'list']: kwargs['stdout'].write(verify.INTEGRATION + '\n')
                    elif argv[:2] == ['go', 'test']: Path(argv[argv.index('-o')+1]).touch()
                    elif '-test.list=.' in argv: kwargs['stdout'].write(leaf + '\n')
                    elif argv[:3] == ['go', 'tool', 'test2json']:
                        kwargs['stdout'].write(''.join(json.dumps(e) + '\n' for e in events))
                    else:
                        kwargs['stdout'].write('--- FAIL: ' + leaf + ' (0.01s)\n')
                        return code
                    return 0
                with patch.dict(os.environ, ROOT=tmp, OUTBOX=tmp), patch.object(verify, 'execute', side_effect=command), \
                     patch.object(verify, 'select_packages', return_value=[verify.INTEGRATION]), \
                     patch.object(verify, 'select_tests', return_value=[leaf]):
                    verify.run('runtime')
                record = json.loads((root/'runtime.json').read_text())
                self.assertEqual(record['status'], expected)
                self.assertEqual(record['test_exit_codes'][verify.INTEGRATION], [code])
                self.assertEqual(record['outcomes']['failed'], 1)
                if expected == 'passed':
                    self.assertEqual(record['tests'][verify.INTEGRATION][leaf], 'fail')
                    self.assertEqual(record['accepted_failures'][verify.INTEGRATION], [leaf])
                else:
                    self.assertFalse(record['accepted_failures'])

    def test_batches_keep_early_and_late_failures_and_complete_selection(self):
        for bad_batch in (None, 0, 1):
            with self.subTest(bad_batch=bad_batch), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                (root/'go.mod').write_text('module fixture\nrequire (\n github.com/colony-2/jobdb ' + verify.INITIAL_HISTORY_VERSION + '\n)\n')
                (root/'go.sum').write_text('')
                names = ['TestRetainedGoalConcurrentInitialHistory'] + ['TestCase%02d' % i for i in range(20)]
                runs, pending = [], []
                def command(argv, log, **kwargs):
                    if argv[:2] == ['go', 'list']: kwargs['stdout'].write(verify.INTEGRATION + '\n')
                    elif argv[:2] == ['go', 'test']: Path(argv[argv.index('-o')+1]).touch()
                    elif '-test.list=.' in argv: kwargs['stdout'].write('\n'.join(names))
                    elif argv[:3] == ['go', 'tool', 'test2json']:
                        kwargs['stdout'].write(''.join(json.dumps(e) + '\n' for e in pending))
                    else:
                        batch = argv[-1].removeprefix('-test.run=^(').removesuffix(')$').split('|')
                        index = len(runs); runs.append(batch); pending.clear()
                        for name in batch:
                            if name == names[0]: pending.extend(self.history_events())
                            else: pending.append({'Test': name, 'Action': 'fail' if index == bad_batch and name == batch[-1] else 'pass'})
                        for e in pending:
                            if e['Action'] in ('pass', 'fail'):
                                kwargs['stdout'].write('--- ' + e['Action'].upper() + ': ' + e['Test'] + ' (0.01s)\n')
                        return int(any(e['Action'] == 'fail' for e in pending))
                    return 0
                with patch.dict(os.environ, ROOT=tmp, OUTBOX=tmp), patch.object(verify, 'execute', side_effect=command), \
                     patch.object(verify, 'select_packages', return_value=[verify.INTEGRATION]), \
                     patch.object(verify, 'select_tests', return_value=names):
                    verify.run('runtime')
                record = json.loads((root/'runtime.json').read_text())
                self.assertEqual([n for batch in runs for n in batch], names)
                self.assertEqual(len(runs), 2)
                self.assertEqual(record['status'], 'passed' if bad_batch is None else 'failed')
                self.assertEqual(record['outcomes']['failed'], 1 if bad_batch is None else 2)
                self.assertEqual(len(record['test_exit_codes'][verify.INTEGRATION]), 2)
                if bad_batch is None:
                    self.assertEqual(len(record['tests'][verify.INTEGRATION]), len(names))

    def test_aggregate_discloses_human_exception(self):
        # Aggregation never rewrites raw failures into passing assertions.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for behavior in verify.BEHAVIORS:
                record = {'behavior': behavior, 'status': 'passed'}
                if behavior == 'runtime':
                    record['accepted_failures'] = {verify.INTEGRATION: ['TestRetainedGoalConcurrentInitialHistory']}
                (root/(behavior + '.json')).write_text(json.dumps(record))
                (root/(behavior + '.log')).write_text('raw evidence')
            with patch.dict(os.environ, INBOX=tmp, OUTBOX=tmp): verify.aggregate()
            report = (root/'verification.md').read_text()
            self.assertIn('raw test outcomes remain failed', report)
            self.assertIn(verify.INITIAL_HISTORY_OWNER_JOB, report)
            self.assertIn(verify.INITIAL_HISTORY_DIRECTION, report)

    def test_focused_failure_affects_behavior(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root/'go.mod').write_text('module fixture')
            (root/'go.sum').write_text('')
            for directory in ('.c2', 'cmd', 'pkg'): (root/directory).mkdir()
            def command(argv, log, **kwargs):
                if argv[0] == 'python3': return 1
                if argv[:2] == ['go','list']: kwargs['stdout'].write('\n'.join(PACKAGES)+'\n')
                elif argv[:2] == ['go','test']: Path(argv[argv.index('-o')+1]).touch()
                elif '-test.list=.' in argv: kwargs['stdout'].write('TestLocal\nTestPostgresConsumerGoalsCommands\nTestPostgresProjectCommands\n')
                elif argv[:3] == ['go','tool','test2json']: kwargs['stdout'].write(json.dumps({'Test':'TestLocal','Action':'pass'})+'\n')
                return 0
            with patch.dict(os.environ, ROOT=tmp, OUTBOX=tmp), patch.object(verify,'execute',side_effect=command):
                verify.run('service_free')
            self.assertEqual(json.loads((root/'service_free.json').read_text())['status'], 'failed')

    def test_prerequisite_failure_records_failed_acceptance(self):
        for phase in ('enumerate', 'compile', 'launch', 'database'):
            with self.subTest(phase=phase), tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp); (root/'go.mod').write_text('module fixture'); (root/'go.sum').write_text('')
                for directory in ('.c2', 'cmd', 'pkg'): (root/directory).mkdir()
                def command(argv, log, **kwargs):
                    if argv[0]=='python3': return 0
                    if argv[:2]==['go','list']:
                        if phase=='enumerate': return 1
                        kwargs['stdout'].write('\n'.join(PACKAGES)+'\n')
                    elif argv[:2]==['go','test']:
                        if phase=='compile': return 1
                        Path(argv[argv.index('-o')+1]).touch()
                    elif '-test.list=.' in argv: kwargs['stdout'].write('TestLocal\nTestPostgresConsumerGoalsCommands\nTestPostgresProjectCommands\n')
                    elif argv[:3]==['go','tool','test2json']: kwargs['stdout'].write(json.dumps({'Test':'TestLocal','Action':'fail'})+'\n')
                    elif phase=='launch': raise OSError('forced launch failure')
                    else: log.write('database startup failed\n'); return 1
                    return 0
                with patch.dict(os.environ,ROOT=tmp,OUTBOX=tmp), patch.object(verify,'execute',side_effect=command): verify.run('service_free')
                self.assertEqual(json.loads((root/'service_free.json').read_text())['status'],'failed')

    def test_retained_source_inventory(self):
        root=HERE.parents[4]
        exceptions={
            'pkg/pgjobdb/goals.go',
            'pkg/pgjobdb/runtime/internal/runtimeadapter/goals.go',
            'pkg/pgjobdb/runtime/integration/native_goal_attachment_compatibility_test.go',
            'pkg/pgjobdb/runtime/integration/native_mutation_probe_test.go',
            'pkg/pgjobdb/runtime/integration/goals_storage_test.go',
            'pkg/pgjobdb/runtime/integration/compact_lists_storage_test.go',
            'pkg/pgjobdb/installer/lease_events_test.go',
        }
        for directory in ('cmd','pkg'):
            for path in (root/directory).rglob('*.go'):
                relative=path.relative_to(root).as_posix(); source=path.read_text()
                self.assertIsNone(re.search(r'\b(?:remote|remoteruntime)\.NewServer(?:WithOptions)?\(',source),relative)
                if relative not in exceptions:
                    self.assertIsNone(re.search(r'(?:func\s+[^\n]*\bAddGoalCandidate|\.AddGoalCandidate\s*\(|\bAddGoalCandidateRequest\b)',source),relative)
                if relative!='pkg/pgjobdb/runtime/runtime.go' and relative not in exceptions and not path.name.endswith('_test.go'):
                    self.assertIsNone(re.search(r'jobdb\.(?:WorkflowRuntime|GoalRuntime)\b',source),relative)


def generate_loader_fixtures(dest):
    """Runtime-backed cell:// loading from the actual candidate text, commands mocked only."""
    dest = Path(dest); dest.mkdir(parents=True, exist_ok=True)
    hook = (HERE.parent / 'verify.yaml').read_text()
    cases = []
    for variant in ('absent', 'null', 'value', 'report-pass', 'report-fail', 'report-missing', 'report-malformed'):
        settings = {'null': '    settings: null\n', 'value': '    settings: {kind: value, format: json, value: {variables: {sample: true}}}\n'}.get(variant, '')
        caller = '''id: bounded-hook-caller
sequence:
- id: invoke
  include: cell://.c2/recipes/hooks/build/verify.yaml
  inputs:
    api_version: c2.build-hook/v1
    request: '$(touch /tmp/PGHOOK_INJECTION)'
    target_directory: '`touch /tmp/PGHOOK_INJECTION`'
    candidate: candidate-fixture
    base: base-fixture
    documents: {}
'''+settings+'''  expect_outputs:
    type: object
    additionalProperties: false
    required: [api_version, status, summary, artifact_refs]
    properties:
      api_version: {type: string, enum: [c2.build-hook/v1]}
      status: {type: string, enum: [passed, failed, skipped]}
      summary: {type: string, minLength: 1, maxLength: 8192}
      artifact_refs: {type: object, maxProperties: 64, additionalProperties: {type: object}}
  catch:
  - when: failure.kind != 'cancellation'
    continue:
      outputs:
        api_version: c2.build-hook/v1
        status: failed
        summary: ${{ failure.message }}
        artifact_refs: {}
outputs:
  result: ${{ sequence.invoke.outputs }}
'''
        (dest / (variant + '.yaml')).write_text(caller)
        artifacts = {b+'.'+ext: ('{}' if ext=='json' else 'fixture') for b in verify.BEHAVIORS for ext in ('json','log')}
        artifacts.update({'verification.md': 'fixture', 'environment.json': '{}'})
        case = {'id': 'settings-' + variant, 'type': 'integration_case', 'runtime': {'cells': {'root': {'files': {'.c2/recipes/hooks/build/verify.yaml': hook, 'untracked-sentinel': 'preserve'}}}},
                'mocks': {'ops': [{'match': {'op': 'command_execution'}, 'repeat': True, 'behavior': {'mode': 'return', 'outputs': {'stdout': 'passed', 'success': True}, 'artifacts': artifacts}}]},
                'assertions': [{'type': 'output_equals', 'path': 'result.status', 'value': 'passed'}, {'type': 'output_equals', 'path': 'result.api_version', 'value': 'c2.build-hook/v1'}]}
        if variant.startswith('report-'):
            files = case['runtime']['cells']['root']['files']
            files['.c2/recipes/hooks/build/verify.py'] = RUNNER.read_text()
            for b in verify.BEHAVIORS:
                artifacts[b + '.json'] = json.dumps({'behavior': b, 'status': 'failed' if variant == 'report-fail' and b == 'installer' else 'passed'})
            if variant == 'report-missing': del artifacts['installer.log']
            if variant == 'report-malformed':
                case['mocks']['ops'].insert(0, {'match': {'node_path': 'bounded-hook-caller/invoke/pgjobdb-native-verify/report'}, 'behavior': {'mode': 'return', 'outputs': {'stdout': 'broken-status', 'success': True}, 'artifacts': artifacts}})
            elif variant != 'report-missing':
                case['mocks']['ops'].insert(0, {'match': {'node_path': 'bounded-hook-caller/invoke/pgjobdb-native-verify/report'}, 'behavior': {'mode': 'passthrough'}})
            case['assertions'][0]['value'] = 'passed' if variant == 'report-pass' else 'failed'
            if variant not in ('report-missing', 'report-malformed'):
                case['assertions'].append({'type': 'cel_true', 'expr': 'size(outputs.result.artifact_refs) == 10'})
                case['assertions'].append({'type': 'cel_true', 'expr': 'size(calls.filter(c, c.op == "command_execution")) == 5'})
        (dest / (variant + '.test.json')).write_text(json.dumps({'recipe': variant + '.yaml', 'cases': [case]}, indent=2))

if __name__ == '__main__':
    if len(sys.argv) == 3 and sys.argv[1] == '--fixtures': generate_loader_fixtures(sys.argv[2])
    else: unittest.main()
