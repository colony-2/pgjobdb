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

PACKAGES = [verify.COMMAND, verify.INTEGRATION, verify.PREFIX+'pkg/pgjobdb/installer',
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
                kwargs['stdout'].write('TestLocal\nTestPostgresConsumerGoalsCommands\n')
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
                kwargs['stdout'].write('TestRequired\nTestPostgresConsumerGoalsCommands\n')
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
        names = ['TestLocal', 'TestConsumerGoalsRejectBeforeHTTP', 'TestPostgresConsumerGoalsCommands']
        free = verify.select_tests(verify.COMMAND, names, 'service_free')
        runtime = verify.select_tests(verify.COMMAND, names, 'runtime')
        self.assertEqual(set(free) | set(runtime), set(names))
        self.assertFalse(set(free) & set(runtime))
        names = ['TestPostgresRuntimeConformance', 'TestWorkflowRuntimeConformance',
                 'TestStartOrAppendConformance', 'TestOther'] + sorted(verify.REQUIRED_CONSUMER)
        conf = verify.select_tests(verify.INTEGRATION, names, 'conformance')
        runtime = verify.select_tests(verify.INTEGRATION, names, 'runtime')
        self.assertEqual(set(conf) | set(runtime), set(names))
        self.assertFalse(set(conf) & set(runtime))
        selected = verify.select_packages(PACKAGES + [verify.INTEGRATION+'/future/nested'], 'runtime')
        self.assertEqual(set(selected), set(PACKAGES)-{verify.PREFIX+'pkg/pgjobdb/installer'} | {verify.INTEGRATION+'/future/nested'})
        self.assertIn(verify.COMMAND, verify.select_packages(PACKAGES, 'service_free'))

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
                elif '-test.list=.' in argv: kwargs['stdout'].write('TestLocal\nTestPostgresConsumerGoalsCommands\n')
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
                    elif '-test.list=.' in argv: kwargs['stdout'].write('TestLocal\nTestPostgresConsumerGoalsCommands\n')
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
                    self.assertNotIn('AddGoalCandidate',source,relative)
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
