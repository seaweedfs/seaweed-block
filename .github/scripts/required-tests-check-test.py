"""Self-test of required-tests-check.py: event streams against the exit code
they must give. A gate that cannot fail is the defect this check exists to
prevent, so every way a named test can fail to pass is here."""
import json
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
CHECK = os.path.join(HERE, 'required-tests-check.py')


def ev(action, test=None, elapsed=None):
    e = {'Action': action, 'Package': 'example/cmd/blockvolume'}
    if test:
        e['Test'] = test
    if elapsed is not None:
        e['Elapsed'] = elapsed
    return json.dumps(e)


def ran(test, end='pass', secs=1.5):
    return [ev('run', test), ev('output', test), ev(end, test, secs)]


def run(names, *streams, status=None):
    with tempfile.TemporaryDirectory() as d:
        nf = os.path.join(d, 'names')
        with open(nf, 'w', encoding='utf-8') as f:
            f.write(names)
        files = []
        for i, lines in enumerate(streams):
            p = os.path.join(d, 'events%d' % i)
            with open(p, 'w', encoding='utf-8') as f:
                f.write('\n'.join(lines) + '\n')
            files.append(p)
        extra = []
        if status is not None:
            sf = os.path.join(d, 'status')
            with open(sf, 'w', encoding='utf-8') as f:
                f.write(status)
            extra = ['--status', sf]
        r = subprocess.run([sys.executable, '-I', CHECK] + extra + [nf] + files, capture_output=True, text=True)
        return r.returncode, r.stdout + r.stderr


CASES = [
    # (name, want exit, names file, streams)
    ('every named test passed', 0, 'TestA\nTestB  # a comment\n', [ran('TestA') + ran('TestB')]),
    ('passes spread over two streams', 0, 'TestA\nTestB\n', [ran('TestA'), ran('TestB')]),
    ('an unnamed test may fail', 0, 'TestA\n', [ran('TestA') + ran('TestOther', 'fail')]),
    ('a passing subtest does not stand for a skipped parent', 1, 'TestA\n',
     [[ev('run', 'TestA'), ev('pass', 'TestA/sub', 1), ev('skip', 'TestA', 0)]]),
    ('a skip is a failure', 1, 'TestA\nTestB\n', [ran('TestA') + ran('TestB', 'skip', 0)]),
    ('a failed test', 1, 'TestA\n', [ran('TestA', 'fail')]),
    ('a test that never ran', 1, 'TestA\nTestB\n', [ran('TestA')]),
    ('a test that started and never ended (killed by the timeout)', 1, 'TestA\n', [[ev('run', 'TestA'), ev('output', 'TestA')]]),
    ('passed in one stream, failed in another', 1, 'TestA\n', [ran('TestA'), ran('TestA', 'fail')]),
    ('no events at all (the build failed)', 1, 'TestA\n', [['# example/cmd/blockvolume', 'build failed']]),
    ('a package-level failure after every named test passed', 1, 'TestA\n', [ran('TestA') + [ev('fail', None, 3)]]),
    ('a name listed twice', 2, 'TestA\nTestA\n', [ran('TestA')]),
    ('a subtest name in the list', 2, 'TestA/sub\n', [ran('TestA')]),
    ('an empty names file', 2, '# nothing\n', [ran('TestA')]),
]


def main():
    fail = 0
    for name, want, names, streams in CASES:
        got, out = run(names, *streams)
        if got != want:
            print('FAIL %s: exit %d, want %d\n%s' % (name, got, want, out))
            fail = 1
    STATUS = [
        ('PASS events and every process exited 0', 0, 'TestA 0\nTestB 0\n'),
        ('PASS, then the process exited 1', 1, 'TestA 0\nTestB 1\n'),
        ('PASS, then the process was killed by the timeout', 1, 'TestA 0\nTestB 124\n'),
        ('a named test with no exit status', 1, 'TestA 0\n'),
        ('a named test ran twice', 1, 'TestA 0\nTestB 0\nTestB 0\n'),
        ('a malformed status line', 2, 'TestA 0\nTestB\n'),
    ]
    for name, want, status in STATUS:
        got, out = run('TestA\nTestB\n', ran('TestA') + ran('TestB'), status=status)
        if got != want:
            print('FAIL %s: exit %d, want %d\n%s' % (name, got, want, out))
            fail = 1
    got, out = run('TestA\n', [ev('run', 'TestA'), json.dumps({'Action': 'output', 'Test': 'TestA', 'Output': '    x_test.go:7: the reason\n'})] + [json.dumps({'Action': 'output', 'Test': 'TestA', 'Output': '    h_test.go:578: blockvolume-r%d log:\n' % i}) for i in range(20)] + [json.dumps({'Action': 'output', 'Test': 'TestA', 'Output': '    w_test.go:128: ---- master log tail ----\n'})] + [json.dumps({'Action': 'output', 'Test': 'TestA', 'Output': '        I1126 harness log line %d\n' % i}) for i in range(60)] + [ev('fail', 'TestA', 1)])
    if got != 1 or 'x_test.go:7: the reason' not in out:
        print('FAIL a failed test''s output is shown: %s' % out)
        fail = 1
    got, out = run('TestA\n', ran('TestA', secs=2.25))
    if got != 0 or '2.2s' not in out and '2.3s' not in out:
        print('FAIL the elapsed time is reported: %s' % out)
        fail = 1
    if not fail:
        print('required-tests-check: %d cases ok' % (len(CASES) + len(STATUS) + 2))
    return fail


if __name__ == '__main__':
    sys.exit(main())
