"""Self-test of cell-record.py: it must refuse a multi-volume cell whose
siblings never ran, never wrote, did not read every acknowledged write back,
or read back other bytes, and accept one whose siblings all did."""
import json
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
TOOL = os.path.join(HERE, 'cell-record.py')


def run(volumes, sibling_lines, env=None):
    with tempfile.TemporaryDirectory() as d:
        names, events, binary = (os.path.join(d, x) for x in ('names', 'events', 'bin'))
        with open(names, 'w') as f:
            f.write('TestA # row A1\n')
        with open(binary, 'wb') as f:
            f.write(b'binary')
        evs = [{'Action': 'run', 'Test': 'TestA'}]
        evs += [{'Action': 'output', 'Test': 'TestA', 'Output': '    x_test.go:1: ' + s + '\n'} for s in sibling_lines]
        evs += [{'Action': 'pass', 'Test': 'TestA/sub'}, {'Action': 'skip', 'Test': 'TestA/other'}, {'Action': 'pass', 'Test': 'TestA'}]
        with open(events, 'w') as f:
            f.write('\n'.join(json.dumps(e) for e in evs) + '\n')
        e = {k: v for k, v in os.environ.items() if k not in ('GITHUB_ACTIONS', 'ImageOS', 'ImageVersion', 'RUNNER_NAME')}
        e.update(env or {})
        r = subprocess.run([sys.executable, '-I', TOOL, str(volumes), names, events, binary], capture_output=True, text=True, env=e)
        return r.returncode, r.stdout


def line(v, acked=10, read=10, bad=0):
    return 'SIBLING %s acked=%d unknown=0 read_back=%d mismatched=%d last_error=""' % (v, acked, read, bad)


cases = [
    ('one volume needs no sibling', 1, [], 0),
    ('two volumes, sibling good', 2, [line('v1s2')], 0),
    ('three volumes, both good', 3, [line('v1s2'), line('v1s3')], 0),
    ('two volumes, no sibling line', 2, [], 1),
    ('three volumes, one sibling line', 3, [line('v1s2')], 1),
    ('sibling acknowledged nothing', 2, [line('v1s2', acked=0, read=0)], 1),
    ('sibling read back fewer than acked', 2, [line('v1s2', read=9)], 1),
    ('sibling read back other bytes', 2, [line('v1s2', bad=1)], 1),
    ('one good and one bad env', 2, [line('v1s2'), line('v1s2', bad=2)], 1),
]
ACTIONS = {'GITHUB_ACTIONS': 'true'}
cases += [
    ('on Actions with the runner image', 2, [line('v1s2')], 0, dict(ACTIONS, ImageOS='ubuntu24', ImageVersion='20261116.1')),
    ('on Actions without a runner image', 2, [line('v1s2')], 1, ACTIONS),
]
failed = 0
for case in cases:
    what, volumes, lines, want = case[:4]
    env = case[4] if len(case) > 4 else None
    rc, out = run(volumes, lines, env)
    if rc != want:
        print('FAIL %s: exit %d, want %d' % (what, rc, want))
        failed += 1
        continue
    rec = json.loads(out)
    t = rec['tests']['TestA']
    want_image = 'ubuntu24 20261116.1' if env and env.get('ImageOS') else ''
    if (t['subtests_run'] != 1 or t['subtests_skipped'] != 1 or rec['binaries'].get('bin') is None
            or 'kernel' not in rec['runner'] or rec['runner']['image'] != want_image):
        print('FAIL %s: record %s' % (what, out))
        failed += 1
        continue
    print('ok   %s' % what)
if failed:
    sys.exit(1)
print('cell-record self-test: %d cases ok' % len(cases))
