"""Record one 1/2/3-volume cell and check its sibling evidence.

Usage: cell-record.py VOLUMES NAMES_FILE EVENTS_FILE BIN... > record.json

VOLUMES is the cell's SWBLOCK_TEST_VOLUMES. NAMES_FILE and EVENTS_FILE are the
ones required-tests-check.py judged (it decides pass, skip and fail). This adds
what a cell must record and what that gate cannot see: the product head
(GITHUB_SHA), each binary's sha256, and per named test the subtests executed and
skipped and the harness's SIBLING lines, and the runner image and kernel. With VOLUMES > 1 every named test must
show at least VOLUMES-1 SIBLING lines, each with acked > 0, read_back == acked
and mismatched == 0: a cell whose siblings never ran or never wrote proves
nothing about 2 or 3 volumes.

Exit 0: the record is written and the sibling evidence holds. 1: it does not.
2: usage.
"""
import hashlib
import json
import os
import re
import sys

SIBLING = re.compile(r'SIBLING (\S+) acked=(\d+) unknown=(\d+) read_back=(\d+) mismatched=(\d+)')


def names_of(path):
    with open(path, encoding='utf-8') as f:
        return [n for n in (line.split('#', 1)[0].strip() for line in f) if n]


def sha256(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for block in iter(lambda: f.read(1 << 20), b''):
            h.update(block)
    return h.hexdigest()


def main(argv):
    if len(argv) < 3 or not argv[0].isdigit():
        print(__doc__.strip().splitlines()[2], file=sys.stderr)
        return 2
    volumes, names, events = int(argv[0]), names_of(argv[1]), argv[2]
    tests = {n: {'subtests_run': 0, 'subtests_skipped': 0, 'siblings': []} for n in names}
    with open(events, encoding='utf-8') as f:
        for raw in f:
            try:
                ev = json.loads(raw)
            except ValueError:
                continue
            test = ev.get('Test') or ''
            top = test.split('/', 1)[0]
            if top not in tests:
                continue
            rec = tests[top]
            if '/' in test and ev.get('Action') in ('pass', 'fail'):
                rec['subtests_run'] += 1
            if '/' in test and ev.get('Action') == 'skip':
                rec['subtests_skipped'] += 1
            if ev.get('Action') == 'output':
                m = SIBLING.search(ev.get('Output', ''))
                if m:
                    v, acked, unknown, read, bad = m.group(1), *map(int, m.groups()[1:])
                    rec['siblings'].append({'volume': v, 'acked': acked, 'unknown': unknown, 'read_back': read, 'mismatched': bad})
    problems = []
    if volumes > 1:
        for n, rec in tests.items():
            good = [s for s in rec['siblings'] if s['acked'] > 0 and s['read_back'] == s['acked'] and s['mismatched'] == 0]
            if len(good) < volumes - 1 or len(good) != len(rec['siblings']):
                problems.append('%s: %d sibling line(s), %d good; want at least %d, all good' % (n, len(rec['siblings']), len(good), volumes - 1))
    # The host the cell ran on, so a runner difference cannot pass as product
    # evidence: the runner image (GitHub-hosted: ImageOS + ImageVersion, else
    # RUNNER_NAME) and the kernel. On Actions a cell without an image fails.
    image = ' '.join(v for v in (os.environ.get('ImageOS', ''), os.environ.get('ImageVersion', '')) if v)
    runner = {'image': image or os.environ.get('RUNNER_NAME', ''), 'kernel': os.uname().release if hasattr(os, 'uname') else '',
              'environment': os.environ.get('RUNNER_ENVIRONMENT', '')}
    if os.environ.get('GITHUB_ACTIONS') == 'true' and not runner['image']:
        problems.append('runner image unknown (no ImageOS/ImageVersion or RUNNER_NAME)')
    record = {'head': os.environ.get('GITHUB_SHA', ''), 'volumes': volumes, 'runner': runner,
              'binaries': {os.path.basename(p): sha256(p) for p in argv[3:]}, 'tests': tests, 'problems': problems}
    json.dump(record, sys.stdout, indent=1, sort_keys=True)
    print()
    for p in problems:
        print('cell-record: ' + p, file=sys.stderr)
    return 1 if problems else 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
