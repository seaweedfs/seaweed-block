"""Every named test must have run and passed.

Usage: required-tests-check.py [--status STATUS_FILE] NAMES_FILE EVENTS_FILE...

NAMES_FILE holds one top-level test name per line (# starts a comment).
EVENTS_FILE is `go test -json` (or `go tool test2json`) output. A named test
that skipped, failed, never started or never ended is a failure: a test that
cannot start proves nothing, so it is not allowed to read as a pass. A name
listed twice, or a test that passed in one stream and failed in another, is a
failure too. A package-level failure event fails the run.

STATUS_FILE (run-named-tests.sh) holds one line per named test, NAME and the
exit status of the process that ran it. With it, a named test passes only if it
also has exactly one line and that status is 0: a test that reports PASS and
then fails, panics or hangs on its way out is not a pass. Prints one line per named test with its outcome and elapsed
seconds, then the total.

For every named test that did not pass, the last lines of its output follow the
table, so a run's log carries its own reasons.

Exit 0: every named test passed. 1: at least one did not. 2: usage or an
unreadable input.
"""
import json
import sys

TAIL = 40  # output lines shown per failed test


def names_of(path):
    names, seen = [], set()
    with open(path, encoding='utf-8') as f:
        for n, raw in enumerate(f, 1):
            line = raw.split('#', 1)[0].strip()
            if not line:
                continue
            if not line.startswith('Test') or '/' in line or ' ' in line:
                raise ValueError('%s:%d: %r is not a top-level test name' % (path, n, line))
            if line in seen:
                raise ValueError('%s:%d: %s is listed twice' % (path, n, line))
            seen.add(line)
            names.append(line)
    if not names:
        raise ValueError('%s: no test names' % path)
    return names


def statuses_of(path):
    """Test name -> list of exit statuses."""
    out = {}
    with open(path, encoding='utf-8') as f:
        for n, raw in enumerate(f, 1):
            parts = raw.split()
            if not parts:
                continue
            if len(parts) != 2 or not parts[1].lstrip('-').isdigit():
                raise ValueError('%s:%d: %r is not NAME EXIT_STATUS' % (path, n, raw.strip()))
            out.setdefault(parts[0], []).append(int(parts[1]))
    return out


def outcomes(paths):
    """Test name -> list of (action, elapsed) for its terminal events, the
    names that started (action run), and the number of package-level
    failure events."""
    ends, started, pkg_fail, output = {}, set(), 0, {}
    for path in paths:
        with open(path, encoding='utf-8', errors='replace') as f:
            for raw in f:
                raw = raw.strip()
                if not raw.startswith('{'):
                    continue  # build output or a panic line between events
                try:
                    ev = json.loads(raw)
                except ValueError:
                    continue
                test = ev.get('Test')
                if not test:
                    if ev.get('Action') == 'fail':
                        pkg_fail += 1  # the binary failed outside any test (or after one)
                    continue  # other package events; a subtest's name never matches a top-level one
                action = ev.get('Action')
                if action == 'output':
                    output.setdefault(test.split('/')[0], []).append(ev.get('Output', ''))
                    continue
                if action == 'run':
                    started.add(test)
                elif action in ('pass', 'fail', 'skip'):
                    ends.setdefault(test, []).append((action, ev.get('Elapsed')))
    return ends, started, pkg_fail, output


def main(argv):
    args = argv[1:]
    status_file = None
    if args[:1] == ['--status']:
        if len(args) < 2:
            print(__doc__.strip().splitlines()[2], file=sys.stderr)
            return 2
        status_file, args = args[1], args[2:]
    if len(args) < 2:
        print(__doc__.strip().splitlines()[2], file=sys.stderr)
        return 2
    try:
        names = names_of(args[0])
        ends, started, pkg_fail, output = outcomes(args[1:])
        statuses = statuses_of(status_file) if status_file else None
    except (OSError, ValueError) as e:
        print('required-tests: %s' % e, file=sys.stderr)
        return 2
    bad, total, failed = 0, 0.0, []
    for name in names:
        got = ends.get(name, [])
        actions = {a for a, _ in got}
        secs = sum(e for _, e in got if isinstance(e, (int, float)))
        total += secs
        rcs = statuses.get(name, []) if statuses is not None else [0]
        if actions == {'pass'} and len(got) == 1 and rcs == [0]:
            print('PASS     %-70s %7.1fs' % (name, secs))
            continue
        bad += 1
        failed.append(name)
        if actions == {'pass'} and len(got) == 1:
            why = ('NO EXIT STATUS' if not rcs else
                   'PASSED, THEN EXIT %s' % ','.join(str(r) for r in rcs))
        elif not got:
            why = 'STARTED, NEVER ENDED' if name in started else 'NEVER RAN'
        elif len(got) > 1:
            why = 'RAN %d TIMES (%s)' % (len(got), ', '.join(a for a, _ in got))
        else:
            why = got[0][0].upper()
        print('%-8s %-70s %7.1fs' % ('FAIL', name, secs) + '  ' + why)
    for name in failed:
        lines = ''.join(output.get(name, [])).splitlines()[-TAIL:]
        print('---- %s: last %d output lines' % (name, len(lines)))
        for line in lines:
            print('    ' + line[:300])
    if pkg_fail:
        print('FAIL     the test binary reported %d package-level failure(s)' % pkg_fail)
    print('required-tests: %d named, %d not passed, %.1fs in named tests' % (len(names), bad, total))
    return 1 if bad or pkg_fail else 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
