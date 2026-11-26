"""Every named test must have run and passed.

Usage: required-tests-check.py NAMES_FILE EVENTS_FILE...

NAMES_FILE holds one top-level test name per line (# starts a comment).
EVENTS_FILE is `go test -json` (or `go tool test2json`) output. A named test
that skipped, failed, never started or never ended is a failure: a test that
cannot start proves nothing, so it is not allowed to read as a pass. A name
listed twice, or a test that passed in one stream and failed in another, is a
failure too. Prints one line per named test with its outcome and elapsed
seconds, then the total.

Exit 0: every named test passed. 1: at least one did not. 2: usage or an
unreadable input.
"""
import json
import sys


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


def outcomes(paths):
    """Test name -> list of (action, elapsed) for its terminal events, and the
    names that started (action run)."""
    ends, started = {}, set()
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
                    continue  # package events (a subtest's name never matches a top-level one)
                action = ev.get('Action')
                if action == 'run':
                    started.add(test)
                elif action in ('pass', 'fail', 'skip'):
                    ends.setdefault(test, []).append((action, ev.get('Elapsed')))
    return ends, started


def main(argv):
    if len(argv) < 3:
        print(__doc__.strip().splitlines()[2], file=sys.stderr)
        return 2
    try:
        names = names_of(argv[1])
        ends, started = outcomes(argv[2:])
    except (OSError, ValueError) as e:
        print('required-tests: %s' % e, file=sys.stderr)
        return 2
    bad, total = 0, 0.0
    for name in names:
        got = ends.get(name, [])
        actions = {a for a, _ in got}
        secs = sum(e for _, e in got if isinstance(e, (int, float)))
        total += secs
        if actions == {'pass'} and len(got) == 1:
            print('PASS     %-70s %7.1fs' % (name, secs))
            continue
        bad += 1
        if not got:
            why = 'STARTED, NEVER ENDED' if name in started else 'NEVER RAN'
        elif len(got) > 1:
            why = 'RAN %d TIMES (%s)' % (len(got), ', '.join(a for a, _ in got))
        else:
            why = got[0][0].upper()
        print('%-8s %-70s %7.1fs' % ('FAIL', name, secs) + '  ' + why)
    print('required-tests: %d named, %d not passed, %.1fs in named tests' % (len(names), bad, total))
    return 1 if bad else 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
