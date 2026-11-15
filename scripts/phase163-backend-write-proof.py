#!/usr/bin/env python3
"""Read-only per-process/per-volume counter proof for the phase 163 I/O window."""
import json
import pathlib
import sys
import urllib.request


def process(binary, address):
    matches = []
    for proc in pathlib.Path('/proc').glob('[0-9]*'):
        try:
            if str((proc / 'exe').readlink()) != binary:
                continue
            argv = (proc / 'cmdline').read_bytes().split(b'\0')
            if [b'--status-addr', address.encode()] != argv[argv.index(b'--status-addr'):][:2]:
                continue
            fields = (proc / 'stat').read_text().rsplit(')', 1)[1].split()
            matches.append({'pid': int(proc.name), 'start': int(fields[19]), 'binary': binary})
        except (FileNotFoundError, PermissionError, ValueError):
            continue
    if len(matches) != 1:
        raise ValueError('expected one blockvolume process for status endpoint')
    return matches[0]


def capture(binary, address):
    identity = process(binary, address)
    url = f'http://{address}/status/durable?volume=v1'
    with urllib.request.urlopen(url, timeout=5) as response:
        raw = response.read(1024 * 1024 + 1)
    if len(raw) > 1024 * 1024 or process(binary, address) != identity:
        raise ValueError('oversized response or process changed during sample')
    return {'process': identity, 'address': address, 'status': json.loads(raw)}


def integer(value):
    if type(value) is not int or value < 0:
        raise ValueError('missing or invalid integer counter/identity')
    return value


def volume(sample):
    identity = sample['process']
    if integer(identity['pid']) == 0 or integer(identity['start']) == 0 or not identity['binary']:
        raise ValueError('missing process binding')
    volumes = sample['status']['Volumes']
    if type(volumes) is not list or len(volumes) != 1:
        raise ValueError('expected exactly one durable volume')
    vol = volumes[0]
    if (vol['VolumeID'], vol['ReplicaID']) != ('v1', 'r1'):
        raise ValueError('wrong volume or replica')
    if vol['Latched'] is not True or vol['Operational'] is not True or vol['Closed'] is not False:
        raise ValueError('backend is not latched and operational')
    if integer(vol['Epoch']) == 0 or integer(vol['EndpointVersion']) == 0:
        raise ValueError('missing lineage')
    return vol


def check(before, after):
    a, b = volume(before), volume(after)
    if before['process'] != after['process'] or before['address'] != after['address']:
        raise ValueError('process/endpoint changed during I/O')
    for key in ('VolumeID', 'ReplicaID', 'Epoch', 'EndpointVersion', 'Path', 'Impl'):
        if not a[key] or a[key] != b[key]:
            raise ValueError(f'backend identity changed: {key}')
    deltas = {}
    for key, minimum in (('BackendWriteOps', 1), ('BackendWriteBytes', 4096), ('BackendStorageWriteCalls', 1)):
        delta = integer(b['WriteProfile'][key]) - integer(a['WriteProfile'][key])
        if delta < minimum:
            raise ValueError(f'backend write not observed: {key} delta={delta}')
        deltas[key] = delta
    return deltas


if __name__ == '__main__':
    try:
        if sys.argv[1] == 'capture':
            result = capture(*sys.argv[2:])
        elif sys.argv[1] == 'check':
            result = check(*(json.loads(pathlib.Path(p).read_bytes()) for p in sys.argv[2:]))
        else:
            raise ValueError('expected capture or check')
        print(json.dumps(result))
    except (KeyError, TypeError, ValueError, OSError) as error:
        sys.exit(f'backend-write proof refused: {error}')
