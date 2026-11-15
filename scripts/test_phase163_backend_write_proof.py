import copy
import json
import pathlib
import subprocess
import sys
import tempfile
import unittest

HELPER = pathlib.Path(__file__).with_name('phase163-backend-write-proof.py')


class BackendProofTest(unittest.TestCase):
    def setUp(self):
        self.before = {'process': {'pid': 123, 'start': 456, 'binary': '/tmp/run/bin/blockvolume'},
                       'address': '127.0.0.1:16334', 'status': {'Volumes': [{
                           'VolumeID': 'v1', 'ReplicaID': 'r1', 'Epoch': 4, 'EndpointVersion': 2,
                           'Latched': True, 'Operational': True, 'Closed': False,
                           'Path': '/tmp/run/store/v1', 'Impl': 'smartwal',
                           'WriteProfile': {'BackendWriteOps': 7, 'BackendWriteBytes': 28672,
                                            'BackendStorageWriteCalls': 7}}]}}
        self.after = copy.deepcopy(self.before)
        self.profile = self.after['status']['Volumes'][0]['WriteProfile']
        self.profile.update(BackendWriteOps=8, BackendWriteBytes=32768, BackendStorageWriteCalls=8)

    def judge(self, want_success):
        with tempfile.TemporaryDirectory() as tmp:
            paths = [pathlib.Path(tmp) / name for name in ('before.json', 'after.json')]
            for path, sample in zip(paths, (self.before, self.after)):
                path.write_text(json.dumps(sample))
            result = subprocess.run([sys.executable, str(HELPER), 'check', *map(str, paths)],
                                    capture_output=True, timeout=5)
        self.assertEqual(result.returncode == 0, want_success, result.stderr)
        return result

    def test_successful_backend_write(self):
        self.assertEqual(json.loads(self.judge(True).stdout)['BackendWriteBytes'], 4096)

    def test_unchanged_missing_reset_or_short_counters_refuse(self):
        original = copy.deepcopy(self.profile)
        for field in original:
            for value in ('missing', self.before['status']['Volumes'][0]['WriteProfile'][field], -1, True):
                with self.subTest(field=field, value=value):
                    self.profile.clear(); self.profile.update(original)
                    if value == 'missing': self.profile.pop(field)
                    else: self.profile[field] = value
                    self.judge(False)
        self.profile.clear(); self.profile.update(original)
        self.profile['BackendWriteBytes'] -= 1
        self.judge(False)

    def test_changed_process_or_backend_refuses(self):
        pristine = copy.deepcopy(self.after)
        for key, value in (('pid', 124), ('start', 457), ('binary', '/other/blockvolume')):
            self.after = copy.deepcopy(pristine); self.after['process'][key] = value
            self.judge(False)
        for key, value in (('VolumeID', 'v2'), ('ReplicaID', 'r2'), ('Epoch', 5),
                           ('EndpointVersion', 3), ('Path', '/other/store'), ('Closed', True)):
            self.after = copy.deepcopy(pristine); self.after['status']['Volumes'][0][key] = value
            self.judge(False)
        self.after = copy.deepcopy(pristine); self.after['status']['Volumes'] = []
        self.judge(False)


if __name__ == '__main__':
    unittest.main()
