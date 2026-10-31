import concurrent.futures
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

import checkpoint


class CheckpointTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='quorumkv-checkpoint-')
        self.root = Path(self.tmp.name)
        self.path = self.root / 'state.json'
        checkpoint.save(self.path, {'writer': -1, 'payload': 'original'})

    def tearDown(self):
        self.tmp.cleanup()

    def test_transient_denial_retries_without_truncating_previous_checkpoint(self):
        replace = os.replace
        calls = []
        def blocked(source, destination):
            calls.append(source)
            self.assertEqual(json.loads(self.path.read_text())['writer'], -1)
            if len(calls) < 3:
                raise PermissionError('synthetic sharing denial')
            replace(source, destination)
        with patch.object(checkpoint.os, 'replace', side_effect=blocked):
            checkpoint.save(self.path, {'writer': 1}, delay=0)
        self.assertEqual(len(calls), 3)
        self.assertEqual(len(set(calls)), 1)
        self.assertEqual(json.loads(self.path.read_text()), {'writer': 1})

    def test_exhaustion_preserves_old_and_complete_unpublished_observation(self):
        with patch.object(checkpoint.os, 'replace', side_effect=PermissionError('synthetic denial')) as replace:
            with self.assertRaises(PermissionError):
                checkpoint.save(self.path, {'writer': 2}, delay=0)
        self.assertEqual(replace.call_count, 8)
        self.assertEqual(json.loads(self.path.read_text())['writer'], -1)
        retained = list(self.root.glob('.state.json.*.tmp'))
        self.assertEqual(len(retained), 1)
        self.assertEqual(json.loads(retained[0].read_text()), {'writer': 2})
        checkpoint.save(self.path, {'writer': 3})
        self.assertEqual(json.loads(self.path.read_text()), {'writer': 3})
        self.assertTrue(retained[0].exists())

    def test_concurrent_publishers_and_readers_never_observe_mixed_json(self):
        stop = threading.Event()
        observed = []
        def reader():
            while not stop.is_set():
                value = checkpoint.read(self.path)
                if value['writer'] != -1:
                    self.assertEqual(value['payload'], str(value['writer']) * 2048)
                observed.append(value['writer'])
        def writer(i):
            checkpoint.save(self.path, {'writer': i, 'payload': str(i) * 2048})
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
            reading = pool.submit(reader)
            try:
                list(pool.map(writer, range(20)))
            finally:
                stop.set()
            reading.result(timeout=5)
        self.assertTrue(observed)
        self.assertFalse(list(self.root.glob('.state.json.*.tmp')))

    def test_reader_retries_sharing_denial_but_never_hides_corrupt_json(self):
        original = Path.open
        calls = []
        def blocked(path, *args, **kwargs):
            calls.append(path)
            if len(calls) < 3:
                raise PermissionError('synthetic read sharing denial')
            return original(path, *args, **kwargs)
        with patch.object(checkpoint.Path, 'open', blocked):
            self.assertEqual(checkpoint.read(self.path, delay=0)['writer'], -1)
        self.assertEqual(len(calls), 3)
        self.path.write_bytes(b'{truncated')
        with self.assertRaises(json.JSONDecodeError):
            checkpoint.read(self.path)

    def test_crash_before_replace_leaves_old_valid_and_restart_can_publish(self):
        code = ("import os,sys;sys.path.insert(0,sys.argv[1]);import checkpoint;"
                "checkpoint.os.replace=lambda *args: os._exit(7);"
                "checkpoint.save(sys.argv[2],{'writer':4})")
        p = subprocess.run([sys.executable, '-B', '-c', code,
                            str(Path(checkpoint.__file__).parent), str(self.path)], timeout=10)
        self.assertEqual(p.returncode, 7)
        self.assertEqual(json.loads(self.path.read_text())['writer'], -1)
        orphan = list(self.root.glob('.state.json.*.tmp'))
        self.assertEqual(len(orphan), 1)
        self.assertEqual(json.loads(orphan[0].read_text()), {'writer': 4})
        checkpoint.save(self.path, {'writer': 5})
        self.assertEqual(json.loads(self.path.read_text()), {'writer': 5})

    def test_invalid_json_and_budgets_do_not_replace_destination(self):
        for value, kwargs in [(float('nan'), {}), ('x' * (1 << 20), {}),
                              ({}, {'attempts': 9}), ({}, {'delay': 1})]:
            with self.assertRaises(ValueError):
                checkpoint.save(self.path, value, **kwargs)
        self.assertEqual(json.loads(self.path.read_text())['writer'], -1)
        self.assertFalse(list(self.root.glob('.state.json.*.tmp')))
