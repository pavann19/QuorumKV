"""Publish one JSON observation without sharing temporary files between writers.

Atomic visibility is not acceptance authority or a read/modify/write transaction.
The last successful writer wins. Callers must serialize state merges themselves.
"""
import json
import os
from pathlib import Path
import tempfile
import time


def read_bytes(path, *, attempts=8, delay=0.025):
    """Read bounded bytes, retrying only transient sharing denial."""
    if type(attempts) is not int or not 1 <= attempts <= 8:
        raise ValueError('attempts must be 1..8')
    if type(delay) not in (int, float) or not 0 <= delay <= 0.025:
        raise ValueError('delay must be 0..0.025 seconds')
    for attempt in range(attempts):
        try:
            with Path(path).open('rb') as source:
                raw = source.read((1 << 20) + 1)
            if len(raw) > 1 << 20:
                raise ValueError('checkpoint exceeds 1 MiB')
            return raw
        except PermissionError:
            if attempt + 1 == attempts:
                raise
            time.sleep(delay)


def read(path, **kwargs):
    """Read one JSON observation; corrupt JSON is never retried or hidden."""
    return json.loads(read_bytes(path, **kwargs))


def save(path, data, *, attempts=8, delay=0.025):
    """Fsync a unique sibling, then retry with at most 175ms of retry sleeps.

    On exhausted replacement, retain the old destination and new sibling for
    explicit recovery. Never truncate the destination or reuse another writer's
    temporary. Parent-directory/power-loss durability is not promised.
    """
    if type(attempts) is not int or not 1 <= attempts <= 8:
        raise ValueError('attempts must be 1..8')
    if type(delay) not in (int, float) or not 0 <= delay <= 0.025:
        raise ValueError('delay must be 0..0.025 seconds')
    path = Path(path)
    raw = (json.dumps(data, indent=2, allow_nan=False) + '\n').encode('utf-8')
    if len(raw) > 1 << 20:
        raise ValueError('checkpoint exceeds 1 MiB')
    # Exclusive creation prevents writer collisions and inherited fixed .tmp
    # files. Close the descriptor before replace, including on Windows.
    with tempfile.NamedTemporaryFile(mode='wb', dir=path.parent,
                                     prefix='.' + path.name + '.', suffix='.tmp',
                                     delete=False) as output:
        temporary = Path(output.name)
        output.write(raw)
        output.flush()
        os.fsync(output.fileno())
    for attempt in range(attempts):
        try:
            os.replace(temporary, path)
            return
        except PermissionError:
            if attempt + 1 == attempts:
                raise
            time.sleep(delay)
