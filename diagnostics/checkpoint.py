"""Publish one JSON observation without sharing temporary files between writers.

Atomic visibility is not acceptance authority or a read/modify/write transaction.
The last successful writer wins. Callers must serialize state merges themselves.
"""
import json
import os
from pathlib import Path
import tempfile
import time


def _open_read(path):
    if os.name != 'nt':
        return Path(path).open('rb')
    # Windows readers must permit rename/delete while retaining their own
    # complete observation. The default Python file sharing mode denies it.
    import ctypes
    from ctypes import wintypes
    import msvcrt
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    create = kernel.CreateFileW
    create.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD,
                       ctypes.c_void_p, wintypes.DWORD, wintypes.DWORD,
                       wintypes.HANDLE]
    create.restype = wintypes.HANDLE
    close = kernel.CloseHandle
    close.argtypes = [wintypes.HANDLE]
    close.restype = wintypes.BOOL
    handle = create(str(Path(path).absolute()), 0x80000000, 0x7, None, 3, 0x80, None)
    if handle == ctypes.c_void_p(-1).value:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        fd = msvcrt.open_osfhandle(handle, os.O_RDONLY | os.O_BINARY)
    except BaseException:
        close(handle)
        raise
    try:
        return os.fdopen(fd, 'rb')
    except BaseException:
        os.close(fd)
        raise


def _replace(source, destination):
    if os.name != 'nt':
        return os.replace(source, destination)
    import ctypes
    from ctypes import wintypes
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    create = kernel.CreateFileW
    create.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD,
                       ctypes.c_void_p, wintypes.DWORD, wintypes.DWORD,
                       wintypes.HANDLE]
    create.restype = wintypes.HANDLE
    close = kernel.CloseHandle
    close.argtypes = [wintypes.HANDLE]
    close.restype = wintypes.BOOL
    rename = kernel.SetFileInformationByHandle
    rename.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD]
    rename.restype = wintypes.BOOL
    target = str(destination.absolute())
    target_bytes = target.encode('utf-16-le')
    class RenameInfo(ctypes.Structure):
        _fields_ = [('flags', wintypes.DWORD), ('root', wintypes.HANDLE),
                    ('length', wintypes.DWORD), ('name', wintypes.WCHAR * (len(target_bytes) // 2 + 1))]
    info = RenameInfo()
    info.flags = 0x3  # Replace existing name with POSIX semantics, preserving open readers.
    info.length = len(target_bytes)
    info.name = target
    handle = create(str(source.absolute()), 0x10000, 0x7, None, 3, 0x80, None)
    if handle == ctypes.c_void_p(-1).value:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        if not rename(handle, 22, ctypes.byref(info), ctypes.sizeof(info)):
            raise ctypes.WinError(ctypes.get_last_error())
    finally:
        close(handle)


def read_bytes(path, *, attempts=8, delay=0.025):
    """Read bounded bytes, retrying only transient sharing denial."""
    if type(attempts) is not int or not 1 <= attempts <= 8:
        raise ValueError('attempts must be 1..8')
    if type(delay) not in (int, float) or not 0 <= delay <= 0.025:
        raise ValueError('delay must be 0..0.025 seconds')
    for attempt in range(attempts):
        try:
            with _open_read(path) as source:
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
            _replace(temporary, path)
            return
        except PermissionError:
            if attempt + 1 == attempts:
                raise
            time.sleep(delay)
