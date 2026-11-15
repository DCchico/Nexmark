"""Post the Data-centric commit semaphore the provider waits on.

The next invocation of the same namespace opens /{key}_{vdag}_{seq}_sem,
waits until its value is positive, and posts it back. Creating that
semaphore is what makes the committed object visible to the successor.
"""

from __future__ import annotations

import ctypes
import ctypes.util

KEY = "nx"


def semaphore_name(namespace: str, seq: int, key: str = KEY) -> str:
    return f"/{key}_{namespace}_{seq}_sem"


def post_commit_semaphore(namespace: str, seq: int, key: str = KEY) -> None:
    name = semaphore_name(namespace, seq, key)
    libc = ctypes.CDLL(ctypes.util.find_library("c") or "libc.so.6", use_errno=True)
    libc.sem_open.restype = ctypes.c_void_p
    libc.sem_open.argtypes = [ctypes.c_char_p, ctypes.c_int, ctypes.c_int, ctypes.c_uint]
    libc.sem_post.argtypes = [ctypes.c_void_p]
    libc.sem_post.restype = ctypes.c_int
    libc.sem_close.argtypes = [ctypes.c_void_p]
    libc.sem_close.restype = ctypes.c_int
    handle = libc.sem_open(name.encode(), 0o100 | 0o2, 0o666, 0)
    failed = ctypes.c_void_p(-1).value
    if not handle or handle == failed:
        error = ctypes.get_errno()
        raise OSError(error, f"sem_open {name} failed")
    try:
        if libc.sem_post(handle) != 0:
            error = ctypes.get_errno()
            raise OSError(error, f"sem_post {name} failed")
    finally:
        libc.sem_close(handle)
