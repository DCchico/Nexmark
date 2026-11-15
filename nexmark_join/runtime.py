"""Redis document and local shared-memory document. Prefix sn stays under 1MiB."""

from __future__ import annotations

import json
import os
import socket
import threading
import urllib.request
from pathlib import Path

from nexmark_join.service import DirectCache

PREFIX = "sn"
LIMIT = 1 << 20


def _encode(args) -> bytes:
    parts = [f"*{len(args)}\r\n".encode()]
    for arg in args:
        raw = arg if isinstance(arg, bytes) else str(arg).encode()
        parts.append(f"${len(raw)}\r\n".encode())
        parts.append(raw)
        parts.append(b"\r\n")
    return b"".join(parts)


def _read(reader):
    prefix = reader.read(1)
    if prefix == b"+":
        return reader.readline().rstrip(b"\r\n").decode()
    if prefix == b"-":
        raise RuntimeError(reader.readline().decode())
    if prefix == b":":
        return int(reader.readline())
    if prefix == b"$":
        size = int(reader.readline())
        if size < 0:
            return None
        data = reader.read(size)
        reader.read(2)
        return data
    raise RuntimeError(f"redis {prefix!r}")


class RedisDoc:
    def __init__(self, address: str) -> None:
        host, port = address.rsplit(":", 1)
        self.host = host
        self.port = int(port)
        self._sock = None
        self._reader = None
        self._lock = threading.Lock()

    def get(self, key: str):
        return self._call("GET", key)

    def set(self, key: str, value: bytes) -> None:
        self._call("SET", key, value)

    def _call(self, *args):
        with self._lock:
            if self._sock is None:
                self._sock = socket.create_connection((self.host, self.port), timeout=30)
                self._reader = self._sock.makefile("rb")
            self._sock.sendall(_encode(args))
            return _read(self._reader)


def shm_path(namespace: str) -> Path:
    return Path("/dev/shm") / f"{PREFIX}_{namespace}"


def shm_read(namespace: str) -> bytes | None:
    path = shm_path(namespace)
    if not path.exists():
        return None
    raw = path.read_bytes()
    if len(raw) < 8:
        return None
    size = int.from_bytes(raw[:8], "little")
    return raw[8:8 + size]


def shm_write(namespace: str, payload: bytes) -> None:
    if len(payload) > LIMIT:
        raise ValueError("nexmark window exceeds 1MiB")
    path = shm_path(namespace)
    blob = len(payload).to_bytes(8, "little") + payload
    temporary = path.with_suffix(".tmp")
    temporary.write_bytes(blob)
    temporary.chmod(0o666)
    temporary.replace(path)


def http_read(endpoint: str, namespace: str) -> bytes | None:
    url = f"http://{endpoint}/social-state?vdag_id={namespace}&prefix={PREFIX}"
    with urllib.request.urlopen(url, timeout=10) as response:
        body = json.loads(response.read())
    if not body.get("success"):
        if body.get("error") == "missing":
            return None
        raise RuntimeError(body.get("error") or "remote read failed")
    data = body.get("data") or ""
    import base64
    return base64.b64decode(data) if isinstance(data, str) else data


def http_write(endpoint: str, namespace: str, payload: bytes) -> None:
    import base64
    raw = json.dumps({
        "vdag_id": namespace,
        "op": "put",
        "prefix": PREFIX,
        "data": base64.b64encode(payload).decode(),
    }).encode()
    request = urllib.request.Request(f"http://{endpoint}/social-state", data=raw, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=10) as response:
        body = json.loads(response.read())
    if not body.get("success"):
        raise RuntimeError(body.get("error") or "remote write failed")


class ObjectBackend:
    def __init__(self) -> None:
        self.direct = DirectCache()
        self.mode = os.environ.get("NEXMARK_BACKEND", "memory")
        redis_addr = os.environ.get("REDIS_ADDR", "")
        self.redis = RedisDoc(redis_addr) if self.mode == "redis-object" and redis_addr else None

    def load(self, namespace: str, headers: dict):
        if self.mode == "redis-object":
            raw = self.redis.get(self._key(namespace))
            return raw, "redis-object"
        mode = headers.get("dependent-state-location") or headers.get("state-location") or "shared_memory"
        if str(mode).startswith("remote:"):
            endpoint = str(mode).split(":", 1)[1]
            return http_read(endpoint, namespace), "remote"
        return shm_read(namespace), "shared_memory"

    def commit(self, namespace: str, headers: dict, payload: bytes, access: str):
        if self.mode == "redis-object":
            self.redis.set(self._key(namespace), payload)
            return len(payload), "redis-object"
        mode = headers.get("dependent-state-location") or headers.get("state-location") or "shared_memory"
        if access == "remote" or str(mode).startswith("remote:"):
            endpoint = str(mode).split(":", 1)[1]
            http_write(endpoint, namespace, payload)
            return len(payload), "remote"
        shm_write(namespace, payload)
        return len(payload), "shared_memory"

    def _key(self, namespace: str) -> str:
        return f"nexmark:q8:{namespace}"
