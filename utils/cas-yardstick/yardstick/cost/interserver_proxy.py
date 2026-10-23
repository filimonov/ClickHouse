"""Byte-counting TCP relay for ClickHouse interserver (replication) traffic in the cost rig.

Relay `i` listens on `127.0.1.<i>:<interserver port of replica i>` and forwards to `127.0.0.1:<same port>`,
where replica `i` really listens. Peers dial the alias, so for replica `i`:

- `from_target` is every byte replica `i` sent, attributed to the sender exactly;
- `to_target` is every byte its peers sent it, with the receiver known by construction: two-replica cards have
  a single receiver, and the three-replica `replacement` card keeps r1 and r2 idle.

The relay does not parse the protocol. `snapshot` counts the bytes of connections that are still open, so
a window delta is exact without waiting for long-lived connections to close.

Log rows (JSONL, one lock for counters and writes):
  {"kind": "snapshot", "label", "t_us", "to_target", "from_target", "connections_total", "open"}
  {"kind": "connection", "t_open_us", "t_close_us", "peer", "to_target", "from_target"}   (written on close)
"""
from __future__ import annotations

import json
import os
import socket
import threading
import time

CHUNK = 1 << 16
CONNECT_TIMEOUT = 10


def _now_us() -> int:
    return time.time_ns() // 1000


def delta(a: dict, b: dict) -> dict:
    """`b - a` for the cumulative counters of two snapshots."""
    return {k: b[k] - a[k] for k in ('to_target', 'from_target', 'connections_total')}


class _Conn:
    def __init__(self, client: socket.socket, peer: str):
        self.client = client
        self.target = None
        self.peer = peer
        self.t_open_us = _now_us()
        self.to_target = 0
        self.from_target = 0
        self.live_pumps = 2


class ByteRelay:
    def __init__(self, listen: tuple, target: tuple, log_path: str):
        self.listen = (listen[0], int(listen[1]))
        self.target = (target[0], int(target[1]))
        self.log_path = log_path
        self.address = self.listen  # the bound address once started (resolves port 0)
        self._lock = threading.Lock()
        self._to_target = 0
        self._from_target = 0
        self._connections_total = 0
        self._open: set = set()
        self._file = None
        self._server = None
        self._stopping = False

    def start(self):
        os.makedirs(os.path.dirname(os.path.abspath(self.log_path)), exist_ok=True)
        self._file = open(self.log_path, 'ab')
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(self.listen)
        server.listen(128)
        self.address = server.getsockname()[:2]
        self._server = server
        threading.Thread(target=self._accept_loop, name='relay-accept', daemon=True).start()

    def stop(self):
        with self._lock:
            self._stopping = True
            conns = list(self._open)
        if self._server is not None:
            # Closing a listening socket does not wake accept() on Linux; shutdown does.
            for call in (lambda: self._server.shutdown(socket.SHUT_RDWR), self._server.close):
                try:
                    call()
                except OSError:
                    pass
            self._server = None
        for conn in conns:
            self._shutdown_both(conn)
        deadline = time.time() + 5
        while time.time() < deadline:
            with self._lock:
                if not self._open:
                    break
            time.sleep(0.01)
        with self._lock:
            if self._file is not None:
                self._file.close()
                self._file = None

    def snapshot(self, label: str) -> dict:
        with self._lock:
            state = {'to_target': self._to_target, 'from_target': self._from_target,
                     'connections_total': self._connections_total, 'open': len(self._open)}
            self._write({'kind': 'snapshot', 'label': label, 't_us': _now_us(), **state})
        return state

    def _write(self, row: dict):
        if self._file is not None:  # a pump finishing after stop(); the log is already closed
            self._file.write((json.dumps(row, separators=(',', ':')) + '\n').encode())
            self._file.flush()

    def _accept_loop(self):
        server = self._server
        while True:
            try:
                client, addr = server.accept()
            except OSError:
                return
            try:
                threading.Thread(target=self._serve, args=(client, f'{addr[0]}:{addr[1]}'),
                                 name='relay-conn', daemon=True).start()
            except Exception:  # noqa: BLE001 - keep accepting
                client.close()

    def _serve(self, client: socket.socket, peer: str):
        conn = _Conn(client, peer)
        try:
            with self._lock:
                self._connections_total += 1
                self._open.add(conn)
                stopping = self._stopping
            if stopping:
                raise OSError('relay stopping')
            target = socket.create_connection(self.target, CONNECT_TIMEOUT)
            target.settimeout(None)
            conn.target = target
            for sock in (client, target):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except Exception:  # noqa: BLE001 - refused or failed target: drop the client, log zero bytes
            self._finish(conn)
            return
        threading.Thread(target=self._pump, args=(conn, client, target, True), daemon=True).start()
        self._pump(conn, target, client, False)

    def _pump(self, conn: _Conn, src: socket.socket, dst: socket.socket, to_target: bool):
        try:
            while True:
                data = src.recv(CHUNK)
                if not data:
                    break
                # Count before forwarding: a peer that has seen the bytes must find them in the next snapshot.
                with self._lock:
                    if to_target:
                        conn.to_target += len(data)
                        self._to_target += len(data)
                    else:
                        conn.from_target += len(data)
                        self._from_target += len(data)
                dst.sendall(data)
            try:
                dst.shutdown(socket.SHUT_WR)  # half-close: the other direction keeps flowing
            except OSError:
                pass
        except Exception:  # noqa: BLE001 - a broken side ends both directions
            self._shutdown_both(conn)
        with self._lock:
            conn.live_pumps -= 1
            last = conn.live_pumps == 0
        if last:
            self._finish(conn)

    @staticmethod
    def _shutdown_both(conn: _Conn):
        for sock in (conn.client, conn.target):
            if sock is not None:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

    def _finish(self, conn: _Conn):
        for sock in (conn.client, conn.target):
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass
        with self._lock:
            self._open.discard(conn)
            self._write({'kind': 'connection', 't_open_us': conn.t_open_us, 't_close_us': _now_us(),
                         'peer': conn.peer, 'to_target': conn.to_target, 'from_target': conn.from_target})
