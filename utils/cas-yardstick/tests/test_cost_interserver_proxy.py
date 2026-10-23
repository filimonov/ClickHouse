import json
import os
import socket
import tempfile
import threading
import time
import unittest

from yardstick.cost.interserver_proxy import ByteRelay, delta


def free_port(host='127.0.0.1'):
    with socket.socket() as s:
        s.bind((host, 0))
        return s.getsockname()[1]


class EchoServer:
    """Echoes every byte back; ends a connection when the client half-closes."""

    def __init__(self, host='127.0.0.1', port=0):
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((host, port))
        self.sock.listen(16)
        self.port = self.sock.getsockname()[1]
        self.thread = threading.Thread(target=self._accept, daemon=True)
        self.thread.start()

    def _accept(self):
        while True:
            try:
                conn, _ = self.sock.accept()
            except OSError:
                return
            threading.Thread(target=self._echo, args=(conn,), daemon=True).start()

    @staticmethod
    def _echo(conn):
        with conn:
            while True:
                data = conn.recv(65536)
                if not data:
                    break
                conn.sendall(data)
            try:
                conn.shutdown(socket.SHUT_WR)
            except OSError:
                pass

    def close(self):
        self.sock.close()


def recv_exact(sock, n):
    buf = b''
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError(f'got {len(buf)} of {n}')
        buf += chunk
    return buf


def wait_for(predicate, timeout=5):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


class RelayCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.log = os.path.join(self.tmp.name, 'relay.jsonl')

    def rows(self, kind=None):
        if not os.path.exists(self.log):
            return []
        with open(self.log) as f:
            rows = [json.loads(line) for line in f if line.strip()]
        return [r for r in rows if kind is None or r['kind'] == kind]

    def start_relay(self, target_port, listen=('127.0.0.1', 0)):
        relay = ByteRelay(listen, ('127.0.0.1', target_port), self.log)
        relay.start()
        self.addCleanup(relay.stop)
        return relay


class ByteRelayTest(RelayCase):
    def test_snapshot_includes_open_connection_bytes(self):
        echo = EchoServer()
        self.addCleanup(echo.close)
        relay = self.start_relay(echo.port)
        client = socket.create_connection(relay.address)
        self.addCleanup(client.close)
        client.sendall(b'a' * 100)
        recv_exact(client, 100)
        first = relay.snapshot('before')
        self.assertEqual((first['to_target'], first['from_target'], first['open']), (100, 100, 1))
        client.sendall(b'b' * 250)
        recv_exact(client, 250)
        second = relay.snapshot('after')
        self.assertEqual(second['open'], 1)
        self.assertEqual(delta(first, second),
                         {'to_target': 250, 'from_target': 250, 'connections_total': 0})
        snaps = self.rows('snapshot')
        self.assertEqual([s['label'] for s in snaps], ['before', 'after'])
        self.assertEqual(snaps[1]['to_target'], 350)
        self.assertEqual(self.rows('connection'), [])  # still open: no connection row yet

    def test_connection_row_on_close(self):
        echo = EchoServer()
        self.addCleanup(echo.close)
        relay = self.start_relay(echo.port)
        client = socket.create_connection(relay.address)
        peer = f'{client.getsockname()[0]}:{client.getsockname()[1]}'
        client.sendall(b'x' * 1000)
        recv_exact(client, 1000)
        client.shutdown(socket.SHUT_WR)  # half-close must reach the target and come back as EOF
        self.assertEqual(client.recv(10), b'')
        client.close()
        self.assertTrue(wait_for(lambda: self.rows('connection')))
        row, = self.rows('connection')
        self.assertEqual((row['to_target'], row['from_target'], row['peer']), (1000, 1000, peer))
        self.assertIsNotNone(row['t_close_us'])
        self.assertLessEqual(row['t_open_us'], row['t_close_us'])
        snap = relay.snapshot('end')
        self.assertEqual((snap['to_target'], snap['from_target'], snap['connections_total'], snap['open']),
                         (1000, 1000, 1, 0))

    def test_relay_survives_target_refusal(self):
        relay = self.start_relay(free_port())  # nothing listens on the target port
        client = socket.create_connection(relay.address)
        client.settimeout(5)
        try:
            data = client.recv(10)
        except ConnectionResetError:
            data = b''
        self.assertEqual(data, b'')
        client.close()
        self.assertTrue(wait_for(lambda: self.rows('connection')))
        row, = self.rows('connection')
        self.assertEqual((row['to_target'], row['from_target']), (0, 0))
        self.assertIsNotNone(row['t_close_us'])
        again = socket.create_connection(relay.address)  # the accept loop is still alive
        again.close()
        self.assertTrue(wait_for(lambda: len(self.rows('connection')) == 2))
        self.assertEqual(relay.snapshot('x')['connections_total'], 2)

    def test_bind_alias_and_real_listener_same_port(self):
        echo = EchoServer()
        self.addCleanup(echo.close)
        relay = self.start_relay(echo.port, listen=('127.0.1.5', echo.port))
        client = socket.create_connection(('127.0.1.5', echo.port))
        self.addCleanup(client.close)
        client.sendall(b'hello')
        self.assertEqual(recv_exact(client, 5), b'hello')
        snap = relay.snapshot('s')
        self.assertEqual((snap['to_target'], snap['from_target']), (5, 5))


if __name__ == '__main__':
    unittest.main()
