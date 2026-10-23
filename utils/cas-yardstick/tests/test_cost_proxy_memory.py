"""The proxy must stream request bodies: RSS growth during a 64 MiB PUT stays far below the body size."""
import pathlib
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = pathlib.Path(__file__).resolve().parent.parent
BODY = 64 << 20
LIMIT = 16 << 20


class Sink(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def log_message(self, *args):
        pass

    def do_PUT(self):
        remaining = int(self.headers['Content-Length'])
        while remaining:
            remaining -= len(self.rfile.read(min(remaining, 1 << 20)))
        self.send_response(200)
        self.send_header('Content-Length', '0')
        self.end_headers()


def rss(pid: int) -> int:
    for line in pathlib.Path(f'/proc/{pid}/status').read_text().splitlines():
        if line.startswith('VmRSS:'):
            return int(line.split()[1]) * 1024
    raise AssertionError('no VmRSS')


class MemoryTest(unittest.TestCase):
    def test_64_mib_put_streams_with_bounded_rss(self):
        sink = ThreadingHTTPServer(('127.0.0.1', 0), Sink)
        sink.daemon_threads = True
        threading.Thread(target=sink.serve_forever, daemon=True).start()
        self.addCleanup(sink.server_close)
        self.addCleanup(sink.shutdown)
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', 0))
            port = probe.getsockname()[1]
        with tempfile.TemporaryDirectory() as tmp:
            proc = subprocess.Popen(
                [sys.executable, '-m', 'yardstick.cost.proxy', '--upstream', f'127.0.0.1:{sink.server_address[1]}',
                 '--store-id', 'mem-test', '--ledger', f'{tmp}/ledger.jsonl', '--caller', f'rig={port}'],
                cwd=ROOT, stdout=subprocess.PIPE, text=True)
            self.addCleanup(proc.stdout.close)
            self.addCleanup(proc.wait)
            self.addCleanup(proc.terminate)
            self.assertTrue(proc.stdout.readline().startswith('ready'))
            baseline = rss(proc.pid)
            peak = [baseline]
            done = threading.Event()

            def sample():
                while not done.is_set():
                    peak[0] = max(peak[0], rss(proc.pid))
                    time.sleep(0.05)
            sampler = threading.Thread(target=sample)
            sampler.start()
            try:
                block = b'\xa5' * (1 << 20)
                with socket.create_connection(('127.0.0.1', port)) as s:
                    s.sendall(b'PUT /b/big HTTP/1.1\r\nHost: h\r\nContent-Length: %d\r\n\r\n' % BODY)
                    for _ in range(BODY >> 20):
                        s.sendall(block)
                    reply = s.recv(4096)
                self.assertTrue(reply.startswith(b'HTTP/1.1 200'), reply)
            finally:
                done.set()
                sampler.join()
            peak[0] = max(peak[0], rss(proc.pid))
            self.assertLess(peak[0] - baseline, LIMIT, f'baseline {baseline}, peak {peak[0]}')


if __name__ == '__main__':
    unittest.main()
