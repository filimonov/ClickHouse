"""Store-side ledger proxy for the cost rig.

One listener per caller (`r1`, `r2`, `r3`, `rig`): the listening port identifies the caller. Every S3 attempt
becomes one `ledger-row/1` JSON line (docs/superpowers/cost-model/schemas). Request lines, `Host`, path, query
and headers are forwarded byte-exact so the client's SigV4 signature still verifies at the store. Bodies are
streamed in 1 MiB pieces; nothing is buffered except the small XML bodies the ledger parses.

Counting conventions: `req_wire_bytes` is what the caller put on the wire (head as received plus raw body),
`resp_wire_bytes` is what the store put on the wire (head plus raw body, chunk framing included). Responses the
proxy writes itself (411, injected faults) count as 0 response bytes.
"""
from __future__ import annotations

import argparse
import hashlib
import http
import json
import os
import re
import select
import signal
import socket
import socketserver
import sys
import threading
import time
import traceback
import urllib.parse
import xml.etree.ElementTree as ET

CHUNK = 1 << 20
IDLE_TIMEOUT = 60
IO_TIMEOUT = 300
CONNECT_TIMEOUT = 10
MAX_LINE = 65536
MAX_HEADER_LINES = 256
REQUEST_CAPTURE_MAX = 8 << 20
ERROR_CAPTURE_MAX = 1 << 20
LISTING_CAPTURE_MAX = 8 << 20

RECORDED_HEADERS = {'content-length', 'content-md5', 'expect', 'transfer-encoding', 'range', 'etag'}
FAULT_KEYS = {'caller', 'op', 'key_regex', 'upload_id', 'part_number', 'nth', 'action', 'status', 'send_upstream'}


def parse_operation(method: str, path: str, query: dict, headers: dict) -> str:
    """Map a request to the ledger `op` enum. `path` is the request path, `headers` may use any case."""
    lowered = {k.lower() for k in headers}
    segments = path.lstrip('/').split('/', 1)
    has_key = len(segments) == 2 and segments[1] != ''
    copy = 'x-amz-copy-source' in lowered
    if method == 'POST':
        if 'uploads' in query:
            return 'CreateMultipartUpload'
        if 'uploadId' in query:
            return 'CompleteMultipartUpload'
        if 'delete' in query:
            return 'DeleteObjects'
    elif method == 'GET':
        if 'uploads' in query:
            return 'ListMultipartUploads'
        if 'uploadId' in query:
            return 'ListParts'
        if not has_key:
            return 'ListObjectsV2' if query.get('list-type') == '2' else 'Other'
        return 'GetObject'
    elif method == 'PUT':
        if 'uploadId' in query and 'partNumber' in query:
            return 'UploadPartCopy' if copy else 'UploadPart'
        if copy:
            return 'CopyObject'
        return 'PutObject' if has_key else 'CreateBucket'
    elif method == 'DELETE':
        if 'uploadId' in query:
            return 'AbortMultipartUpload'
        if has_key:
            return 'DeleteObject'
    elif method == 'HEAD':
        return 'HeadObject' if has_key else 'HeadBucket'
    return 'Other'


# --- body parsing -----------------------------------------------------------------------------

def _local(tag: str) -> str:
    return tag.rsplit('}', 1)[-1]


def _kids(node: ET.Element, name: str) -> list:
    return [c for c in node if _local(c.tag) == name]


def _text(node: ET.Element, name: str):
    found = _kids(node, name)
    return (found[0].text or '') if found else None


def _xml(body: bytes):
    try:
        return ET.fromstring(body)
    except ET.ParseError:
        return None


def _etag(value: str) -> str:
    return value.strip().strip('"')


def error_code(body: bytes):
    root = _xml(body)
    if root is None or _local(root.tag) != 'Error':
        return None
    return _text(root, 'Code')


def parse_upload_id(body: bytes):
    root = _xml(body)
    return _text(root, 'UploadId') if root is not None else None


def parse_completed_parts(body: bytes):
    root = _xml(body)
    if root is None:
        return None
    return [{'part_number': int(_text(p, 'PartNumber')), 'etag': _etag(_text(p, 'ETag') or '')}
            for p in _kids(root, 'Part') if (_text(p, 'PartNumber') or '').isdigit()]


def parse_delete_results(request: bytes, response: bytes):
    """Verbose mode lists every key in the response; quiet mode lists only errors, absent keys were deleted."""
    req, resp = _xml(request), _xml(response)
    if req is None or resp is None:
        return None
    quiet = (_text(req, 'Quiet') or '').strip().lower() == 'true'
    failed = {_text(e, 'Key'): _text(e, 'Code') for e in _kids(resp, 'Error')}
    if quiet:
        keys = [_text(o, 'Key') for o in _kids(req, 'Object')]
        return [{'key': k, 'deleted': k not in failed, 'code': failed.get(k)} for k in keys]
    results = []
    for entry in resp:
        name = _local(entry.tag)
        if name == 'Deleted':
            results.append({'key': _text(entry, 'Key'), 'deleted': True, 'code': None})
        elif name == 'Error':
            results.append({'key': _text(entry, 'Key'), 'deleted': False, 'code': _text(entry, 'Code')})
    return results


def parse_listing(body: bytes):
    root = _xml(body)
    if root is None:
        return None
    count = _text(root, 'KeyCount')
    return {'key_count': int(count) if count and count.isdigit() else len(_kids(root, 'Contents')),
            'truncated': (_text(root, 'IsTruncated') or '').strip().lower() == 'true'}


# --- framing helpers --------------------------------------------------------------------------

class Gone(Exception):
    pass


class ClientGone(Gone):
    pass


class UpstreamGone(Gone):
    pass


def _tokens(headers: dict, name: str) -> set:
    return {t.strip().lower() for t in headers.get(name, '').split(',') if t.strip()}


def _read1(f, n: int, gone: type) -> bytes:
    try:
        data = f.read1(n)
    except (OSError, ValueError) as e:
        raise gone(str(e)) from None
    if not data:
        raise gone('connection closed')
    return data


def _readline(f, gone: type) -> bytes:
    try:
        line = f.readline(MAX_LINE)
    except (OSError, ValueError) as e:
        raise gone(str(e)) from None
    if not line.endswith(b'\n'):
        raise gone('connection closed or line too long')
    return line


def read_head(f, gone: type):
    """Return (raw header lines, total head bytes). The caller already consumed the first line."""
    lines = []
    total = 0
    while True:
        line = _readline(f, gone)
        total += len(line)
        if line in (b'\r\n', b'\n'):
            return lines, total
        lines.append(line)
        if len(lines) > MAX_HEADER_LINES:
            raise gone('too many header lines')


def parse_headers(lines: list) -> dict:
    """Lower-cased name to value; repeated headers are joined with ', '."""
    out = {}
    for line in lines:
        name, sep, value = line.decode('latin-1').partition(':')
        if not sep:
            continue
        name = name.strip().lower()
        value = value.strip()
        out[name] = f'{out[name]}, {value}' if name in out else value
    return out


def relay_chunked(src, gone: type, raw_cb=None, data_cb=None, last_cb=None):
    """Read a chunked body piece by piece. `raw_cb` sees the raw stream (framing included) as it arrives,
    `data_cb` the payload, `last_cb` runs just before the final piece goes to `raw_cb`.
    Return (payload bytes, wire bytes)."""
    payload = wire = 0

    def raw(piece):
        nonlocal wire
        wire += len(piece)
        if raw_cb:
            raw_cb(piece)
    while True:
        line = _readline(src, gone)
        raw(line)
        try:
            size = int(line.split(b';', 1)[0].strip(), 16)
        except ValueError:
            raise gone('bad chunk size') from None
        if size == 0:
            while True:
                trailer = _readline(src, gone)
                if trailer in (b'\r\n', b'\n') and last_cb:
                    last_cb()
                raw(trailer)
                if trailer in (b'\r\n', b'\n'):
                    return payload, wire
        remaining = size
        while remaining:
            data = _read1(src, min(remaining, CHUNK), gone)
            remaining -= len(data)
            payload += len(data)
            raw(data)
            if data_cb:
                data_cb(data)
        raw(_readline(src, gone))


# --- the proxy --------------------------------------------------------------------------------

class Attempt:
    """One ledger row under construction. Defaults describe a request that died before anything was known."""

    def __init__(self, caller, store_id, seq, t_start_us):
        self.row = {
            'kind': 'attempt', 'seq': seq, 'caller': caller, 'store_id': store_id, 't_start_us': t_start_us,
            't_end_us': None, 'method': '', 'path': '', 'query': {}, 'req_headers': {}, 'resp_headers': {},
            'bucket': '', 'key': '', 'op': 'Other', 'upload_id': None, 'part_number': None, 'copy_source': None,
            'req_payload_bytes': 0, 'req_wire_bytes': 0, 'resp_payload_bytes': 0, 'resp_wire_bytes': 0,
            'payload_sha256': None, 'expect_negotiated': False,
            'downstream': {'outcome': 'dropped', 'status': None},
            'upstream': {'outcome': 'unknown', 'http_status': None, 's3_code': None},
            'response_body': {'upload_id': None, 'etag': None, 'complete_error': None, 'completed_parts': None,
                              'delete_results': None, 'list': None},
            'injected_rule': None, 'phase': 'outside',
        }
        self.request_body = None
        self.response_body = None
        self.relayed = False  # response head already sent to the client
        self.finishing = False  # the last response byte is about to reach the client


class _Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True
    block_on_close = False
    request_queue_size = 512

    def __init__(self, address, proxy, caller):
        self.proxy = proxy
        self.caller = caller
        super().__init__(address, _Handler)


class LedgerProxy:
    def __init__(self, upstream: tuple, store_id: str, ledger_path, callers: dict):
        self.upstream = (upstream[0], int(upstream[1]))
        self.store_id = store_id
        self.ledger_path = ledger_path
        self.callers = dict(callers)
        self.ports: dict = {}
        self._lock = threading.Lock()
        self._finishing = 0
        self._finished = threading.Condition(self._lock)
        self._seq = 0
        self._last_us = 0
        self._file = None
        self._servers: list = []
        self._connections: set = set()
        self._faults: list = []
        self._fault_ids = 0

    def start(self):
        os.makedirs(os.path.dirname(os.path.abspath(self.ledger_path)), exist_ok=True)
        self._file = open(self.ledger_path, 'ab')
        for caller, port in self.callers.items():
            server = _Server(('127.0.0.1', port), self, caller)
            self.ports[caller] = server.server_address[1]
            threading.Thread(target=server.serve_forever, kwargs={'poll_interval': 0.05}, name=f'proxy-{caller}', daemon=True).start()
            self._servers.append(server)

    def stop(self):
        for server in self._servers:
            server.shutdown()
            server.server_close()
        self._servers = []
        with self._lock:
            connections = list(self._connections)
        for sock in connections:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        if self._file is not None:
            self.flush()
            with self._lock:
                self._file.close()
                self._file = None

    # Seq and timestamp are taken under one lock for attempts and marks, so seq order is time order and an
    # attempt that starts after a mark belongs to the next window.
    def _stamp(self):
        self._seq += 1
        self._last_us = max(self._last_us, time.time_ns() // 1000)
        return self._seq, self._last_us

    def begin(self, caller: str) -> Attempt:
        with self._lock:
            seq, t = self._stamp()
        return Attempt(caller, self.store_id, seq, t)

    def finishing(self, attempt: Attempt):
        """Call just before the last response byte goes out: once the client has seen a complete response,
        `flush` must not return before that attempt's row is written."""
        if not attempt.finishing:
            with self._lock:
                attempt.finishing = True
                self._finishing += 1

    def finish(self, attempt: Attempt):
        with self._lock:
            self._last_us = max(self._last_us, time.time_ns() // 1000)
            attempt.row['t_end_us'] = self._last_us
            self._write(attempt.row)
            if attempt.finishing:
                self._finishing -= 1
                self._finished.notify_all()

    def mark(self, label: str) -> int:
        with self._lock:
            seq, t = self._stamp()
            self._write({'kind': 'mark', 'seq': seq, 'label': label, 't_us': t})
        return seq

    def _write(self, row: dict):
        if self._file is None:  # a handler finishing after stop(); the ledger is already closed
            return
        self._file.write((json.dumps(row, separators=(',', ':')) + '\n').encode())

    def flush(self):
        """Write rows to disk. Attempts still in flight are not waited for, except those whose response has
        already been completed towards the client."""
        with self._lock:
            if not self._finished.wait_for(lambda: self._finishing == 0, timeout=10):
                raise TimeoutError('attempts with a delivered response did not finish')
            if self._file is not None:
                self._file.flush()
                os.fsync(self._file.fileno())

    def track(self, sock, add: bool):
        with self._lock:
            (self._connections.add if add else self._connections.discard)(sock)

    # --- faults ---

    def set_fault(self, rule: dict) -> str:
        """Add a fault rule and return its id. Matching fields: `caller`, `op`, `key_regex` (re.search),
        `upload_id` (`*` = any), `part_number` (None = any). `nth` is the 1-based count of matching attempts
        since the rule was set (`*` = every match). `action` is `status` (needs `status`; `send_upstream` picks
        whether the store sees the request) or `drop_response` (forwards, then closes without answering)."""
        spec = dict(rule)
        action = spec.pop('action', None)
        if isinstance(action, dict):
            spec.update({k: v for k, v in action.items() if k != 'type'})
            action = action.get('type', action.get('kind'))
        unknown = set(spec) - FAULT_KEYS
        if unknown:
            raise ValueError(f'unknown fault keys: {sorted(unknown)}')
        if action == 'status':
            if not isinstance(spec.get('status'), int) or not 100 <= spec['status'] <= 599:
                raise ValueError('status fault needs an integer status')
            spec.setdefault('send_upstream', True)
        elif action == 'drop_response':
            if spec.get('send_upstream', True) is not True:
                raise ValueError('drop_response needs send_upstream true')
            spec['send_upstream'] = True
        else:
            raise ValueError(f'unknown fault action {action!r}')
        nth = spec.get('nth', 1)
        if nth != '*' and not (isinstance(nth, int) and nth >= 1):
            raise ValueError('nth must be a positive integer or "*"')
        with self._lock:
            self._fault_ids += 1
            fault_id = f'fault-{self._fault_ids}'
            self._faults.append({
                'id': fault_id, 'action': action, 'status': spec.get('status'),
                'send_upstream': spec['send_upstream'], 'caller': spec.get('caller', '*'),
                'op': spec.get('op', '*'), 'key': re.compile(spec.get('key_regex', '.*')),
                'upload_id': spec.get('upload_id', '*'), 'part_number': spec.get('part_number'),
                'nth': nth, 'seen': 0})
        return fault_id

    def clear_faults(self):
        with self._lock:
            self._faults = []

    def match_fault(self, row: dict):
        """Return the first rule that fires for this attempt. Every rule that matches counts the attempt."""
        with self._lock:
            chosen = None
            for f in self._faults:
                if f['caller'] not in ('*', row['caller']) or f['op'] not in ('*', row['op']):
                    continue
                if f['upload_id'] not in ('*', row['upload_id']):
                    continue
                if f['part_number'] is not None and f['part_number'] != row['part_number']:
                    continue
                if not f['key'].search(row['key']):
                    continue
                f['seen'] += 1
                if chosen is None and (f['nth'] == '*' or f['seen'] == f['nth']):
                    chosen = dict(f)
            return chosen


def _recorded(headers: dict) -> dict:
    return {k: v for k, v in headers.items()
            if k in RECORDED_HEADERS or k.startswith('x-amz-') or k.startswith('if-')}


def _error_xml(code: str, message: str) -> bytes:
    return f'<Error><Code>{code}</Code><Message>{message}</Message></Error>'.encode()


class _Handler(socketserver.BaseRequestHandler):
    def setup(self):
        self.proxy: LedgerProxy = self.server.proxy
        self.caller: str = self.server.caller
        self.sock: socket.socket = self.request
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.cf = self.sock.makefile('rb', 65536)
        self.up = None
        self.proxy.track(self.sock, True)

    def finish(self):
        self.proxy.track(self.sock, False)
        self.up_close()
        for closer in (self.cf.close, self.sock.close):
            try:
                closer()
            except OSError:
                pass

    def handle(self):
        while self.serve_one():
            pass

    # --- I/O wrappers that name the failing side ---

    def c_send(self, data: bytes):
        try:
            self.sock.sendall(data)
        except OSError as e:
            raise ClientGone(str(e)) from None

    def u_send(self, data: bytes):
        try:
            self.up[0].sendall(data)
        except OSError as e:
            raise UpstreamGone(str(e)) from None

    def up_close(self):
        if self.up:
            for closer in (self.up[1].close, self.up[0].close):
                try:
                    closer()
                except OSError:
                    pass
            self.up = None

    def up_connect(self):
        if self.up and select.select([self.up[0]], [], [], 0)[0]:
            self.up_close()
        if self.up:
            return
        try:
            sock = socket.create_connection(self.proxy.upstream, CONNECT_TIMEOUT)
        except OSError as e:
            raise UpstreamGone(f'connect: {e}') from None
        sock.settimeout(IO_TIMEOUT)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.up = (sock, sock.makefile('rb', 65536))

    # --- one request ---

    def serve_one(self) -> bool:
        self.sock.settimeout(IDLE_TIMEOUT)
        try:
            line = self.cf.readline(MAX_LINE)
            while line in (b'\r\n', b'\n'):
                line = self.cf.readline(MAX_LINE)
        except (OSError, ValueError):
            return False
        if not line:
            return False
        self.sock.settimeout(IO_TIMEOUT)
        try:
            lines, head_bytes = read_head(self.cf, ClientGone)
        except ClientGone:
            return False
        parts = line.decode('latin-1').split()
        if len(parts) != 3 or not parts[2].startswith('HTTP/1.'):
            self.sock.sendall(b'HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n')
            return False
        attempt = self.proxy.begin(self.caller)
        keep = False
        try:
            keep = self.process(attempt, line, lines, len(line) + head_bytes, parts)
        except ClientGone:
            self.up_close()
        except UpstreamGone:
            self.up_close()
            self.answer_upstream_failure(attempt)
        except Exception:  # noqa: BLE001 - the row must still be written
            traceback.print_exc()
            self.up_close()
        finally:
            self.proxy.finish(attempt)
        return keep

    def answer_upstream_failure(self, attempt: Attempt):
        row = attempt.row
        if attempt.relayed:
            return
        body = _error_xml('BadGateway', 'upstream failure')
        try:
            self.c_send(b'HTTP/1.1 502 Bad Gateway\r\nContent-Type: application/xml\r\nContent-Length: %d\r\n'
                        b'Connection: close\r\n\r\n%s' % (len(body), body))
            row['downstream'] = {'outcome': 'delivered', 'status': 502}
        except ClientGone:
            pass

    def process(self, a: Attempt, first: bytes, lines: list, head_bytes: int, parts: list) -> bool:
        row = a.row
        method, target, version = parts
        headers = parse_headers(lines)
        raw_path, _, raw_query = target.partition('?')
        query = dict(urllib.parse.parse_qsl(raw_query, keep_blank_values=True))
        path = urllib.parse.unquote(raw_path)
        bucket, _, key = path.lstrip('/').partition('/')
        row.update(method=method, path=raw_path, query=query, req_headers=_recorded(headers), bucket=bucket,
                   key=key, op=parse_operation(method, raw_path, query, headers),
                   upload_id=query.get('uploadId'),
                   part_number=int(query['partNumber']) if query.get('partNumber', '').isdigit() else None,
                   req_wire_bytes=head_bytes)
        source = headers.get('x-amz-copy-source')
        if source:
            src_bucket, _, src_key = urllib.parse.unquote(source.partition('?')[0]).lstrip('/').partition('/')
            row['copy_source'] = {'bucket': src_bucket, 'key': src_key,
                                  'range': headers.get('x-amz-copy-source-range')}
        conn = _tokens(headers, 'connection')
        keep = 'close' not in conn and (version == 'HTTP/1.1' or 'keep-alive' in conn)
        expect = headers.get('expect', '').lower() == '100-continue'

        if 'chunked' in headers.get('transfer-encoding', '').lower():
            return self.reject_chunked(a, keep and not expect)
        try:
            length = int(headers.get('content-length', '0'))
            if length < 0 or ',' in headers.get('content-length', ''):
                raise ValueError
        except ValueError:
            self.reject(a, 400, 'InvalidRequest', 'bad Content-Length')
            return False

        if expect and length > 0:
            self.c_send(b'HTTP/1.1 100 Continue\r\n\r\n')
            row['expect_negotiated'] = True
        fault = self.proxy.match_fault(row)
        if fault:
            row['injected_rule'] = fault['id']
        capture_request = row['op'] in ('CompleteMultipartUpload', 'DeleteObjects') and length <= REQUEST_CAPTURE_MAX
        a.request_body = bytearray() if capture_request else None

        if fault and fault['action'] == 'status' and not fault['send_upstream']:
            self.pump_request(a, length, send=False)
            row['upstream'] = {'outcome': 'not_sent', 'http_status': None, 's3_code': None}
            self.parse_request_body(a)
            return self.inject(a, fault['status'], keep)

        head = b''.join([first] + [ln for ln in lines
                                    if not (expect and ln.split(b':', 1)[0].strip().lower() == b'expect')]
                        + [b'\r\n'])
        try:
            self.up_connect()
        except UpstreamGone:
            self.pump_request(a, length, send=False)
            row['upstream'] = {'outcome': 'not_sent', 'http_status': None, 's3_code': None}
            raise
        self.u_send(head)
        self.pump_request(a, length, send=True)
        self.parse_request_body(a)

        discard = bool(fault)
        reusable = self.read_response(a, method, relay=not discard)
        if fault and fault['action'] == 'drop_response':
            row['downstream'] = {'outcome': 'dropped', 'status': None}
            return False
        if fault:
            return self.inject(a, fault['status'], keep and reusable)
        if not reusable:
            self.up_close()
        resp_conn = _tokens(a.resp_headers, 'connection')
        return keep and reusable and 'close' not in resp_conn

    def reject(self, a: Attempt, status: int, code: str, message: str, close: bool = True):
        body = _error_xml(code, message)
        self.proxy.finishing(a)
        self.c_send(b'HTTP/1.1 %d %s\r\nContent-Type: application/xml\r\nContent-Length: %d\r\n%s\r\n%s' % (
            status, http.HTTPStatus(status).phrase.encode(), len(body), b'Connection: close\r\n' if close else b'',
            body))
        a.row['downstream'] = {'outcome': 'rejected', 'status': status}
        a.row['upstream'] = {'outcome': 'not_sent', 'http_status': None, 's3_code': None}

    def reject_chunked(self, a: Attempt, keep: bool) -> bool:
        a.row['op'] = 'Unsupported'
        if keep:
            # Drain the chunked body so the connection stays in sync for the next request.
            _, wire = relay_chunked(self.cf, ClientGone)
            a.row['req_wire_bytes'] += wire
        self.reject(a, 411, 'MissingContentLength', 'chunked request bodies are not supported', close=not keep)
        return keep

    def inject(self, a: Attempt, status: int, keep: bool) -> bool:
        body = _error_xml('InternalError', 'injected by the ledger proxy')
        phrase = http.HTTPStatus(status).phrase if status in http.HTTPStatus._value2member_map_ else 'Injected'
        self.proxy.finishing(a)
        self.c_send(b'HTTP/1.1 %d %s\r\nContent-Type: application/xml\r\nContent-Length: %d\r\n%s\r\n' % (
            status, phrase.encode(), len(body), b'' if keep else b'Connection: close\r\n')
            + (b'' if a.row['method'] == 'HEAD' else body))
        a.row['downstream'] = {'outcome': 'injected', 'status': status}
        return keep

    def pump_request(self, a: Attempt, length: int, send: bool):
        """Stream `length` body bytes from the client, hashing and counting. With `send`, forward them to the
        store; a store that stops reading (early error reply) does not stop the drain."""
        row = a.row
        hasher = hashlib.sha256()
        remaining = length
        forwarding = send
        while remaining:
            data = _read1(self.cf, min(remaining, CHUNK), ClientGone)
            remaining -= len(data)
            hasher.update(data)
            row['req_payload_bytes'] += len(data)
            row['req_wire_bytes'] += len(data)
            if a.request_body is not None:
                a.request_body.extend(data)
            if forwarding:
                try:
                    self.u_send(data)
                except UpstreamGone:
                    forwarding = False
        if length:
            row['payload_sha256'] = hasher.hexdigest()

    def parse_request_body(self, a: Attempt):
        row = a.row
        if a.request_body is None:
            return
        if row['op'] == 'CompleteMultipartUpload':
            row['response_body']['completed_parts'] = parse_completed_parts(bytes(a.request_body))

    # --- response ---

    def read_response(self, a: Attempt, method: str, relay: bool) -> bool:
        """Read the store's response, relaying it to the client when `relay`. Return whether the upstream
        connection can be reused."""
        row = a.row
        usock, uf = self.up
        while True:
            status_line = _readline(uf, UpstreamGone)
            lines, head_bytes = read_head(uf, UpstreamGone)
            fields = status_line.decode('latin-1').split(None, 2)
            if len(fields) < 2 or not fields[1].isdigit():
                raise UpstreamGone('bad status line')
            status = int(fields[1])
            if not (100 <= status < 200 and status != 101):
                break
        headers = parse_headers(lines)
        a.resp_headers = headers
        row['resp_headers'] = _recorded(headers)
        row['resp_wire_bytes'] = len(status_line) + head_bytes
        row['upstream'] = {'outcome': 'unknown', 'http_status': status, 's3_code': None}
        if 'etag' in headers:
            row['response_body']['etag'] = _etag(headers['etag'])
        ctype = headers.get('content-type', '').lower()
        op = row['op']
        if status >= 300 and status != 304:
            limit = ERROR_CAPTURE_MAX
        elif (op == 'CompleteMultipartUpload' or op in ('CreateMultipartUpload', 'DeleteObjects', 'ListObjectsV2')) \
                and 'xml' in ctype:
            limit = LISTING_CAPTURE_MAX
        else:
            limit = 0
        if method == 'HEAD':
            limit = 0
        capture = bytearray()
        overflow = False

        def on_data(data):
            nonlocal overflow
            if limit and not overflow:
                if len(capture) + len(data) > limit:
                    overflow = True
                else:
                    capture.extend(data)

        head_raw = status_line + b''.join(lines) + b'\r\n'
        no_body = method == 'HEAD' or status in (204, 304) or 100 <= status < 200
        chunked = 'chunked' in headers.get('transfer-encoding', '').lower()
        empty = no_body or (not chunked and headers.get('content-length') == '0')
        if relay:
            if empty:
                self.proxy.finishing(a)
            self.c_send(head_raw)
            a.relayed = True
        reusable = 'close' not in _tokens(headers, 'connection') and fields[0] == 'HTTP/1.1'

        def raw_cb(piece):
            if relay:
                self.c_send(piece)

        if no_body:
            pass
        elif chunked:
            payload, wire = relay_chunked(uf, UpstreamGone, raw_cb, on_data,
                                          (lambda: self.proxy.finishing(a)) if relay else None)
            row['resp_payload_bytes'] = payload
            row['resp_wire_bytes'] += wire
        elif 'content-length' in headers:
            remaining = int(headers['content-length'])
            while remaining:
                data = _read1(uf, min(remaining, CHUNK), UpstreamGone)
                remaining -= len(data)
                row['resp_payload_bytes'] += len(data)
                row['resp_wire_bytes'] += len(data)
                on_data(data)
                if relay and not remaining:
                    self.proxy.finishing(a)
                raw_cb(data)
        else:
            reusable = False
            while True:
                try:
                    data = uf.read1(CHUNK)
                except (OSError, ValueError) as e:
                    raise UpstreamGone(str(e)) from None
                if not data:
                    break
                row['resp_payload_bytes'] += len(data)
                row['resp_wire_bytes'] += len(data)
                on_data(data)
                raw_cb(data)
        a.response_body = None if overflow or not limit else bytes(capture)
        row['downstream'] = {'outcome': 'delivered', 'status': status} if relay else row['downstream']
        self.record_response(a, status)
        return reusable

    def record_response(self, a: Attempt, status: int):
        row = a.row
        body = a.response_body
        op = row['op']
        code = error_code(body) if body is not None and status >= 300 else None
        outcome = 'ok' if status < 400 else 'error'
        if body is not None and status < 300:
            if op == 'CompleteMultipartUpload':
                code = error_code(body)
                if code is not None:
                    outcome = 'error'
                    row['response_body']['complete_error'] = code
            elif op == 'CreateMultipartUpload':
                upload = parse_upload_id(body)
                row['response_body']['upload_id'] = upload
                if upload and not row['upload_id']:
                    row['upload_id'] = upload
            elif op == 'DeleteObjects':
                row['response_body']['delete_results'] = parse_delete_results(bytes(a.request_body or b''), body)
            elif op == 'ListObjectsV2':
                row['response_body']['list'] = parse_listing(body)
        row['upstream'] = {'outcome': outcome, 'http_status': status, 's3_code': code}


def main(argv=None):
    parser = argparse.ArgumentParser(description='Ledger proxy: one JSONL row per S3 attempt.')
    parser.add_argument('--upstream', required=True, help='HOST:PORT of the store')
    parser.add_argument('--store-id', required=True)
    parser.add_argument('--ledger', required=True)
    parser.add_argument('--caller', action='append', required=True, metavar='NAME=PORT')
    args = parser.parse_args(argv)
    host, _, port = args.upstream.rpartition(':')
    callers = {name: int(p) for name, _, p in (c.partition('=') for c in args.caller)}
    proxy = LedgerProxy((host, int(port)), args.store_id, args.ledger, callers)
    proxy.start()
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    print('ready ' + ' '.join(f'{k}={v}' for k, v in proxy.ports.items()), flush=True)
    stop.wait()
    proxy.stop()


if __name__ == '__main__':
    main()
