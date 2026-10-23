import contextlib
import hashlib
import http.client
import io
import json
import pathlib
import socket
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from yardstick.cost.proxy import LedgerProxy, parse_operation

TOOLS = pathlib.Path(__file__).resolve().parents[3] / 'utils' / 'storage-cost-calculator' / 'tools'
sys.path.insert(0, str(TOOLS))
from artifacts import validate_ledger_row  # noqa: E402

XML = {'Content-Type': 'application/xml'}


class FakeUpstream:
    """Records every request it receives and answers through `respond(handler, request)`."""

    def __init__(self):
        self.requests = []
        self.respond = lambda h, req: reply(h, 200, b'', {'ETag': '"etag0"'})
        outer = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'

            def log_message(self, *args):
                pass

            def serve(self):
                length = int(self.headers.get('Content-Length', 0))
                body = self.rfile.read(length)
                req = {'method': self.command, 'target': self.path, 'headers': list(self.headers.items()),
                       'body': body}
                outer.requests.append(req)
                if len(body) != length:  # the proxy cut the request short: nobody is left to answer
                    self.close_connection = True
                    return
                outer.respond(self, req)

            do_GET = do_PUT = do_POST = do_DELETE = do_HEAD = serve

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.server.daemon_threads = True
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, kwargs={'poll_interval': 0.05}, daemon=True).start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()


def reply(h, status, body=b'', headers=None):
    h.send_response(status)
    for k, v in (headers or {}).items():
        h.send_header(k, v)
    if 'Content-Length' not in (headers or {}):
        h.send_header('Content-Length', str(len(body)))
    h.end_headers()
    if h.command != 'HEAD':
        h.wfile.write(body)


def read_until(sock, marker, limit=1 << 20):
    data = b''
    while marker not in data:
        piece = sock.recv(65536)
        if not piece:
            break
        data += piece
        assert len(data) < limit
    return data


def read_response(sock):
    """Read one Content-Length response from a raw socket; return (head, body)."""
    data = read_until(sock, b'\r\n\r\n')
    head, _, rest = data.partition(b'\r\n\r\n')
    length = 0
    for line in head.split(b'\r\n')[1:]:
        name, _, value = line.partition(b':')
        if name.strip().lower() == b'content-length':
            length = int(value)
    while len(rest) < length:
        piece = sock.recv(65536)
        assert piece
        rest += piece
    return head, rest


class ProxyBase(unittest.TestCase):
    def setUp(self):
        self.up = FakeUpstream()
        self.addCleanup(self.up.close)
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.ledger = pathlib.Path(tmp.name) / 'ledger.jsonl'
        self.proxy = LedgerProxy(('127.0.0.1', self.up.port), 'rustfs-19121', self.ledger, {'r1': 0, 'rig': 0})
        self.proxy.start()
        self.addCleanup(self.proxy.stop)
        self.port = self.proxy.ports['rig']

    def rows(self):
        self.proxy.flush()
        return [json.loads(line) for line in self.ledger.read_text().splitlines()]

    def attempts(self):
        return sorted((r for r in self.rows() if r['kind'] == 'attempt'), key=lambda r: r['seq'])

    def wait_attempts(self, n, timeout=10):
        deadline = time.time() + timeout
        while time.time() < deadline:
            got = self.attempts()
            if len(got) >= n:
                return got
            time.sleep(0.02)
        self.fail(f'expected {n} attempts, got {len(self.attempts())}')

    def conn(self, caller='rig'):
        c = http.client.HTTPConnection('127.0.0.1', self.proxy.ports[caller], timeout=30)
        self.addCleanup(c.close)
        return c

    def raw(self):
        s = socket.create_connection(('127.0.0.1', self.port))
        s.settimeout(30)
        self.addCleanup(s.close)
        return s


class ParseOperationTest(unittest.TestCase):
    def test_rules(self):
        cases = [
            (('POST', '/b/k', {'uploads': ''}, {}), 'CreateMultipartUpload'),
            (('GET', '/b', {'uploads': '', 'prefix': 'x'}, {}), 'ListMultipartUploads'),
            (('PUT', '/b/k', {'uploadId': 'u', 'partNumber': '1'}, {}), 'UploadPart'),
            (('PUT', '/b/k', {'uploadId': 'u', 'partNumber': '1'}, {'X-Amz-Copy-Source': '/b/o'}), 'UploadPartCopy'),
            (('POST', '/b/k', {'uploadId': 'u'}, {}), 'CompleteMultipartUpload'),
            (('DELETE', '/b/k', {'uploadId': 'u'}, {}), 'AbortMultipartUpload'),
            (('GET', '/b/k', {'uploadId': 'u'}, {}), 'ListParts'),
            (('POST', '/b', {'delete': ''}, {}), 'DeleteObjects'),
            (('PUT', '/b/k', {}, {'x-amz-copy-source': '/b/o'}), 'CopyObject'),
            (('GET', '/b', {'list-type': '2'}, {}), 'ListObjectsV2'),
            (('GET', '/b/', {'list-type': '2'}, {}), 'ListObjectsV2'),
            (('HEAD', '/b', {}, {}), 'HeadBucket'),
            (('HEAD', '/b/k', {}, {}), 'HeadObject'),
            (('PUT', '/b', {}, {}), 'CreateBucket'),
            (('PUT', '/b/k', {}, {}), 'PutObject'),
            (('GET', '/b/k', {}, {}), 'GetObject'),
            (('DELETE', '/b/k', {}, {}), 'DeleteObject'),
            (('GET', '/b', {}, {}), 'Other'),
            (('PATCH', '/b/k', {}, {}), 'Other'),
        ]
        for args, op in cases:
            self.assertEqual(parse_operation(*args), op, args)


class ForwardingTest(ProxyBase):
    def test_forwards_put_preserving_request_line_host_query_and_signed_headers(self):
        c = self.conn()
        headers = {'Host': 'signed.example:9999', 'x-amz-date': '20261024T000000Z',
                   'x-amz-content-sha256': hashlib.sha256(b'hello').hexdigest(),
                   'Authorization': 'AWS4-HMAC-SHA256 Credential=a/b,SignedHeaders=host,Signature=00',
                   'Content-MD5': 'XUFAKrxLKna5cZ2REBfFkg=='}
        c.request('PUT', '/bkt/a%20b/c+d?z=1&a=%2F&uploads', b'hello', headers)
        r = c.getresponse()
        self.assertEqual((r.status, r.read(), r.getheader('ETag')), (200, b'', '"etag0"'))
        got = self.up.requests[0]
        self.assertEqual(got['target'], '/bkt/a%20b/c+d?z=1&a=%2F&uploads')
        self.assertEqual(got['body'], b'hello')
        names = dict(got['headers'])
        for k, v in headers.items():
            self.assertEqual(names[k], v)
        row = self.wait_attempts(1)[0]
        self.assertEqual((row['method'], row['bucket'], row['key'], row['caller'], row['store_id']),
                         ('PUT', 'bkt', 'a b/c+d', 'rig', 'rustfs-19121'))
        self.assertEqual(row['path'], '/bkt/a%20b/c+d')
        self.assertEqual(row['query'], {'z': '1', 'a': '/', 'uploads': ''})
        self.assertEqual(row['op'], 'PutObject')
        self.assertEqual(row['req_headers']['content-md5'], headers['Content-MD5'])
        self.assertNotIn('authorization', row['req_headers'])
        self.assertEqual(row['resp_headers']['etag'], '"etag0"')
        self.assertEqual(row['response_body']['etag'], 'etag0')
        self.assertEqual(row['phase'], 'outside')

    def test_caller_is_identified_by_listening_port(self):
        c = self.conn('r1')
        c.request('GET', '/b/k')
        c.getresponse().read()
        self.assertEqual(self.wait_attempts(1)[0]['caller'], 'r1')

    def test_rows_valid_per_artifacts_validator(self):
        up_id = 'UP1'

        def respond(h, req):
            if req['method'] == 'POST' and req['target'].endswith('?uploads'):
                reply(h, 200, f'<InitiateMultipartUploadResult><UploadId>{up_id}</UploadId></InitiateMultipartUploadResult>'.encode(), XML)
            elif req['method'] == 'GET' and 'list-type=2' in req['target']:
                reply(h, 200, b'<ListBucketResult><KeyCount>2</KeyCount><IsTruncated>true</IsTruncated></ListBucketResult>', XML)
            elif req['method'] == 'GET':
                reply(h, 404, b'<Error><Code>NoSuchKey</Code></Error>', XML)
            else:
                reply(h, 200, b'', {'ETag': '"e"'})
        self.up.respond = respond
        c = self.conn()
        for method, target, body, extra in [
                ('PUT', '/b/k', b'x' * 10, {}),
                ('PUT', '/b/k2', b'', {'x-amz-copy-source': '/b/k?versionId=1', 'x-amz-copy-source-range': 'bytes=0-4'}),
                ('POST', '/b/k?uploads', b'', {}),
                ('PUT', f'/b/k?partNumber=3&uploadId={up_id}', b'p', {}),
                ('GET', '/b?list-type=2&prefix=a', b'', {}),
                ('GET', '/b/missing', b'', {}),
                ('HEAD', '/b/k', b'', {}),
                ('DELETE', '/b/k', b'', {})]:
            c.request(method, target, body or None, extra)
            c.getresponse().read()
        self.proxy.mark('m1')
        rows = self.rows()
        self.assertEqual(len(rows), 9)
        for row in rows:
            self.assertEqual(validate_ledger_row(row), [], row)
        by_op = {r['op']: r for r in rows if r['kind'] == 'attempt'}
        self.assertEqual(by_op['CreateMultipartUpload']['upload_id'], up_id)
        self.assertEqual(by_op['CreateMultipartUpload']['response_body']['upload_id'], up_id)
        self.assertEqual((by_op['UploadPart']['upload_id'], by_op['UploadPart']['part_number']), (up_id, 3))
        self.assertEqual(by_op['CopyObject']['copy_source'], {'bucket': 'b', 'key': 'k', 'range': 'bytes=0-4'})
        self.assertEqual(by_op['ListObjectsV2']['response_body']['list'], {'key_count': 2, 'truncated': True})
        self.assertEqual(by_op['GetObject']['upstream'],
                         {'outcome': 'error', 'http_status': 404, 's3_code': 'NoSuchKey'})

    def test_head_no_body(self):
        self.up.respond = lambda h, req: reply(h, 200, b'', {'Content-Length': '123', 'ETag': '"h"'})
        c = self.conn()
        c.request('HEAD', '/b/k')
        r = c.getresponse()
        self.assertEqual((r.status, r.getheader('Content-Length'), r.read()), (200, '123', b''))
        sock = c.sock
        self.up.respond = lambda h, req: reply(h, 200, b'abc')
        c.request('GET', '/b/k')
        r = c.getresponse()
        self.assertEqual(r.read(), b'abc')
        self.assertIs(c.sock, sock)
        head, get = self.wait_attempts(2)
        self.assertEqual((head['op'], head['resp_payload_bytes'], head['resp_headers']['content-length']),
                         ('HeadObject', 0, '123'))
        self.assertEqual((get['op'], get['resp_payload_bytes']), ('GetObject', 3))

    def test_payload_sha256_and_wire_bytes(self):
        body = bytes(range(256)) * 400
        self.up.respond = lambda h, req: reply(h, 200, b'0123456789', {'ETag': '"p"'})
        head = (b'PUT /b/k HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: %d\r\n\r\n' % len(body))
        s = self.raw()
        s.sendall(head + body)
        raw_head, raw_body = read_response(s)
        s.close()
        self.assertEqual(raw_body, b'0123456789')
        row = self.wait_attempts(1)[0]
        self.assertEqual(row['payload_sha256'], hashlib.sha256(body).hexdigest())
        self.assertEqual(row['req_payload_bytes'], len(body))
        self.assertEqual(row['req_wire_bytes'], len(head) + len(body))
        self.assertEqual(row['resp_payload_bytes'], 10)
        self.assertEqual(row['resp_wire_bytes'], len(raw_head) + 4 + 10)

    def test_no_body_has_null_hash(self):
        c = self.conn()
        c.request('GET', '/b/k')
        c.getresponse().read()
        self.assertIsNone(self.wait_attempts(1)[0]['payload_sha256'])


class FramingTest(ProxyBase):
    def test_expect_100_continue_answered_by_proxy_and_stripped_upstream(self):
        s = self.raw()
        s.sendall(b'PUT /b/k HTTP/1.1\r\nHost: h\r\nExpect: 100-continue\r\nContent-Length: 5\r\n\r\n')
        interim = read_until(s, b'\r\n\r\n')
        self.assertEqual(interim, b'HTTP/1.1 100 Continue\r\n\r\n')
        self.assertEqual(self.up.requests, [])
        s.sendall(b'hello')
        head, _ = read_response(s)
        s.close()
        self.assertTrue(head.startswith(b'HTTP/1.1 200'))
        self.assertNotIn('expect', [k.lower() for k, _ in self.up.requests[0]['headers']])
        self.assertEqual(self.up.requests[0]['body'], b'hello')
        row = self.wait_attempts(1)[0]
        self.assertTrue(row['expect_negotiated'])
        self.assertEqual(row['req_headers']['expect'], '100-continue')
        self.assertEqual(validate_ledger_row(row), [])

    def test_no_expect_means_not_negotiated(self):
        c = self.conn()
        c.request('PUT', '/b/k', b'abc')
        c.getresponse().read()
        self.assertFalse(self.wait_attempts(1)[0]['expect_negotiated'])

    def test_chunked_request_rejected_411_recorded(self):
        s = self.raw()
        s.sendall(b'PUT /b/k HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n')
        head, body = read_response(s)
        s.close()
        self.assertTrue(head.startswith(b'HTTP/1.1 411'), head)
        self.assertIn(b'<Code>MissingContentLength</Code>', body)
        self.assertEqual(self.up.requests, [])
        row = self.wait_attempts(1)[0]
        self.assertEqual(row['op'], 'Unsupported')
        self.assertEqual(row['downstream'], {'outcome': 'rejected', 'status': 411})
        self.assertEqual(row['upstream'], {'outcome': 'not_sent', 'http_status': None, 's3_code': None})
        self.assertEqual(row['req_headers']['transfer-encoding'], 'chunked')
        self.assertEqual(validate_ledger_row(row), [])

    def test_chunked_upstream_response_rechunked_streaming(self):
        finish = threading.Event()

        def respond(h, req):
            h.send_response(200)
            h.send_header('Transfer-Encoding', 'chunked')
            h.end_headers()
            for piece in (b'aaaa', b'bbbbbb', b'cc'):
                h.wfile.write(b'%x\r\n%s\r\n' % (len(piece), piece))
                h.wfile.flush()
                time.sleep(0.1)
            finish.wait(30)
            h.wfile.write(b'0\r\n\r\n')
            h.wfile.flush()
        self.up.respond = respond
        s = self.raw()
        s.sendall(b'GET /b/k HTTP/1.1\r\nHost: h\r\n\r\n')
        data = b''
        while not (b'aaaa' in data and b'bbbbbb' in data and b'cc\r\n' in data):
            piece = s.recv(65536)
            self.assertTrue(piece, 'connection closed before the three chunks arrived')
            data += piece
        self.assertFalse(finish.is_set())
        self.assertIn(b'Transfer-Encoding: chunked', data)
        self.assertNotIn(b'0\r\n\r\n', data.partition(b'\r\n\r\n')[2])
        finish.set()
        while not data.endswith(b'0\r\n\r\n'):
            data += s.recv(65536)
        s.close()
        row = self.wait_attempts(1)[0]
        self.assertEqual(row['resp_payload_bytes'], 12)
        self.assertEqual(row['downstream'], {'outcome': 'delivered', 'status': 200})
        self.assertEqual(row['resp_headers']['transfer-encoding'], 'chunked')
        self.assertEqual(row['resp_wire_bytes'], len(data))

    def test_keepalive_after_rejected_and_injected_requests(self):
        c = self.conn()
        c.request('PUT', '/b/k', iter([b'hello', b'world']), {'Transfer-Encoding': 'chunked'}, encode_chunked=True)
        r = c.getresponse()
        self.assertEqual(r.status, 411)
        r.read()
        sock = c.sock
        self.assertIsNotNone(sock)
        c.request('GET', '/b/ok')
        r = c.getresponse()
        self.assertEqual(r.status, 200)
        r.read()
        self.assertIs(c.sock, sock)
        self.proxy.set_fault({'op': 'PutObject', 'action': 'status', 'status': 503, 'send_upstream': False})
        c.request('PUT', '/b/k', b'z' * 100000)
        r = c.getresponse()
        self.assertEqual(r.status, 503)
        r.read()
        self.assertIs(c.sock, sock)
        c.request('GET', '/b/ok')
        r = c.getresponse()
        self.assertEqual(r.status, 200)
        r.read()
        self.assertIs(c.sock, sock)
        self.assertEqual(len(self.up.requests), 2)

    def test_client_disconnect_marks_unknown(self):
        s = self.raw()
        s.sendall(b'PUT /b/k HTTP/1.1\r\nHost: h\r\nContent-Length: 1000\r\n\r\n0123456789')
        time.sleep(0.2)
        s.close()
        row = self.wait_attempts(1)[0]
        self.assertEqual(row['downstream'], {'outcome': 'dropped', 'status': None})
        self.assertEqual(row['upstream'], {'outcome': 'unknown', 'http_status': None, 's3_code': None})
        self.assertEqual(row['req_payload_bytes'], 10)
        self.assertIsInstance(row['t_end_us'], int)
        self.assertEqual(validate_ledger_row(row), [])
        # The proxy dropped its upstream connection: the fake saw a truncated body, not a full request.
        deadline = time.time() + 5
        while not self.up.requests and time.time() < deadline:
            time.sleep(0.02)
        self.assertEqual(len(self.up.requests[0]['body']), 10)


class FaultTest(ProxyBase):
    def test_fault_status_not_sent_drains_body(self):
        rule = self.proxy.set_fault({'op': 'PutObject', 'action': 'status', 'status': 503, 'send_upstream': False})
        self.assertIsInstance(rule, str)
        body = b'q' * (3 << 20)
        c = self.conn()
        c.request('PUT', '/b/k', body)
        r = c.getresponse()
        data = r.read()
        self.assertEqual(r.status, 503)
        self.assertIn(b'<Code>InternalError</Code>', data)
        self.assertEqual(self.up.requests, [])
        c.request('PUT', '/b/k', body)
        r = c.getresponse()
        r.read()
        self.assertEqual(r.status, 200)
        first, second = self.wait_attempts(2)
        self.assertEqual(first['downstream'], {'outcome': 'injected', 'status': 503})
        self.assertEqual(first['upstream'], {'outcome': 'not_sent', 'http_status': None, 's3_code': None})
        self.assertEqual(first['injected_rule'], rule)
        self.assertEqual(first['req_payload_bytes'], len(body))
        self.assertEqual(first['payload_sha256'], hashlib.sha256(body).hexdigest())
        self.assertIsNone(second['injected_rule'])
        self.assertEqual(second['downstream'], {'outcome': 'delivered', 'status': 200})
        for row in (first, second):
            self.assertEqual(validate_ledger_row(row), [])

    def test_fault_status_with_send_upstream_forwards_then_replaces_response(self):
        self.up.respond = lambda h, req: reply(h, 200, b'', {'ETag': '"real"'})
        self.proxy.set_fault({'op': 'PutObject', 'action': 'status', 'status': 500, 'send_upstream': True})
        c = self.conn()
        c.request('PUT', '/b/k', b'abc')
        r = c.getresponse()
        r.read()
        self.assertEqual(r.status, 500)
        self.assertEqual(len(self.up.requests), 1)
        row = self.wait_attempts(1)[0]
        self.assertEqual(row['downstream'], {'outcome': 'injected', 'status': 500})
        self.assertEqual(row['upstream'], {'outcome': 'ok', 'http_status': 200, 's3_code': None})

    def test_fault_drop_response_upstream_ok_downstream_dropped(self):
        rule = self.proxy.set_fault({'op': 'PutObject', 'action': 'drop_response', 'send_upstream': True})
        c = self.conn()
        with self.assertRaises((http.client.RemoteDisconnected, ConnectionError)):
            c.request('PUT', '/b/k', b'abc')
            c.getresponse()
        self.assertEqual(len(self.up.requests), 1)
        row = self.wait_attempts(1)[0]
        self.assertEqual(row['downstream'], {'outcome': 'dropped', 'status': None})
        self.assertEqual(row['upstream'], {'outcome': 'ok', 'http_status': 200, 's3_code': None})
        self.assertEqual(row['injected_rule'], rule)
        self.assertEqual(validate_ledger_row(row), [])
        with self.assertRaises(ValueError):
            self.proxy.set_fault({'action': 'drop_response', 'send_upstream': False})

    def put_part(self, c, upload, part, caller_conn=None):
        c.request('PUT', f'/b/k?partNumber={part}&uploadId={upload}', b'data')
        r = c.getresponse()
        r.read()
        return r.status

    def test_fault_targets_upload_and_part(self):
        self.proxy.set_fault({'op': 'UploadPart', 'upload_id': 'U1', 'part_number': 2, 'nth': 1,
                              'action': 'status', 'status': 500, 'send_upstream': False})
        c = self.conn()
        self.assertEqual(self.put_part(c, 'U1', 1), 200)
        self.assertEqual(self.put_part(c, 'U2', 2), 200)
        self.assertEqual(self.put_part(c, 'U1', 2), 500)
        self.assertEqual(self.put_part(c, 'U1', 2), 200)
        rows = self.wait_attempts(4)
        retries = [(r['upload_id'], r['part_number'], r['downstream']['status']) for r in rows
                   if (r['upload_id'], r['part_number']) == ('U1', 2)]
        self.assertEqual(retries, [('U1', 2, 500), ('U1', 2, 200)])

    def test_fault_nth_star_caller_key_regex_and_clear(self):
        self.proxy.set_fault({'caller': 'r1', 'op': 'GetObject', 'key_regex': r'^hot/', 'nth': '*',
                              'action': 'status', 'status': 500, 'send_upstream': False})
        rig, r1 = self.conn('rig'), self.conn('r1')

        def get(c, key):
            c.request('GET', '/b/' + key)
            r = c.getresponse()
            r.read()
            return r.status
        self.assertEqual([get(rig, 'hot/a'), get(r1, 'cold/a'), get(r1, 'hot/a'), get(r1, 'hot/b')],
                         [200, 200, 500, 500])
        self.proxy.clear_faults()
        self.assertEqual(get(r1, 'hot/a'), 200)

    def test_fault_nth_counts_only_matching_attempts(self):
        self.proxy.set_fault({'op': 'GetObject', 'nth': 2, 'action': 'status', 'status': 500,
                              'send_upstream': False})
        c = self.conn()
        codes = []
        for key in ('a', 'b', 'c'):
            c.request('GET', '/b/' + key)
            r = c.getresponse()
            r.read()
            codes.append(r.status)
        self.assertEqual(codes, [200, 500, 200])


class ParsingTest(ProxyBase):
    def test_complete_200_with_error_parsed_and_get_payload_never_parsed(self):
        err = b'<Error><Code>InternalError</Code><Message>m</Message></Error>'

        def respond(h, req):
            if req['method'] == 'POST':
                reply(h, 200, err, XML)
            else:
                reply(h, 200, err, XML)
        self.up.respond = respond
        c = self.conn()
        c.request('POST', '/b/k?uploadId=U9', b'<CompleteMultipartUpload></CompleteMultipartUpload>')
        self.assertEqual(c.getresponse().read(), err)
        c.request('GET', '/b/k')
        self.assertEqual(c.getresponse().read(), err)
        complete, get = self.wait_attempts(2)
        self.assertEqual(complete['op'], 'CompleteMultipartUpload')
        self.assertEqual(complete['response_body']['complete_error'], 'InternalError')
        self.assertEqual(complete['upstream'], {'outcome': 'error', 'http_status': 200, 's3_code': 'InternalError'})
        self.assertEqual(complete['upload_id'], 'U9')
        self.assertEqual(get['op'], 'GetObject')
        self.assertIsNone(get['response_body']['complete_error'])
        self.assertEqual(get['upstream'], {'outcome': 'ok', 'http_status': 200, 's3_code': None})
        self.assertEqual(get['resp_payload_bytes'], len(err))

    def test_complete_200_success_has_no_error(self):
        ok = b'<CompleteMultipartUploadResult><ETag>"x"</ETag></CompleteMultipartUploadResult>'
        self.up.respond = lambda h, req: reply(h, 200, ok, XML)
        c = self.conn()
        c.request('POST', '/b/k?uploadId=U9', b'<CompleteMultipartUpload/>')
        c.getresponse().read()
        row = self.wait_attempts(1)[0]
        self.assertIsNone(row['response_body']['complete_error'])
        self.assertEqual(row['upstream']['outcome'], 'ok')

    def test_delete_objects_verbose_and_quiet_parsed(self):
        verbose_resp = (b'<DeleteResult><Deleted><Key>a</Key></Deleted>'
                        b'<Error><Key>b</Key><Code>AccessDenied</Code></Error></DeleteResult>')
        quiet_resp = b'<DeleteResult><Error><Key>b</Key><Code>AccessDenied</Code></Error></DeleteResult>'
        responses = [verbose_resp, quiet_resp]
        self.up.respond = lambda h, req: reply(h, 200, responses.pop(0), XML)
        c = self.conn()
        c.request('POST', '/b?delete', b'<Delete><Object><Key>a</Key></Object><Object><Key>b</Key></Object></Delete>')
        c.getresponse().read()
        c.request('POST', '/b?delete', b'<Delete><Quiet>true</Quiet><Object><Key>a</Key></Object>'
                                       b'<Object><Key>b</Key></Object><Object><Key>c &amp; d</Key></Object></Delete>')
        c.getresponse().read()
        verbose, quiet = self.wait_attempts(2)
        self.assertEqual(verbose['op'], 'DeleteObjects')
        self.assertEqual(verbose['response_body']['delete_results'],
                         [{'key': 'a', 'deleted': True, 'code': None},
                          {'key': 'b', 'deleted': False, 'code': 'AccessDenied'}])
        self.assertEqual(quiet['response_body']['delete_results'],
                         [{'key': 'a', 'deleted': True, 'code': None},
                          {'key': 'b', 'deleted': False, 'code': 'AccessDenied'},
                          {'key': 'c & d', 'deleted': True, 'code': None}])
        for row in (verbose, quiet):
            self.assertEqual(validate_ledger_row(row), [])

    def test_completed_parts_parsed(self):
        self.up.respond = lambda h, req: reply(h, 200, b'<CompleteMultipartUploadResult/>', XML)
        body = (b'<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                b'<Part><PartNumber>1</PartNumber><ETag>&quot;e1&quot;</ETag></Part>'
                b'<Part><PartNumber>2</PartNumber><ETag>"e2"</ETag></Part></CompleteMultipartUpload>')
        c = self.conn()
        c.request('POST', '/b/k?uploadId=U', body)
        c.getresponse().read()
        row = self.wait_attempts(1)[0]
        self.assertEqual(row['response_body']['completed_parts'],
                         [{'part_number': 1, 'etag': 'e1'}, {'part_number': 2, 'etag': 'e2'}])
        self.assertEqual(validate_ledger_row(row), [])

    def test_error_body_parsed_for_non_2xx(self):
        self.up.respond = lambda h, req: reply(h, 404, b'<Error><Code>NoSuchUpload</Code></Error>', XML)
        c = self.conn()
        c.request('GET', '/b/k?uploadId=U')
        c.getresponse().read()
        row = self.wait_attempts(1)[0]
        self.assertEqual(row['op'], 'ListParts')
        self.assertEqual(row['upstream'], {'outcome': 'error', 'http_status': 404, 's3_code': 'NoSuchUpload'})


class RawUpstream:
    """Answers every connection with fixed bytes after reading the request head, then closes."""

    def __init__(self, reply_bytes: bytes):
        self.reply_bytes = reply_bytes
        self.listener = socket.create_server(('127.0.0.1', 0))
        self.port = self.listener.getsockname()[1]
        threading.Thread(target=self.accept, daemon=True).start()

    def accept(self):
        while True:
            try:
                conn, _ = self.listener.accept()
            except OSError:
                return
            threading.Thread(target=self.serve, args=(conn,), daemon=True).start()

    def serve(self, conn):
        with conn:
            read_until(conn, b'\r\n\r\n')
            conn.sendall(self.reply_bytes)

    def close(self):
        self.listener.close()


class BrokenUpstreamTest(ProxyBase):
    def point_at(self, reply_bytes):
        raw = RawUpstream(reply_bytes)
        self.addCleanup(raw.close)
        self.proxy.upstream = ('127.0.0.1', raw.port)

    def get_through_proxy(self):
        """Return (status, body or None on truncation, stderr text) of one GET."""
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            c = self.conn()
            c.request('GET', '/b/k')
            try:
                r = c.getresponse()
                result = (r.status, r.read())
            except (http.client.IncompleteRead, http.client.RemoteDisconnected, ConnectionError):
                result = (None, None)
            rows = self.rows()
        return result, err.getvalue(), rows[-1]

    def test_connect_refused_answers_502_and_row_present_right_after_flush(self):
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', 0))
            dead = probe.getsockname()[1]
        self.proxy.upstream = ('127.0.0.1', dead)
        (status, _), stderr, row = self.get_through_proxy()
        self.assertEqual(status, 502)
        self.assertEqual(stderr, '')
        self.assertEqual(row['upstream'], {'outcome': 'error', 'http_status': None, 's3_code': None})
        self.assertEqual(row['downstream'], {'outcome': 'delivered', 'status': 502})
        self.assertTrue(row['error'].startswith('upstream: connect'), row['error'])
        self.assertEqual(validate_ledger_row(row), [])

    def test_flush_right_after_502_always_sees_the_row(self):
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', 0))
            self.proxy.upstream = ('127.0.0.1', probe.getsockname()[1])
        c = self.conn()
        for i in range(300):
            c.request('GET', '/b/k')
            r = c.getresponse()
            r.read()
            self.assertEqual(r.status, 502)
            self.assertEqual(len(self.attempts()), i + 1)
            c.close()

    def test_upstream_closes_mid_body(self):
        self.point_at(b'HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n0123456789')
        (status, body), stderr, _ = self.get_through_proxy()
        self.assertIsNone(body)
        self.assertEqual(stderr, '')
        row = self.wait_attempts(1)[0]
        self.assertEqual(row['upstream'], {'outcome': 'unknown', 'http_status': 200, 's3_code': None})
        self.assertEqual(row['downstream'], {'outcome': 'dropped', 'status': None})
        self.assertIn('closed', row['error'])
        self.assertEqual(row['resp_payload_bytes'], 10)
        self.assertEqual(validate_ledger_row(row), [])

    def test_bad_status_line(self):
        self.point_at(b'garbage\r\n\r\n')
        (status, _), stderr, row = self.get_through_proxy()
        self.assertEqual(status, 502)
        self.assertEqual(stderr, '')
        self.assertEqual(row['upstream']['outcome'], 'unknown')
        self.assertEqual(row['error'], 'upstream: bad status line')
        self.assertEqual(row['downstream'], {'outcome': 'delivered', 'status': 502})
        self.assertEqual(validate_ledger_row(row), [])

    def test_bad_upstream_content_length(self):
        self.point_at(b'HTTP/1.1 200 OK\r\nContent-Length: abc\r\n\r\nxyz')
        (status, _), stderr, row = self.get_through_proxy()
        self.assertEqual(status, 502)
        self.assertEqual(stderr, '')
        self.assertEqual(row['upstream']['outcome'], 'unknown')
        self.assertIn('bad Content-Length', row['error'])
        self.assertEqual(validate_ledger_row(row), [])

    def test_read_until_close_response_row_present_after_eof(self):
        self.point_at(b'HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nwithout length')
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            s = self.raw()
            s.sendall(b'GET /b/k HTTP/1.1\r\nHost: h\r\n\r\n')
            data = b''
            while True:
                piece = s.recv(65536)
                if not piece:
                    break
                data += piece
            row = self.rows()[-1]
        self.assertTrue(data.endswith(b'without length'))
        self.assertEqual(err.getvalue(), '')
        self.assertEqual(row['upstream'], {'outcome': 'ok', 'http_status': 200, 's3_code': None})
        self.assertEqual(row['resp_payload_bytes'], len(b'without length'))
        self.assertIsNone(row['error'])

    def test_complete_200_with_leading_whitespace_and_declaration_is_an_error(self):
        body = (b'\n\n<?xml version="1.0" encoding="UTF-8"?>\n'
                b'<Error><Code>InternalError</Code><Message>m</Message></Error>')
        self.up.respond = lambda h, req: reply(h, 200, body, XML)
        c = self.conn()
        c.request('POST', '/b/k?uploadId=U', b'<CompleteMultipartUpload/>')
        self.assertEqual(c.getresponse().read(), body)
        row = self.wait_attempts(1)[0]
        self.assertEqual(row['response_body']['complete_error'], 'InternalError')
        self.assertEqual(row['upstream'], {'outcome': 'error', 'http_status': 200, 's3_code': 'InternalError'})

    def test_complete_200_unparseable_is_not_ok(self):
        self.up.respond = lambda h, req: reply(h, 200, b'<<not xml', XML)
        c = self.conn()
        c.request('POST', '/b/k?uploadId=U', b'<CompleteMultipartUpload/>')
        c.getresponse().read()
        row = self.wait_attempts(1)[0]
        self.assertEqual(row['upstream']['outcome'], 'unknown')
        self.assertEqual(row['response_body']['complete_error'], 'unparseable')
        self.assertIn('not XML', row['error'])
        self.assertEqual(validate_ledger_row(row), [])


class MarkAndLoadTest(ProxyBase):
    def test_mark_and_seq_share_lock(self):
        stop = threading.Event()
        errors = []

        def worker():
            try:
                c = self.conn()
                for _ in range(40):
                    c.request('GET', '/b/k')
                    c.getresponse().read()
            except Exception as e:  # noqa: BLE001
                errors.append(e)
        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        marks = []
        while any(t.is_alive() for t in threads):
            marks.append(self.proxy.mark(f'm{len(marks)}'))
            time.sleep(0.001)
        for t in threads:
            t.join()
        stop.set()
        self.assertEqual(errors, [])
        rows = self.rows()
        mark_rows = [r for r in rows if r['kind'] == 'mark']
        attempts = [r for r in rows if r['kind'] == 'attempt']
        self.assertEqual(len(attempts), 320)
        self.assertEqual([m['seq'] for m in mark_rows], marks)
        self.assertGreater(len(mark_rows), 5)
        self.assertEqual(len({r['seq'] for r in rows}), len(rows))
        for m in mark_rows:
            for a in attempts:
                if a['seq'] > m['seq']:
                    self.assertGreaterEqual(a['t_start_us'], m['t_us'])
                else:
                    self.assertLessEqual(a['t_start_us'], m['t_us'])

    def test_concurrent_500_rows_distinct_seq(self):
        errors = []

        def worker(n):
            try:
                c = self.conn('r1' if n % 2 else 'rig')
                for i in range(25):
                    c.request('PUT', f'/b/k{n}-{i}', b'x' * 100)
                    c.getresponse().read()
            except Exception as e:  # noqa: BLE001
                errors.append(e)
        threads = [threading.Thread(target=worker, args=(n,)) for n in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        rows = self.attempts()
        self.assertEqual(len(rows), 500)
        self.assertEqual(len({r['seq'] for r in rows}), 500)
        self.assertEqual({r['caller'] for r in rows}, {'r1', 'rig'})
        for row in rows:
            self.assertEqual(validate_ledger_row(row), [])
        lines = self.ledger.read_text().splitlines()
        self.assertEqual(len(lines), 500)


if __name__ == '__main__':
    unittest.main()
