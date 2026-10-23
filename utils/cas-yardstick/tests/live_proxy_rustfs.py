"""Live tests of the ledger proxy in front of the RustFS binary. Run with YARDSTICK_LIVE=1 from utils/cas-yardstick:
YARDSTICK_LIVE=1 python3 -m unittest tests.live_proxy_rustfs -v"""
import hashlib
import json
import pathlib
import sys
import unittest
import urllib.error

from tests.live_rustfs_bin import BUCKET, PORT, LiveBase, script
from yardstick.cost.proxy import LedgerProxy
from yardstick.cost.s3client import ACCESS_KEY, SECRET_KEY, S3Client

TOOLS = pathlib.Path(__file__).resolve().parents[3] / 'utils' / 'storage-cost-calculator' / 'tools'
sys.path.insert(0, str(TOOLS))
from artifacts import validate_ledger_row  # noqa: E402

CALLERS = {'r1': 19131, 'r2': 19132, 'r3': 19133, 'rig': 19134}
PART = 5 * 1024 * 1024


class ProxyLiveTest(LiveBase):
    def setUp(self):
        super().setUp()
        result = script('ensure', '--data', str(self.data))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.ledger = self.data.parent / 'ledger.jsonl'
        self.proxy = LedgerProxy(('127.0.0.1', PORT), f'rustfs-{PORT}', self.ledger, CALLERS)
        self.proxy.start()
        self.addCleanup(self.proxy.stop)
        self.s3 = S3Client(f'http://127.0.0.1:{CALLERS["rig"]}', ACCESS_KEY, SECRET_KEY)
        self.s3.ensure_bucket(BUCKET)

    def attempts(self):
        self.proxy.flush()
        rows = [json.loads(line) for line in self.ledger.read_text().splitlines()]
        for row in rows:
            self.assertEqual(validate_ledger_row(row), [], row)
        return sorted((r for r in rows if r['kind'] == 'attempt'), key=lambda r: r['seq'])

    def test_signed_roundtrip_through_rig_caller(self):
        s3 = self.s3
        s3.put(BUCKET, 'px/a b+c.bin', b'hello')
        self.assertEqual(s3.get(BUCKET, 'px/a b+c.bin'), b'hello')
        self.assertEqual(s3.head(BUCKET, 'px/a b+c.bin')['size'], 5)
        self.assertEqual([o['key'] for o in s3.list(BUCKET, 'px/')], ['px/a b+c.bin'])
        self.assertEqual(s3.delete_prefix(BUCKET, 'px/'), 1)
        rows = [r for r in self.attempts() if r['key'].startswith('px/') or r['op'] in ('ListObjectsV2', 'DeleteObjects')]
        ops = [r['op'] for r in rows]
        self.assertEqual(ops, ['PutObject', 'GetObject', 'HeadObject', 'ListObjectsV2', 'ListObjectsV2', 'DeleteObjects'])
        self.assertEqual({r['caller'] for r in rows}, {'rig'})
        put = rows[0]
        self.assertEqual(put['payload_sha256'], hashlib.sha256(b'hello').hexdigest())
        self.assertEqual((put['bucket'], put['key'], put['req_payload_bytes']), (BUCKET, 'px/a b+c.bin', 5))
        self.assertEqual(put['upstream'], {'outcome': 'ok', 'http_status': 200, 's3_code': None})
        self.assertEqual(rows[3]['response_body']['list'], {'key_count': 1, 'truncated': False})
        self.assertEqual(rows[5]['response_body']['delete_results'],
                         [{'key': 'px/a b+c.bin', 'deleted': True, 'code': None}])

    def test_caller_ports_identify_callers(self):
        for caller, port in CALLERS.items():
            S3Client(f'http://127.0.0.1:{port}', ACCESS_KEY, SECRET_KEY).head(BUCKET, 'px/none')
        self.assertEqual([r['caller'] for r in self.attempts() if r['op'] == 'HeadObject'], list(CALLERS))

    def test_multipart_create_upload_complete_and_abort(self):
        s3 = self.s3
        key = 'px/mp'
        upload = s3.create_multipart(BUCKET, key)
        etags = {n: s3.upload_part(BUCKET, key, upload, n, bytes([n]) * PART) for n in (1, 2)}
        s3.complete_multipart(BUCKET, key, upload, [{'part_number': n, 'etag': etags[n]} for n in (1, 2)])
        aborted = s3.create_multipart(BUCKET, key + '2')
        s3.upload_part(BUCKET, key + '2', aborted, 1, b'y' * 1024)
        s3.abort_multipart(BUCKET, key + '2', aborted)
        rows = [r for r in self.attempts() if r['key'].startswith('px/mp')]
        self.assertEqual([r['op'] for r in rows], ['CreateMultipartUpload', 'UploadPart', 'UploadPart',
                                                   'CompleteMultipartUpload', 'CreateMultipartUpload', 'UploadPart',
                                                   'AbortMultipartUpload'])
        self.assertEqual(rows[0]['response_body']['upload_id'], upload)
        self.assertEqual([(r['upload_id'], r['part_number'], r['response_body']['etag']) for r in rows[1:3]],
                         [(upload, 1, etags[1]), (upload, 2, etags[2])])
        self.assertEqual(rows[3]['response_body']['completed_parts'],
                         [{'part_number': 1, 'etag': etags[1]}, {'part_number': 2, 'etag': etags[2]}])
        self.assertEqual(rows[3]['upstream']['outcome'], 'ok')
        self.assertEqual(rows[6]['upload_id'], aborted)
        self.assertEqual(s3.head(BUCKET, key)['size'], 2 * PART)
        s3.delete(BUCKET, key)

    def test_drop_response_on_upload_part_then_client_retry(self):
        s3 = self.s3
        key = 'px/dropped'
        upload = s3.create_multipart(BUCKET, key)
        rule = self.proxy.set_fault({'op': 'UploadPart', 'part_number': 1, 'nth': 1, 'action': 'drop_response',
                                     'send_upstream': True})
        data = b'z' * PART
        with self.assertRaises((urllib.error.URLError, ConnectionError)):
            s3.upload_part(BUCKET, key, upload, 1, data)
        etag = s3.upload_part(BUCKET, key, upload, 1, data)
        parts = [r for r in self.attempts() if r['op'] == 'UploadPart']
        self.assertEqual([(r['upload_id'], r['part_number']) for r in parts], [(upload, 1), (upload, 1)])
        first, second = parts
        self.assertEqual(first['downstream'], {'outcome': 'dropped', 'status': None})
        self.assertEqual(first['upstream'], {'outcome': 'ok', 'http_status': 200, 's3_code': None})
        self.assertEqual(first['injected_rule'], rule)
        self.assertEqual(second['downstream'], {'outcome': 'delivered', 'status': 200})
        self.assertEqual(first['payload_sha256'], second['payload_sha256'])
        self.assertEqual(second['response_body']['etag'], etag)
        s3.abort_multipart(BUCKET, key, upload)


if __name__ == '__main__':
    unittest.main()
