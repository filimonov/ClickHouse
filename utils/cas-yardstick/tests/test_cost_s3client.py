import pathlib
import re
import threading
import unittest
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from yardstick.cost.s3client import S3Client, sign_v4


class SigV4Test(unittest.TestCase):
    def test_sigv4_matches_aws_doc_example(self):
        """GET Object example from
        https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html"""
        headers = {
            'Host': 'examplebucket.s3.amazonaws.com',
            'Range': 'bytes=0-9',
            'x-amz-content-sha256': 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
            'x-amz-date': '20130524T000000Z',
        }
        auth = sign_v4('GET', '/test.txt', {}, headers, 'AKIAIOSFODNN7EXAMPLE',
                       'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY', 'us-east-1', '20130524T000000Z')
        self.assertEqual(
            auth,
            'AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,'
            'SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,'
            'Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41')

    def test_sigv4_list_example(self):
        """GET Bucket (List Objects) example, same page: query parameters are sorted and encoded."""
        headers = {
            'Host': 'examplebucket.s3.amazonaws.com',
            'x-amz-content-sha256': 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
            'x-amz-date': '20130524T000000Z',
        }
        auth = sign_v4('GET', '/', {'max-keys': '2', 'prefix': 'J'}, headers, 'AKIAIOSFODNN7EXAMPLE',
                       'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY', 'us-east-1', '20130524T000000Z')
        self.assertTrue(auth.endswith('Signature=34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7'))


class FakeS3(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def reply(self, body, status=200):
        data = body.encode()
        self.send_response(status)
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        url = urllib.parse.urlparse(self.path)
        query = dict(urllib.parse.parse_qsl(url.query, keep_blank_values=True))
        self.server.requests.append(('GET', self.path, self.headers.get('Host')))
        keys = sorted(k for k in self.server.keys if k.startswith(query.get('prefix', '')))
        start = int(query.get('continuation-token', '0') or 0)
        limit = int(query.get('max-keys', '1000'))
        page = keys[start:start + limit]
        truncated = start + limit < len(keys)
        body = ('<?xml version="1.0"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                f'<IsTruncated>{str(truncated).lower()}</IsTruncated>'
                + (f'<NextContinuationToken>{start + limit}</NextContinuationToken>' if truncated else '')
                + ''.join(f'<Contents><Key>{k}</Key><Size>{len(k)}</Size><ETag>&quot;e-{k}&quot;</ETag></Contents>'
                          for k in page)
                + '</ListBucketResult>')
        self.reply(body)

    def do_POST(self):
        url = urllib.parse.urlparse(self.path)
        body = self.rfile.read(int(self.headers['Content-Length'])).decode()
        self.server.requests.append(('POST', self.path, body))
        if 'delete' in urllib.parse.parse_qs(url.query, keep_blank_values=True):
            keys = re.findall(r'<Key>([^<]*)</Key>', body)
            for k in keys:
                self.server.keys.discard(k)
            self.reply('<DeleteResult>' + ''.join(f'<Deleted><Key>{k}</Key></Deleted>' for k in keys) + '</DeleteResult>')
        else:
            self.reply('', 400)


class S3ClientTest(unittest.TestCase):
    def serve(self, keys):
        server = ThreadingHTTPServer(('127.0.0.1', 0), FakeS3)
        server.keys = set(keys)
        server.requests = []
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(lambda: (server.shutdown(), server.server_close(), thread.join()))
        return server, S3Client(f'http://127.0.0.1:{server.server_port}', 'ak', 'sk')

    def test_list_pagination(self):
        server, client = self.serve([f'p/{i:04d}' for i in range(2500)] + ['other/x'])
        # max_keys is not part of the public contract; the fake server's default page is 1000.
        got = client.list('yardstick-cost', 'p/')
        self.assertEqual([o['key'] for o in got], [f'p/{i:04d}' for i in range(2500)])
        self.assertEqual(got[0], {'key': 'p/0000', 'size': 6, 'etag': 'e-p/0000'})
        gets = [r for r in server.requests if r[0] == 'GET']
        self.assertEqual(len(gets), 3)
        self.assertTrue(all('list-type=2' in r[1] for r in gets))

    def test_delete_prefix_batches(self):
        server, client = self.serve([f'p/{i:04d}' for i in range(2500)] + ['other/x'])
        self.assertEqual(client.delete_prefix('yardstick-cost', 'p/'), 2500)
        posts = [r for r in server.requests if r[0] == 'POST']
        self.assertEqual([len(re.findall('<Key>', r[2])) for r in posts], [1000, 1000, 500])
        self.assertTrue(all('<Quiet>' not in r[2] for r in posts))
        self.assertEqual(server.keys, {'other/x'})

    def test_host_header_is_signed_as_sent(self):
        server, client = self.serve(['a'])
        client.list('yardstick-cost', '')
        self.assertEqual(server.requests[0][2], f'127.0.0.1:{server.server_port}')


class RustfsEnvParityTest(unittest.TestCase):
    def test_binary_launcher_sets_the_container_launcher_env(self):
        """The rig must measure the server configuration of the container baseline."""
        root = pathlib.Path(__file__).resolve().parent.parent

        def names(script):
            return set(re.findall(r'RUSTFS_[A-Z_]+(?==)', (root / script).read_text())) - {'RUSTFS_ADDRESS'}

        self.assertEqual(names('rustfs-bin.sh'), names('rustfs.sh'))
        self.assertIn('RUSTFS_OBS_USE_STDOUT', names('rustfs.sh'))


if __name__ == '__main__':
    unittest.main()
