"""Live tests against the RustFS binary. Run with YARDSTICK_LIVE=1 from utils/cas-yardstick:
YARDSTICK_LIVE=1 python3 -m unittest tests.live_rustfs_bin -v"""
import os
import pathlib
import socket
import subprocess
import tempfile
import unittest

from yardstick.cost.s3client import ACCESS_KEY, SECRET_KEY, S3Client, S3Error

ROOT = pathlib.Path(__file__).resolve().parent.parent
SCRIPT = ROOT / 'rustfs-bin.sh'
PORT = 19121
BUCKET = 'yardstick-cost'


def listening() -> bool:
    with socket.socket() as s:
        s.settimeout(1)
        return s.connect_ex(('127.0.0.1', PORT)) == 0


def script(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run([str(SCRIPT), *args], capture_output=True, text=True, timeout=180)


@unittest.skipUnless(os.environ.get('YARDSTICK_LIVE') == '1', 'set YARDSTICK_LIVE=1')
class LiveBase(unittest.TestCase):
    def setUp(self):
        if listening():
            self.skipTest(f'something already listens on 127.0.0.1:{PORT}')
        tmp = tempfile.TemporaryDirectory(dir=ROOT.parent.parent / 'tmp' if (ROOT.parent.parent / 'tmp').is_dir() else None)
        self.addCleanup(tmp.cleanup)
        self.data = pathlib.Path(tmp.name) / 'data'
        self.addCleanup(lambda: script('stop', '--data', str(self.data), '--force'))


class LifecycleTest(LiveBase):
    def test_start_status_stop_cycle_pid_ownership_and_refusal(self):
        data = str(self.data)
        self.assertNotEqual(script('status', '--data', data).returncode, 0)
        started = script('start', '--data', data)
        self.assertEqual(started.returncode, 0, started.stderr)
        pid = int((self.data / '.rustfs.pid').read_text())
        self.assertEqual(pathlib.Path(f'/proc/{pid}/comm').read_text().strip(), 'rustfs')
        self.assertIn(f'pid={pid}', script('status', '--data', data).stdout)
        again = script('start', '--data', data)
        self.assertNotEqual(again.returncode, 0)
        self.assertIn('already up', again.stderr)
        # `start` does not mark ownership, so a plain stop leaves the process running.
        self.assertEqual(script('stop', '--data', data).returncode, 0)
        self.assertTrue(pathlib.Path(f'/proc/{pid}').exists())
        self.assertEqual(script('stop', '--data', data, '--force').returncode, 0)
        self.assertFalse(pathlib.Path(f'/proc/{pid}').exists())
        self.assertFalse((self.data / '.rustfs.pid').exists())
        self.assertNotEqual(script('status', '--data', data).returncode, 0)

    def test_start_refuses_to_wipe_foreign_directory(self):
        self.data.mkdir(parents=True)
        (self.data / 'precious').write_text('x')
        result = script('start', '--data', str(self.data))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('refusing to wipe', result.stderr)
        self.assertTrue((self.data / 'precious').exists())

    def test_ensure_is_idempotent_and_stop_respects_ownership(self):
        data = str(self.data)
        first = script('ensure', '--data', data)
        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertTrue((self.data / '.started-by-rig').exists())
        pid = (self.data / '.rustfs.pid').read_text()
        second = script('ensure', '--data', data)
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertEqual((self.data / '.rustfs.pid').read_text(), pid)
        self.assertEqual(script('stop', '--data', data).returncode, 0)
        self.assertNotEqual(script('status', '--data', data).returncode, 0)
        # Ownership marker is gone with the process.
        self.assertFalse((self.data / '.started-by-rig').exists())

        # A server that was already up is not ours: `ensure` leaves no marker and `stop` keeps it.
        self.assertEqual(script('start', '--data', data).returncode, 0)
        other = str(self.data.parent / 'other')
        self.assertEqual(script('ensure', '--data', other).returncode, 0)
        self.assertFalse(pathlib.Path(other, '.started-by-rig').exists())
        self.assertEqual(script('stop', '--data', other).returncode, 0)
        self.assertEqual(script('status', '--data', data).returncode, 0)


class ClientTest(LiveBase):
    def setUp(self):
        super().setUp()
        result = script('ensure', '--data', str(self.data))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.s3 = S3Client(f'http://127.0.0.1:{PORT}', ACCESS_KEY, SECRET_KEY)

    def test_put_get_head_delete_roundtrip(self):
        s3 = self.s3
        s3.ensure_bucket(BUCKET)
        s3.ensure_bucket(BUCKET)
        s3.put(BUCKET, 'rt/a b+c.bin', b'hello')
        s3.put(BUCKET, 'rt/d', b'x' * 10)
        self.assertEqual(s3.get(BUCKET, 'rt/a b+c.bin'), b'hello')
        self.assertEqual(s3.head(BUCKET, 'rt/a b+c.bin')['size'], 5)
        self.assertEqual([(o['key'], o['size']) for o in s3.list(BUCKET, 'rt/')], [('rt/a b+c.bin', 5), ('rt/d', 10)])
        s3.delete(BUCKET, 'rt/d')
        self.assertIsNone(s3.head(BUCKET, 'rt/d'))
        self.assertEqual(s3.delete_prefix(BUCKET, 'rt/'), 1)
        self.assertEqual(s3.list(BUCKET, 'rt/'), [])
        with self.assertRaises(S3Error) as ctx:
            s3.get(BUCKET, 'rt/missing')
        self.assertEqual(ctx.exception.status, 404)

    def test_multipart_create_upload_list_complete_retires_all_parts(self):
        """Completing with 2 of 3 uploaded parts.

        AWS documents that parts not listed in the complete request are discarded and the upload is
        finished, so ListParts afterwards fails with NoSuchUpload. RustFS 1.0.0-rc.3 behaves the same: the object
        holds parts 1 and 2 and ListParts on the upload answers 404 NoSuchUpload.
        """
        s3 = self.s3
        s3.ensure_bucket(BUCKET)
        key = 'mp/three'
        upload = s3.create_multipart(BUCKET, key)
        self.assertIn(upload, [u['upload_id'] for u in s3.list_multipart_uploads(BUCKET, key)])
        size = 5 * 1024 * 1024
        etags = {n: s3.upload_part(BUCKET, key, upload, n, bytes([n]) * size) for n in (1, 2, 3)}
        self.assertEqual([p['part_number'] for p in s3.list_parts(BUCKET, key, upload)], [1, 2, 3])
        s3.complete_multipart(BUCKET, key, upload, [{'part_number': n, 'etag': etags[n]} for n in (1, 2)])
        self.assertEqual(s3.head(BUCKET, key)['size'], 2 * size)
        self.assertNotIn(upload, [u['upload_id'] for u in s3.list_multipart_uploads(BUCKET, key)])
        with self.assertRaises(S3Error) as ctx:
            s3.list_parts(BUCKET, key, upload)
        self.assertEqual(ctx.exception.status, 404)
        self.assertIn('NoSuchUpload', ctx.exception.body)

    def test_multipart_abort_roundtrip(self):
        s3 = self.s3
        s3.ensure_bucket(BUCKET)
        key = 'mp/aborted'
        upload = s3.create_multipart(BUCKET, key)
        s3.upload_part(BUCKET, key, upload, 1, b'y' * 1024)
        s3.abort_multipart(BUCKET, key, upload)
        self.assertNotIn(upload, [u['upload_id'] for u in s3.list_multipart_uploads(BUCKET, key)])
        self.assertIsNone(s3.head(BUCKET, key))


if __name__ == '__main__':
    unittest.main()
