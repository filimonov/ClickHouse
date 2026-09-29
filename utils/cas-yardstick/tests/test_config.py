import os
import tempfile
import unittest
import xml.etree.ElementTree as ET

from yardstick.config import POLICIES, write_config

ENDPOINT = 'http://127.0.0.1:19121'
DISK_KINDS = ('cas', 's3', 's3plain')


def load_disks(run_dir):
    root = ET.parse(os.path.join(run_dir, 'etc', 'config.d', 'yardstick.xml')).getroot()
    disks = root.find('storage_configuration/disks')
    return root, {d.tag: d for d in disks}


class ConfigTest(unittest.TestCase):
    def write(self, policy, gc=False, prefix='test/run/x/k1'):
        d = tempfile.mkdtemp()
        self.addCleanup(lambda: __import__('shutil').rmtree(d, ignore_errors=True))
        return d, write_config(d, policy, ENDPOINT, prefix, gc)

    def test_one_disk_per_policy(self):
        for policy in POLICIES:
            with self.subTest(policy=policy):
                d, _ = self.write(policy)
                _, disks = load_disks(d)
                base = policy.replace('_cached', '')
                self.assertEqual(sorted(set(disks) & set(DISK_KINDS)), [base])
                self.assertIn('default', disks)
                cached = policy.endswith('_cached')
                self.assertEqual(policy in disks and policy != base, cached)
                self.assertEqual(len(disks), 2 + cached)
                if cached:
                    self.assertEqual(disks[policy].findtext('disk'), base)
                    self.assertEqual(disks[policy].findtext('type'), 'cache')

    def test_policy_points_at_the_outermost_disk(self):
        for policy in POLICIES:
            with self.subTest(policy=policy):
                d, _ = self.write(policy)
                root, _ = load_disks(d)
                pol = root.find('storage_configuration/policies')
                self.assertEqual([p.tag for p in pol], [policy])
                self.assertEqual(pol.find(policy + '/volumes/main/disk').text, policy)

    def test_endpoint_carries_prefix(self):
        for policy in POLICIES:
            with self.subTest(policy=policy):
                d, _ = self.write(policy, prefix='test/run/p/k7')
                _, disks = load_disks(d)
                base = policy.replace('_cached', '')
                self.assertEqual(disks[base].findtext('endpoint'), ENDPOINT + '/test/run/p/k7/')
                self.assertEqual(disks[base].findtext('access_key_id'), 'clickhouse')
                self.assertEqual(disks[base].findtext('secret_access_key'), 'clickhouse')
                self.assertEqual(disks[base].findtext('http_keep_alive_timeout'), '30')
                self.assertEqual(disks[base].findtext('http_keep_alive_max_requests'), '10000')

    def test_cas_gc_flag_follows_argument(self):
        for gc, want in ((False, '0'), (True, '1')):
            d, _ = self.write('cas', gc=gc)
            _, disks = load_disks(d)
            self.assertEqual(disks['cas'].findtext('cas_gc_enabled'), want)
            self.assertEqual(disks['cas'].findtext('cas_server_root_id'), 'yardstick')

    def test_s3_has_metadata_path_under_run_dir(self):
        d, _ = self.write('s3')
        _, disks = load_disks(d)
        self.assertTrue(disks['s3'].findtext('metadata_path').startswith(d))

    def test_cache_wrapper_block(self):
        d, _ = self.write('cas_cached')
        _, disks = load_disks(d)
        c = disks['cas_cached']
        self.assertEqual(c.findtext('path'), 'cas_cache/')
        self.assertEqual(c.findtext('max_size'), '1Gi')
        self.assertEqual(c.findtext('cache_on_write_operations'), '1')
        self.assertEqual(c.findtext('background_download_threads'), '0')
        self.assertEqual(c.findtext('load_metadata_asynchronously'), '0')

    def test_hash_changes_with_gc_and_is_stable(self):
        d, h1 = self.write('cas', gc=False)
        self.assertEqual(len(h1), 64)
        self.assertEqual(h1, write_config(d, 'cas', ENDPOINT, 'test/run/x/k1', False))
        self.assertNotEqual(h1, write_config(d, 'cas', ENDPOINT, 'test/run/x/k1', True))

    def test_base_files_written(self):
        d, _ = self.write('s3')
        for f in ('config.xml', 'users.xml'):
            ET.parse(os.path.join(d, 'etc', f))
        for sub in ('data', 'tmp', 'uf', 'caches'):
            self.assertTrue(os.path.isdir(os.path.join(d, sub)))

    def test_unknown_policy_rejected(self):
        with self.assertRaises(ValueError):
            write_config(tempfile.mkdtemp(), 'nope', ENDPOINT, 'p', False)


if __name__ == '__main__':
    unittest.main()
