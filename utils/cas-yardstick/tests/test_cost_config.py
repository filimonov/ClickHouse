import os
import shutil
import socket
import tempfile
import unittest
import xml.etree.ElementTree as ET

from yardstick.config import write_config, write_cost_config
from yardstick.cost.topology import Topology

# Hash of write_config(tmp, 's3', 'http://127.0.0.1:19121', 'test/x', True) before write_cost_config existed.
PINNED_S3_HASH = 'af565251a90ecb3f5f21479a79100844a64bd85680bd5ace70d6e93185fdc9f8'


def free_port_base():
    s = socket.socket()
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _endpoints_bind(t: Topology) -> bool:
    for host, port in [('127.0.0.1', p) for i in range(1, t.replicas + 1) for p in (t.tcp(i), t.http(i), t.interserver(i))] \
            + [(t.relay_host(i), t.interserver(i)) for i in range(1, t.replicas + 1)] \
            + [('127.0.0.1', t.keeper_client()), ('127.0.0.1', t.keeper_raft())]:
        s = socket.socket()
        try:
            s.bind((host, port))
        except OSError:
            return False
        finally:
            s.close()
    return True


def free_topology_base(replicas: int, candidates=None, tries: int = 50) -> int:
    """A port base whose every rig endpoint was just checked free: the test then preflights exactly that range."""
    candidates = candidates if candidates is not None else (free_port_base() // 10 * 10 for _ in range(tries))
    for base in candidates:
        if base + 10 * replicas < 65536 and _endpoints_bind(Topology(port_base=base, replicas=replicas)):
            return base
    raise RuntimeError(f'no free port base for {replicas} replica(s) in {tries} tries')


class Fake:
    pass


class CostConfigTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.top = Topology(replicas=3)

    def write(self, replica=1, policy='s3', prefix='p/run1', local_dir=None, gc=True, settings=None, top=None):
        d = os.path.join(self.tmp, f'r{replica}')
        h = write_cost_config(d, replica, policy, top or self.top, prefix, local_dir, gc, 'root-id', settings or {})
        return d, h

    def root(self, d):
        return ET.parse(os.path.join(d, 'etc', 'config.d', 'cost.xml')).getroot()

    def test_ports_distinct_for_three_replicas_including_keeper(self):
        t = Topology(replicas=3)
        ports = [f(i) for i in (1, 2, 3) for f in (t.tcp, t.http, t.interserver)]
        self.assertEqual(ports[:3], [19700, 19701, 19702])
        self.assertEqual(ports[3:6], [19710, 19711, 19712])
        self.assertEqual(ports[6:], [19720, 19721, 19722])
        self.assertEqual((t.keeper_client(), t.keeper_raft()), (19708, 19709))
        allp = ports + [t.keeper_client(), t.keeper_raft()]
        self.assertEqual(len(allp), len(set(allp)))
        self.assertEqual(t.relay_host(2), '127.0.1.2')

    def test_endpoint_uses_callers_port_and_bucket_and_prefix(self):
        t = Topology()
        self.assertEqual(t.endpoint(2, 'a/b'), 'http://127.0.0.1:19132/yardstick-cost/a/b/')
        self.assertEqual(t.caller_port(3), 19133)
        self.assertEqual(t.store, ('127.0.0.1', 19121))

    def test_key_layout_per_replica_vs_shared_cas(self):
        for policy, suffix in (('s3', 'r2/'), ('s3plain', 'r2/'), ('cas', 'shared/')):
            d, _ = self.write(replica=2, policy=policy)
            ep = self.root(d).find(f'storage_configuration/disks/{policy}/endpoint').text
            self.assertEqual(ep, f'http://127.0.0.1:19132/yardstick-cost/p/run1/{suffix}', policy)
        d, _ = self.write(policy='cas', gc=False)
        disk = self.root(d).find('storage_configuration/disks/cas')
        self.assertEqual(disk.find('cas_server_root_id').text, 'root-id')
        self.assertEqual(disk.find('cas_gc_enabled').text, '0')

    def test_replica1_keeper_server_block_and_all_zookeeper_macros_listen_host(self):
        for r in (1, 2, 3):
            d, _ = self.write(replica=r)
            root = self.root(d)
            self.assertEqual(root.find('listen_host').text, '127.0.0.1')
            self.assertEqual(root.find('interserver_http_host').text, f'127.0.1.{r}')
            self.assertEqual(root.find('zookeeper/node/host').text, '127.0.0.1')
            self.assertEqual(root.find('zookeeper/node/port').text, '19708')
            self.assertEqual(root.find('zookeeper/session_timeout_ms').text, '10000')
            self.assertEqual(root.find('macros/shard').text, '1')
            self.assertEqual(root.find('macros/replica').text, f'r{r}')
            ks = root.find('keeper_server')
            if r != 1:
                self.assertIsNone(ks)
                continue
            self.assertEqual(ks.find('tcp_port').text, '19708')
            self.assertEqual(ks.find('server_id').text, '1')
            self.assertEqual(ks.find('log_storage_path').text, f'{d}/keeper/log')
            self.assertEqual(ks.find('snapshot_storage_path').text, f'{d}/keeper/snapshots')
            self.assertEqual(ks.find('coordination_settings/operation_timeout_ms').text, '10000')
            self.assertEqual(ks.find('coordination_settings/session_timeout_ms').text, '30000')
            s = ks.find('raft_configuration/server')
            self.assertEqual([s.find(k).text for k in ('id', 'hostname', 'port')], ['1', '127.0.0.1', '19709'])

    def test_local_policy_path_is_the_given_dir(self):
        d, _ = self.write(policy='local', local_dir='/data/pass1/r1')
        disk = self.root(d).find('storage_configuration/disks/local')
        self.assertEqual(disk.find('type').text, 'local')
        self.assertEqual(disk.find('path').text, '/data/pass1/r1/')

    def test_unknown_policy_rejected(self):
        with self.assertRaises(ValueError):
            self.write(policy='local_cached')

    def test_cached_policy_wraps_disk(self):
        d, _ = self.write(policy='s3_cached')
        disks = self.root(d).find('storage_configuration/disks')
        self.assertEqual(disks.find('s3_cached/type').text, 'cache')
        self.assertEqual(disks.find('s3_cached/disk').text, 's3')

    def test_existing_write_config_hash_unchanged(self):
        h = write_config(os.path.join(self.tmp, 'old'), 's3', 'http://127.0.0.1:19121', 'test/x', True)
        self.assertEqual(h, PINNED_S3_HASH)

    def test_hash_masks_dir_and_prefix_but_not_settings(self):
        _, h1 = self.write(prefix='p/a')
        d2 = os.path.join(self.tmp, 'other')
        h2 = write_cost_config(d2, 1, 's3', self.top, 'p/b', None, True, 'root-id', {})
        self.assertEqual(h1, h2)
        _, h3 = self.write(settings={'merge_tree.old_parts_lifetime': 5})
        self.assertNotEqual(h1, h3)

    def test_preflight_reports_busy_port(self):
        base = free_topology_base(2)
        t = Topology(port_base=base, replicas=2)
        s = socket.socket()
        self.addCleanup(s.close)
        s.bind(('127.0.0.1', t.http(2)))
        problems = t.preflight_new_endpoints()
        self.assertEqual(len(problems), 1, problems)
        self.assertIn(f'127.0.0.1:{t.http(2)}', problems[0])

    def test_preflight_accepts_port_in_time_wait(self):
        base = free_topology_base(1)
        t = Topology(port_base=base, replicas=1)
        port = t.http(1)
        listener = socket.socket()
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(('127.0.0.1', port))
        listener.listen(1)
        client = socket.create_connection(('127.0.0.1', port))
        served, _ = listener.accept()
        served.close()  # the side that closes first keeps the TIME_WAIT entry on the listening port
        client.close()
        listener.close()
        plain = socket.socket()
        self.addCleanup(plain.close)
        with self.assertRaises(OSError):
            plain.bind(('127.0.0.1', port))  # without SO_REUSEADDR the port really is unusable
        self.assertEqual(t.preflight_new_endpoints(), [])

    def test_free_topology_base_skips_a_base_with_a_busy_endpoint(self):
        busy_base = free_topology_base(1)
        busy = socket.socket()
        self.addCleanup(busy.close)
        busy.bind(('127.0.0.1', Topology(port_base=busy_base, replicas=1).keeper_raft()))
        good = free_topology_base(1, candidates=iter([busy_base, free_topology_base(1)]))
        self.assertNotEqual(good, busy_base)
        with self.assertRaises(RuntimeError):
            free_topology_base(1, candidates=iter([busy_base]))

    def test_preflight_new_endpoints_ignores_proxy_and_store_ports(self):
        socks = []
        for port in (19131, 19121):
            s = socket.socket()
            self.addCleanup(s.close)
            try:
                s.bind(('127.0.0.1', port))
            except OSError:
                pass  # already held elsewhere: the ports are still not ours to check
            socks.append(s)
        t = Topology(port_base=free_topology_base(3), replicas=3)
        self.assertEqual(t.preflight_new_endpoints(), [])

    def test_server_settings_rendered_at_xml_path(self):
        d, _ = self.write(policy='cas', settings={
            'storage_configuration.disks.cas.cas_gc_interval_sec': 7,
            'merge_tree.old_parts_lifetime': '1',
        })
        root = self.root(d)
        self.assertEqual(root.find('storage_configuration/disks/cas/cas_gc_interval_sec').text, '7')
        self.assertEqual(len(root.findall('storage_configuration/disks/cas/cas_gc_interval_sec')), 1)
        self.assertEqual(root.find('merge_tree/old_parts_lifetime').text, '1')

    def test_written_xml_parses(self):
        for policy in ('s3', 'cas', 'local', 'cas_cached', 's3plain_cached'):
            d, _ = self.write(policy=policy, local_dir='/x', settings={'a.b': 'c&d<e>'})
            self.assertEqual(self.root(d).find('a/b').text, 'c&d<e>')
            for name in ('config.xml', 'users.xml'):
                self.assertTrue(os.path.exists(os.path.join(d, 'etc', name)))

    def test_verify_services(self):
        t = Topology()
        proxy, rig = Fake(), Fake()
        proxy.ports = dict(t.callers)
        rig.ensure_bucket = lambda b: None
        self.assertEqual(t.verify_services(proxy, rig), [])
        proxy.ports = {'r1': 1}
        self.assertEqual(len(t.verify_services(proxy, rig)), 1)
        proxy.ports = dict(t.callers)

        def boom(b):
            raise OSError('down')
        rig.ensure_bucket = boom
        problems = t.verify_services(proxy, rig)
        self.assertEqual(len(problems), 1)
        self.assertIn('down', problems[0])


if __name__ == '__main__':
    unittest.main()
