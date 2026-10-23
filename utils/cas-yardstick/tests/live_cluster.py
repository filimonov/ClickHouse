"""Live tests of the 2-replica cluster in the CLI start order RustFS -> proxy -> verify -> cluster. Run from
utils/cas-yardstick: YARDSTICK_LIVE=1 python3 -m unittest tests.live_cluster -v"""
import os
import pathlib
import time
import unittest

from tests.live_rustfs_bin import BUCKET, PORT, SCRIPT, listening, script
from yardstick.client import Client
from yardstick.cost.cluster import Cluster
from yardstick.cost.proxy import LedgerProxy
from yardstick.cost.s3client import ACCESS_KEY, SECRET_KEY, S3Client
from yardstick.cost.topology import Topology

ROOT = pathlib.Path(__file__).resolve().parents[3]
BINARY = ROOT / 'tmp' / 'bin' / '26.8' / 'clickhouse'
CREATE = ("CREATE TABLE {db}.t (k UInt64) ENGINE = ReplicatedMergeTree('/yardstick/{{database}}/{{table}}', "
          "'{{replica}}') ORDER BY k SETTINGS storage_policy = 's3'")


@unittest.skipUnless(os.environ.get('YARDSTICK_LIVE') == '1', 'set YARDSTICK_LIVE=1')
class LiveClusterTest(unittest.TestCase):
    def setUp(self):
        if not BINARY.exists():
            self.skipTest(f'no binary {BINARY}')
        self.pass_dir = ROOT / 'tmp' / 'cas-yardstick' / 'cost-runs' / f'live-cluster-{time.strftime("%Y%m%d-%H%M%S")}'
        self.pass_dir.mkdir(parents=True)
        self.key_prefix = self.pass_dir.name
        was_up = listening()
        # ensure is idempotent and prints nothing we need; remember whether we own the process.
        result = script('ensure')
        self.assertEqual(result.returncode, 0, result.stderr)
        if not was_up:
            self.addCleanup(script, 'stop')
        self.topology = Topology(port_base=19700, replicas=2)
        self.proxy = LedgerProxy(('127.0.0.1', PORT), f'rustfs-{PORT}', self.pass_dir / 'ledger.jsonl',
                                 self.topology.callers)
        self.proxy.start()
        self.addCleanup(self.proxy.stop)
        self.rig = S3Client(f'http://127.0.0.1:{self.topology.callers["rig"]}', ACCESS_KEY, SECRET_KEY)
        self.addCleanup(self.rig.delete_prefix, BUCKET, self.key_prefix + '/')
        self.assertEqual(self.topology.verify_services(self.proxy, self.rig), [])
        self.cluster = Cluster(str(BINARY), str(self.pass_dir), self.topology, 's3', self.key_prefix, None, False, {})
        self.addCleanup(self.cluster.stop_all)

    def create_table(self):
        r1, r2 = self.cluster.clients
        for client in (r1, r2):
            client.query('CREATE DATABASE IF NOT EXISTS d')
            client.query(CREATE.format(db='d'))
        return r1, r2

    def test_cli_startup_sequence_rustfs_proxy_cluster(self):
        self.cluster.start_all()
        for i, client in enumerate(self.cluster.clients, 1):
            self.assertEqual(client.query('SELECT 1').strip(), '1')
            self.assertEqual(client.query('SELECT getMacro(\'replica\')').strip(), f'r{i}')
        self.assertEqual(len(self.cluster.relays), 2)
        self.assertEqual([len(g) for g in self.cluster.generations.values()], [1, 1])
        self.assertTrue(all(s > 0 for s in self.cluster.startup_seconds))

    def test_two_replicas_replicate_through_relay(self):
        self.cluster.start_all()
        r1, r2 = self.create_table()
        r1.query('INSERT INTO d.t VALUES (1)')
        r2.query('SYSTEM SYNC REPLICA d.t')
        self.assertEqual(r2.query('SELECT count() FROM d.t').strip(), '1')
        seen = [relay.snapshot('after-insert') for relay in self.cluster.relays]
        self.assertGreater(seen[0]['from_target'], 0, seen)

    def test_restart_replica1_then_replicate(self):
        self.cluster.start_all()
        r1, r2 = self.create_table()
        r1.query('INSERT INTO d.t VALUES (1)')
        r2.query('SYSTEM SYNC REPLICA d.t')
        self.cluster.stop(1)
        self.cluster.start(1)
        r1 = self.cluster.clients[0]
        self.assertEqual(r1.query("SELECT count() FROM system.zookeeper WHERE path = '/yardstick/d/t/replicas'").strip(), '2')
        r2.query('INSERT INTO d.t VALUES (2)')
        r1.query('SYSTEM SYNC REPLICA d.t')
        self.assertEqual(r1.query('SELECT count() FROM d.t').strip(), '2')
        self.assertEqual(len(self.cluster.generations[1]), 2)


if __name__ == '__main__':
    unittest.main()
