import unittest, tempfile, os
from yardstick import report as R

class Aggregates(unittest.TestCase):
    def test_delta_and_aggregate(self):
        before = {'DiskS3GetObject': 10, 'DiskS3PutObject': 5, 'DiskS3UploadPart': 0, 'DiskS3CreateMultipartUpload': 0,
                  'DiskS3CompleteMultipartUpload': 0, 'ReadBufferFromS3Bytes': 100}
        after = dict(before, DiskS3GetObject=93, DiskS3PutObject=60, DiskS3UploadPart=4, DiskS3CreateMultipartUpload=2,
                     DiskS3CompleteMultipartUpload=2, ReadBufferFromS3Bytes=600)
        d = R.delta(before, after)
        agg = R.aggregate(d, set(after))
        self.assertEqual(agg['GET'], 83)
        self.assertEqual(agg['PUT'], 55 + 4 + 2 + 2)          # multipart: N parts + create + complete
        self.assertEqual(agg['bytes_read'], 500)
        self.assertEqual(agg['HEAD'], R.UNAVAILABLE)          # DiskS3HeadObject absent from the snapshot
    def test_aggregate_missing_counter(self):
        agg = R.aggregate({'DiskS3GetObject': 1}, {'DiskS3GetObject'})
        self.assertEqual(agg['GET'], 1)
        self.assertEqual(agg['PUT'], R.UNAVAILABLE)

class Stats(unittest.TestCase):
    def test_summarize(self):
        self.assertEqual(R.summarize([3.0, 1.0, 2.0]), (2.0, 1.0, 3.0))
    def test_paired_ratio(self):
        self.assertEqual(R.paired_ratio([83, 84, 83], [12, 12, 12]), '6.92 (6.92..7.00)')
        self.assertEqual(R.paired_ratio([0, 0, 0], [0, 0, 0]), '0 → 0')
        self.assertEqual(R.paired_ratio([5, 5, 5], [0, 0, 0]), '0 → 5')
    def test_marks(self):
        self.assertEqual(R.mark_requests([83, 83, 83]), '')
        self.assertEqual(R.mark_requests([83, 84, 83]), '~')
        self.assertEqual(R.mark_wall([1.0, 1.1, 1.0]), '')
        self.assertEqual(R.mark_wall([1.0, 1.3, 1.0]), 'noisy')
    def test_per_unit(self):
        self.assertAlmostEqual(R.per_unit(16600, 200), 83.0)

class Files(unittest.TestCase):
    def test_samples_summary_roundtrip_and_table(self):
        fixture = os.path.join(os.path.dirname(__file__), 'fixtures', 'rca_2026_09_28_samples.tsv')
        rows = R.read_samples(fixture)                       # cas GET 83/cycle, PUT 64; s3 GET 12, PUT 31; 800 cycles; 3 repeats
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, 'samples.tsv'); R.write_samples(p, rows)
            self.assertEqual(R.read_samples(p), rows)
            summary = R.build_summary(rows)
            get_cas = [r for r in summary if r['policy'] == 'cas' and r['metric'] == 'GET'][0]
            self.assertEqual((get_cas['median'], get_cas['min'], get_cas['max'], get_cas['repeats']), (66400, 66400, 66400, 3))
            self.assertAlmostEqual(get_cas['per_unit'], 83.0)
            s = os.path.join(d, 'summary.tsv'); R.write_summary(s, summary)
            self.assertEqual(R.read_summary(s), summary)
            table = R.render_table(summary, ['cas', 's3', 's3plain'], [])
            self.assertIn('cycle', table); self.assertIn('cas/s3', table); self.assertIn('6.92', table)   # 83/12
            self.assertIn('median_op_ms', table)              # per-operation latency columns next to wall
    def test_diagnostics(self):
        rows = [dict(policy='cas', card='cycle', phase='cold', repeat=1, metric='CASRootGet', value=5, unit_count=1),
                dict(policy='s3', card='cycle', phase='cold', repeat=1, metric='CASRootGet', value=0, unit_count=1)]
        out = R.render_diagnostics(R.build_summary(rows))
        self.assertIn('CASRootGet', out); self.assertNotIn('\ns3', out)   # CAS policies only
    def test_unsupported_and_failed_cells(self):
        rows = [dict(policy='s3plain', card='mutation', phase='cold', repeat=1, metric='status', value='UNSUPPORTED (write-once)', unit_count=0),
                dict(policy='cas', card='mutation', phase='cold', repeat=1, metric='status', value='FAILED', unit_count=0)]
        table = R.render_table(R.build_summary(rows), ['cas', 's3', 's3plain'], [])
        self.assertIn('UNSUPPORTED (write-once)', table); self.assertIn('FAILED', table)

class Compare(unittest.TestCase):
    def manifest(self, **over):
        m = dict(workload_rev='abc', scale='1', counts='cycle=200', gc='on', repeats='3', config_hash='h1', build_type='Release', store='rustfs/rustfs:1.0.0-rc.3')
        m.update(over); return m
    def test_compare_incompatible(self):
        self.assertEqual(R.compatible(self.manifest(), self.manifest(scale='0.5', repeats='1')), ['scale', 'repeats'])
        self.assertEqual(R.compatible(self.manifest(), self.manifest()), [])
    def test_manifest_roundtrip(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, 'manifest.tsv'); m = self.manifest()
            R.write_manifest(p, m)
            self.assertEqual(R.read_manifest(p), m)
    def test_render_compare_marks(self):
        cur = [dict(policy='cas', card='cycle', phase='cold', metric='wall', median=3.5, min=3.4, max=3.6, repeats=3, per_unit=0.0175, mark=''),
               dict(policy='cas', card='cycle', phase='cold', metric='GET', median=16800, min=16800, max=16800, repeats=3, per_unit=84, mark='')]
        ref = [dict(cur[0], median=3.0, min=2.9, max=3.1), dict(cur[1], median=16600, min=16600, max=16600, per_unit=83)]
        out = R.render_compare(cur, ref)
        self.assertIn('+16.7%', out); self.assertIn('wall', out); self.assertIn('GET', out)
        self.assertRegex(out, r'wall.*!')      # |Δ wall| > 10 % is marked
        self.assertRegex(out, r'GET.*!')       # a request-median change is marked

if __name__ == '__main__':
    unittest.main()
