"""Fixture tests of the s3 shape cards (R2): manifests, the part-type predicate, layout attribution, multipart."""
import json
import os
import sys
import unittest

from yardstick.cost.cards import CARDS, probes, shapes
from yardstick.cost.cards.base import CardContext

TOOLS = os.path.join(probes.REPO, 'utils', 'storage-cost-calculator', 'tools')
sys.path.insert(0, TOOLS)
import artifacts  # noqa: E402

R = os.path.join(probes.REPO, 'docs', 'superpowers', 'reports', '2026-10-24-storage-cost-s3')
EXPERIMENTS = os.path.join(R, 'experiments')
S3_CARDS = {'objects-per-part', 'big-insert-multipart', 'mutation', 'drop', 'cold-select', 'startup', 'merge-forced'}
MIB = 1 << 20


def manifest(experiment_id):
    with open(os.path.join(EXPERIMENTS, f'{experiment_id}.json')) as f:
        m = json.load(f)
    m['params']['report_dir'] = R
    return m


def registry(**values):
    base = {'min_bytes_for_wide_part': 10 * MIB, 'min_rows_for_wide_part': 0, 'min_level_for_wide_part': 0}
    return {'quantities': [{'id': k, 'default': v} for k, v in {**base, **values}.items()]}


class FakeClient:
    def __init__(self, answers=None):
        self.answers, self.log = answers or {}, []

    def query(self, sql, settings=None, timeout=3600):
        self.log.append(sql)
        return next((a for needle, a in self.answers.items() if needle in sql), '0\n')


class FakeCluster:
    def __init__(self, answers=None):
        self.clients = [FakeClient(answers)]
        self.events = []

    def stop(self, i):
        self.events.append(('stop', i, len(self.clients[0].log)))

    def start(self, i):
        self.events.append(('start', i, len(self.clients[0].log)))
        return 1.25


_PASSES = iter(range(1 << 30))


def ctx_for(m, cluster, policy='s3'):
    # Card state is keyed by pass directory and cards are singletons: every context gets its own pass.
    return CardContext(cluster=cluster, proxy=None, rig=None, pass_dir=f'/nonexistent/{next(_PASSES)}', local_dirs=[],
                       policy=policy,
                       variant='s3', cache_enabled=policy.endswith('_cached'), manifest=m, replay=None, window=None)


class ManifestsTest(unittest.TestCase):
    def test_every_manifest_validates_and_names_an_s3_card(self):
        names = sorted(os.listdir(EXPERIMENTS))
        self.assertTrue(names)
        for name in names:
            with open(os.path.join(EXPERIMENTS, name)) as f:
                m = json.load(f)
            self.assertEqual(artifacts.validate_experiment(m), [], name)
            self.assertIn(m['card'], S3_CARDS, name)
            self.assertIn(m['card'], CARDS, name)
            self.assertTrue(set(m['policies']) <= {'s3', 's3_cached'}, name)
            stopped = m['settings']['table'].get('max_parts_to_merge_at_once') == 1
            self.assertEqual(stopped, m['controls']['merges'] == 'stop', name)

    def test_every_manifest_has_a_valid_prediction_per_policy(self):
        for name in sorted(os.listdir(EXPERIMENTS)):
            m = manifest(name[:-5])
            for policy in m['policies']:
                pred = probes.prediction(m['experiment_id'], policy, R)
                self.assertEqual(artifacts.validate_prediction(pred), [], (name, policy))
                self.assertTrue(pred['source_refs'], name)
                for ref in pred['source_refs']:
                    self.assertTrue(ref['path'].startswith('tmp/src-26.8/'), ref)
                    with open(os.path.join(probes.REPO, ref['path'])) as f:
                        self.assertLessEqual(ref['line'], sum(1 for _ in f), ref)

    def test_the_brief_cases_exist(self):
        ids = {n[:-5] for n in os.listdir(EXPERIMENTS)}
        for exp in ('objects-per-part.narrow-compact', 'objects-per-part.narrow-wide', 'objects-per-part.wide-compact',
                    'objects-per-part.wide-wide', 'objects-per-part.types-fit', 'objects-per-part.types-holdout',
                    'big-insert-multipart.multipart', 'mutation.wide-update-one-column',
                    'mutation.compact-update-one-column', 'mutation.wide-delete', 'mutation.lightweight-delete',
                    'drop.partition', 'drop.ttl', 'drop.table-sync', 'cold-select.cache-off', 'cold-select.cache-on',
                    'cold-select.cache-off-types', 'startup.intact'):
            self.assertIn(exp, ids)
        # Cut by owner directive D14: no manifest.
        self.assertFalse({'detach-attach.default', 'cache-eviction.2x'} & ids)


class PartTypeTest(unittest.TestCase):
    def test_predicate_uses_registry_thresholds_and_table_overrides(self):
        reg = registry()
        self.assertEqual(shapes.predicted_part_type(reg, {}, 10 * MIB - 1, 100), 'Compact')
        self.assertEqual(shapes.predicted_part_type(reg, {}, 10 * MIB, 100), 'Wide')
        self.assertEqual(shapes.predicted_part_type(reg, {'min_bytes_for_wide_part': 0}, 1, 1), 'Wide')
        self.assertEqual(shapes.predicted_part_type(registry(min_rows_for_wide_part=1000), {}, 20 * MIB, 999), 'Compact')
        self.assertEqual(shapes.predicted_part_type(reg, {}, 20 * MIB, 10, level=0), 'Wide')
        self.assertEqual(shapes.predicted_part_type(registry(min_level_for_wide_part=1), {}, 20 * MIB, 10), 'Compact')

    def test_insert_part_type_rejects_bounds_straddling_the_threshold(self):
        reg = registry()
        table = {'name': 't', 'row_bytes_bounds': [100, 1000]}
        self.assertEqual(shapes.insert_part_type(reg, table, {}, 1000), 'Compact')
        self.assertEqual(shapes.insert_part_type(reg, table, {}, 200000), 'Wide')
        with self.assertRaises(ValueError):
            shapes.insert_part_type(reg, table, {}, 50000)

    def test_shape_manifests_land_on_the_type_their_name_says(self):
        reg = shapes.load_registry()
        for case, want in (('narrow-compact', 'Compact'), ('narrow-wide', 'Wide'), ('wide-compact', 'Compact'),
                           ('wide-wide', 'Wide')):
            m = manifest(f'objects-per-part.{case}')
            types = CARDS['objects-per-part'].expected_types(ctx_for(m, FakeCluster()))
            self.assertEqual({t[2] for t in types}, {want}, case)
        fit = CARDS['objects-per-part'].expected_types(ctx_for(manifest('objects-per-part.types-fit'), FakeCluster()))
        self.assertEqual(sorted(t[2] for t in fit), ['Compact', 'Wide'])
        self.assertIsNotNone(reg)

    def test_part_type_assertion_fails_when_system_parts_disagrees(self):
        card, m = CARDS['objects-per-part'], manifest('objects-per-part.narrow-compact')
        ctx = ctx_for(m, FakeCluster())
        card.state(ctx).update(rows=[300000])
        good = {'part_types': [['t', 100000, 'Compact']] * 3, 'objects_total': 33, 'object_writes': 33}
        draft = {'ledger': {'foreground': {'r1': {'PutObject': {'attempts': 33}}}}, 'observations': good,
                 'window': {'crossing': []}}
        out = card.assertions(ctx, draft)
        self.assertEqual(out['part_types.registry_predicate'][0], out['part_types.registry_predicate'][1])
        self.assertTrue(all(e == a for e, a in out.values()), {k: v for k, v in out.items() if v[0] != v[1]})
        bad = dict(good, part_types=[['t', 100000, 'Wide']] * 3, object_writes=32)
        out = card.assertions(ctx, dict(draft, observations=bad))
        self.assertNotEqual(*out['part_types.registry_predicate'])
        self.assertNotEqual(*out['object_writes==objects_total'])


class LayoutTest(unittest.TestCase):
    def test_layout_attributes_files_to_table_and_part_including_projections(self):
        rows = [{'local_path': 'store/abc/abc-1/all_1_1_0/checksums.txt', 'remote_path': 'p/r1/aaa', 'size': 10},
                {'local_path': 'store/abc/abc-1/all_1_1_0/p.proj/data.bin', 'remote_path': 'p/r1/bbb', 'size': 20},
                {'local_path': 'store/def/def-2/all_1_1_0/data.bin', 'remote_path': 'p/r1/ccc', 'size': 30},
                {'local_path': 'store/zzz/zzz-9/all_1_1_0/data.bin', 'remote_path': 'p/r1/ddd', 'size': 40}]
        text = ''.join(json.dumps(r) + '\n' for r in rows)
        card, m = CARDS['objects-per-part'], manifest('objects-per-part.narrow-compact')
        ctx = ctx_for(m, FakeCluster({'remote_data_paths': text}))
        parts = [{'table': 'tc', 'name': 'all_1_1_0', 'path': '/srv/disks/s3/store/abc/abc-1/all_1_1_0/'},
                 {'table': 'tw', 'name': 'all_1_1_0', 'path': '/srv/disks/s3/store/def/def-2/all_1_1_0/'}]
        layout = card.snapshot_layout(ctx, parts)
        self.assertEqual(layout, {'tc/all_1_1_0': {'checksums.txt': ['p/r1/aaa', 10], 'p.proj/data.bin': ['p/r1/bbb', 20]},
                                  'tw/all_1_1_0': {'data.bin': ['p/r1/ccc', 30]}})
        self.assertEqual(card.object_counts(layout, parts), {'tc/all_1_1_0': 2, 'tw/all_1_1_0': 1})


class MetadataTest(unittest.TestCase):
    def test_metadata_walk_counts_files_of_the_listed_part_directories_only(self):
        import tempfile
        with tempfile.TemporaryDirectory() as root:
            for rel, size in (('store/a/u1/all_1_1_0/checksums.txt', 100), ('store/a/u1/all_1_1_0/p.proj/data.bin', 50),
                              ('store/a/u1/all_2_2_0/count.txt', 70)):
                os.makedirs(os.path.dirname(os.path.join(root, rel)), exist_ok=True)
                with open(os.path.join(root, rel), 'wb') as f:
                    f.write(b'x' * size)
            got = shapes.allocated(root, ['store/a/u1/all_1_1_0/'])
            self.assertEqual((got['files'], got['bytes']), (2, 150))
            self.assertGreaterEqual(got['allocated'], 0)

    def test_metadata_ratios_are_per_object_and_absent_without_metadata(self):
        out = shapes.metadata_ratios({'files': 22, 'bytes': 2200, 'allocated': 90112}, 22, 44)
        self.assertEqual(out, {'metadata_files_per_object': 1.0, 'metadata_bytes_per_object': 100.0,
                               'metadata_allocated_per_object': 4096.0, 'write_syscalls_per_object': 2.0})
        self.assertEqual(shapes.metadata_ratios(None, 22, 44), {})


class SizeHistogramTest(unittest.TestCase):
    def test_buckets_cover_the_upload_buffer_boundaries(self):
        h = shapes.size_histogram([10, 4096, 4097, 1 << 20, (1 << 20) + 1, 32 << 20, (32 << 20) + 1, 1 << 40])
        self.assertEqual(h, {'le_4KiB': 0.25, '4KiB_1MiB': 0.25, '1MiB_32MiB': 0.25, 'gt_32MiB': 0.25})
        self.assertEqual(shapes.size_histogram([]), dict.fromkeys(h, 0.0))


class MultipartTest(unittest.TestCase):
    def test_multipart_assertions_hold_for_one_upload_of_two_or_more_parts(self):
        ops = {'CreateMultipartUpload': {'attempts': 1}, 'UploadPart': {'attempts': 3},
               'CompleteMultipartUpload': {'attempts': 1}}
        out = shapes.multipart_assertions(ops, {'multipart_files': ['s.bin']}, 's.bin')
        self.assertTrue(all(e == a for e, a in out.values()), out)

    def test_multipart_assertions_fail_on_a_single_put_or_one_part(self):
        out = shapes.multipart_assertions({'PutObject': {'attempts': 15}}, {'multipart_files': []}, 's.bin')
        self.assertNotEqual(*out['CreateMultipartUpload==1'])
        self.assertNotEqual(*out['UploadPart>=2'])
        self.assertNotEqual(*out['multipart_files'])
        one = {'CreateMultipartUpload': {'attempts': 1}, 'UploadPart': {'attempts': 1},
               'CompleteMultipartUpload': {'attempts': 1}}
        self.assertNotEqual(*shapes.multipart_assertions(one, {'multipart_files': ['s.bin']}, 's.bin')['UploadPart>=2'])

    def test_multipart_prediction_follows_the_registry_buffer_policy(self):
        from yardstick.cost.cards import lifecycle
        reg = shapes.load_registry()
        d = lifecycle.defaults(reg)
        pred = probes.prediction('big-insert-multipart.multipart', 's3', R)
        size = pred['inputs']['s_bin_bytes']
        kind, parts = lifecycle.upload_requests(size, d['multipart_first_part_bytes'], d['multipart_next_part_bytes'])
        self.assertEqual(kind, 'multipart')
        self.assertEqual(pred['expected']['observations']['upload_part_sizes'], [parts])
        # 600 000 values of 100 bytes in 8192-row granules, one 25-byte frame per granule block (CODEC(NONE)).
        rows = manifest('big-insert-multipart.multipart')['params']['inserts'][0]['rows']
        self.assertEqual(size, rows * 100 + 25 * -(-rows // 8192))


if __name__ == '__main__':
    unittest.main()
