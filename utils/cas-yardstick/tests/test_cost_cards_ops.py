"""Fixture tests of the s3 operation cards (R2): mutation rewrites, drops, cold reads and startup."""
import unittest

from yardstick.cost.cards import CARDS, ops

from .test_cost_cards_shapes import FakeCluster, ctx_for, manifest


def layout(table, part, keys):
    return {f'{table}/{part}': {f'f{i}': [k, 1] for i, k in enumerate(keys)}}


def parts(table, names, part_type='Compact', active=1):
    return [{'table': table, 'name': n, 'part_type': part_type, 'rows': 1, 'level': 0, 'active': active,
             'partition_id': 'all'} for n in names]


class RewriteTest(unittest.TestCase):
    def test_compact_mutation_is_a_full_rewrite_by_object_count(self):
        before = layout('tc', 'all_1_1_0', ['a', 'b', 'c'])
        after = layout('tc', 'all_1_1_0_2', ['d', 'e', 'f'])
        view = ops.rewrite_view(before, after, parts('tc', ['all_1_1_0']), parts('tc', ['all_1_1_0_2']))
        self.assertEqual(view['tc'], {'active_parts': [['all_1_1_0_2', 'Compact']], 'objects': 3, 'new_objects': 3,
                                      'kept_objects': 0, 'removed_objects': 3})

    def test_wide_mutation_keeps_hard_linked_objects(self):
        before = layout('tw', 'all_1_1_0', ['a', 'b', 'c', 'd'])
        after = layout('tw', 'all_1_1_0_2', ['a', 'b', 'x', 'y'])
        view = ops.rewrite_view(before, after, parts('tw', ['all_1_1_0'], 'Wide'), parts('tw', ['all_1_1_0_2'], 'Wide'))
        self.assertEqual((view['tw']['kept_objects'], view['tw']['new_objects'], view['tw']['removed_objects']), (2, 2, 2))

    def test_mutation_assertions_flag_a_compact_part_that_kept_objects(self):
        card = CARDS['mutation']
        m = manifest('mutation.compact-update-one-column')
        ctx = ctx_for(m, FakeCluster())
        st = card.state(ctx)
        st.update(rows=[200000], parts_before=parts('tc', ['all_1_1_0', 'all_2_2_0']),
                  layout_before=layout('tc', 'all_1_1_0', ['a', 'b']), live_keys={'c', 'd'},
                  parts=parts('tc', ['all_1_1_0_3']), layout=layout('tc', 'all_1_1_0_3', ['c', 'd']))
        rewrite = {'tc': {'objects': 2, 'new_objects': 2, 'kept_objects': 0}}
        draft = {'ledger': {'foreground': {'r1': {'PutObject': {'attempts': 20}}}}, 'window': {'crossing': []},
                 'observations': {'rewrite': rewrite}}
        out = card.assertions(ctx, draft)
        for key in ('full_rewrite.tc.kept_objects', 'full_rewrite.tc.new_objects', 'source_part_type.tc',
                    'replaced_objects_deleted'):
            self.assertEqual(out[key][0], out[key][1], key)
        draft['observations']['rewrite'] = {'tc': {'objects': 2, 'new_objects': 1, 'kept_objects': 1}}
        st['live_keys'] = {'a', 'c', 'd'}
        out = card.assertions(ctx, draft)
        self.assertNotEqual(*out['full_rewrite.tc.kept_objects'])
        self.assertNotEqual(*out['full_rewrite.tc.new_objects'])
        self.assertNotEqual(*out['replaced_objects_deleted'])

    def test_every_mutation_case_covers_the_part_types_it_names(self):
        types = set()
        for case in ('wide-update-one-column', 'compact-update-one-column', 'wide-delete', 'lightweight-delete'):
            m = manifest(f'mutation.{case}')
            types |= {t['part_type'] for t in m['params']['tables']}
            self.assertTrue(m['params']['wait_outdated_removed'], case)
        self.assertEqual(types, {'Compact', 'Wide'})
        for case in ('wide-delete', 'lightweight-delete'):
            self.assertEqual({t['part_type'] for t in manifest(f'mutation.{case}')['params']['tables']},
                             {'Compact', 'Wide'}, case)


class DropTest(unittest.TestCase):
    def test_dropped_scope_selects_the_partition_and_asserts_its_objects_are_gone(self):
        card = CARDS['drop']
        ctx = ctx_for(manifest('drop.partition'), FakeCluster())
        st = card.state(ctx)
        before = {**layout('tp', '0_1_1_0', ['a', 'b']), **layout('tp', '1_2_2_0', ['c'])}
        pb = parts('tp', ['0_1_1_0']) + parts('tp', ['1_2_2_0'])
        pb[0]['partition_id'], pb[1]['partition_id'] = '0', '1'
        st.update(parts_before=pb, layout_before=before, rows=[100000], parts=[pb[1]], layout=layout('tp', '1_2_2_0', ['c']),
                  live_keys={'c'})
        self.assertEqual(card.dropped_keys(ctx), {'a', 'b'})
        draft = {'ledger': {'foreground': {'r1': {'DeleteObjects': {'attempts': 1, 'delete_keys': 2}}}},
                 'window': {'crossing': []}, 'observations': {}}
        out = card.assertions(ctx, draft)
        self.assertEqual(out['dropped_objects_deleted'], ([], []))
        st['live_keys'] = {'a', 'c'}
        self.assertNotEqual(*card.assertions(ctx, draft)['dropped_objects_deleted'])


class ColdSelectTest(unittest.TestCase):
    def test_cold_select_restarts_after_the_inserts_and_before_the_window(self):
        card, m = CARDS['cold-select'], manifest('cold-select.cache-off')
        cluster = FakeCluster()
        ctx = ctx_for(m, cluster)
        card.setup(ctx)
        log = cluster.clients[0].log
        inserts = [i for i, sql in enumerate(log) if sql.startswith('INSERT')]
        (stop, _, at_stop), (start, _, at_start) = cluster.events
        self.assertEqual((stop, start), ('stop', 'start'))
        self.assertGreater(at_stop, max(inserts))
        self.assertTrue(any('count()' in sql for sql in log[at_start:]))
        self.assertEqual(card.state(ctx)['restarted_before_window'], 1)
        n = len(log)
        card.measured(ctx)
        self.assertEqual(log[n:], [m['params']['query'].format(db='yardstick')])

    def test_cache_on_runs_its_warm_pass_after_the_restart(self):
        card, m = CARDS['cold-select'], manifest('cold-select.cache-on')
        cluster = FakeCluster()
        ctx = ctx_for(m, cluster, 's3_cached')
        card.setup(ctx)
        query = m['params']['query'].format(db='yardstick')
        after_start = cluster.clients[0].log[cluster.events[1][2]:]
        self.assertEqual(after_start.count(query), 1)

    def test_measuring_without_a_restart_fails(self):
        card, m = CARDS['cold-select'], manifest('cold-select.cache-off')
        ctx = ctx_for(m, FakeCluster())
        with self.assertRaises(RuntimeError):
            card.measured(ctx)


class StartupTest(unittest.TestCase):
    def test_startup_stops_and_starts_inside_the_window_and_records_seconds(self):
        card, m = CARDS['startup'], manifest('startup.intact')
        cluster = FakeCluster()
        ctx = ctx_for(m, cluster)
        card.setup(ctx)
        self.assertEqual(cluster.events, [])
        card.measured(ctx)
        self.assertEqual([e[0] for e in cluster.events], ['stop', 'start'])
        st = card.state(ctx)
        self.assertEqual(st['start_seconds'], 1.25)
        self.assertGreaterEqual(st['startup_seconds'], 0)


if __name__ == '__main__':
    unittest.main()


class WindowContentsTest(unittest.TestCase):
    def test_row_counts_run_after_the_window_before_the_drop(self):
        # A count() over lightweight-deleted parts reads `_row_exists`: it must not land in the measured window.
        card, m = CARDS['mutation'], manifest('mutation.lightweight-delete')
        cluster = FakeCluster({'FROM system.parts': '', 'remote_data_paths': ''})
        ctx = ctx_for(m, cluster)
        ctx.replay = lambda: type('B', (), {'upper': type('I', (), {'objects': {}})()})()
        card.complete(ctx)
        self.assertFalse([q for q in cluster.clients[0].log if q.startswith('SELECT count() FROM yardstick.')])
        card.teardown(ctx)
        log = cluster.clients[0].log
        counts = [i for i, q in enumerate(log) if q.startswith('SELECT count() FROM yardstick.')]
        drop = [i for i, q in enumerate(log) if q.startswith('DROP DATABASE')]
        self.assertEqual(len(counts), 2)
        self.assertLess(max(counts), min(drop))
        self.assertEqual(card.state(ctx)['rows'], [0])


class MergeOnlyTest(unittest.TestCase):
    def test_inserts_run_in_the_setup_and_the_window_holds_only_the_merge(self):
        card, m = CARDS['merge-only'], manifest('merge-only.narrow-compact-sources')
        cluster = FakeCluster({'FROM system.parts': '', 'remote_data_paths': ''})
        ctx = ctx_for(m, cluster)
        card.setup(ctx)
        log = cluster.clients[0].log
        self.assertEqual(sum(q.startswith('INSERT') for q in log), 8)
        n = len(log)
        card.measured(ctx)
        self.assertEqual(log[n:], ['OPTIMIZE TABLE yardstick.t FINAL'])
