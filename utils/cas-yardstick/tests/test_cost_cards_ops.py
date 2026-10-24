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


def no_metadata(card):
    """No local metadata directory: completion tests run on a fake pass directory."""
    from unittest import mock
    return mock.patch.object(card, 'metadata_files', return_value=None)


def no_ledger(card):
    """An empty ledger for card tests that run without a proxy."""
    import contextlib
    from unittest import mock
    stack = contextlib.ExitStack()
    stack.enter_context(mock.patch.object(card, 'last_seq', return_value=0))
    stack.enter_context(mock.patch.object(card, 'rows_after', return_value=[]))
    return stack


class ColdSelectTest(unittest.TestCase):
    def test_cold_select_restarts_after_the_inserts_and_before_the_window(self):
        card, m = CARDS['cold-select'], manifest('cold-select.cache-off')
        cluster = FakeCluster()
        ctx = ctx_for(m, cluster)
        with no_ledger(card):  # the order of statements is under test, not the ledger reads
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
        with no_ledger(card):
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
        with no_metadata(card):
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


class RemovalWaitTest(unittest.TestCase):
    def test_completion_waits_until_the_replay_shows_the_replaced_objects_deleted(self):
        # system.parts drops an outdated part before its DeleteObjects reply reaches the ledger: poll the replay.
        card, m = CARDS['mutation'], manifest('mutation.wide-update-one-column')
        ctx = ctx_for(m, FakeCluster({'FROM system.parts': '', 'remote_data_paths': ''}))
        st = card.state(ctx)
        st.update(layout_before=layout('tw', 'all_1_1_0', ['a', 'b']), parts_before=parts('tw', ['all_1_1_0'], 'Wide'))
        views = iter([{'a', 'b'}, {'a'}, set()])
        Inv = type('I', (), {})
        def replay():
            inv = Inv()
            inv.objects = {('bucket', k): None for k in next(views)}
            return type('B', (), {'upper': inv})()
        ctx.replay = replay
        with no_metadata(card):
            card.complete(ctx)
        self.assertEqual(st['live_keys'], set())

    def test_completion_gives_up_after_the_timeout_and_keeps_the_live_keys(self):
        card, m = CARDS['mutation'], manifest('mutation.wide-update-one-column')
        ctx = ctx_for(m, FakeCluster({'FROM system.parts': '', 'remote_data_paths': ''}))
        st = card.state(ctx)
        st.update(layout_before=layout('tw', 'all_1_1_0', ['a']), parts_before=parts('tw', ['all_1_1_0'], 'Wide'))
        Inv = type('I', (), {})
        def replay():
            inv = Inv()
            inv.objects = {('bucket', 'a'): None}
            return type('B', (), {'upper': inv})()
        ctx.replay = replay
        with no_metadata(card):
            card.complete(ctx, timeout_s=0)
        self.assertEqual(st['live_keys'], {'a'})


class PolicyTest(unittest.TestCase):
    """The shape and operation cards run on every rig policy; object-model checks only where a part file is one object."""

    CARD_NAMES = ('objects-per-part', 'big-insert-multipart', 'mutation', 'drop', 'cold-select', 'startup', 'merge-only')

    def test_every_rig_policy_is_declared_and_local_refuses_the_multipart_graph(self):
        from yardstick.config import COST_POLICIES
        for name in self.CARD_NAMES:
            card = CARDS[name]
            self.assertEqual(set(card.policies), set(COST_POLICIES), name)
            for policy in ('s3', 's3_cached', 'cas', 'cas_cached', 'local'):
                ok, why = card.supports(policy)
                if (name, policy) == ('big-insert-multipart', 'local'):
                    self.assertFalse(ok)
                    self.assertIn('Disks/DiskLocal.cpp:464', why)
                else:
                    self.assertTrue(ok, (name, policy, why))

    def mutation_state(self, policy):
        from unittest import mock
        card = CARDS['mutation']
        ctx = ctx_for(manifest('mutation.compact-update-one-column'), FakeCluster(), policy=policy)
        st = card.state(ctx)
        st.update(rows=[200000], parts_before=parts('tc', ['all_1_1_0', 'all_2_2_0']),
                  layout_before=layout('tc', 'all_1_1_0', ['a', 'b']), live_keys={'a', 'c', 'd'},
                  parts=parts('tc', ['all_1_1_0_3']), layout=layout('tc', 'all_1_1_0_3', ['c', 'd']))
        draft = {'ledger': {'foreground': {'r1': {'PutObject': {'attempts': 20}}}}, 'window': {'crossing': []},
                 'observations': {'rewrite': {'tc': {'objects': 2, 'new_objects': 2, 'kept_objects': 0}}}}
        s3_prediction = card.predicted(ctx_for(manifest('mutation.compact-update-one-column'), FakeCluster()))
        with mock.patch.object(card, 'predicted', return_value=s3_prediction):
            return card, ctx, card.assertions(ctx, draft)

    def test_replaced_objects_are_checked_only_on_object_per_file_disks(self):
        card, ctx, out = self.mutation_state('s3')
        self.assertEqual(card.expected_gone(ctx), {'a', 'b'})
        self.assertNotEqual(*out['replaced_objects_deleted'])  # 'a' is still live
        for policy in ('cas', 'local'):
            card, ctx, out = self.mutation_state(policy)
            self.assertEqual(card.expected_gone(ctx), set(), policy)
            self.assertNotIn('replaced_objects_deleted', out, policy)
            self.assertIn('full_rewrite.tc.kept_objects', out, policy)

    def test_dropped_objects_are_waited_for_only_on_object_per_file_disks(self):
        card = CARDS['drop']
        for policy, want in (('s3', {'a', 'b'}), ('s3plain', {'a', 'b'}), ('cas', set()), ('local', set())):
            ctx = ctx_for(manifest('drop.partition'), FakeCluster(), policy=policy)
            st = card.state(ctx)
            table = card.p(ctx)['dropped'].partition('/')[0]
            st.update(parts_before=[{'table': table, 'name': 'p_1_1_0', 'partition_id': card.p(ctx)['dropped'].partition('/')[2] or 'all'}],
                      layout_before=layout(table, 'p_1_1_0', ['a', 'b']))
            self.assertEqual(card.expected_gone(ctx), want, policy)


class ModeTest(unittest.TestCase):
    """A card checks counts in its own mode; a manifest that names another mode is refused, not silently ignored."""

    def test_setup_refuses_a_manifest_mode_the_card_does_not_run(self):
        card = CARDS['mutation']
        m = manifest('mutation.compact-update-one-column')
        self.assertEqual(m['mode'], card.mode)
        card.setup(ctx_for(m, FakeCluster({'FROM system.parts': '', 'remote_data_paths': ''})))
        other = dict(m, mode='stochastic' if card.mode == 'deterministic' else 'deterministic')
        with self.assertRaisesRegex(ValueError, 'mode'):
            card.setup(ctx_for(other, FakeCluster({'FROM system.parts': '', 'remote_data_paths': ''})))


class ManifestModeTest(unittest.TestCase):
    def test_every_s3_manifest_names_its_cards_mode(self):
        import glob
        import json
        import os
        names = sorted(glob.glob(os.path.join(os.path.dirname(manifest.__code__.co_filename), '..', '..', '..', 'docs',
                                              'superpowers', 'reports', '2026-10-24-storage-cost-s3', 'experiments', '*.json')))
        self.assertTrue(names)
        for name in names:
            with open(name) as f:
                m = json.load(f)
            self.assertEqual(m['mode'], CARDS[m['card']].mode, os.path.basename(name))


class SmallPartChecksTest(unittest.TestCase):
    def test_merge_only_asserts_the_result_part_type_from_the_registry_predicate(self):
        from unittest import mock
        card = CARDS['merge-only']
        ctx = ctx_for(manifest('merge-only.narrow-compact-1k'), FakeCluster())
        st = card.state(ctx)
        st.update(rows=[8000], parts_before=parts('t', [f'all_{i}_{i}_0' for i in range(1, 9)]),
                  parts=parts('t', ['all_1_8_1']), layout_before={}, layout={}, live_keys=set())
        st['parts_before'] = [dict(p, rows=1000) for p in st['parts_before']]
        draft = {'ledger': {'foreground': {'r1': {}}}, 'window': {'crossing': []},
                 'observations': {'results': [{'table': 't', 'name': 'all_1_8_1', 'part_type': 'Compact', 'rows': 8000}]}}
        with mock.patch.object(card, 'predicted', return_value={'expected': {'foreground': {}, 'observations': {}},
                                                                'tolerance': {'abs': {}, 'rel': {}}}):
            out = card.assertions(ctx, draft)
            self.assertEqual(*out['result_part_type.registry_predicate'])
            draft['observations']['results'][0]['part_type'] = 'Wide'
            out = card.assertions(ctx, draft)
            self.assertNotEqual(*out['result_part_type.registry_predicate'])

    def test_startup_seconds_are_bounded_by_the_case_timeout(self):
        from unittest import mock
        card = CARDS['startup']
        ctx = ctx_for(manifest('startup.intact'), FakeCluster())
        card.state(ctx).update(rows=[300000])
        with mock.patch.object(card, 'predicted', return_value={'expected': {'foreground': {}, 'observations': {}},
                                                                'tolerance': {'abs': {}, 'rel': {}}}):
            for seconds, ok in ((3.0, True), (0.0, False), (10 ** 6, False)):
                draft = {'ledger': {'foreground': {'r1': {}}}, 'window': {'crossing': []}, 'observations': {'startup_seconds': seconds}}
                got = card.assertions(ctx, draft)['startup_seconds.within_timeout']
                self.assertEqual(got[0] == got[1], ok, seconds)

    def test_ledger_reads_need_the_proxy(self):
        card = CARDS['cold-select']
        ctx = ctx_for(manifest('cold-select.cache-off'), FakeCluster())
        with self.assertRaises(RuntimeError):
            card.last_seq(ctx)
        with self.assertRaises(RuntimeError):
            card.rows_after(ctx, 0)


class MetadataFilesTest(unittest.TestCase):
    def test_missing_s3_meta_is_an_error_only_where_the_disk_keeps_local_metadata(self):
        card = CARDS['objects-per-part']
        parts = [{'path': '/x/store/abc/all_1_1_0/', 'active': 1}]
        for policy in ('s3', 's3_cached'):
            with self.assertRaisesRegex(RuntimeError, 's3_meta'):
                card.metadata_files(ctx_for(manifest('objects-per-part.narrow-compact'), FakeCluster(), policy=policy), parts)
        for policy in ('cas', 'local'):
            self.assertIsNone(card.metadata_files(ctx_for(manifest('objects-per-part.narrow-compact'), FakeCluster(), policy=policy), parts))


class CasInventoryOutsideTheWindowTest(unittest.TestCase):
    # On a CAS disk `system.remote_data_paths` reads the catalog, lists the table and probes roots: the snapshot
    # must not land in the measured window. Local-metadata disks keep it in `complete`.
    def run_case(self, name, policy):
        card, m = CARDS[name.split('.')[0]], manifest(name)
        cluster = FakeCluster({'FROM system.parts': '', 'remote_data_paths': ''})
        ctx = ctx_for(m, cluster, policy)
        with no_metadata(card):
            card.complete(ctx)
        log = cluster.clients[0].log
        in_window = [q for q in log if 'remote_data_paths' in q]
        card.teardown(ctx)
        snapshots = [i for i, q in enumerate(log) if 'remote_data_paths' in q]
        drop = [i for i, q in enumerate(log) if q.startswith('DROP DATABASE')]
        return card.state(ctx), in_window, snapshots, drop

    def test_cas_policies_snapshot_the_layout_in_the_teardown_before_the_drop(self):
        for name in ('cold-select.cache-off', 'startup.intact'):
            for policy in ('cas', 'cas_cached'):
                st, in_window, snapshots, drop = self.run_case(name, policy)
                self.assertEqual(in_window, [], (name, policy))
                self.assertEqual(len(snapshots), 1, (name, policy))
                self.assertLess(snapshots[0], min(drop), (name, policy))
                self.assertIn('layout', st)

    def test_s3_keeps_the_snapshot_in_complete(self):
        for name in ('cold-select.cache-off', 'startup.intact'):
            _, in_window, snapshots, _ = self.run_case(name, 's3')
            self.assertEqual(len(in_window), 1, name)
            self.assertEqual(len(snapshots), 1, name)
