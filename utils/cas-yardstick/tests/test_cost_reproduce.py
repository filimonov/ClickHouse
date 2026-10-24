import copy
import gzip
import json
import os
import tempfile
import unittest

from yardstick.cost import ledger as L
from yardstick.cost import reproduce

MID = 'run/s3/probe_s3_insert/gate/k1'


def rows():
    def att(seq, op, key, caller='r1', t=0, size=0, up='ok'):
        return {'kind': 'attempt', 'seq': seq, 'caller': caller, 'store_id': 's', 't_start_us': t, 't_end_us': t + 10,
                'bucket': 'b', 'key': key, 'op': op, 'upload_id': None, 'part_number': None, 'copy_source': None,
                'req_payload_bytes': size, 'resp_payload_bytes': 0, 'payload_sha256': None, 'req_headers': {},
                'downstream': {'outcome': 'delivered', 'status': 200},
                'upstream': {'outcome': up, 'http_status': 200, 's3_code': None},
                'response_body': {'delete_results': None, 'completed_parts': None}}
    mark = lambda seq, role, t: {'kind': 'mark', 'seq': seq, 'label': f'{role}@{MID}', 't_us': t}  # noqa: E731
    return [mark(1, 'setup_start', 0), att(2, 'PutObject', 'a', t=100, size=1000), mark(3, 'window_start', 1000),
            att(4, 'PutObject', 'b', t=2000, size=500), att(5, 'GetObject', 'b', caller='r2', t=3000),
            att(6, 'ListObjectsV2', '', caller='rig', t=3500), mark(7, 'window_end', 4000),
            att(8, 'DeleteObject', 'b', t=4500), mark(9, 'teardown_end', 5000)]


def record_for(raw):
    """What the runner would record for these rows (one family-less card)."""
    s = L.slice(copy.deepcopy(raw), {r: f'{r}@{MID}' for r in L.MARK_ROLES})
    p = s.attempts_by_phase
    bounds = L.replay(sorted(p['setup'] + p['window'] + p['teardown'], key=lambda a: a['seq']), L.Inventory(), [])
    work = lambda ph: [a for a in p[ph] if a['caller'] != 'rig']  # noqa: E731
    return {'measurement_id': MID, 'card': 'probe_s3_insert', 'policy': 's3', 'variant': 's3', 'status': 'ok',
            'window': {'t_start_us': 1000, 't_end_us': 4000},
            'ledger': {'foreground': L.summarize(work('window')), 'maintenance': {}, 'setup': L.summarize(work('setup')),
                       'teardown': L.summarize(work('teardown')),
                       'rig': L.summarize([a for a in p['window'] if a['caller'] == 'rig']).get('rig', {})},
            'inventory': {'byte_hours': L.byte_hours(bounds, 1000, 4000), 'reclassified': []}}


class ReproduceTest(unittest.TestCase):
    def test_matching_record_has_no_problems_and_tampering_is_caught(self):
        raw = rows()
        rec = record_for(raw)
        self.assertEqual(reproduce.problems(rec, raw), [])
        bad = copy.deepcopy(rec)
        bad['ledger']['foreground']['r1']['PutObject']['attempts'] = 2
        bad['inventory']['byte_hours']['active']['upper'] *= 2
        found = '\n'.join(reproduce.problems(bad, raw))
        self.assertIn('r1 PutObject attempts', found)
        self.assertIn('byte_hours.active.upper', found)

    def test_cards_choosing_families_by_policy_replay(self):
        # faults and replication pick their key families from the pass policy.
        from unittest import mock
        seen = []

        class ByPolicy:
            def inventory_families(self, ctx):
                seen.append((ctx.policy, ctx.variant))
                return []
        raw = rows()
        rec = {**record_for(raw), 'card': 'by-policy', 'policy': 'cas_cached', 'variant': 'cas'}
        with mock.patch.dict('yardstick.cost.cards.CARDS', {'by-policy': ByPolicy()}):
            self.assertEqual(reproduce.problems(rec, raw), [])
        self.assertEqual(seen, [('cas_cached', 'cas')])

    def test_maintenance_split_is_checked_as_integrated_usage(self):
        raw = rows()
        rec = record_for(raw)
        rec['ledger']['maintenance'] = {'r2': rec['ledger']['foreground'].pop('r2')}
        self.assertEqual(reproduce.problems(rec, raw), [])

    def test_cli_reads_gzip_slices(self):
        raw = rows()
        with tempfile.TemporaryDirectory() as tmp:
            rp, lp = os.path.join(tmp, 'r.json'), os.path.join(tmp, 'l.jsonl.gz')
            with open(rp, 'w') as f:
                json.dump(record_for(raw), f)
            with gzip.open(lp, 'wt') as f:
                f.writelines(json.dumps(r) + '\n' for r in raw)
            self.assertEqual(reproduce.main([rp, lp]), 0)

    def test_replay_that_raises_exits_2_with_a_line_on_stdout(self):
        import contextlib
        import io
        import subprocess
        import sys
        raw = rows()
        no_marks = [r for r in raw if r['kind'] != 'mark']
        with tempfile.TemporaryDirectory() as tmp:
            rp, lp = os.path.join(tmp, 'r.json'), os.path.join(tmp, 'l.jsonl')
            with open(rp, 'w') as f:
                json.dump(record_for(raw), f)
            with open(lp, 'w') as f:
                f.writelines(json.dumps(r) + '\n' for r in no_marks)
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                self.assertEqual(reproduce.main([rp, lp]), 2)
            self.assertTrue(out.getvalue().startswith('replay failed: '), out.getvalue())
            # The closure check runs it as a module: the exit code and the stdout line must survive that path.
            done = subprocess.run([sys.executable, '-m', 'yardstick.cost.reproduce', rp, lp], capture_output=True,
                                  text=True, cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            self.assertEqual(done.returncode, 2, done.stderr)
            self.assertTrue(done.stdout.startswith('replay failed: '), done.stdout)


if __name__ == '__main__':
    unittest.main()
