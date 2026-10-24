import unittest
from collections import defaultdict

from yardstick.cost import ledger
from yardstick.cost.ledger import Inventory

H = 3_600_000_000
FAMILIES = [
    {'name': 'meta', 'regex': r'^meta/', 'category': 'metadata', 'maintenance': False},
    {'name': 'gc', 'regex': r'^gc/', 'category': 'metadata', 'maintenance': True},
]
_seq = [0]


def att(op='PutObject', key='data/a', t=(0, 10), up='ok', down='delivered', caller='r1', **kw):
    _seq[0] += 1
    row = {'kind': 'attempt', 'seq': kw.pop('seq', _seq[0]), 'caller': caller, 'store_id': 's', 't_start_us': t[0],
           't_end_us': t[1], 'bucket': 'b', 'key': key, 'op': op, 'upload_id': None, 'part_number': None,
           'copy_source': None, 'req_payload_bytes': 0, 'resp_payload_bytes': 0, 'payload_sha256': None,
           'req_headers': {}, 'downstream': {'outcome': down, 'status': 200},
           'upstream': {'outcome': up, 'http_status': 200, 's3_code': None},
           'response_body': {'upload_id': None, 'etag': None, 'complete_error': None, 'completed_parts': None,
                             'delete_results': None, 'list': None}, 'injected_rule': None, 'phase': 'window'}
    body = kw.pop('body', {})
    row['response_body'].update(body)
    row.update(kw)
    return row


def put(key, size, t=(0, 10), **kw):
    return att('PutObject', key, t, req_payload_bytes=size, **kw)


def part(key, uid, n, size, t=(0, 10), **kw):
    return att('UploadPart', key, t, upload_id=uid, part_number=n, req_payload_bytes=size, **kw)


def complete(key, uid, parts, t=(20, 30), **kw):
    body = kw.pop('body', {})
    body.setdefault('completed_parts', [{'part_number': n, 'etag': 'e'} for n in parts])
    return att('CompleteMultipartUpload', key, t, upload_id=uid, body=body, **kw)


def rep(attempts, initial=None):
    return ledger.replay(attempts, initial or Inventory(), FAMILIES)


def mark(seq, label, t):
    return {'kind': 'mark', 'seq': seq, 'label': label, 't_us': t}


class ParseOperation(unittest.TestCase):
    def test_parse_operation_table(self):
        cases = [
            (('PUT', '/b/k', {}, {}), 'PutObject'),
            (('PUT', '/b/k', {}, {'X-Amz-Copy-Source': '/b/x'}), 'CopyObject'),
            (('PUT', '/b/k', {'uploadId': 'u', 'partNumber': '1'}, {}), 'UploadPart'),
            (('PUT', '/b/k', {'uploadId': 'u', 'partNumber': '1'}, {'x-amz-copy-source': '/b/x'}), 'UploadPartCopy'),
            (('POST', '/b/k', {'uploads': ''}, {}), 'CreateMultipartUpload'),
            (('POST', '/b/k', {'uploadId': 'u'}, {}), 'CompleteMultipartUpload'),
            (('POST', '/b', {'delete': ''}, {}), 'DeleteObjects'),
            (('DELETE', '/b/k', {'uploadId': 'u'}, {}), 'AbortMultipartUpload'),
            (('DELETE', '/b/k', {}, {}), 'DeleteObject'),
            (('GET', '/b', {'list-type': '2'}, {}), 'ListObjectsV2'),
            (('GET', '/b', {'uploads': ''}, {}), 'ListMultipartUploads'),
            (('GET', '/b/k', {'uploadId': 'u'}, {}), 'ListParts'),
            (('GET', '/b/k', {}, {}), 'GetObject'),
            (('HEAD', '/b/k', {}, {}), 'HeadObject'),
            (('HEAD', '/b', {}, {}), 'HeadBucket'),
            (('PUT', '/b', {}, {}), 'CreateBucket'),
            (('GET', '/', {}, {}), 'Other'),
        ]
        for args, op in cases:
            self.assertEqual(ledger.parse_operation(*args), op, args)


class Slicing(unittest.TestCase):
    MARKS = {'setup_start': 's0', 'window_start': 'w0', 'window_end': 'w1', 'teardown_end': 'e1'}

    def test_slice_phases_and_crossing_both_boundaries(self):
        rows = [att(seq=1, t=(0, 1)), mark(2, 's0', 10), att(seq=3, t=(11, 12)), att(seq=4, t=(15, 25)),
                mark(5, 'w0', 20), att(seq=6, t=(21, 22)), att(seq=7, t=(95, 105)), mark(8, 'w1', 100),
                att(seq=9, t=(101, 102)), att(seq=10, t=(103, None)), mark(11, 'e1', 200), att(seq=12, t=(201, 202))]
        s = ledger.slice(rows, self.MARKS)
        phases = {p: [a['seq'] for a in v] for p, v in s.attempts_by_phase.items()}
        self.assertEqual(phases, {'setup': [3, 4], 'window': [6, 7], 'teardown': [9, 10], 'outside': [1, 12]})
        self.assertEqual(s.crossing, [4, 7, 10])
        self.assertEqual(rows[3]['phase'], 'setup')

    def test_missing_mark_raises(self):
        with self.assertRaisesRegex(ValueError, "'w0'.*window_start"):
            ledger.slice([mark(1, 's0', 0)], self.MARKS)

    def test_duplicate_mark_raises(self):
        rows = [mark(1, 's0', 0), mark(2, 'w0', 1), mark(3, 'w0', 2), mark(4, 'w1', 3), mark(5, 'e1', 4)]
        with self.assertRaisesRegex(ValueError, "'w0'.*2 times"):
            ledger.slice(rows, self.MARKS)

    def test_misordered_marks_raise(self):
        rows = [mark(1, 's0', 0), mark(2, 'w1', 1), mark(3, 'w0', 2), mark(4, 'e1', 3)]
        with self.assertRaisesRegex(ValueError, 'out of order.*window_start=.w0.@3'):
            ledger.slice(rows, self.MARKS)


class Classify(unittest.TestCase):
    def test_classify_maintenance_by_family(self):
        a = [att(key='gc/x'), att(key='meta/x'), att(key='data/x'), att(key='gc/y', caller='rig')]
        out = ledger.classify(a, FAMILIES)
        self.assertEqual([x['key'] for x in out['maintenance']], ['gc/x'])
        self.assertEqual([x['key'] for x in out['foreground']], ['meta/x', 'data/x'])
        self.assertEqual([x['key'] for x in out['rig']], ['gc/y'])

    def test_classify_keyless_attempts(self):
        def dels(*keys):
            return att('DeleteObjects', None, body={'delete_results': [
                {'key': k, 'deleted': True, 'code': None} for k in keys]})
        a = [dels('gc/1', 'gc/2'), dels('gc/1', 'data/2'), dels(), att('ListObjectsV2', None)]
        out = ledger.classify(a, FAMILIES)
        self.assertEqual(out['maintenance'], [a[0]])
        self.assertEqual(out['foreground'], a[1:])


class Summarize(unittest.TestCase):
    def test_summarize_shape(self):
        a = [put('k', 5, resp_payload_bytes=1),
             put('k', 7, up='error', down='dropped'),
             att('ListObjectsV2', None, body={'list': {'key_count': 3, 'truncated': True}}),
             att('ListObjectsV2', None, body={'list': {'key_count': 2, 'truncated': False}}),
             att('DeleteObject', 'k'),
             att('DeleteObjects', None, body={'delete_results': [{'key': 'x', 'deleted': True, 'code': None},
                                                                  {'key': 'y', 'deleted': True, 'code': None}]}),
             put('z', 1, caller='rig', up='unknown', down='injected')]
        out = ledger.summarize(a)
        self.assertEqual(set(out), {'r1', 'rig'})
        put_c = out['r1']['PutObject']
        self.assertEqual(set(put_c), set(ledger.COUNT_FIELDS))
        self.assertEqual((put_c['attempts'], put_c['delivered'], put_c['dropped'], put_c['upstream_ok'],
                          put_c['upstream_error'], put_c['req_payload_bytes'], put_c['resp_payload_bytes']),
                         (2, 1, 1, 1, 1, 12, 1))
        lst = out['r1']['ListObjectsV2']
        self.assertEqual((lst['list_pages'], lst['list_keys']), (2, 5))
        self.assertEqual((out['r1']['DeleteObject']['delete_calls'], out['r1']['DeleteObject']['delete_keys']), (1, 1))
        self.assertEqual((out['r1']['DeleteObjects']['delete_calls'], out['r1']['DeleteObjects']['delete_keys']), (1, 2))
        rig = out['rig']['PutObject']
        self.assertEqual((rig['injected'], rig['upstream_unknown']), (1, 1))


class AcceptedDropped(unittest.TestCase):
    def test_upstream_ok_with_a_lost_reply_is_counted_apart(self):
        a = [put('k', 1, up='ok', down='dropped'), put('k', 1, up='unknown', down='dropped'), put('k', 1)]
        c = ledger.summarize(a)['r1']['PutObject']
        self.assertEqual((c['dropped'], c['accepted_dropped'], c['upstream_ok']), (2, 1, 2))


class ClientErrors(unittest.TestCase):
    def test_4xx_counted_apart_from_other_errors(self):
        a = [att('HeadObject', 'k', up='error', upstream={'outcome': 'error', 'http_status': 404, 's3_code': None}),
             att('HeadObject', 'k', up='error', upstream={'outcome': 'error', 'http_status': 503, 's3_code': 'SlowDown'}),
             att('HeadObject', 'k')]
        c = ledger.summarize(a)['r1']['HeadObject']
        self.assertEqual((c['upstream_error'], c['upstream_4xx'], c['upstream_ok']), (2, 1, 1))


class Creators(unittest.TestCase):
    def test_bounds_name_the_caller_that_created_each_identity(self):
        r = rep([put('a', 1, caller='r1'), put('b', 1, caller='r2'), put('a', 2, caller='r2'), put('c', 1, up='error')])
        self.assertEqual(r.creators, {('b', 'a'): 'r1', ('b', 'b'): 'r2'})


class Retries(unittest.TestCase):
    def failed(self, **kw):
        return put('k', 5, up='error', down='dropped', payload_sha256='h', **kw)

    def test_retry_correlation_identity_rules(self):
        base = self.failed(seq=1)
        # 1. same identity after a failure -> group
        self.assertEqual(ledger.correlate_retries([base, put('k', 5, payload_sha256='h', seq=2)]), [[1, 2]])
        # 2. different payload hash -> a new write, not a retry
        self.assertEqual(ledger.correlate_retries([base, put('k', 5, payload_sha256='other', seq=2)]), [])
        # 3. different caller
        self.assertEqual(ledger.correlate_retries([base, put('k', 5, payload_sha256='h', caller='r2', seq=2)]), [])
        # 4. different part number
        f = part('k', 'u', 1, 5, up='error', payload_sha256='h', seq=1)
        self.assertEqual(ledger.correlate_retries([f, part('k', 'u', 2, 5, payload_sha256='h', seq=2)]), [])
        # 5. different conditional header
        f = self.failed(seq=1, req_headers={'if-none-match': '*'})
        self.assertEqual(ledger.correlate_retries([f, put('k', 5, payload_sha256='h', seq=2)]), [])
        # 6. predecessor succeeded -> uncorrelated
        ok = put('k', 5, payload_sha256='h', seq=1)
        self.assertEqual(ledger.correlate_retries([ok, put('k', 5, payload_sha256='h', seq=2)]), [])

    def test_chain_and_volatile_headers(self):
        a = self.failed(seq=1, req_headers={'x-amz-date': '1'})
        b = put('k', 5, up='unknown', payload_sha256='h', seq=2, req_headers={'x-amz-date': '2'})
        c = put('k', 5, payload_sha256='h', seq=3, req_headers={'x-amz-date': '3'})
        d = put('k', 5, payload_sha256='h', seq=4)  # follows a success
        self.assertEqual(ledger.correlate_retries([d, c, b, a]), [[1, 2, 3]])


class Replay(unittest.TestCase):
    def test_replay_put_replaces_identity_only(self):
        r = rep([put('data/a', 5, (0, 10)), put('meta/b', 7, (5, 6)), put('data/a', 9, (20, 30))])
        for inv in (r.lower, r.upper):
            self.assertEqual({k: (v['size'], v['category']) for k, v in inv.objects.items()},
                             {('b', 'data/a'): (9, 'active'), ('b', 'meta/b'): (7, 'metadata')})
        self.assertEqual(r.lower.objects[('b', 'meta/b')]['family'], 'meta')

    def test_replay_complete_retires_all_parts_including_omitted_and_sums_listed(self):
        a = [part('k', 'u', 1, 4), part('k', 'u', 2, 6), part('k', 'u', 3, 100), part('k', 'other', 1, 9),
             complete('k', 'u', [1, 2])]
        r = rep(a)
        self.assertEqual(r.lower.objects[('b', 'k')]['size'], 10)
        self.assertFalse(r.lower.objects[('b', 'k')]['uncertain'])
        self.assertEqual(list(r.lower.incomplete), [('b', 'k', 'other', 1)])

    def test_complete_with_embedded_error_does_not_create_object(self):
        a = [part('k', 'u', 1, 4), complete('k', 'u', [1], body={'complete_error': 'InternalError'})]
        r = rep(a)
        self.assertEqual(r.lower.objects, {})
        self.assertIn(('b', 'k', 'u', 1), r.lower.incomplete)

    def test_replay_abort_retires_parts(self):
        a = [part('k', 'u', 1, 4), part('k', 'u', 2, 4), att('AbortMultipartUpload', 'k', (20, 30), upload_id='u')]
        self.assertEqual(rep(a).lower.incomplete, {})

    def test_batch_delete_absent_key_is_deleted(self):
        a = [put('x', 1), att('DeleteObjects', None, (20, 30), body={'delete_results': [
            {'key': 'x', 'deleted': True, 'code': None}, {'key': 'absent', 'deleted': True, 'code': None}]})]
        self.assertEqual(rep(a).lower.objects, {})

    def test_batch_delete_quiet_mode(self):
        # Quiet mode: the proxy reports unlisted keys as deleted with no code.
        a = [put('x', 1), put('y', 1), att('DeleteObjects', None, (20, 30), body={'delete_results': [
            {'key': 'x', 'deleted': True, 'code': None}, {'key': 'y', 'deleted': False, 'code': 'AccessDenied'}]})]
        self.assertEqual(list(rep(a).lower.objects), [('b', 'y')])

    def test_batch_delete_per_key_error_keeps_object(self):
        a = [put('x', 1), att('DeleteObjects', None, (20, 30), body={'delete_results': [
            {'key': 'x', 'deleted': False, 'code': 'InternalError'}]})]
        self.assertIn(('b', 'x'), rep(a).upper.objects)

    def test_unknown_outcome_bounds_differ(self):
        a = [put('x', 1), att('DeleteObject', 'x', (20, 30), up='unknown'), put('y', 2, up='unknown'),
             put('z', 3, up='error'), put('w', 3, up='not_sent'), put('v', 3, down='rejected', up='not_sent')]
        r = rep(a)
        self.assertEqual(set(r.lower.objects), set())          # unknown delete applies, unknown put does not
        self.assertEqual(set(r.upper.objects), {('b', 'x'), ('b', 'y')})   # the reverse
        self.assertTrue(r.upper.objects[('b', 'y')]['outcome_unknown'])
        self.assertFalse(r.upper.objects[('b', 'x')].get('outcome_unknown', False))

    def test_time_bounds_creation_late_in_lower_retirement_early_in_lower(self):
        r = rep([put('x', 1, (100, 200))])
        self.assertEqual(r.lower.objects[('b', 'x')]['created_us'], 200)
        self.assertEqual(r.upper.objects[('b', 'x')]['created_us'], 100)
        # Delete at t_start in lower, t_end in upper: the object is gone earlier in lower.
        r = rep([put('x', H, (0, 0)), att('DeleteObject', 'x', (H, 2 * H))])
        out = ledger.byte_hours(r, 0, 3 * H)
        self.assertAlmostEqual(out['active']['lower'], H)
        self.assertAlmostEqual(out['active']['upper'], 2 * H)

    def test_complete_creates_late_and_retires_early_in_lower(self):
        a = [part('k', 'u', 1, H, (0, 0)), complete('k', 'u', [1], t=(H, 2 * H))]
        r = rep(a)
        out = ledger.byte_hours(r, 0, 3 * H)
        # lower: part 0..H, object 2H..3H; upper: part 0..2H, object H..3H
        self.assertAlmostEqual(out['incomplete']['lower'], H)
        self.assertAlmostEqual(out['incomplete']['upper'], 2 * H)
        self.assertAlmostEqual(out['active']['lower'], H)
        self.assertAlmostEqual(out['active']['upper'], 2 * H)
        self.assertEqual(r.lower.objects[('b', 'k')]['size'], H)   # size survives retire-before-create

    def test_overlapping_put_and_delete_may_invert_replays(self):
        a = [put('x', 1, (0, 10)), att('DeleteObject', 'x', (50, 60)), put('x', 2, (55, 70))]
        r = rep(a)
        # The delete or the second put can land last: lower ends absent, upper with the 2 B object.
        self.assertNotIn(('b', 'x'), r.lower.objects)
        self.assertEqual(r.upper.objects[('b', 'x')]['size'], 2)
        out = ledger.byte_hours(r, 0, 10 * H)
        self.assertLessEqual(out['active']['lower'], out['active']['upper'])

    def test_lower_never_exceeds_upper(self):
        a = [put('data/a', 5, (0, 10)), put('meta/b', 7, (5, 6)), put('data/u', 9, (8, 12), up='unknown'),
             put('data/d', 4, (1, 2)), att('DeleteObject', 'data/d', (H, H + 5)),
             put('data/e', 6, (3, 4)), att('DeleteObject', 'data/e', (H, H + 5), up='unknown'),
             put('data/o', 1, (0, 10)), att('DeleteObject', 'data/o', (50, 60)), put('data/o', 2, (55, 70)),
             part('data/m', 'u', 1, 3, (0, 1)), part('data/m', 'u', 2, 5, (0, 1)), part('data/gone', 'v', 1, 8, (0, 1)),
             att('AbortMultipartUpload', 'data/gone', (2 * H, 2 * H + 3), upload_id='v'),
             complete('data/m', 'u', [1, 2], t=(H, H + 20)),
             part('data/p', 'w', 1, 2, (0, 1), up='unknown')]
        out = ledger.byte_hours(rep(a), 0, 4 * H)
        self.assertGreater(out['active']['upper'], 0)
        for cat, v in out.items():
            self.assertLessEqual(v['lower'], v['upper'], cat)

    def test_copy_size_from_source_identity_else_uncertain(self):
        src = {'bucket': 'b', 'key': 'src', 'range': None}
        a = [put('src', 42), att('CopyObject', 'dst', (20, 30), copy_source=src),
             att('CopyObject', 'dst2', (20, 30), copy_source={'bucket': 'b', 'key': 'missing', 'range': None})]
        r = rep(a)
        for inv in (r.lower, r.upper):
            self.assertEqual((inv.objects[('b', 'dst')]['size'], inv.objects[('b', 'dst')]['uncertain']), (42, False))
            self.assertTrue(inv.objects[('b', 'dst2')]['uncertain'])

    def test_initial_inventory_not_mutated(self):
        init = Inventory(objects={('b', 'x'): {'size': 3, 'created_us': 0, 'family': None, 'category': 'active',
                                               'uncertain': False}})
        r = rep([att('DeleteObject', 'x', (5, 6))], init)
        self.assertIn(('b', 'x'), init.objects)
        self.assertEqual(r.lower.objects, {})


class ByteHours(unittest.TestCase):
    def test_byte_hours_lower_upper_clipped(self):
        # Object size is H bytes, so byte-hours equal the lifetime in microseconds.
        a = [put('data/x', H, (0, 100)), att('DeleteObject', 'data/x', (3 * H, 3 * H + 10))]
        r = rep(a)
        out = ledger.byte_hours(r, H, 2 * H)            # window fully inside the lifetime
        self.assertAlmostEqual(out['active']['lower'], H)
        self.assertAlmostEqual(out['active']['upper'], H)
        out = ledger.byte_hours(r, 0, 10 * H)           # lower: 100..3H, upper: 0..3H+10
        self.assertAlmostEqual(out['active']['lower'], 3 * H - 100, places=3)
        self.assertAlmostEqual(out['active']['upper'], 3 * H + 10, places=3)
        self.assertEqual(out['incomplete'], {'lower': 0.0, 'upper': 0.0})

    def test_unknown_delete_shortens_lower_only(self):
        r = rep([put('x', H, (0, 0)), att('DeleteObject', 'x', (H, H), up='unknown')])
        out = ledger.byte_hours(r, 0, 4 * H)
        self.assertAlmostEqual(out['active']['lower'], H)
        self.assertAlmostEqual(out['active']['upper'], 4 * H)

    def test_incomplete_and_uncertain_categories(self):
        a = [part('k', 'u', 1, H, (0, 0)), att('CopyObject', 'd', (0, 0),
                                              copy_source={'bucket': 'b', 'key': 'nope', 'range': None})]
        out = ledger.byte_hours(rep(a), 0, H)
        self.assertAlmostEqual(out['incomplete']['lower'], H)
        self.assertEqual(out['uncertain']['lower'], 0.0)

    def test_initial_objects_count_from_creation(self):
        init = Inventory(objects={('b', 'x'): {'size': H, 'created_us': 0, 'family': None, 'category': 'active',
                                               'uncertain': False}})
        out = ledger.byte_hours(rep([], init), 0, 2 * H)
        self.assertAlmostEqual(out['active']['lower'], 2 * H)

    def test_reclassify_outdated_then_garbage(self):
        r = rep([put('data/x', H, (0, 0))])
        r.reclassify([('b', 'data/x')], 'outdated', H, evidence='superseded')
        r.reclassify([('b', 'data/x')], 'garbage', 2 * H, evidence='unreferenced')
        out = ledger.byte_hours(r, 0, 3 * H)
        for bound in ('lower', 'upper'):
            self.assertAlmostEqual(out['active'][bound], H)
            self.assertAlmostEqual(out['outdated'][bound], H)
            self.assertAlmostEqual(out['garbage'][bound], H)
        self.assertEqual(r.lower.objects[('b', 'data/x')]['category'], 'garbage')


class ByteHourBounds(unittest.TestCase):
    """Bounds follow each transition's effect on bytes, not its kind (shrinks early in lower, growths late)."""

    def test_overwrite_shrink_then_grow(self):
        # 100 B over hours 0..5, overwritten with 1 B during [1, 2], back to 100 B during [3, 4].
        a = [put('x', 100, (0, 0)), put('x', 1, (H, 2 * H)), put('x', 100, (3 * H, 4 * H))]
        out = ledger.byte_hours(rep(a), 0, 5 * H)
        self.assertAlmostEqual(out['active']['lower'], 203)
        self.assertAlmostEqual(out['active']['upper'], 401)

    def test_overwrite_growing(self):
        a = [put('x', 1, (0, 0)), put('x', 100, (H, 2 * H))]
        out = ledger.byte_hours(rep(a), 0, 3 * H)
        self.assertAlmostEqual(out['active']['lower'], 2 * 1 + 100)    # grows at 2H
        self.assertAlmostEqual(out['active']['upper'], 1 + 2 * 100)    # grows at H

    def test_overwrite_shrinking(self):
        a = [put('x', 100, (0, 0)), put('x', 1, (H, 2 * H))]
        out = ledger.byte_hours(rep(a), 0, 3 * H)
        self.assertAlmostEqual(out['active']['lower'], 100 + 2 * 1)    # shrinks at H
        self.assertAlmostEqual(out['active']['upper'], 2 * 100 + 1)    # shrinks at 2H

    def test_overlapping_overwrites_land_in_either_order(self):
        # Reviewer's case: 10 B, then 1 B during [1, 3] and 50 B during [2, 4]. The 50 B PUT can land at 2.5 h and
        # the 1 B PUT at 2.8 h: 42.2 byte-hours and a final size of 1 B, although the 50 B PUT started later.
        a = [put('x', 10, (0, 0)), put('x', 1, (H, 3 * H)), put('x', 50, (2 * H, 4 * H))]
        r = rep(a)
        out = ledger.byte_hours(r, 0, 5 * H)['active']
        real = 10 * 2.5 + 50 * 0.3 + 1 * 2.2
        self.assertAlmostEqual(real, 42.2)
        self.assertLessEqual(out['lower'], real)
        self.assertGreaterEqual(out['upper'], real)
        self.assertAlmostEqual(out['lower'], 10 + 1 + 1 + 1 + 1)
        self.assertAlmostEqual(out['upper'], 10 + 10 + 50 + 50 + 50)
        self.assertEqual(r.lower.objects[('b', 'x')]['size'], 1)   # either PUT can be the last to land
        self.assertEqual(r.upper.objects[('b', 'x')]['size'], 50)

    def test_unknown_overwrite(self):
        # An unknown shrink counts in lower only; an unknown growth in upper only.
        shrink = ledger.byte_hours(rep([put('x', 100, (0, 0)), put('x', 1, (H, H), up='unknown')]), 0, 2 * H)
        self.assertAlmostEqual(shrink['active']['lower'], 101)
        self.assertAlmostEqual(shrink['active']['upper'], 200)
        grow = ledger.byte_hours(rep([put('x', 1, (0, 0)), put('x', 100, (H, H), up='unknown')]), 0, 2 * H)
        self.assertAlmostEqual(grow['active']['lower'], 2)
        self.assertAlmostEqual(grow['active']['upper'], 101)

    def test_every_schedule_lies_inside_the_bounds(self):
        # Brute force: every ordered choice of effect times inside each attempt's interval stays within the bounds.
        a = [put('x', 7, (0, H)), put('x', 3, (H // 2, 2 * H)), att('DeleteObject', 'x', (H, 3 * H)),
             put('x', 9, (2 * H, 4 * H))]
        out = ledger.byte_hours(rep(a), 0, 5 * H)['active']
        grid = [i * H // 4 for i in range(21)]
        sizes = [7, 3, 0, 9]
        import itertools
        choices = [[t for t in grid if x['t_start_us'] <= t <= x['t_end_us']] for x in a]
        # Every schedule, in any order the effect times allow: the last effect at or before t decides the size.
        for times in itertools.product(*choices):
            total, prev, size = 0, 0, 0
            for t, s in sorted(zip(times, sizes)) + [(5 * H, None)]:
                total += size * (t - prev) / H
                prev, size = t, (s if s is not None else size)
            self.assertLessEqual(out['lower'] - 1e-9, total, times)
            self.assertGreaterEqual(out['upper'] + 1e-9, total, times)


    def test_every_schedule_with_unknown_transitions_lies_inside_bounds_and_end_states(self):
        # Brute force with `unknown` transitions (they may never land) and the end states: lower holds the smallest
        # final size and is absent when any schedule ends absent; upper the largest, present when any ends present.
        a = [put('x', 7, (0, H)), put('x', 0, (H // 2, 2 * H)), att('DeleteObject', 'x', (H, 3 * H), up='unknown'),
             put('x', 9, (2 * H, 4 * H), up='unknown'), put('x', 4, (5 * H // 2, 3 * H))]
        r = rep(a)
        out = ledger.byte_hours(r, 0, 5 * H)['active']
        grid = [i * H // 4 for i in range(21)]
        sizes = [7, 0, None, 9, 4]
        import itertools
        choices = [[t for t in grid if x['t_start_us'] <= t <= x['t_end_us']]
                   + ([None] if x['upstream']['outcome'] == 'unknown' else []) for x in a]
        finals = set()
        for times in itertools.product(*choices):
            landed = [(t, i) for i, t in enumerate(times) if t is not None]
            for order in (sorted(landed), sorted(landed, key=lambda x: (x[0], -x[1]))):
                total, prev, state = 0, 0, None
                for t, i in order + [(5 * H, -1)]:
                    total += (state or 0) * (t - prev) / H
                    prev = t
                    if i >= 0:
                        state = sizes[i]
                self.assertLessEqual(out['lower'] - 1e-9, total, times)
                self.assertGreaterEqual(out['upper'] + 1e-9, total, times)
                finals.add(state)
        lo, hi = r.lower.objects.get(('b', 'x')), r.upper.objects.get(('b', 'x'))
        self.assertIn(None, finals)
        self.assertIsNone(lo)                               # some schedule ends absent
        self.assertEqual(hi['size'], max(f for f in finals if f is not None))

    def test_zero_byte_tie_breaks_toward_absent_in_lower_and_present_in_upper(self):
        # A zero-byte PUT and a DELETE overlap: either may land last. Both end states hold 0 bytes; lower must take
        # the absent one and upper the present one, never the other way round.
        r = rep([put('x', 0, (0, 2 * H)), att('DeleteObject', 'x', (H, 3 * H))])
        self.assertNotIn(('b', 'x'), r.lower.objects)
        self.assertEqual(r.upper.objects[('b', 'x')]['size'], 0)
        r = rep([att('DeleteObject', 'x', (0, 2 * H)), put('x', 0, (H, 3 * H))])
        self.assertNotIn(('b', 'x'), r.lower.objects)
        self.assertEqual(r.upper.objects[('b', 'x')]['size'], 0)

    def test_sweep_matches_the_pointwise_definition(self):
        # The sweep must give exactly the bounds of testing every state with `_possible` at every interval.
        import random
        rng = random.Random(7)

        def reference(transitions, cats, t0, t1, sign):
            totals = defaultdict(float)
            states = [None] + [tr[3] for tr in transitions]
            earliest = [float('-inf')] + [tr[0] for tr in transitions]
            points = sorted({t0, t1} | {t for tr in transitions for t in tr[:2] if t0 < t < t1}
                            | {tc for tc, _ in cats if t0 < tc < t1})
            for lo, hi in zip(points, points[1:]):
                mid = (lo + hi) / 2
                values = defaultdict(list)
                possible = [k for k in range(len(transitions) + 1) if ledger._possible(transitions, k - 1, mid)]
                for k in possible:
                    if states[k] is None:
                        continue
                    size, category, uncertain = states[k]
                    for tc, c in cats:
                        if earliest[k] <= tc <= mid:
                            category = c
                    values['uncertain' if uncertain else category].append(size)
                for category, sz in values.items():
                    pick = max(sz) if sign > 0 else (min(sz) if len(sz) == len(possible) else 0)
                    totals[category] += pick * (hi - lo) / ledger.US_PER_HOUR
            return totals

        for case in range(300):
            n = rng.randint(1, 7)
            trs = []
            for _ in range(n):
                ts = rng.randint(0, 20) * H // 4
                te = ts + rng.randint(0, 8) * H // 4
                state = None if rng.random() < 0.25 else (rng.choice([0, 1, 5, 9]), 'active', rng.random() < 0.1)
                trs.append((ts, te, rng.random() < 0.8, state))
            trs.sort(key=lambda tr: tr[0])
            cats = sorted([(rng.randint(0, 20) * H // 4, 'garbage')] if rng.random() < 0.3 else [])
            for sign in (-1, 1):
                got = defaultdict(float)
                ledger._envelope(trs, cats, 0, 6 * H, got, sign)
                want = reference(trs, cats, 0, 6 * H, sign)
                for cat in set(got) | set(want):
                    self.assertAlmostEqual(got[cat], want[cat], places=6, msg=(case, sign, trs, cats))

    def test_many_overwrites_of_one_key_are_fast(self):
        # A 4 h run with 10 s lease renewals: 1440 overwrites of one key.
        import time as _time
        a = [put('x', 100 + i % 7, (i * 10_000_000, i * 10_000_000 + 50_000)) for i in range(1440)]
        start = _time.monotonic()
        r = rep(a)
        out = ledger.byte_hours(r, 0, 1440 * 10_000_000)
        self.assertLess(_time.monotonic() - start, 2.0)
        self.assertLessEqual(out['active']['lower'], out['active']['upper'])


class BatchDeleteKeys(unittest.TestCase):
    def test_unknown_batch_delete_retires_requested_keys_in_lower_only(self):
        a = [put('x', H, (0, 0)), att('DeleteObjects', '', (H, H), up='unknown', delete_keys=['x'])]
        r = rep(a)
        self.assertEqual(r.lower.objects, {})
        self.assertIn(('b', 'x'), r.upper.objects)
        out = ledger.byte_hours(r, 0, 3 * H)
        self.assertAlmostEqual(out['active']['lower'], H)
        self.assertAlmostEqual(out['active']['upper'], 3 * H)

    def test_classify_treats_empty_key_as_keyless(self):
        a = att('DeleteObjects', '', delete_keys=['gc/1', 'gc/2'])
        self.assertEqual(ledger.classify([a], FAMILIES)['maintenance'], [a])
        b = att('DeleteObjects', '', body={'delete_results': [{'key': 'gc/1', 'deleted': True, 'code': None}]})
        self.assertEqual(ledger.classify([b], FAMILIES)['maintenance'], [b])


class RetryIdentity(unittest.TestCase):
    def test_range_and_query_are_part_of_the_identity(self):
        f = att('GetObject', 'k', up='error', down='dropped', seq=1, req_headers={'range': 'bytes=0-9'})
        other = att('GetObject', 'k', seq=2, req_headers={'range': 'bytes=10-19'})
        same = att('GetObject', 'k', seq=3, req_headers={'range': 'bytes=0-9'})
        self.assertEqual(ledger.correlate_retries([f, other]), [])
        self.assertEqual(ledger.correlate_retries([f, same]), [[1, 3]])
        page1 = att('ListObjectsV2', '', up='error', seq=4, query={'list-type': '2', 'prefix': 'p/'})
        page2 = att('ListObjectsV2', '', seq=5, query={'list-type': '2', 'prefix': 'p/', 'continuation-token': 't'})
        self.assertEqual(ledger.correlate_retries([page1, page2]), [])


class Reclassified(unittest.TestCase):
    def test_bounds_log_each_reclassification_for_the_record(self):
        r = rep([put('data/x', H, (0, 0))])
        r.reclassify([('b', 'data/x')], 'garbage', H, evidence='DROP TABLE SYNC')
        self.assertEqual(r.reclassified, [{'category': 'garbage', 't_us': H, 'evidence': 'DROP TABLE SYNC',
                                           'keys': [['b', 'data/x']]}])


class CheckRows(unittest.TestCase):
    def test_check_rows_reports_problems(self):
        good = put('x', 1, seq=1)
        bad = [put('x', 1, seq=1), put('x', 1, seq=1, t=(10, 5)), put('x', 1, seq=2, t=(1, None)),
               att('DeleteObjects', None, seq=3), complete('k', 'u', [1], seq=4, body={'completed_parts': None})]
        self.assertEqual(ledger.check_rows([good]), [])
        text = '\n'.join(ledger.check_rows(bad))
        for needle in ('duplicate', 't_end_us before', 'unfinished', 'without delete_results', 'without completed_parts'):
            self.assertIn(needle, text)


if __name__ == '__main__':
    unittest.main()
