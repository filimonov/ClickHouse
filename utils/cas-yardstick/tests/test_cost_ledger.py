import unittest

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
        with self.assertRaises(ValueError):
            ledger.slice([mark(1, 's0', 0)], self.MARKS)


class Classify(unittest.TestCase):
    def test_classify_maintenance_by_family(self):
        a = [att(key='gc/x'), att(key='meta/x'), att(key='data/x'), att(key='gc/y', caller='rig')]
        out = ledger.classify(a, FAMILIES)
        self.assertEqual([x['key'] for x in out['maintenance']], ['gc/x'])
        self.assertEqual([x['key'] for x in out['foreground']], ['meta/x', 'data/x'])
        self.assertEqual([x['key'] for x in out['rig']], ['gc/y'])


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
        self.assertEqual(set(r.lower.objects), {('b', 'x')})
        self.assertEqual(set(r.upper.objects), {('b', 'y')})

    def test_time_bounds_lower_at_end_upper_at_start(self):
        r = rep([put('x', 1, (100, 200))])
        self.assertEqual(r.lower.objects[('b', 'x')]['created_us'], 200)
        self.assertEqual(r.upper.objects[('b', 'x')]['created_us'], 100)
        # Delete overlapping a put: ordering is decided per bound.
        r = rep([put('x', 1, (0, 10)), att('DeleteObject', 'x', (50, 60)), put('x', 2, (55, 70))])
        self.assertEqual(r.lower.objects[('b', 'x')]['size'], 2)   # delete at 60, put at 70
        self.assertEqual(r.upper.objects[('b', 'x')]['size'], 2)   # delete at 50, put at 55

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
        a = [put('data/x', H, (0, 100)), att('DeleteObject', 'data/x', (3 * H, 3 * H + 10)),
             att('DeleteObject', 'data/x', (9 * H, 9 * H), up='unknown')]
        r = rep(a)
        out = ledger.byte_hours(r, H, 2 * H)           # window fully inside the lifetime of the object
        self.assertAlmostEqual(out['active']['lower'], H * 1.0)
        self.assertAlmostEqual(out['active']['upper'], H * 1.0)
        out = ledger.byte_hours(r, 0, 10 * H)           # object lives 0..3h (lower: 100us..3h+10us)
        self.assertAlmostEqual(out['active']['lower'], H * (3 * H + 10 - 100) / H, places=3)
        self.assertAlmostEqual(out['active']['upper'], H * (3 * H - 0) / H, places=3)
        self.assertEqual(out['incomplete'], {'lower': 0.0, 'upper': 0.0})

    def test_unknown_delete_shortens_upper_only(self):
        r = rep([put('x', H, (0, 0)), att('DeleteObject', 'x', (H, H), up='unknown')])
        out = ledger.byte_hours(r, 0, 4 * H)
        self.assertAlmostEqual(out['active']['lower'], 4 * H)
        self.assertAlmostEqual(out['active']['upper'], H)

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
