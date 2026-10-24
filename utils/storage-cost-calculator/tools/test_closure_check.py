import contextlib
import copy
import io
import json
import os
import tempfile
import unittest
from unittest import mock

import closure_check as cc

HERE = os.path.dirname(os.path.abspath(__file__))
EXAMPLES = os.path.join(HERE, '..', '..', '..', 'docs', 'superpowers', 'cost-model', 'schemas', 'examples')


def run_main(args):
    """Run `main` with its report and notes captured, so test output stays clean."""
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        return cc.main(args)


def example(name):
    with open(os.path.join(EXAMPLES, name)) as f:
        return json.load(f)


def quantity(qid, kind='input', scope='cluster', **kw):
    q = {'id': qid, 'block': 'simple', 'label': qid, 'kind': kind, 'scope': scope, 'unit': '', 'value_type': 'number',
         'enum': None, 'table': None, 'default': 1, 'rule': None, 'coef': None, 'validity': None,
         'evidence': {'kind': 'assumed', 'ref': 'test', 'version': '1'}, 'hint': None, 'status': 'ok'}
    q.update(kw)
    return q


def registry(*quantities, precedence=()):
    return {'schema': 'registry/1', 'version': '1', 'quantities': list(quantities), 'sets': {}, 'precedence': list(precedence)}


def inp(qid):
    return {'kind': 'input', 'id': qid, 'dim': 'count'}


def coef(qid):
    return {'kind': 'coef', 'id': qid, 'dim': 'count'}


def formulas(row_scope, expr, variant='s3', op='insert'):
    row = {'row_type': 'request', 'op_kind': 'PutObject', 'outcome_class': 'ok', 'category': None, 'route': None,
           'scope': row_scope, 'basis': 'predicted', 'expr': expr}
    return {'schema': 'formulas/1', 'owner': 'test', 'variant': variant,
            'cells': {op: {'ledger_rows': [row], 'allocation': None, 'states': [], 'notes': []}}}


class DependencyTest(unittest.TestCase):
    def setUp(self):
        self.reg = registry(
            quantity('parts'), quantity('hit', scope='replica'), quantity('replicas'),
            quantity('shape', value_type='enum', enum=['narrow', 'wide'], default='narrow'),
            quantity('opp', kind='measured', value_type='table', default=None, table={'index': ['shape'], 'dim': 'count'}))

    def test_dependency_unbound_var_scope_and_table_arity(self):
        self.assertEqual(cc.dependency(self.reg, [formulas('variant', inp('parts'))]), [])
        unbound = cc.dependency(self.reg, [formulas('variant', {'kind': 'var', 'name': 'x', 'dim': 'count'})])
        self.assertTrue(any('sum_over' in p for p in unbound), unbound)
        replica_in_cluster = cc.dependency(self.reg, [formulas('variant', inp('hit'))])
        self.assertTrue(any('replica' in p for p in replica_in_cluster), replica_in_cluster)
        expanded = {'kind': 'mul', 'dim': 'count', 'a': inp('hit'), 'b': inp('replicas')}
        self.assertEqual(cc.dependency(self.reg, [formulas('variant', expanded)]), [])
        lookup = {'kind': 'lookup', 'dim': 'count', 'table_id': 'opp', 'keys': [inp('shape'), inp('shape')]}
        self.assertTrue(any('arity' in p for p in cc.dependency(self.reg, [formulas('variant', lookup)])))
        self.assertTrue(any('unknown' in p for p in cc.dependency(self.reg, [formulas('variant', inp('nope'))])))

    def test_dependency_cycle_and_default_outside_validity(self):
        a = quantity('a', kind='derivation', default=None, rule=inp('b'))
        b = quantity('b', kind='derivation', default=None, rule=inp('a'))
        self.assertTrue(any('cycle' in p for p in cc.dependency(registry(a, b), [])))
        bad = quantity('r', default=9, validity={'r': [1, 6]})
        self.assertTrue(any('validity' in p for p in cc.dependency(registry(bad), [])))
        dup = registry(quantity('x'), quantity('x'))
        self.assertTrue(cc.dependency(dup, []))


class SupportTest(unittest.TestCase):
    def coverage(self, case_status='planned', evidence_status='done'):
        case = {'case_id': 'c', 'policies': ['s3'], 'replicas': 1, 'scale': 1.0, 'repeats': 3, 'timeout_s': 900, 'duration_s': 120,
                'priority': 'P1', 'status': case_status}
        return {'schema': 'coverage/1', 'version': '1',
                'cards': [{'name': 'card', 'family': 'fam', 'model_variants': ['s3'], 'cases': [case]}],
                'evidence': [{'coefficient': 'k', 'experiment_id': 'card.c', 'policy': 's3', 'span': {}, 'role': 'fit',
                              'prediction_commit': None, 'measurement_ids': [], 'status': evidence_status}],
                'domain': {'variants': ['s3'], 'operations': ['insert', 'merge'], 'families': ['fam']}, 'excluded': []}

    def test_support_enumerates_domain_and_cut_rule(self):
        reg = registry(quantity('k', kind='measured', default=None))
        cov = self.coverage()
        problems = cc.support(reg, cov, [formulas('variant', coef('k'), op='insert')], require_formulas=True)
        self.assertTrue(any('s3/merge' in p for p in problems), problems)
        both = formulas('variant', coef('k'), op='insert')
        both['cells']['merge'] = both['cells']['insert']
        self.assertEqual(cc.support(reg, cov, [both], require_formulas=True), [])
        self.assertEqual(cc.support(reg, cov, [], require_formulas=False), [])
        cut = self.coverage('cut')
        live = cc.support(registry(quantity('k', kind='measured', default=None)), cut, [], require_formulas=False)
        self.assertTrue(any('k' in p and 'unsupported' in p for p in live), live)
        nonzero = cc.support(registry(quantity('k', kind='measured', default=5, status='unsupported')), cut, [], require_formulas=False)
        self.assertTrue(any('non-zero' in p for p in nonzero), nonzero)
        ok = cc.support(registry(quantity('k', kind='measured', default=None, status='unsupported')), cut, [], require_formulas=False)
        self.assertEqual(ok, [])
        stray = self.coverage()
        stray['cards'][0]['family'] = 'other'
        self.assertTrue(cc.support(reg, stray, [], require_formulas=False))


    def test_support_cells_evidence_or_not_supported(self):
        def case(cid, policies=('s3', 'cas')):
            return {'case_id': cid, 'policies': list(policies), 'replicas': 1, 'scale': 1.0, 'repeats': 3, 'timeout_s': 900, 'duration_s': 120,
                    'priority': 'P1', 'status': 'planned'}
        cov = {'schema': 'coverage/1', 'version': '1', 'domain': {'variants': ['s3', 'cas'], 'operations': ['x', 'y'], 'families': ['f', 'g']},
               'cards': [{'name': 'cf', 'family': 'f', 'model_variants': ['s3', 'cas'], 'cases': [case('c')]},
                         {'name': 'cg', 'family': 'g', 'model_variants': ['s3'], 'cases': [case('c', ('s3',))]}],
               'evidence': [{'coefficient': k, 'experiment_id': e, 'policy': 's3', 'span': {}, 'role': 'fit', 'prediction_commit': None,
                             'measurement_ids': [], 'status': 'done'} for k, e in (('kf', 'cf.c'), ('kg', 'cg.c'))],
               'excluded': []}
        # s3: x uses kf and kg; y uses kf only. A cell needs completed evidence for every coefficient it uses, so
        # s3/x has none for f (kg is only measured by a family-g card) and none for g (kf only by family f).
        # cas: x is unconditionally not_supported; y uses kf (evidence for f, none for g; card cg does not cover cas).
        s3 = formulas('variant', {'kind': 'add', 'dim': 'count', 'a': coef('kf'), 'b': coef('kg')}, variant='s3', op='x')
        s3['cells']['y'] = formulas('variant', coef('kf'), variant='s3', op='y')['cells']['y']
        cas = formulas('variant', coef('kf'), variant='cas', op='y')
        cas['cells']['x'] = {'ledger_rows': [], 'allocation': None, 'notes': [],
                             'states': [{'id': 'not_supported', 'cond': True, 'reason': 'no such operation'}]}
        pending = []
        self.assertEqual(cc.support(registry(), cov, [s3, cas], pending=pending), [])
        self.assertEqual(sorted(pending), [('cas', 'y', 'g'), ('s3', 'x', 'f'), ('s3', 'x', 'g'), ('s3', 'y', 'g')])
        final = cc.support(registry(), cov, [s3, cas], require_formulas=True)
        self.assertEqual(len(final), 4)
        self.assertTrue(any('s3/y/g' in p for p in final) and any('cas/y/g' in p for p in final), final)
        conditional = copy.deepcopy(cas)
        conditional['cells']['x']['states'][0]['cond'] = {'kind': 'const', 'value': 1, 'dim': 'bool'}
        self.assertEqual(len(cc.support(registry(), cov, [s3, conditional], require_formulas=True)), 6)

    def test_card_variant_without_a_measuring_policy_is_flagged(self):
        cov = self.coverage()
        cov['cards'][0]['model_variants'] = ['s3', 'cas']
        cov['domain']['variants'] = ['s3', 'cas']
        problems = cc.support(registry(), cov, [], require_formulas=False)
        self.assertTrue(any('cas is not measured by any case policy' in p for p in problems), problems)

    def test_planned_evidence_backs_nothing_and_cuts_reach_cells(self):
        reg = registry(quantity('k', kind='measured', default=None))
        cell = formulas('variant', coef('k'), op='insert')
        cell['cells']['merge'] = cell['cells']['insert']
        planned = cc.support(reg, self.coverage(evidence_status='planned'), [cell], require_formulas=True)
        self.assertTrue(any('s3/insert/fam' in p for p in planned), planned)
        cut = cc.support(registry(quantity('k', kind='measured', default=None, status='unsupported')),
                         self.coverage('cut'), [cell], require_formulas=False)
        self.assertTrue(any('s3/insert depends on cut coefficient k' in p for p in cut), cut)
        unsupported = copy.deepcopy(cell)
        for c in unsupported['cells'].values():
            c['states'] = [{'id': 'not_supported', 'cond': True, 'reason': 'cut'}]
        self.assertEqual(cc.support(registry(quantity('k', kind='measured', default=None, status='unsupported')),
                                    self.coverage('cut'), [unsupported], require_formulas=False), [])


class EvidenceTest(unittest.TestCase):
    def row(self, role, exp, span, mids, status='done'):
        return {'coefficient': 'k', 'experiment_id': exp, 'policy': 's3', 'span': span, 'role': role,
                'prediction_commit': 'abc' if role != 'fit' else None, 'measurement_ids': mids, 'status': status}

    def cov(self, *rows):
        case = {'case_id': 'c1', 'policies': ['s3'], 'replicas': 1, 'scale': 1.0, 'repeats': 3, 'timeout_s': 900, 'duration_s': 120,
                'priority': 'P1', 'status': 'planned'}
        case2 = dict(case, case_id='c2')
        return {'schema': 'coverage/1', 'version': '1', 'cards': [{'name': 'card', 'family': 'f', 'model_variants': ['s3'], 'cases': [case, case2]}],
                'evidence': list(rows), 'domain': {'variants': ['s3'], 'operations': ['insert'], 'families': ['f']}, 'excluded': []}

    coefficient = {'id': 'k', 'kind': 'scalar', 'dim': 'count', 'value': 1, 'validity': {}, 'tolerance': {},
                   'evidence': {'kind': 'measured', 'ref': 'report', 'version': '1', 'measurement_ids': ['m1'],
                                'binary_sha256': 'a' * 64, 'commit': 'abc'}, 'status': 'ok'}

    def test_evidence_distinct_holdout(self):
        fit = self.row('fit', 'card.c1', {'x': [0, 1]}, ['m1'])
        same = self.row('holdout', 'card.c1', {'x': [0, 1]}, ['m1'])
        self.assertTrue(any('overlaps the fit data' in p for p in cc.evidence(self.cov(fit, same), [self.coefficient])))
        other = self.row('holdout', 'card.c2', {'x': [1, 2]}, ['m2'])
        self.assertEqual(cc.evidence(self.cov(fit, other), [self.coefficient]), [])
        self.assertTrue(any('holdout' in p for p in cc.evidence(self.cov(fit), [self.coefficient])))
        planned = self.row('holdout', 'card.c2', {'x': [1, 2]}, [], status='planned')
        self.assertTrue(cc.evidence(self.cov(fit, planned), [self.coefficient]))
        bare = copy.deepcopy(self.coefficient)
        del bare['evidence']['commit']
        self.assertTrue(any('commit' in p for p in cc.evidence(self.cov(fit, other), [bare])))
        pinned = {'id': 'p', 'kind': 'scalar', 'dim': 'bytes', 'value': 1, 'validity': {}, 'tolerance': {},
                  'evidence': {'kind': 'source', 'ref': 'src/Foo.cpp', 'version': 'abc'}, 'status': 'ok'}
        self.assertTrue(any('path:line' in p for p in cc.evidence(self.cov(), [pinned])))
        pinned['evidence']['ref'] = 'src/Foo.cpp:12'
        self.assertEqual(cc.evidence(self.cov(), [pinned]), [])

    def test_holdout_disjoint_from_every_fit_validity_and_resolvable_ids(self):
        fit1 = self.row('fit', 'card.c1', {'x': [0, 1]}, ['m1'])
        fit2 = self.row('fit', 'card.c1', {'x': [5, 6]}, ['m3'])
        overlap = self.row('holdout', 'card.c2', {'x': [1, 2]}, ['m2', 'm3'])
        found = cc.evidence(self.cov(fit1, fit2, overlap), [self.coefficient])
        self.assertTrue(any('overlaps the fit data' in p for p in found), found)
        other = self.row('holdout', 'card.c2', {'x': [1, 2]}, ['m2'])
        self.assertEqual(cc.evidence(self.cov(fit1, fit2, other), [self.coefficient], measurement_ids={'m1', 'm2', 'm3'}), [])
        self.assertTrue(any('m2 does not resolve' in p for p in
                            cc.evidence(self.cov(fit1, other), [self.coefficient], measurement_ids={'m1'})))
        nodomain = dict(self.coefficient, validity=None)
        self.assertTrue(any('validity domain' in p for p in cc.evidence(self.cov(fit1, other), [nodomain])))


class AccountingTest(unittest.TestCase):
    def test_accounting_exactness_and_bounds(self):
        m, p = example('measurement-ok.json'), example('prediction.json')
        self.assertEqual(cc.accounting([m], [p], node_test_cmd=['true']), [])
        off = copy.deepcopy(m)
        off['ledger']['foreground']['r1']['PutObject']['attempts'] = 4
        self.assertTrue(any('PutObject' in x for x in cc.accounting([off], [p], node_test_cmd=['true'])))
        extra = copy.deepcopy(m)
        extra['ledger']['foreground']['r1']['DeleteObject'] = copy.deepcopy(m['ledger']['foreground']['r1']['PutObject'])
        self.assertTrue(any('DeleteObject' in x for x in cc.accounting([extra], [p], node_test_cmd=['true'])))
        inverted = copy.deepcopy(m)
        inverted['inventory']['byte_hours']['active'] = {'lower': 2.0, 'upper': 1.0}
        self.assertTrue(any('lower' in x for x in cc.accounting([inverted], [p], node_test_cmd=['true'])))
        self.assertTrue(any('prediction' in x for x in cc.accounting([m], [], node_test_cmd=['true'])))
        self.assertTrue(any('node' in x.lower() for x in cc.accounting([m], [p], node_test_cmd=['false'])))
        stochastic = copy.deepcopy(off)
        stochastic['mode'] = 'stochastic'
        loose = copy.deepcopy(p)
        loose['tolerance'] = {'abs': {'PutObject': 1}, 'rel': {}}
        self.assertEqual(cc.accounting([stochastic], [loose], node_test_cmd=['true']), [])

    def test_observation_bounds_and_distributions_follow_the_card_check(self):
        self.assertTrue(cc.observation_holds({'min': 1, 'max': 2}, 1.5))
        self.assertFalse(cc.observation_holds({'min': 1, 'max': 2}, 2.5))
        self.assertFalse(cc.observation_holds({'min': 1}, None))
        self.assertTrue(cc.observation_holds({'reference': {'a': 0.5, 'b': 0.5}, 'max_tv': 0.1}, {'a': 0.55, 'b': 0.45}))
        self.assertFalse(cc.observation_holds({'reference': {'a': 0.5, 'b': 0.5}, 'max_tv': 0.1}, {'a': 0.7, 'b': 0.3}))
        self.assertTrue(cc.observation_holds([1, 2], [1, 2]))
        self.assertFalse(cc.observation_holds({'x': 1}, {'x': 2}))
        m, p = example('measurement-ok.json'), example('prediction.json')
        bounded = copy.deepcopy(p)
        bounded['expected']['observations'] = {'ratio': {'min': 0.5, 'max': 1.5}}
        inside, outside = copy.deepcopy(m), copy.deepcopy(m)
        inside['observations']['ratio'], outside['observations']['ratio'] = 1.0, 2.0
        self.assertEqual(cc.accounting([inside], [bounded], node_test_cmd=['true']), [])
        self.assertTrue(any('ratio' in x for x in cc.accounting([outside], [bounded], node_test_cmd=['true'])))


REPORT = os.path.join(HERE, '..', '..', '..', 'docs', 'superpowers', 'reports', '2026-10-24-storage-cost-phase0')


class AccountingReplayTest(unittest.TestCase):
    def gate(self, card):
        with open(os.path.join(REPORT, 'measurements.jsonl')) as f:
            record = next(r for r in map(json.loads, f) if r['card'] == card)
        return record, cc.raw_slices([REPORT])[record['measurement_id']]

    def test_gate_record_replays_from_its_raw_slice_and_a_tampered_one_does_not(self):
        record, raw = self.gate('probe_multipart_fault')
        self.assertEqual(cc.replay_problems(record, raw), [])
        bad = copy.deepcopy(record)
        bad['ledger']['foreground']['r1']['UploadPart']['attempts'] += 1
        bad['inventory']['byte_hours']['incomplete']['upper'] *= 1.5
        found = '\n'.join(cc.replay_problems(bad, raw))
        self.assertIn('UploadPart attempts', found)
        self.assertIn('byte_hours.incomplete.upper', found)

    def test_raw_slice_of_a_run_wins_over_the_card_slice(self):
        # Every run's ledger restarts its seq numbers, so a card measured in two runs keeps one slice per run.
        with tempfile.TemporaryDirectory() as d:
            records = [{'measurement_id': f'{run}/s3/c/x/k1', 'run_id': run, 'card': 'c'} for run in ('r-a', 'r-b', 'r-c')]
            with open(os.path.join(d, 'measurements.jsonl'), 'w') as f:
                f.writelines(json.dumps(r) + '\n' for r in records)
            for sub in ('c', 'c/r-a', 'c/r-b'):
                os.makedirs(os.path.join(d, 'raw', sub), exist_ok=True)
                open(os.path.join(d, 'raw', sub, 'ledger.jsonl.gz'), 'w').close()
            found = {k: str(v) for k, v in cc.raw_slices([d]).items()}
        self.assertTrue(found['r-a/s3/c/x/k1'].endswith('raw/c/r-a/ledger.jsonl.gz'))
        self.assertTrue(found['r-b/s3/c/x/k1'].endswith('raw/c/r-b/ledger.jsonl.gz'))
        self.assertTrue(found['r-c/s3/c/x/k1'].endswith('raw/c/ledger.jsonl.gz'))

    def test_final_mode_needs_a_raw_slice_and_the_pricing_tests_are_the_checked_in_ones(self):
        m, p = example('measurement-ok.json'), example('prediction.json')
        self.assertTrue(any('raw ledger slice' in x for x in cc.accounting([m], [p], node_test_cmd=['true'], mode='final', raw={})))
        self.assertTrue(str(cc.NODE_TESTS).endswith('utils/storage-cost-calculator/test'))
        self.assertTrue(list(cc.NODE_TESTS.glob('billing.test.js')))


class BillingTest(unittest.TestCase):
    def seed(self):
        with open(os.path.join(HERE, '..', '..', '..', 'docs', 'superpowers', 'cost-model', 'billing-contract.json')) as f:
            return json.load(f)

    def test_billing_seed_passes(self):
        self.assertEqual(cc.billing(self.seed()), [])

    def test_billing_detects_missing_url_bad_date_and_unexplained_null(self):
        missing = self.seed()
        del missing['presets'][0]['request_classes'][0]['source_url']
        self.assertTrue(any('source_url' in p for p in cc.billing(missing)))
        bad_date = self.seed()
        bad_date['presets'][0]['storage']['checked'] = '24/10/2026'
        self.assertTrue(any('storage' in p and 'checked' in p for p in cc.billing(bad_date)))
        null = self.seed()
        internet = next(t for t in null['presets'][0]['transfer'] if t['id'] == 'internet')
        del internet['note']
        self.assertTrue(any('note' in p for p in cc.billing(null)))

    def test_billing_units_enum_and_evidence(self):
        bad = self.seed()
        bad['presets'][0]['units'] = 'gib'
        self.assertTrue(any('units' in p for p in cc.billing(bad)))
        decimal = self.seed()
        decimal['presets'][0]['units'] = 'decimal'
        self.assertEqual(cc.billing(decimal), [])
        bare = self.seed()
        del bare['presets'][0]['units_evidence']
        self.assertTrue(any('units_evidence' in p for p in cc.billing(bare)))


class ReproductionTest(unittest.TestCase):
    def test_reproduction_stale_copy(self):
        with tempfile.TemporaryDirectory() as d:
            src, gen = os.path.join(d, 'src.json'), os.path.join(d, 'gen.json')
            for path, text in ((src, '{"a": 1}'), (gen, '{"a": 1}')):
                with open(path, 'w') as f:
                    f.write(text)
            self.assertEqual(cc.reproduction(copies=[(src, gen)]), [])
            with open(gen, 'w') as f:
                f.write('{"a": 2}')
            self.assertTrue(any('stale' in p for p in cc.reproduction(copies=[(src, gen)])))
            raw = os.path.join(d, 'raw')
            os.mkdir(raw)
            with open(os.path.join(raw, 'a.txt'), 'w') as f:
                f.write('hello')
            sums = os.path.join(raw, 'SHA256SUMS')
            with open(sums, 'w') as f:
                f.write('2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824  ./a.txt\n')
            self.assertEqual(cc.reproduction(sums=[sums]), [])
            with open(os.path.join(raw, 'a.txt'), 'w') as f:
                f.write('HELLO')
            self.assertTrue(any('a.txt' in p for p in cc.reproduction(sums=[sums])))

    def test_identities_agree_within_a_report_and_drills_render(self):
        m = example('measurement-ok.json')
        other = dict(m, rig_commit='0' * 40)
        self.assertTrue(any('rig_commit' in p for p in cc.identities([m, other])))
        self.assertEqual(cc.identities([m, dict(m)]), [])
        with tempfile.TemporaryDirectory() as d:
            os.makedirs(os.path.join(d, 'raw', 'p'))
            with open(os.path.join(d, 'raw', 'p', 'drill.json'), 'w') as f:
                json.dump({'probe': 'p', 'cells': {}, 'mismatches': ['x']}, f)
            self.assertTrue(any('render' in p for p in cc.rerender([d])))
        self.assertEqual(cc.rerender([REPORT]), [])


class NodeEnvTest(unittest.TestCase):
    """Dev mode lets the Node suite run before measured fixtures exist; final mode requires them."""

    def env_for(self, mode, outer):
        seen = {}

        def fake_run(cmd, **kw):
            seen.update(kw.get("env") or {})
            return mock.Mock(returncode=0, stdout="", stderr="")

        with mock.patch.dict(os.environ, outer, clear=False), mock.patch.object(cc.subprocess, "run", fake_run):
            if "SCC_DEV" not in outer:
                os.environ.pop("SCC_DEV", None)
            self.assertEqual(cc._node_fixtures(["node", "--test"], mode), [])
        return seen

    def test_dev_mode_sets_scc_dev(self):
        self.assertEqual(self.env_for("dev", {}).get("SCC_DEV"), "1")

    def test_final_mode_clears_scc_dev(self):
        env = self.env_for("final", {"SCC_DEV": "1"})
        self.assertIn("PATH", env)
        self.assertNotIn("SCC_DEV", env)


class MainTest(unittest.TestCase):
    def test_final_mode_fails_on_missing_artifact(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(run_main(['--mode', 'final', '--cost-model', d, '--reports', os.path.join(d, 'none-*')]), 1)
            self.assertEqual(run_main(['--mode', 'dev', '--cost-model', d, '--reports', os.path.join(d, 'none-*')]), 0)

    def test_malformed_inputs_are_reported_not_raised(self):
        with tempfile.TemporaryDirectory() as d:
            cm, report = os.path.join(d, 'cm'), os.path.join(d, 'rep-1')
            os.makedirs(cm)
            os.makedirs(report)
            open(os.path.join(report, 'README.md'), 'w').close()  # a report directory has a README
            with open(os.path.join(cm, 'formulas-s3.json'), 'w') as f:
                f.write('{"schema": ')
            with open(os.path.join(cm, 'inputs-registry.json'), 'w') as f:
                f.write('not json')
            with open(os.path.join(report, 'measurements.jsonl'), 'w') as f:
                f.write(json.dumps(example('measurement-ok.json')) + '\n{broken\n[1]\n')
            err = io.StringIO()
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(err):
                code = cc.main(['--mode', 'dev', '--cost-model', cm, '--reports', os.path.join(d, 'rep-*')])
            self.assertEqual(code, 1)
            text = err.getvalue()
            self.assertIn('formulas-s3.json: cannot load', text)
            self.assertIn('inputs-registry.json: cannot load', text)
            self.assertIn('measurements.jsonl:2: bad JSON line', text)
            self.assertIn('measurements.jsonl:3: expected a JSON object', text)

    def test_default_reports_glob_takes_any_date(self):
        import fnmatch
        pattern = os.path.basename(cc.DEFAULT_REPORTS)
        for name in ('2026-10-24-storage-cost-phase0', '2027-01-05-storage-cost-lifecycle', '2026-11-30-storage-cost-x'):
            self.assertTrue(fnmatch.fnmatch(name, pattern), name)
        self.assertFalse(fnmatch.fnmatch('2026-10-23-cas-yardstick', pattern))

    def test_report_formulas_file_may_hold_an_array(self):
        def formulas(variant, qid):
            row = {'row_type': 'request', 'op_kind': 'PutObject', 'outcome_class': 'ok', 'category': None,
                   'route': None, 'scope': 'variant', 'basis': 'predicted', 'expr': inp(qid)}
            return {'schema': 'formulas/1', 'owner': 'o', 'variant': variant,
                    'cells': {'c': {'ledger_rows': [row], 'allocation': None, 'states': [], 'notes': []}}}
        with tempfile.TemporaryDirectory() as d:
            cm, report = os.path.join(d, 'cm'), os.path.join(d, 'rep-1')
            os.makedirs(cm)
            os.makedirs(report)
            open(os.path.join(report, 'README.md'), 'w').close()  # a report directory has a README
            with open(os.path.join(cm, 'inputs-registry.json'), 'w') as f:
                json.dump(registry(quantity('x')), f)
            args = ['--mode', 'dev', '--cost-model', cm, '--reports', os.path.join(d, 'rep-*')]
            with open(os.path.join(report, 'formulas.json'), 'w') as f:
                json.dump([formulas('s3', 'x'), formulas('cas', 'x')], f)
            self.assertEqual(run_main(args), 0)
            with open(os.path.join(report, 'formulas.json'), 'w') as f:
                json.dump([formulas('s3', 'x'), formulas('cas', 'y')], f)
            err = io.StringIO()
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(err):
                self.assertEqual(cc.main(args), 1)
            self.assertIn('formulas[1] (cas): unknown input y', err.getvalue())

    def test_seeds_pass_dev_mode(self):
        self.assertEqual(run_main(['--mode', 'dev']), 0)


if __name__ == '__main__':
    unittest.main()


class DerivationRefsTest(unittest.TestCase):
    """A cell that reads a measured table through registry derivations finds that table's evidence."""

    COV = {"domain": {"variants": ["cas"], "operations": ["gc"], "families": ["cas_gc"]},
           "cards": [{"name": "gc-scale", "family": "cas_gc", "model_variants": ["cas"],
                      "cases": [{"case_id": "objects-1k"}]}],
           "evidence": [{"coefficient": "gc_round_requests", "experiment_id": "gc-scale.objects-1k", "status": "done"}]}
    APP = {"cas": {"gc": {"cas_gc": "supported"}}}

    def registry(self):
        lookup = {"kind": "lookup", "table_id": "gc_round_requests", "keys": [{"kind": "const", "value": "fold", "dim": "enum:t"}], "dim": "count"}
        return {"quantities": [
            {"id": "outer", "kind": "derivation", "rule": {"kind": "mul", "dim": "count", "a": {"kind": "input", "id": "inner", "dim": "count"},
                                                             "b": {"kind": "input", "id": "outer", "dim": "count"}}},
            {"id": "inner", "kind": "derivation", "rule": lookup},
            {"id": "plain", "kind": "input", "rule": None}]}

    def files(self):
        cell = {"ledger_rows": [{"expr": {"kind": "input", "id": "outer", "dim": "count"}}], "states": [], "notes": []}
        return [{"variant": "cas", "cells": {"gc": cell}}]

    def test_chain_reaches_the_table_and_a_cycle_terminates(self):
        self.assertEqual(cc._pending_cells(self.COV, self.files(), self.APP, registry=self.registry()), [])
        self.assertEqual(cc.derived_refs({"kind": "input", "id": "outer"}, self.registry()),
                         {("table", "gc_round_requests")})

    def test_without_the_registry_the_cell_has_no_evidence(self):
        self.assertEqual(cc._pending_cells(self.COV, self.files(), self.APP), [("cas", "gc", "cas_gc")])
