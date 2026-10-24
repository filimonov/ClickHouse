import contextlib
import copy
import io
import json
import os
import tempfile
import unittest

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
           'scope': row_scope, 'expr': expr}
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
    def coverage(self, case_status='planned'):
        case = {'case_id': 'c', 'policies': ['s3'], 'replicas': 1, 'scale': 1.0, 'repeats': 3, 'timeout_s': 900,
                'priority': 'P1', 'status': case_status}
        return {'schema': 'coverage/1', 'version': '1',
                'cards': [{'name': 'card', 'family': 'fam', 'model_variants': ['s3'], 'cases': [case]}],
                'evidence': [{'coefficient': 'k', 'experiment_id': 'card.c', 'policy': 's3', 'span': {}, 'role': 'fit',
                              'prediction_commit': None, 'measurement_ids': [], 'status': 'planned'}],
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
            return {'case_id': cid, 'policies': list(policies), 'replicas': 1, 'scale': 1.0, 'repeats': 3, 'timeout_s': 900,
                    'priority': 'P1', 'status': 'planned'}
        cov = {'schema': 'coverage/1', 'version': '1', 'domain': {'variants': ['s3', 'cas'], 'operations': ['x', 'y'], 'families': ['f', 'g']},
               'cards': [{'name': 'cf', 'family': 'f', 'model_variants': ['s3', 'cas'], 'cases': [case('c')]},
                         {'name': 'cg', 'family': 'g', 'model_variants': ['s3'], 'cases': [case('c', ('s3',))]}],
               'evidence': [{'coefficient': k, 'experiment_id': e, 'policy': 's3', 'span': {}, 'role': 'fit', 'prediction_commit': None,
                             'measurement_ids': [], 'status': 'planned'} for k, e in (('kf', 'cf.c'), ('kg', 'cg.c'))],
               'excluded': []}
        # s3: x uses kf and kg (evidence for f and g); y uses kf only (evidence for f, none for g).
        # cas: x is unconditionally not_supported; y uses kf (evidence for f, none for g; card cg does not cover cas).
        s3 = formulas('variant', {'kind': 'add', 'dim': 'count', 'a': coef('kf'), 'b': coef('kg')}, variant='s3', op='x')
        s3['cells']['y'] = formulas('variant', coef('kf'), variant='s3', op='y')['cells']['y']
        cas = formulas('variant', coef('kf'), variant='cas', op='y')
        cas['cells']['x'] = {'ledger_rows': [], 'allocation': None, 'notes': [],
                             'states': [{'id': 'not_supported', 'cond': True, 'reason': 'no such operation'}]}
        pending = []
        self.assertEqual(cc.support(registry(), cov, [s3, cas], pending=pending), [])
        self.assertEqual(sorted(pending), [('cas', 'y', 'g'), ('s3', 'y', 'g')])
        final = cc.support(registry(), cov, [s3, cas], require_formulas=True)
        self.assertEqual(len(final), 2)
        self.assertTrue(any('s3/y/g' in p for p in final) and any('cas/y/g' in p for p in final), final)
        conditional = copy.deepcopy(cas)
        conditional['cells']['x']['states'][0]['cond'] = {'kind': 'const', 'value': 1, 'dim': 'bool'}
        self.assertEqual(len(cc.support(registry(), cov, [s3, conditional], require_formulas=True)), 4)


class EvidenceTest(unittest.TestCase):
    def row(self, role, exp, span, mids, status='done'):
        return {'coefficient': 'k', 'experiment_id': exp, 'policy': 's3', 'span': span, 'role': role,
                'prediction_commit': 'abc' if role != 'fit' else None, 'measurement_ids': mids, 'status': status}

    def cov(self, *rows):
        case = {'case_id': 'c1', 'policies': ['s3'], 'replicas': 1, 'scale': 1.0, 'repeats': 3, 'timeout_s': 900,
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
        self.assertTrue(any('distinct' in p for p in cc.evidence(self.cov(fit, same), [self.coefficient])))
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

    def test_report_formulas_file_may_hold_an_array(self):
        def formulas(variant, qid):
            row = {'row_type': 'request', 'op_kind': 'PutObject', 'outcome_class': 'ok', 'category': None,
                   'route': None, 'scope': 'variant', 'expr': inp(qid)}
            return {'schema': 'formulas/1', 'owner': 'o', 'variant': variant,
                    'cells': {'c': {'ledger_rows': [row], 'allocation': None, 'states': [], 'notes': []}}}
        with tempfile.TemporaryDirectory() as d:
            cm, report = os.path.join(d, 'cm'), os.path.join(d, 'rep-1')
            os.makedirs(cm)
            os.makedirs(report)
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
