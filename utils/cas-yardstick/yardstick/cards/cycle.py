"""Create / 3 x insert / select / drop, from concurrent workers."""
import time
from concurrent.futures import ThreadPoolExecutor

from .base import DB, Card, table_engine

WORKERS = 8
ROWS_PER_INSERT = 1000
INSERTS = 3
EXPECTED = (INSERTS * ROWS_PER_INSERT, sum(range(INSERTS * ROWS_PER_INSERT)))


class Cycle(Card):
    name = 'cycle'
    unit = 'cycle'
    stops_merges = False
    COUNTS = {'iterations': 25}

    def __init__(self):
        self._mismatches = {}

    @property
    def table(self) -> str:
        return f'{DB}.*'

    def setup(self, c, policy, counts):
        c.query(f'CREATE DATABASE IF NOT EXISTS {DB}')

    def cold_reset_needed(self):
        return False

    def _worker(self, c, policy, phase, w, iterations):
        ops = []
        for i in range(iterations):
            t = f'{DB}.t_{phase}_{w}_{i}'
            a = time.monotonic()
            c.query(f'CREATE TABLE {t} (k UInt64, s String) {table_engine(policy)}')
            b = time.monotonic()
            for j in range(INSERTS):
                c.query(f'INSERT INTO {t} SELECT number, toString(number) '
                        f'FROM numbers({j * ROWS_PER_INSERT}, {ROWS_PER_INSERT})')
            d = time.monotonic()
            count, total = (int(x) for x in c.query(f'SELECT count(), sum(k) FROM {t}').split())
            e = time.monotonic()
            c.query(f'DROP TABLE {t} SYNC')
            f = time.monotonic()
            ops.append(dict(worker=w, iteration=i, create_ms=(b - a) * 1e3, insert_ms=(d - b) * 1e3,
                            select_ms=(e - d) * 1e3, drop_ms=(f - e) * 1e3, ms=(f - a) * 1e3,
                            count=count, sum=total))
        return ops

    def measured(self, c, policy, counts, phase):
        iterations = counts['iterations']
        with ThreadPoolExecutor(WORKERS) as pool:
            futures = [pool.submit(self._worker, c, policy, phase, w, iterations) for w in range(WORKERS)]
            ops = [op for f in futures for op in f.result()]
        self._mismatches[phase] = sum((op['count'], op['sum']) != EXPECTED for op in ops)
        return {'unit_count': len(ops), 'ops': ops}

    def invariants(self, c, counts, phase, when):
        tables = int(c.query(f"SELECT count() FROM system.tables WHERE database = '{DB}'"))
        out = {'tables': (0, tables)}
        if when == 'after':
            out['select_count_sum_mismatches'] = (0, self._mismatches.pop(phase))
        return out
