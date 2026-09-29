"""One SELECT of one column out of ten, from 200 compact parts."""
import time

from .base import DB, SINGLE_PART, Card, parts_and_rows, table_engine

ROWS_PER_PART = 50000
COLUMNS = 10
TABLE_SETTINGS = 'min_bytes_for_wide_part = 1073741824'


class SelectSubset(Card):
    name = 'select_subset'
    unit = 'query'
    stops_merges = True
    COUNTS = {'parts': 200}

    def __init__(self):
        self._sums = {}

    def cold_reset_needed(self):
        return True

    def setup(self, c, policy, counts):
        c.query(f'CREATE DATABASE IF NOT EXISTS {DB}')
        columns = ', '.join(f'c{i} UInt64' for i in range(COLUMNS))
        c.query(f'CREATE TABLE {self.table} (k UInt64, {columns}) {table_engine(policy, TABLE_SETTINGS)}')
        c.query(f'SYSTEM STOP MERGES {self.table}')
        values = ', '.join(f'number * {i}' for i in range(COLUMNS))
        for i in range(counts['parts']):
            c.query(f'INSERT INTO {self.table} SELECT number, {values} '
                    f'FROM numbers({i * ROWS_PER_PART}, {ROWS_PER_PART})', SINGLE_PART)

    def measured(self, c, policy, counts, phase):
        a = time.monotonic()
        total = int(c.query(f'SELECT sum(c3) FROM {self.table}'))
        self._sums[phase] = total
        return {'unit_count': 1, 'ops': [dict(ms=(time.monotonic() - a) * 1e3)]}

    def invariants(self, c, counts, phase, when):
        parts, rows = parts_and_rows(c, self.table)
        n = counts['parts'] * ROWS_PER_PART
        out = {'active_parts': (counts['parts'], parts), 'rows': (n, rows)}
        if when == 'after':
            out['sum_c3'] = (3 * (n * (n - 1) // 2), self._sums.pop(phase))
        return out
