"""500 inserts of 20000 rows into a 30-column table, merges stopped."""
import time

from .base import DB, SINGLE_PART, Card, parts_and_rows, table_engine

ROWS_PER_INSERT = 20000
COLUMNS = 15


class WideInsert(Card):
    name = 'wide_insert'
    unit = 'insert'
    stops_merges = True
    COUNTS = {'inserts': 500}

    def cold_reset_needed(self):
        return False

    def setup(self, c, policy, counts):
        c.query(f'CREATE DATABASE IF NOT EXISTS {DB}')
        columns = ', '.join([f'u{i} UInt64' for i in range(COLUMNS)] + [f's{i} String' for i in range(COLUMNS)])
        c.query(f'CREATE TABLE {self.table} (k UInt64, {columns}) {table_engine(policy)}')
        c.query(f'SYSTEM STOP MERGES {self.table}')

    def before_window(self, c, phase):
        super().before_window(c, phase)
        if phase == 'hot':
            c.query(f'TRUNCATE TABLE {self.table}')

    def measured(self, c, policy, counts, phase):
        ops = []
        for i in range(counts['inserts']):
            numeric = ', '.join(f'number * {i}' for _ in range(COLUMNS))
            strings = ', '.join(f'toString(number * {i})' for _ in range(COLUMNS))
            a = time.monotonic()
            c.query(f'INSERT INTO {self.table} SELECT number, {numeric}, {strings} '
                    f'FROM numbers({i * ROWS_PER_INSERT}, {ROWS_PER_INSERT})', SINGLE_PART)
            ops.append(dict(insert=i, ms=(time.monotonic() - a) * 1e3))
        return {'unit_count': len(ops), 'ops': ops}

    def invariants(self, c, counts, phase, when):
        parts, rows = parts_and_rows(c, self.table)
        inserts = counts['inserts'] if when == 'after' else 0
        return {'active_parts': (inserts, parts), 'rows': (inserts * ROWS_PER_INSERT, rows)}
