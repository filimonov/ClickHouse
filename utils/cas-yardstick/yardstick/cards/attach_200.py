"""DETACH + ATTACH of a table with 200 small parts."""
import time

from .base import DB, NO_MERGES, SINGLE_PART, Card, parts_and_rows, table_engine

ROWS_PER_PART = 10000


class Attach200(Card):
    name = 'attach_200'
    unit = 'part'
    stops_merges = True
    COUNTS = {'parts': 200}

    def cold_reset_needed(self):
        return True

    def setup(self, c, policy, counts):
        c.query(f'CREATE DATABASE IF NOT EXISTS {DB}')
        c.query(f'CREATE TABLE {self.table} (k UInt64, s String) {table_engine(policy, NO_MERGES)}')
        for i in range(counts['parts']):
            c.query(f'INSERT INTO {self.table} SELECT number, toString(number) '
                    f'FROM numbers({i * ROWS_PER_PART}, {ROWS_PER_PART})', SINGLE_PART)

    def measured(self, c, policy, counts, phase):
        a = time.monotonic()
        c.query(f'DETACH TABLE {self.table}')
        b = time.monotonic()
        c.query(f'ATTACH TABLE {self.table}')
        d = time.monotonic()
        op = dict(detach_ms=(b - a) * 1e3, attach_ms=(d - b) * 1e3, ms=(d - a) * 1e3)
        return {'unit_count': counts['parts'], 'ops': [op]}

    def invariants(self, c, counts, phase, when):
        parts, rows = parts_and_rows(c, self.table)
        return {'active_parts': (counts['parts'], parts), 'rows': (counts['parts'] * ROWS_PER_PART, rows)}
