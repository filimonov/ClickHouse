"""80 lightweight deletes from 8 concurrent workers over 50 parts of 100k rows."""
import time
import threading
from concurrent.futures import ThreadPoolExecutor

from .base import DB, SINGLE_PART, Card, parts_and_rows, supports_mutations, table_engine

WORKERS = 8
DELETES_PER_WORKER = 10
RESIDUES = 20
ROWS_PER_PART = 100000
# k % 8 = w AND intDiv(k, 8) % 20 = r selects 1/160 of the rows and ROWS_PER_PART is a multiple of 160,
# so each phase deletes exactly half of what is left.
TABLE_SETTINGS = 'enable_block_number_column = 1, enable_block_offset_column = 1'
# Patch parts: the default mode (a mutation per DELETE) loses some of 80 concurrent deletes.
DELETE_SETTINGS = {'lightweight_deletes_sync': 2, 'lightweight_delete_mode': 'lightweight_update_force'}


class LwdConcurrent(Card):
    name = 'lwd_concurrent'
    unit = 'delete'
    stops_merges = False
    COUNTS = {'parts': 50}

    def supports(self, policy):
        return supports_mutations(policy)

    def cold_reset_needed(self):
        return True

    def setup(self, c, policy, counts):
        c.query(f'CREATE DATABASE IF NOT EXISTS {DB}')
        c.query(f'CREATE TABLE {self.table} (k UInt64, s String) {table_engine(policy, TABLE_SETTINGS)}')
        for i in range(counts['parts']):
            c.query(f'INSERT INTO {self.table} SELECT number, toString(number) '
                    f'FROM numbers({i * ROWS_PER_PART}, {ROWS_PER_PART})', SINGLE_PART)

    def _worker(self, c, w, offset, stop):
        try:
            ops = []
            for i in range(DELETES_PER_WORKER):
                if stop.is_set():
                    break
                a = time.monotonic()
                c.query(f'DELETE FROM {self.table} WHERE k % {WORKERS} = {w} '
                        f'AND intDiv(k, {WORKERS}) % {RESIDUES} = {i + offset}', DELETE_SETTINGS)
                ops.append(dict(worker=w, iteration=i, ms=(time.monotonic() - a) * 1e3))
            return ops
        except BaseException:
            stop.set()
            raise

    def measured(self, c, policy, counts, phase):
        offset = 0 if phase == 'cold' else DELETES_PER_WORKER
        stop = threading.Event()
        with ThreadPoolExecutor(WORKERS) as pool:
            futures = [pool.submit(self._worker, c, w, offset, stop) for w in range(WORKERS)]
            ops = [op for f in futures for op in f.result()]
        return {'unit_count': len(ops), 'ops': ops}

    def invariants(self, c, counts, phase, when):
        total = counts['parts'] * ROWS_PER_PART
        expected = {('cold', 'before'): total, ('cold', 'after'): total // 2,
                    ('hot', 'before'): total // 2, ('hot', 'after'): 0}[(phase, when)]
        if (phase, when) == ('cold', 'before'):
            # Nothing is deleted yet, so part metadata is exact and no data is read before the cold window.
            _, rows = parts_and_rows(c, self.table)
        else:
            rows = int(c.query(f'SELECT count() FROM {self.table}'))
        return {'rows': (expected, rows)}
