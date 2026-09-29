import re
import threading
import time
import unittest

from yardstick.cards.cycle import Cycle, WORKERS

WORK_SEC = 0.02


class FailingWorkerClient:
    """Worker 0's CREATE fails; the other workers' queries each take WORK_SEC."""
    def __init__(self):
        self.creates = {}
        self.lock = threading.Lock()

    def query(self, sql, settings=None, timeout=3600):
        worker = int(re.search(r't_\w+?_(\d+)_\d+', sql).group(1))
        if sql.startswith('CREATE'):
            with self.lock:
                self.creates[worker] = self.creates.get(worker, 0) + 1
            if worker == 0:
                raise RuntimeError('create failed')
        time.sleep(WORK_SEC)
        return '3000\t4498500\n' if sql.startswith('SELECT') else ''


class CycleTest(unittest.TestCase):
    def test_counts(self):
        self.assertEqual(Cycle().counts(1.0), {'iterations': 25})
        self.assertEqual(Cycle().counts(0.01), {'iterations': 1})

    def test_failed_worker_stops_the_others(self):
        c = FailingWorkerClient()
        with self.assertRaisesRegex(RuntimeError, 'create failed'):
            Cycle().measured(c, 'cas', {'iterations': 25}, 'cold')
        others = sum(n for w, n in c.creates.items() if w != 0)
        self.assertLess(others, (WORKERS - 1) * 25)


if __name__ == '__main__':
    unittest.main()
