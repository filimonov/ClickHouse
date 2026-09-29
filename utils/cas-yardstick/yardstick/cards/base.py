"""The card interface: one fixed workload measured on one disk."""
DB = 'yardstick'
TABLE_SETTINGS = 'primary_key_lazy_load = 0, prewarm_mark_cache = 0, prewarm_primary_key_cache = 0'


def table_engine(policy: str) -> str:
    """Engine clause every card table uses, so "cold" means the same on every disk."""
    return f"ENGINE = MergeTree ORDER BY k SETTINGS storage_policy = '{policy}', {TABLE_SETTINGS}"


class Card:
    """Lifecycle per pass: setup, optional cold reset, measured cold, measured hot if `has_hot`, teardown.

    `invariants` is checked before and after every measured pass; any mismatch fails the pass.
    A card instance is reused across passes, so state kept on it must be reset by `measured`."""
    name: str
    unit: str
    has_hot: bool = True
    stops_merges: bool
    COUNTS: dict[str, int] = {}

    @property
    def table(self) -> str:
        """What the runner settles before each window: `db.name`, or `db.*`."""
        return f'{DB}.{self.name}'

    def supports(self, policy: str) -> tuple[bool, str]:
        return True, ''

    def counts(self, scale: float) -> dict[str, int]:
        return {k: max(1, round(scale * n)) for k, n in self.COUNTS.items()}

    def setup(self, c, policy: str, counts: dict) -> None:
        raise NotImplementedError

    def cold_reset_needed(self) -> bool:
        """True when `setup` builds a fixture that the cold pass must read from a restarted process."""
        raise NotImplementedError

    def measured(self, c, policy: str, counts: dict, phase: str) -> dict:
        """Return {'unit_count': int, 'ops': list[dict]}; each op row carries `ms`, the whole unit's time."""
        raise NotImplementedError

    def invariants(self, c, counts: dict, phase: str, when: str) -> dict:
        """Return name -> (expected, actual) for `when` in ('before', 'after')."""
        raise NotImplementedError

    def teardown(self, c) -> None:
        c.query(f'DROP DATABASE IF EXISTS {DB} SYNC')
