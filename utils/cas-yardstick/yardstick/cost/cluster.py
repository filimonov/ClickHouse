"""N clickhouse-server replicas of the cost rig: start order, byte relays, restarts and per-process counters."""
from __future__ import annotations

import os
import subprocess
import time

from ..client import Client
from ..config import write_cost_config
from .interserver_proxy import ByteRelay

TOOL_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
START_TIMEOUT_SEC = 300
STOP_TIMEOUT_SEC = 120
KEEPER_READY_TIMEOUT_SEC = 60
KEEPER_POLL_SEC = 1
ZK_PROBE = "SELECT count() FROM system.zookeeper WHERE path = '/'"
PROC_IO_FIELDS = ('rchar', 'wchar', 'syscr', 'syscw', 'read_bytes', 'write_bytes')


class GenerationCrashed(RuntimeError):
    """A server process died before its final counters were read; the counters of its generation are lost."""
    def __init__(self, replica: int, pid: int, error: str):
        super().__init__(f'replica {replica} process {pid} died before its counters were read: {error}')
        self.replica, self.pid, self.error = replica, pid, error


def _now_us() -> int:
    return time.time_ns() // 1000


def _proc_io(pid: int) -> dict:
    with open(f'/proc/{pid}/io') as f:
        values = {name: int(value) for name, value in (line.split(': ') for line in f.read().splitlines())}
    return {name: values[name] for name in PROC_IO_FIELDS}


def _add(total: dict, part: dict, sign: int = 1) -> None:
    for key, value in part.items():
        total[key] = total.get(key, 0) + sign * value


class Cluster:
    def __init__(self, binary: str, pass_dir: str, topology, policy: str, key_prefix: str, local_dirs,
                 gc_enabled: bool, server_settings: dict, server_sh: str = os.path.join(TOOL_DIR, 'server.sh'),
                 sh_runner=None, config_writer=write_cost_config, client_factory=Client,
                 relay_factory=ByteRelay, proc_io_reader=_proc_io):
        self.binary = os.path.abspath(binary)
        self.pass_dir = os.path.abspath(pass_dir)
        self.topology = topology
        self.policy = policy
        self.key_prefix = key_prefix
        self.local_dirs = local_dirs
        self.gc_enabled = gc_enabled
        self.server_settings = server_settings
        self.server_sh = server_sh
        self._sh = sh_runner or self._run_server_sh
        self._write_config = config_writer
        self._client_factory = client_factory
        self._relay_factory = relay_factory
        self._proc_io = proc_io_reader
        self.clients: list = []
        self.relays: list = []
        self.startup_seconds: list[float] = []
        self.generations: dict[int, list[dict]] = {}
        self._running: set[int] = set()

    def run_dir(self, i: int) -> str:
        return os.path.join(self.pass_dir, f'r{i}')

    def _run_server_sh(self, args: list) -> None:
        run_dir = args[2] if args[0] == 'start' else args[1]
        timeout = START_TIMEOUT_SEC if args[0] == 'start' else STOP_TIMEOUT_SEC
        with open(os.path.join(run_dir, 'server_control.log'), 'a') as log:
            # SYSTEM SHUTDOWN signals the server's process group; keep the caller out of it.
            subprocess.run(['bash', self.server_sh] + [str(a) for a in args], stdout=log, stderr=subprocess.STDOUT,
                           check=True, timeout=timeout, start_new_session=args[0] == 'start')

    def start_all(self) -> None:
        problems = self.topology.preflight_new_endpoints()
        if problems:
            raise RuntimeError('endpoints not free, nothing launched: ' + '; '.join(problems))
        n = self.topology.replicas
        self.clients = [self._client_factory(self.topology.http(i)) for i in range(1, n + 1)]
        self.startup_seconds = [0.0] * n
        os.makedirs(self.pass_dir, exist_ok=True)
        for i in range(1, n + 1):
            local_dir = self.local_dirs[i - 1] if self.local_dirs else None
            self._write_config(self.run_dir(i), i, self.policy, self.topology, self.key_prefix, local_dir,
                               self.gc_enabled, f'yardstick-r{i}', self.server_settings)
        launched = []
        try:
            for i in range(1, n + 1):
                launched.append(i)
                try:
                    self.startup_seconds[i - 1] = self.start(i)
                except Exception as e:
                    raise RuntimeError(f'replica {i} failed to start: {e}; see '
                                       f'{os.path.join(self.run_dir(i), "server.err.log")}') from e
            for i in range(1, n + 1):
                relay = self._relay_factory((self.topology.relay_host(i), self.topology.interserver(i)),
                                            ('127.0.0.1', self.topology.interserver(i)),
                                            os.path.join(self.pass_dir, f'relay.r{i}.jsonl'))
                self.relays.append(relay)
                try:
                    relay.start()
                except Exception as e:
                    raise RuntimeError(f'relay of replica {i} failed to start: {e}') from e
        except RuntimeError as failure:
            errors = self._teardown(launched)
            if errors:
                raise RuntimeError(f'{failure}; cleanup also failed: {"; ".join(errors)}') from failure
            raise

    def _teardown(self, launched: list) -> list[str]:
        errors = []
        for i in reversed(launched):
            try:
                if i in self._running:
                    self.stop(i)
                else:
                    self._sh(['stop', self.run_dir(i)])
            except Exception as e:
                errors.append(f'stop replica {i}: {e}')
        errors += self._stop_relays()
        return errors

    def _stop_relays(self) -> list[str]:
        errors = []
        for n, relay in enumerate(self.relays, 1):
            try:
                relay.stop()
            except Exception as e:
                errors.append(f'stop relay {n}: {e}')
        self.relays = []
        return errors

    def start(self, i: int) -> float:
        """Launch replica `i`; return seconds until it answers (replica 1: until Keeper does)."""
        t0 = time.monotonic()
        t_start_us = _now_us()
        self._sh(['start', self.binary, self.run_dir(i), str(self.topology.tcp(i))])
        with open(os.path.join(self.run_dir(i), 'server.pid')) as f:
            pid = int(f.read())
        self.generations.setdefault(i, []).append({'pid': pid, 't_start_us': t_start_us, 't_end_us': None,
                                                   'closed': None, 'crashed': False, 'crash_error': None})
        self._running.add(i)
        if i == 1:
            self._wait_keeper()
        return time.monotonic() - t0

    def _wait_keeper(self) -> None:
        deadline = time.monotonic() + KEEPER_READY_TIMEOUT_SEC
        while True:
            try:
                self.clients[0].query(ZK_PROBE)
                return
            except Exception as e:
                if time.monotonic() >= deadline:
                    raise RuntimeError(f'Keeper not ready after {KEEPER_READY_TIMEOUT_SEC} s: {e}') from e
                time.sleep(KEEPER_POLL_SEC)

    def stop(self, i: int) -> None:
        """Read the final counters, then shut replica `i` down.

        A server that died earlier cannot be read: the generation is marked crashed (see `counters_since`)
        and the shutdown still runs.
        """
        if i not in self._running:
            return
        generation = self.generations[i][-1]
        try:
            generation['closed'] = self._read(i, generation)
        except Exception as e:
            generation['crashed'] = True
            generation['crash_error'] = f'{type(e).__name__}: {e}'
        finally:
            generation['t_end_us'] = _now_us()
            self._running.discard(i)
            self._sh(['stop', self.run_dir(i)])

    def stop_all(self) -> None:
        errors = []
        for i in range(self.topology.replicas, 0, -1):
            try:
                self.stop(i)
            except Exception as e:
                errors.append(f'stop replica {i}: {e}')
        errors += self._stop_relays()
        if errors:
            raise RuntimeError('; '.join(errors))

    def _read(self, i: int, generation: dict) -> dict:
        return {'events': self.clients[i - 1].events(), 'proc_io': self._proc_io(generation['pid'])}

    def snapshot_counters(self, i: int) -> dict:
        """Counters of the running process of replica `i`, tagged with its generation index."""
        index = len(self.generations[i]) - 1
        return {'generation': index, **self._read(i, self.generations[i][index])}

    def counters_since(self, i: int, baseline: dict) -> dict:
        """Counters accumulated since `baseline` across restarts: closed generations plus the live one.

        Raises GenerationCrashed if a generation in the range lost its counters.
        """
        total = {'events': {}, 'proc_io': {}}
        for index in range(baseline['generation'], len(self.generations[i])):
            generation = self.generations[i][index]
            if generation['crashed']:
                raise GenerationCrashed(i, generation['pid'], generation['crash_error'])
            counters = generation['closed'] or self._read(i, generation)
            for part in total:
                _add(total[part], counters[part])
        for part in total:
            _add(total[part], baseline[part], -1)
        return total
