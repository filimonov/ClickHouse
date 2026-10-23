"""Port and endpoint layout of the cost rig: N replicas, one Keeper, one ledger proxy in front of the store."""
from __future__ import annotations

import socket
from dataclasses import dataclass, field


@dataclass
class Topology:
    port_base: int = 19700
    replicas: int = 1
    callers: dict = field(default_factory=lambda: {'r1': 19131, 'r2': 19132, 'r3': 19133, 'rig': 19134})
    store: tuple = ('127.0.0.1', 19121)
    bucket: str = 'yardstick-cost'

    def _base(self, i: int) -> int:
        return self.port_base + 10 * (i - 1)

    def tcp(self, i: int) -> int:
        return self._base(i)

    def http(self, i: int) -> int:
        return self._base(i) + 1

    def interserver(self, i: int) -> int:
        return self._base(i) + 2

    def keeper_client(self) -> int:
        return self.port_base + 8

    def keeper_raft(self) -> int:
        return self.port_base + 9

    def relay_host(self, i: int) -> str:
        return f'127.0.1.{i}'

    def caller_port(self, i: int) -> int:
        return self.callers[f'r{i}']

    def endpoint(self, i: int, key_prefix: str) -> str:
        return f'http://127.0.0.1:{self.caller_port(i)}/{self.bucket}/{key_prefix}/'

    def preflight_new_endpoints(self) -> list[str]:
        """Problems for the endpoints this rig creates; proxy and store ports belong to services started earlier."""
        wanted = []
        for i in range(1, self.replicas + 1):
            wanted += [('127.0.0.1', p) for p in (self.tcp(i), self.http(i), self.interserver(i))]
            wanted.append((self.relay_host(i), self.interserver(i)))
        if self.replicas >= 1:
            wanted += [('127.0.0.1', self.keeper_client()), ('127.0.0.1', self.keeper_raft())]
        problems = []
        for host, port in wanted:
            s = socket.socket()
            # Like the servers' listeners: a port in TIME_WAIT from a server stopped moments ago is usable.
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                s.bind((host, port))
            except OSError as e:
                problems.append(f'cannot bind {host}:{port}: {e}')
            finally:
                s.close()
        return problems

    def verify_services(self, proxy, rig) -> list[str]:
        """Problems if the running proxy does not own the caller ports or the store lacks the bucket."""
        problems = []
        try:
            if set(proxy.ports.values()) != set(self.callers.values()):
                problems.append(f'proxy ports {sorted(proxy.ports.values())} != caller ports '
                                f'{sorted(self.callers.values())}')
        except Exception as e:
            problems.append(f'cannot read proxy ports: {e}')
        try:
            rig.ensure_bucket(self.bucket)
        except Exception as e:
            problems.append(f'store bucket {self.bucket!r} unavailable: {e}')
        return problems
