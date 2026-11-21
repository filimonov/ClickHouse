"""The docker stand of one profile run (`soak.run --profile`): bring-up, restart, evidence copy, teardown.

The stand is the main harness's compose (`docker-compose.yml`, or a cloud variant passed with
`--compose-file`) under a compose project of its own, plus a generated override in the run directory
that publishes the two servers on host ports of the run's choosing and keeps their logs inside the
containers (copied into the run directory at the end), so a run never touches another project's ports or
`logs/chN`. The binary under test is bind-mounted through `CA_SOAK_CLICKHOUSE_BINARY`, the variable the
compose files already read.

Everything is addressed by the compose project name: containers are `<project>-<service>-1`, and the
environment variables `soak.cluster`, `soak.chaos`, `soak.pool` and `run.fsck_container` read are set
here, before any of them is used.
"""

import os
import shlex
import subprocess
import time

CA_SOAK_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Host ports relative to `--port-base`: (http, native) per server.
PORT_OFFSETS = {1: (11, 10), 2: (21, 20)}


def server_ports(port_base: int) -> dict:
    return {n: (port_base + h, port_base + t) for n, (h, t) in PORT_OFFSETS.items()}


def container(project: str, service: str) -> str:
    return f"{project}-{service}-1"


class Stand:
    def __init__(self, run_dir, project, binary, port_base, compose_files, env_files, log_fn):
        self.run_dir = run_dir
        self.project = project
        self.binary = os.path.abspath(binary)
        self.ports = server_ports(port_base)
        self.compose_files = [os.path.join(CA_SOAK_DIR, f) if not os.path.isabs(f) else f for f in compose_files]
        self.env_files = env_files
        self.log = log_fn
        self.override = os.path.join(run_dir, "compose.override.yml")
        self.has_rustfs = False

    # -- compose -------------------------------------------------------------------------------
    def _compose(self, *args, timeout=900, check=True):
        cmd = ["docker", "compose", "-p", self.project, "--project-directory", CA_SOAK_DIR]
        for e in self.env_files:
            cmd += ["--env-file", e]
        for f in self.compose_files + [self.override]:
            cmd += ["-f", f]
        cmd += list(args)
        env = dict(os.environ, CA_SOAK_CLICKHOUSE_BINARY=self.binary)
        # Own session: a signal sent to the harness's process group (`timeout`, Ctrl-C) must not cut a
        # `down` in half and leave containers behind.
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=env, start_new_session=True)
        if check and p.returncode != 0:
            raise RuntimeError(f"docker compose {' '.join(args)} failed rc={p.returncode}: {p.stderr.strip()[-2000:]}")
        return p

    def write_override(self):
        """Ports replace the base file's list (`!override`), and the log directory becomes an anonymous
        volume (same target as the base file's `./logs/chN` bind mount, so it replaces it); the system-log
        config partitions `cas_log` by ten minutes so the janitor can drop old partitions."""
        syslogs = os.path.join(CA_SOAK_DIR, "configs", "soak_system_logs.xml")
        lines = ["services:"]
        for n, (http, native) in self.ports.items():
            lines += [
                f"  ch{n}:",
                "    ports: !override",
                f"      - \"127.0.0.1:{http}:8123\"",
                f"      - \"127.0.0.1:{native}:9000\"",
                "    volumes:",
                "      - /var/log/clickhouse-server",
                f"      - {syslogs}:/etc/clickhouse-server/config.d/zz_soak_system_logs.xml:ro",
            ]
        with open(self.override, "w") as f:
            f.write("\n".join(lines) + "\n")

    def export_env(self):
        """Point every harness module that addresses a node or a container at this stand."""
        for n, (http, _) in self.ports.items():
            os.environ[f"CA_SOAK_NODE{n}_HOST"] = "127.0.0.1"
            os.environ[f"CA_SOAK_NODE{n}_PORT"] = str(http)
            os.environ[f"CA_SOAK_NODE{n}_CONTAINER"] = container(self.project, f"ch{n}")
        os.environ["CA_SOAK_NODE_COUNT"] = "2"
        os.environ["CA_SOAK_RUSTFS_CONTAINER"] = container(self.project, "rustfs1")
        os.environ["CA_SOAK_FSCK_CONTAINER"] = container(self.project, "ch1")

    def up(self):
        self.write_override()
        services = self._compose("config", "--services", timeout=120).stdout.split()
        self.has_rustfs = "rustfs1" in services
        self.log(f"stand: compose project {self.project}, services {services}, binary {self.binary}, "
                 f"ports {self.ports}")
        self._compose("up", "-d", timeout=1200)
        self.export_env()
        for n in self.ports:
            self.wait_ping(n, 300)
        self.log("stand: both servers answer /ping")

    def wait_ping(self, n, limit_s):
        import urllib.request
        http = self.ports[n][0]
        t0 = time.time()
        while time.time() - t0 < limit_s:
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{http}/ping", timeout=3) as r:
                    if r.status == 200:
                        return True
            except Exception:
                pass
            time.sleep(1)
        raise RuntimeError(f"server {n} did not answer /ping on port {http} within {limit_s} s")

    def restart(self, n):
        """Clean stop and start of one server; the other keeps serving."""
        c = container(self.project, f"ch{n}")
        p = subprocess.run(["docker", "restart", "-t", "90", c], capture_output=True, text=True, timeout=300)
        if p.returncode != 0:
            raise RuntimeError(f"docker restart {c} failed: {p.stderr.strip()}")
        self.wait_ping(n, 600)

    def running(self, n) -> bool:
        p = subprocess.run(["docker", "inspect", "--format", "{{.State.Status}}", container(self.project, f"ch{n}")],
                           capture_output=True, text=True, timeout=30)
        return p.returncode == 0 and p.stdout.strip() == "running"

    # -- evidence and teardown -------------------------------------------------------------------
    def unpause_servers(self):
        """A paused container refuses `docker cp` and queries; a fault left running at an abort must not cost the evidence."""
        for n in self.ports:
            subprocess.run(["docker", "unpause", container(self.project, f"ch{n}")], capture_output=True, timeout=60)

    def removal_command(self) -> str:
        """The shell command that removes the stand's containers, network and volumes."""
        cmd = ["docker", "compose", "-p", self.project, "--project-directory", CA_SOAK_DIR]
        for e in self.env_files:
            cmd += ["--env-file", e]
        for f in self.compose_files + [self.override]:
            cmd += ["-f", f]
        cmd += ["down", "-v", "--remove-orphans", "-t", "30"]
        return f"CA_SOAK_CLICKHOUSE_BINARY={shlex.quote(self.binary)} " + " ".join(shlex.quote(c) for c in cmd)

    def copy_evidence(self):
        """Server logs (inside the containers) and container stdout into `<run>/srv<n>/`."""
        self.unpause_servers()
        for n in self.ports:
            c = container(self.project, f"ch{n}")
            dst = os.path.join(self.run_dir, f"srv{n}")
            os.makedirs(dst, exist_ok=True)
            p = subprocess.run(["docker", "cp", f"{c}:/var/log/clickhouse-server/.", dst],
                               capture_output=True, text=True, timeout=1800)
            if p.returncode != 0:
                self.log(f"stand: copying the logs of {c} failed: {p.stderr.strip()[:300]}")
            with open(os.path.join(dst, "docker.log"), "w") as f:
                subprocess.run(["docker", "logs", c], stdout=f, stderr=subprocess.STDOUT, timeout=600)

    def disk_usage(self) -> str:
        """MB used by the run directory and by the data directories inside the stand's containers."""
        def du(cmd):
            try:
                p = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
                return p.stdout.split()[0] if p.stdout.strip() else "?"
            except Exception:
                return "?"
        parts = [f"run_dir={du(['du', '-sm', self.run_dir])}MB"]
        for service, path in (("ch1", "/var/lib/clickhouse"), ("ch2", "/var/lib/clickhouse"), ("rustfs1", "/data")):
            if service == "rustfs1" and not self.has_rustfs:
                continue
            parts.append(f"{service}:{path}={du(['docker', 'exec', container(self.project, service), 'du', '-sm', path])}MB")
        return " ".join(parts)

    def leftovers(self) -> list:
        p = subprocess.run(["docker", "ps", "-a", "-q", "--filter", f"label=com.docker.compose.project={self.project}"],
                           capture_output=True, text=True, timeout=60)
        return p.stdout.split()

    def down(self):
        """Removes the stand; raises if a container of the project is still there after two attempts."""
        for _ in range(2):
            self._compose("down", "-v", "--remove-orphans", "-t", "30", timeout=900, check=False)
            if not self.leftovers():
                self.log(f"stand: compose project {self.project} removed (containers, network, volumes)")
                return
        raise RuntimeError(f"containers of compose project {self.project} remain after `down`: {self.leftovers()}")
