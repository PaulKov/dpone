"""Export Docker-assigned route-live service ports to ``GITHUB_ENV``.

The route-live workflow publishes database services as ``127.0.0.1::PORT``.
Docker therefore owns allocation and reservation as one operation.  This
helper runs only after ``docker compose up --wait`` and projects the observed
numeric ports into the environment aliases consumed by the live suites.

Discovery is fail-closed: every requested service must have exactly one IPv4
loopback mapping, ports must be unique and valid, and ``GITHUB_ENV`` is not
opened until every mapping has passed validation.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from collections.abc import Callable, Iterable
from pathlib import Path

ReadPort = Callable[[str, int], str]

# service -> (container port, environment aliases)
SERVICE_PROJECTIONS: dict[str, tuple[int, tuple[str, ...]]] = {
    "postgres": (
        5432,
        (
            "DPONE_IT_PG_PORT_FORWARD",
            "DPONE_IT_PG_PORT",
            "DPONE_IT_POSTGRES_PORT",
        ),
    ),
    "postgis": (
        5432,
        (
            "DPONE_IT_POSTGIS_PORT_FORWARD",
            "DPONE_IT_POSTGIS_PORT",
        ),
    ),
    "postgres-authority-primary": (
        5432,
        (
            "DPONE_IT_PG_AUTHORITY_PRIMARY_PORT_FORWARD",
            "DPONE_IT_PG_AUTHORITY_PRIMARY_PORT",
        ),
    ),
    "postgres-authority-standby": (
        5432,
        (
            "DPONE_IT_PG_AUTHORITY_STANDBY_PORT_FORWARD",
            "DPONE_IT_PG_AUTHORITY_STANDBY_PORT",
        ),
    ),
    "mssql": (
        1433,
        (
            "DPONE_IT_MSSQL_PORT_FORWARD",
            "DPONE_IT_MSSQL_PORT",
            "DPONE_IT_MSSQL_CDC_PORT",
        ),
    ),
}

_IPV4_LOOPBACK_MAPPING = re.compile(r"127\.0\.0\.1:([0-9]+)")


def parse_compose_port(output: str) -> int:
    """Return one safe host port from ``docker compose port`` output."""

    mappings = [line.strip() for line in output.splitlines() if line.strip()]
    if len(mappings) != 1:
        raise ValueError(f"expected exactly one Compose port mapping, observed {len(mappings)}")
    match = _IPV4_LOOPBACK_MAPPING.fullmatch(mappings[0])
    if match is None:
        raise ValueError("Compose port must be published on IPv4 loopback")
    port = int(match.group(1))
    if not 1 <= port <= 65535:
        raise ValueError("Compose host port must be in range 1..65535")
    return port


def discover_environment(services: Iterable[str], *, read_port: ReadPort) -> dict[str, int]:
    """Discover and validate all requested service ports without side effects."""

    requested = tuple(services)
    if len(set(requested)) != len(requested):
        raise ValueError("route-live service list contains repeated entries")
    unsupported = sorted(set(requested).difference(SERVICE_PROJECTIONS))
    if unsupported:
        raise ValueError(f"unsupported route-live port services: {', '.join(unsupported)}")

    assignments: dict[str, int] = {}
    observed_ports: dict[int, str] = {}
    for service in requested:
        container_port, environment_names = SERVICE_PROJECTIONS[service]
        host_port = parse_compose_port(read_port(service, container_port))
        owner = observed_ports.get(host_port)
        if owner is not None:
            raise ValueError(f"duplicate Compose host port for {owner} and {service}")
        observed_ports[host_port] = service
        for name in environment_names:
            if name in assignments:
                raise ValueError(f"duplicate environment projection: {name}")
            assignments[name] = host_port
    return assignments


def export_environment(
    services: Iterable[str],
    *,
    github_env: Path,
    read_port: ReadPort,
) -> dict[str, int]:
    """Append one complete validated assignment block to ``GITHUB_ENV``."""

    assignments = discover_environment(services, read_port=read_port)
    block = "".join(f"{name}={port}\n" for name, port in assignments.items())
    github_env.parent.mkdir(parents=True, exist_ok=True)
    with github_env.open("a", encoding="utf-8") as handle:
        handle.write(block)
    return assignments


def _compose_reader(compose_file: Path) -> ReadPort:
    def read_port(service: str, container_port: int) -> str:
        result = subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                str(compose_file),
                "port",
                service,
                str(container_port),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"docker compose port failed for {service} (exit {result.returncode})")
        return result.stdout

    return read_port


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--compose-file", type=Path, required=True)
    parser.add_argument("--github-env", type=Path, required=True)
    parser.add_argument("--services", nargs="+", required=True)
    args = parser.parse_args(argv)
    if not args.compose_file.is_file():
        parser.error(f"Compose file does not exist: {args.compose_file}")
    try:
        assignments = export_environment(
            args.services,
            github_env=args.github_env,
            read_port=_compose_reader(args.compose_file),
        )
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"route-live port discovery failed: {exc}", file=sys.stderr)
        return 1
    print(f"Exported {len(assignments)} route-live runtime port aliases.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
