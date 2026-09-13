"""Run the protected Linux host probe with explicit administrator configuration.

Use ``python -m dpone.app.composition_supervisor_host_service --help``. Imports
of the concrete observer are deferred until arguments have been validated.
SIGTERM/SIGINT stop between bounded requests; a failed accepted request ends the
service without automatic recapture. No raw exception or host facts are logged.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any, TextIO


def _arguments(argv: Sequence[str] | None) -> Any:
    import argparse
    import re

    def absolute_path(value: str) -> Path:
        path = Path(value)
        if not path.is_absolute() or ".." in path.parts or str(path) != value or "\x00" in value:
            raise argparse.ArgumentTypeError("must be a canonical absolute configuration path")
        return path

    def configuration_digest(value: str) -> str:
        if re.fullmatch(r"sha256:[0-9a-f]{64}", value) is None:
            raise argparse.ArgumentTypeError("must be a canonical sha256 digest")
        return value

    parser = argparse.ArgumentParser(
        description="Run the root Linux host observer for one protected dispatcher enrollment.",
        allow_abbrev=False,
    )
    parser.add_argument("--config", required=True, type=absolute_path, help="Root-owned canonical configuration file.")
    parser.add_argument(
        "--configuration-sha256",
        required=True,
        type=configuration_digest,
        help="Administrator-pinned SHA256 of the exact configuration bytes.",
    )
    return parser.parse_args(argv)


def _serve(server: Any, signals: Any) -> None:
    """Own listener cleanup and restore signal handlers on every exit path.

    No extra thread or timer is started. Normal socket I/O obeys the server's
    absolute request deadline (configuration maximum 60 seconds). Host filesystem
    syscalls can be uninterruptible; the service manager supplies the final stop
    deadline without turning an interrupted observation into success.
    """
    stopped = False
    previous: dict[int, Any] = {}

    def stop(signum: int, frame: Any) -> None:
        nonlocal stopped
        stopped = True

    try:
        for signum in (signals.SIGTERM, signals.SIGINT):
            previous[signum] = signals.signal(signum, stop)
        with server:
            while not stopped:
                result = server.serve_once()
                if type(result) is not bool:
                    raise RuntimeError("host_probe_service_result")
    finally:
        for signum, handler in previous.items():
            signals.signal(signum, handler)


def main(
    argv: Sequence[str] | None = None,
    *,
    build_server: Callable[..., Any] | None = None,
    signals: Any = None,
    stderr: TextIO | None = None,
) -> int:
    """Exit 0 on orderly shutdown, 1 on unavailable authority, 2 on bad arguments.

    Injection seams support offline lifecycle tests. The CLI always selects the
    concrete protected loader/server, whose entry validates Linux root and socket
    ancestry before binding. Invalid configuration never starts the serve loop.
    """
    args = _arguments(argv)
    import signal
    import sys

    try:
        if build_server is None:
            from dpone.app.composition_supervisor_host import build_supervisor_facts_server

            build_server = build_supervisor_facts_server
        server = build_server(args.config, expected_configuration_sha256=args.configuration_sha256)
        _serve(server, signal if signals is None else signals)
        return 0
    except Exception:
        print(
            "dpone host probe: service unavailable; inspect protected configuration and host authority.",
            file=sys.stderr if stderr is None else stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
