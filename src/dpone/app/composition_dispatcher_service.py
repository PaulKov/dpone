"""Start the explicitly configured v2 dispatcher without provisioning authority.

``python -m dpone.app.composition_dispatcher_service --help`` loads no concrete
adapter or connector. Configuration, UID/GID and digest are mandatory external
pins. This entry point never restarts a failed request or prints exception data.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any, TextIO


def _arguments(argv: Sequence[str] | None) -> Any:
    import argparse
    import re

    class Once(argparse.Action):
        def __call__(self, parser: Any, namespace: Any, values: Any, option_string: str | None = None) -> None:
            if getattr(namespace, self.dest, None) is not None:
                raise argparse.ArgumentError(self, "must be supplied exactly once")
            setattr(namespace, self.dest, values)

    def path(value: str) -> Path:
        result = Path(value)
        if (
            not result.is_absolute()
            or value.startswith("//")
            or len(result.parts) < 3
            or ".." in result.parts
            or str(result) != value
            or any(ord(char) < 32 or ord(char) == 127 for char in value)
        ):
            raise argparse.ArgumentTypeError("must be a canonical absolute path with a directory and filename")
        return result

    def digest(value: str) -> str:
        if re.fullmatch(r"sha256:[0-9a-f]{64}", value) is None:
            raise argparse.ArgumentTypeError("must be a canonical sha256 digest")
        return value

    def identity(value: str) -> int:
        if re.fullmatch(r"[1-9][0-9]{0,9}", value) is None or int(value) >= 2147483648:
            raise argparse.ArgumentTypeError("must be a canonical decimal ID from 1 to 2147483647")
        return int(value)

    parser = argparse.ArgumentParser(
        description="Run the protected v2 whole-cell dispatcher using administrator-provisioned configuration.",
        allow_abbrev=False,
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--config", action=Once, type=path, help="Canonical absolute legacy startup configuration file.")
    mode.add_argument(
        "--policy", action=Once, type=path, help="Canonical absolute policy for closed-listener bootstrap."
    )
    parser.add_argument(
        "--configuration-sha256",
        action=Once,
        type=digest,
        help="Externally pinned SHA256 of the exact configuration bytes.",
    )
    parser.add_argument(
        "--policy-sha256", action=Once, type=digest, help="Externally pinned SHA256 of exact policy bytes."
    )
    parser.add_argument(
        "--dispatcher-uid", required=True, action=Once, type=identity, help="Enrolled nonzero Linux dispatcher UID."
    )
    parser.add_argument(
        "--dispatcher-gid", required=True, action=Once, type=identity, help="Enrolled nonzero Linux dispatcher GID."
    )
    args = parser.parse_args(argv)
    if args.config is not None:
        if args.configuration_sha256 is None or args.policy_sha256 is not None:
            parser.error("--config requires --configuration-sha256 and excludes --policy-sha256")
    elif args.policy_sha256 is None or args.configuration_sha256 is not None:
        parser.error("--policy requires --policy-sha256 and excludes --configuration-sha256")
    return args


def _pause_startup() -> None:
    """Bound idle polling without creating another thread or process."""
    import time

    time.sleep(0.1)


def _serve(server: Any, signals: Any, *, startup: Any = None) -> None:
    """Notify from signals; close and join actual request work from coordinator.

    handle_request uses a short idle poll. We never call shutdown from the serving
    thread, which would deadlock socketserver. Context exit joins admitted work;
    driver overruns retain their slots until they actually unwind, not until a
    timer claims cancellation. Signal installation failures also close the server.
    """
    previous: dict[int, Any] = {}

    def stop(signum: int, frame: Any) -> None:
        server.stop_admission()

    try:
        with server:
            server.timeout = 0.25
            for signum in (signals.SIGTERM, signals.SIGINT):
                previous[signum] = signals.signal(signum, stop)
            active = startup is None
            while not server.admission_stop.is_set():
                if not active:
                    active = startup.poll()
                    if not active:
                        if not server.admission_stop.is_set():
                            _pause_startup()
                        continue
                if not server.admission_stop.is_set():
                    server.handle_request()
    finally:
        failure = None
        if startup is not None:
            try:
                startup.close()
            except Exception as error:
                failure = error
        for signum, handler in previous.items():
            try:
                signals.signal(signum, handler)
            except Exception as error:
                failure = error
        if failure is not None:
            raise failure


def main(
    argv: Sequence[str] | None = None,
    *,
    build_server: Callable[..., Any] | None = None,
    build_policy_server: Callable[..., Any] | None = None,
    signals: Any = None,
    stderr: TextIO | None = None,
) -> int:
    """Exit 0 on orderly stop, 1 on unavailable authority, 2 on invalid arguments.

    Tests can inject startup/lifecycle boundaries. Production defaults construct
    the real protected configuration loader, authenticator and whole-cell handler.
    No signal handlers are installed until protected startup successfully binds.
    """
    args = _arguments(argv)
    import signal
    import sys

    try:
        if args.policy is not None:
            if build_policy_server is None:
                from dpone.app.composition_dispatcher_policy_server import build_dispatcher_policy_server

                build_policy_server = build_dispatcher_policy_server
            service = build_policy_server(
                args.policy,
                expected_policy_sha256=args.policy_sha256,
                dispatcher_uid=args.dispatcher_uid,
                dispatcher_gid=args.dispatcher_gid,
            )
            _serve(service.server, signal if signals is None else signals, startup=service.startup)
        else:
            if build_server is None:
                from dpone.app.composition_dispatcher_service_factory import build_dispatcher_server

                build_server = build_dispatcher_server
            server = build_server(
                args.config,
                expected_configuration_sha256=args.configuration_sha256,
                dispatcher_uid=args.dispatcher_uid,
                dispatcher_gid=args.dispatcher_gid,
            )
            _serve(server, signal if signals is None else signals)
        return 0
    except Exception:
        print(
            "dpone dispatcher: service unavailable; inspect protected configuration, TLS credentials and dispatcher authority.",
            file=sys.stderr if stderr is None else stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
