"""Argument boundary for the fixed SQLClient departure helper."""

from __future__ import annotations

import argparse
import os


def run_main() -> int:
    """Parse fixed launch facts and invoke the departure bootstrap once."""
    from dpone.app.mssql_sqlclient_departure_bootstrap import run_departure

    try:
        with open(os.devnull, "wb") as sink:
            os.dup2(sink.fileno(), 1)
            os.dup2(sink.fileno(), 2)
        parser = argparse.ArgumentParser(add_help=False, allow_abbrev=False)
        for name, kind in (
            ("parent", int),
            ("address-space", int),
            ("startup-deadline", float),
            ("operation-deadline", float),
            ("startup-fd", int),
            ("request-fd", int),
            ("result-fd", int),
            ("launch-nonce", str),
            ("implementation-sha256", str),
            ("admission", str),
        ):
            parser.add_argument("--" + name, type=kind, required=True)
        args = parser.parse_args()
        nonce = bytes.fromhex(args.launch_nonce)
        if nonce.hex() != args.launch_nonce:
            return 1
        return run_departure(
            expected_parent_pid=args.parent,
            address_space=args.address_space,
            startup_deadline=args.startup_deadline,
            operation_deadline=args.operation_deadline,
            startup_fd=args.startup_fd,
            request_fd=args.request_fd,
            result_fd=args.result_fd,
            launch_nonce=nonce,
            implementation_sha256=args.implementation_sha256,
            admission=args.admission.encode("utf-8"),
        )
    except BaseException:
        return 1
