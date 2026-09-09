"""Pure concurrency and parsing helpers for the Studio HTTP adapter."""

from __future__ import annotations

import threading
from collections.abc import Callable, Mapping
from concurrent.futures import Future
from http.server import BaseHTTPRequestHandler
from typing import Any
from urllib.parse import parse_qs


def query_dict(query: str) -> dict[str, str]:
    parsed = parse_qs(query, keep_blank_values=True)
    return {key: values[-1] if values else "" for key, values in parsed.items()}


def dispatch_async(
    *,
    dispatch: Callable[..., dict[str, Any]],
    method: str,
    path: str,
    query: Mapping[str, str],
    body: Mapping[str, Any],
    release_slot: Callable[[], None],
) -> Future[dict[str, Any]]:
    """Run one bounded application call while retaining its worker slot."""

    future: Future[dict[str, Any]] = Future()

    def invoke() -> None:
        try:
            future.set_result(
                dispatch(
                    method=method,
                    path=path,
                    query=query,
                    body=body,
                )
            )
        except BaseException as exc:
            future.set_exception(exc)
        finally:
            release_slot()

    threading.Thread(
        target=invoke,
        daemon=True,
        name="dpone-studio-request",
    ).start()
    return future


def server_authority(server_address: object) -> str:
    if not isinstance(server_address, tuple) or len(server_address) < 2:
        return ""
    host = str(server_address[0])
    port = int(server_address[1])
    return f"[{host}]:{port}" if ":" in host else f"{host}:{port}"


def request_method(handler: BaseHTTPRequestHandler) -> str:
    command = getattr(handler, "command", None)
    return command if isinstance(command, str) and command else "INVALID"


__all__ = [
    "dispatch_async",
    "query_dict",
    "request_method",
    "server_authority",
]
