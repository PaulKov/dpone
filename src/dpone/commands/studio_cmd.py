"""CLI composition root for the local dpone Studio API adapter."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections.abc import Mapping
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import Any

from dpone.adapters.studio_http import (
    StudioThreadingHTTPServer,
    build_studio_http_handler,
)
from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.readiness.studio_entrypoint import (
    StudioApplicationService,
    StudioAuditLog,
    StudioError,
    StudioHttpConfig,
    build_studio_api_service,
    build_studio_router,
    render_studio_metadata_markdown,
    studio_metadata,
)


def cmd_studio(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx
    config = _config_from_args(args)
    try:
        config.validate()
    except StudioError as exc:
        _render_error(exc, fmt=args.format)
        return 4
    payload = studio_metadata(host=config.host, port=int(args.port), config=config)
    if args.serve:
        try:
            _serve(
                config.host,
                int(args.port),
                payload,
                config=config,
                logger=logger,
            )
        except OSError:
            _render_error(
                StudioError(
                    "DPONE_STUDIO_BIND_FAILED",
                    "Studio could not bind the requested host and port.",
                    stage="studio_startup",
                ),
                fmt=args.format,
            )
            return 3
        return 0
    if args.format == "json":
        write_json(payload)
    elif args.format == "md":
        write_text(render_studio_metadata_markdown(payload))
    else:
        write_text(_metadata_text(payload))
    return 0


def build_studio_handler(
    payload: dict[str, Any],
    *,
    config: StudioHttpConfig | None = None,
    service: StudioApplicationService | None = None,
    logger: logging.Logger | None = None,
) -> type[BaseHTTPRequestHandler]:
    """Compatibility factory used by local servers and contract tests."""

    effective_config = config or StudioHttpConfig(
        host=str(payload.get("host") or "127.0.0.1"),
        token=os.environ.get("DPONE_STUDIO_TOKEN", "").strip(),
    )
    effective_config.validate()
    audit = StudioAuditLog(capacity=effective_config.audit_capacity)
    application = service or build_studio_api_service(root=Path.cwd(), audit=audit)
    router = build_studio_router(
        application,
        config=effective_config,
        legacy_studio_metadata=payload,
    )
    return build_studio_http_handler(
        router=router,
        config=effective_config,
        audit=audit,
        logger=logger,
    )


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "studio",
        help="Run or describe the local dpone Studio API bridge",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=_valid_port, default=8765, help="TCP port from 1 to 65535")
    parser.add_argument("--serve", action="store_true", help="Start the local HTTP API server and block")
    parser.add_argument(
        "--allow-remote",
        action="store_true",
        help="Allow a non-loopback development bind with token and exact CORS origins",
    )
    parser.add_argument(
        "--cors-origin",
        dest="cors_origins",
        action="append",
        help="Exact allowed remote UI origin; repeat for multiple origins",
    )
    parser.add_argument("--format", choices=["text", "json", "md"], default="text")
    return parser


def _serve(
    host: str,
    port: int,
    payload: dict[str, Any],
    *,
    config: StudioHttpConfig,
    logger: logging.Logger,
) -> None:
    server = StudioThreadingHTTPServer(
        (host, port),
        build_studio_handler(payload, config=config, logger=logger),
    )
    server.daemon_threads = True
    logger.info(f"dpone Studio API listening on http://{host}:{port}")
    server.serve_forever()


def _config_from_args(args: argparse.Namespace) -> StudioHttpConfig:
    return StudioHttpConfig(
        host=str(args.host),
        token=os.environ.get("DPONE_STUDIO_TOKEN", "").strip(),
        allow_remote=bool(args.allow_remote),
        cors_origins=tuple(args.cors_origins or ()),
    )


def _valid_port(value: str) -> int:
    try:
        port = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("port must be an integer from 1 to 65535") from exc
    if not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError("port must be an integer from 1 to 65535")
    return port


def _metadata_text(payload: Mapping[str, Any]) -> str:
    return (
        "dpone Studio API\n"
        f"- URL: {payload['url']}\n"
        f"- Mode: {payload['mode']}\n"
        f"- UI: {payload['ui_status']}\n"
        f"- Usability: {payload['usability_status']}\n"
        f"- Release verdict: {payload['release_verdict']}\n"
        "- Start: dpone studio --serve\n"
    )


def _render_error(error: StudioError, *, fmt: str) -> None:
    payload = error.to_payload()
    if fmt == "json":
        sys.stderr.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
        return
    sys.stderr.write(f"dpone studio blocked: {error.code}\n{error.message}\n")


__all__ = ["build_studio_handler", "cmd_studio", "register_parser"]
