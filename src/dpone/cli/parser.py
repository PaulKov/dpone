from __future__ import annotations

import argparse
from importlib.metadata import version

from ..commands.registry import get_commands
from .public_argument_parser import PublicArgumentParser


def build_parser() -> argparse.ArgumentParser:
    parser = PublicArgumentParser(prog="dpone")
    parser.add_argument("-v", "--version", action="version", version=f"%(prog)s {version('dpone')}")
    sub = parser.add_subparsers(dest="command", required=True)

    for cmd in get_commands():
        cmd.register(sub)

    return parser
