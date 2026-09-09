from __future__ import annotations


def build_root_parser():
    """Return the canonical argparse tree for CLI reference generation.

    Kept in the app layer so docs/services can depend on it without importing
    dpone.cli.* directly.
    """

    from ..cli.parser import build_parser

    return build_parser()
