"""Shared output boundary for the ``dpone run`` command family."""

from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text

__all__ = ["write_json", "write_text"]
