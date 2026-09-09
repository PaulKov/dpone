"""Compatibility facade for CLI output helpers."""

from __future__ import annotations

from dpone.output_files import write_text_file
from dpone.output_format import OutputFormat, is_machine_output, resolve_output_format
from dpone.output_json import dumps_json, write_json
from dpone.output_text import write_line, write_text
from dpone.output_yaml import dumps_yaml, write_yaml

__all__ = [
    "OutputFormat",
    "dumps_json",
    "dumps_yaml",
    "is_machine_output",
    "resolve_output_format",
    "write_json",
    "write_line",
    "write_text",
    "write_text_file",
    "write_yaml",
]
