from __future__ import annotations

from dpone._compat import StrEnum


class OutputFormat(StrEnum):
    """Supported output formats for CLI commands."""

    text = "text"
    json = "json"
    yaml = "yaml"

    TEXT = "text"
    JSON = "json"
    YAML = "yaml"


def resolve_output_format(
    args: object,
    *,
    default: OutputFormat = OutputFormat.text,
    attr_names: tuple[str, ...] = ("output", "format"),
) -> OutputFormat:
    """Resolve output format from argparse-like args."""

    for attr in attr_names:
        raw = getattr(args, attr, None)
        if raw is None:
            continue
        value = str(raw).strip().lower()
        if not value:
            continue
        try:
            return OutputFormat(value)
        except Exception:
            pass

    if bool(getattr(args, "json", False)):
        return OutputFormat.json

    return default


def is_machine_output(fmt: OutputFormat) -> bool:
    return fmt in (OutputFormat.json, OutputFormat.yaml)


__all__ = ["OutputFormat", "is_machine_output", "resolve_output_format"]
