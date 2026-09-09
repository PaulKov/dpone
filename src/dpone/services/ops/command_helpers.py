from __future__ import annotations

from dpone.output_json import write_json
from dpone.output_text import write_text

_ROUTE_ATTESTATION_CONFIGURATION_CODES = frozenset(
    {
        "DPONE_ROUTE_ATTESTATION_INPUT_INVALID",
        "DPONE_ROUTE_ATTESTATION_INPUT_UNAVAILABLE",
        "DPONE_ROUTE_ATTESTATION_POLICY_INVALID",
        "DPONE_ROUTE_ATTESTATION_SUBJECT_INVALID",
    }
)


def _emit(payload: dict, markdown: str, fmt: str) -> None:
    if fmt == "json":
        write_json(payload)
    else:
        write_text(markdown)


def _route_attestation_exit_code(code: object) -> int:
    """Map route trust decisions to stable CLI configuration or safety exits."""

    return 2 if str(code or "") in _ROUTE_ATTESTATION_CONFIGURATION_CODES else 4


def _parse_artifacts(values: list[str]) -> dict[str, str]:
    artifacts: dict[str, str] = {}
    for value in values:
        if "=" not in value:
            raise ValueError("--artifact must use name=/path/to/artifact.json")
        name, path = value.split("=", 1)
        artifacts[name.strip()] = path.strip()
    return artifacts


def _parse_bool_checks(values: list[str]) -> dict[str, bool]:
    checks: dict[str, bool] = {}
    for value in values:
        if "=" not in value:
            raise ValueError("--check must use name=true|false")
        name, raw = value.split("=", 1)
        checks[name.strip()] = raw.strip().lower() in {"1", "true", "yes", "y", "passed", "pass"}
    return checks


def _parse_type_hints(values: list[str]) -> dict[str, str]:
    hints: dict[str, str] = {}
    for value in values:
        if "=" not in value:
            raise ValueError("--type must use name=type")
        name, raw_type = value.split("=", 1)
        hints[name.strip()] = raw_type.strip()
    return hints


def _parse_dataset_refs(values: list[str]) -> dict[str, str]:
    datasets: dict[str, str] = {}
    for value in values:
        if "=" not in value:
            raise ValueError("dataset references must use namespace=name")
        namespace, name = value.split("=", 1)
        datasets[namespace.strip()] = name.strip()
    return datasets


def _parse_post_checks(values: list[str]) -> dict[str, bool]:
    checks: dict[str, bool] = {}
    for value in values:
        if "=" not in value:
            raise ValueError("--post-check must use name=true|false")
        name, raw_status = value.split("=", 1)
        normalized = raw_status.strip().lower()
        if normalized not in {"true", "false", "pass", "fail", "passed", "failed"}:
            raise ValueError("--post-check status must be true|false")
        checks[name.strip()] = normalized in {"true", "pass", "passed"}
    return checks


def _parse_thresholds(values: list[str]) -> dict[str, dict[str, float]]:
    thresholds: dict[str, dict[str, float]] = {}
    for value in values:
        if "=" not in value:
            raise ValueError("--alert must use metric.max=value or metric.min=value")
        left, raw_number = value.split("=", 1)
        metric, _, bound = left.partition(".")
        if bound not in {"min", "max"}:
            raise ValueError("--alert must use metric.max=value or metric.min=value")
        thresholds.setdefault(metric.strip(), {})[bound] = float(raw_number)
    return thresholds


def _markdown_table(title: str, payload: dict) -> str:
    lines = [f"# dpone {title}", "", "| field | value |", "|---|---|"]
    for key, value in payload.items():
        lines.append(f"| `{key}` | `{value}` |")
    return "\n".join(lines) + "\n"
