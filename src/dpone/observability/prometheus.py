"""Prometheus text exposition rendering."""

from __future__ import annotations

from collections.abc import Iterable

from dpone.observability.metrics import MetricPoint


class PrometheusTextRenderer:
    """Render runtime metrics in Prometheus text exposition format."""

    def render(self, points: Iterable[MetricPoint]) -> str:
        lines: list[str] = []
        seen_headers: set[str] = set()
        for point in points:
            if point.name not in seen_headers:
                lines.append(f"# HELP {point.name} {self._escape_help(point.description)}")
                lines.append(f"# TYPE {point.name} gauge")
                seen_headers.add(point.name)
            labels = self._labels(point)
            value = int(point.value) if point.value.is_integer() else point.value
            lines.append(f"{point.name}{labels} {value}")
        return "\n".join(lines) + "\n"

    @classmethod
    def _labels(cls, point: MetricPoint) -> str:
        if not point.labels:
            return ""
        sanitized = {
            cls._label_name(str(key)): str(value) for key, value in sorted(point.labels.items()) if str(value) != ""
        }
        rendered = ",".join(f'{key}="{cls._escape_label(value)}"' for key, value in sorted(sanitized.items()))
        return f"{{{rendered}}}" if rendered else ""

    @staticmethod
    def _label_name(value: str) -> str:
        safe = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in value)
        if not safe:
            return "_"
        if safe[0].isdigit():
            return f"_{safe}"
        return safe

    @staticmethod
    def _escape_label(value: str) -> str:
        return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')

    @staticmethod
    def _escape_help(value: str) -> str:
        return value.replace("\\", "\\\\").replace("\n", "\\n")
