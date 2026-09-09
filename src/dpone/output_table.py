"""Table rendering helpers for CLI.

We use PrettyTable for human-readable output. This small module keeps table
formatting consistent across commands.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any

try:
    from prettytable import PrettyTable  # type: ignore
except Exception:  # pragma: no cover
    # Minimal fallback for environments without prettytable.
    class PrettyTable:  # type: ignore
        def __init__(self):
            self.field_names: list[str] = []
            self.align = "l"
            self._rows: list[list[str]] = []

        def add_row(self, row):
            self._rows.append(["" if v is None else str(v) for v in row])

        def __str__(self) -> str:
            cols = [self.field_names] + self._rows
            widths: list[int] = [0] * (len(self.field_names) or 0)
            for r in cols:
                for i, v in enumerate(r):
                    if i >= len(widths):
                        widths.append(0)
                    widths[i] = max(widths[i], len(str(v)))

            def fmt_row(r):
                out = []
                for i, v in enumerate(r):
                    s = str(v)
                    w = widths[i] if i < len(widths) else len(s)
                    out.append(s.ljust(w))
                return " | ".join(out)

            if not self.field_names:
                return "\n".join(" | ".join(r) for r in self._rows)

            sep = "-+-".join("-" * w for w in widths)
            lines = [fmt_row(self.field_names), sep]
            for r in self._rows:
                lines.append(fmt_row(r))
            return "\n".join(lines)


def render_table(
    headers: Sequence[str],
    rows: Iterable[Sequence[Any]],
    *,
    align: str = "l",
) -> str:
    t = PrettyTable()
    t.field_names = list(headers)
    t.align = str(align)
    for row in rows:
        t.add_row(list(row))
    return str(t)
