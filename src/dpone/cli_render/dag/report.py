from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.services.dag.views import DagReportView


from dataclasses import dataclass
from typing import Any

from dpone.output import dumps_json, write_json, write_text, write_text_file


@dataclass(frozen=True, slots=True)
class RenderedReport:
    """Rendered outputs for a DAG report."""

    markdown: str
    json_obj: Any


def render_report(view: DagReportView) -> RenderedReport:
    md_max_edges = int(view.meta.options.get("md_max_edges", 200) or 200)
    md_text = view.report.to_markdown(max_edges=md_max_edges)
    json_obj = view.to_jsonable()
    return RenderedReport(markdown=md_text, json_obj=json_obj)


def emit_report(
    view: DagReportView,
    *,
    fmt: str,
    out_md: str | None = None,
    out_json: str | None = None,
) -> None:
    """Emit report outputs to stdout or files."""

    fmt = (fmt or "md").strip().lower()
    if fmt not in ("md", "json", "both"):
        fmt = "md"

    rendered = render_report(view)

    if fmt in ("md", "both"):
        if out_md:
            write_text_file(out_md, rendered.markdown)
        else:
            write_text(rendered.markdown)

    if fmt in ("json", "both"):
        if out_json:
            write_text_file(out_json, dumps_json(rendered.json_obj))
        else:
            if fmt == "both" and not out_md:
                write_text("\n\n---\n\n```json\n")
                write_text(dumps_json(rendered.json_obj).rstrip() + "\n")
                write_text("```\n")
            else:
                write_json(rendered.json_obj)
