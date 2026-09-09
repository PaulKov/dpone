from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol, cast

from dpone.commands.gitops.common import GitOpsOutputContext, write_optional_output
from dpone.commands.output_json import dumps_json, write_json
from dpone.commands.output_text import write_text


class AirflowCommandView(Protocol):
    @property
    def exit_code(self) -> int: ...

    def to_jsonable(self) -> dict[str, Any]: ...


def emit_airflow_view(
    *,
    ctx: object,
    args: object,
    view: AirflowCommandView,
    markdown_renderer: Callable[[Any], str],
) -> int:
    if getattr(args, "format", "json") == "markdown":
        rendered = markdown_renderer(getattr(view, "report"))
        write_optional_output(cast(GitOpsOutputContext, ctx), getattr(args, "output", None), rendered)
        write_text(rendered)
        return view.exit_code
    payload = view.to_jsonable()
    rendered = dumps_json(payload)
    write_optional_output(cast(GitOpsOutputContext, ctx), getattr(args, "output", None), rendered)
    write_json(payload)
    return view.exit_code


__all__ = ["emit_airflow_view"]
