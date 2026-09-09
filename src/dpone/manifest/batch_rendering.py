from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from jinja2 import Environment, StrictUndefined

from dpone.manifest.batch_merge import _is_mapping
from dpone.manifest.batch_naming import _to_identifier, _to_snake
from dpone.manifest.errors import ManifestConfigurationError


class TemplateRenderer:
    """Renders Jinja-like templates in YAML values.

    Supports two modes for strings:
    - full template rendering: "prefix {{ var }} suffix" -> str
    - expression-only: "{{ var }}" -> typed value (int/bool/list/dict)
    """

    def __init__(self) -> None:
        env = Environment(undefined=StrictUndefined, autoescape=False)
        # Small set of safe helpers; users may still use standard jinja filters.
        env.filters.setdefault("dpone_snake", _to_snake)
        env.filters.setdefault("dpone_ident", _to_identifier)
        self._env = env

    def render(self, value: Any, context: Mapping[str, Any]) -> Any:
        if isinstance(value, str):
            return self._render_str(value, context)
        if isinstance(value, list):
            return [self.render(v, context) for v in value]
        if _is_mapping(value):
            return {k: self.render(v, context) for k, v in value.items()}
        return value

    def _render_str(self, template: str, context: Mapping[str, Any]) -> Any:
        s = template
        # Fast-path: not a template
        if "{{" not in s and "{%" not in s and "{#" not in s:
            return s
        stripped = s.strip()
        if stripped.startswith("{{") and stripped.endswith("}}") and stripped.count("{{") == 1:
            expr = stripped[2:-2].strip()
            try:
                fn = self._env.compile_expression(expr)
                return fn(**dict(context))
            except Exception as exc:
                raise ManifestConfigurationError(f"Ошибка вычисления выражения '{{{{ {expr} }}}}': {exc}") from exc

        try:
            tmpl = self._env.from_string(s)
            return tmpl.render(dict(context))
        except Exception as exc:
            raise ManifestConfigurationError(f"Ошибка рендера шаблона '{s}': {exc}") from exc
