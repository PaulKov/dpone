from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dpone.dag.post_parse_explain import ParsedWhyExplanation, PostParseResult
from dpone.manifest.explain_models import ExplainResult, WhyExplanation

from .common import ManifestViewMeta


@dataclass(frozen=True, slots=True)
class ManifestExplainView:
    meta: ManifestViewMeta
    explain: ExplainResult
    why: tuple[WhyExplanation, ...] = ()
    post_parse: PostParseResult | None = None
    why_parsed: tuple[ParsedWhyExplanation, ...] = ()

    def to_jsonable(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "explain": self.explain.to_json_dict(),
        }
        if self.why:
            payload["why"] = [w.to_json_dict() for w in self.why]
        if self.post_parse is not None:
            payload["post_parse"] = self.post_parse.to_json_dict()
        if self.why_parsed:
            payload["why_parsed"] = [w.to_json_dict() for w in self.why_parsed]
        payload["meta"] = self.meta.to_jsonable()
        return payload
