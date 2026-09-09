"""Legacy dbt preview capability downgrade policy."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from dpone.contracts.dbt_publish_models import DbtPublishIssue

PREVIEW_REASON = "legacy_dbt_publish_preview_only"


def legacy_preview_capability(capability: dict[str, Any]) -> dict[str, Any]:
    """Force one compatibility capability to remain explicitly unverified."""

    preview = dict(capability)
    raw_reasons = preview.get("evidence_reason_codes")
    reasons = list(raw_reasons) if isinstance(raw_reasons, list | tuple) else []
    if PREVIEW_REASON not in reasons:
        reasons.append(PREVIEW_REASON)
    preview.update(
        {
            "certification_level": "experimental",
            "evidence_status": "UNVERIFIED",
            "evidence_refs": [],
            "evidence_reason_codes": reasons,
        }
    )
    return preview


class LegacyRouteCapabilityAdapter:
    """Adapt the historical route-capability signature to canonical kwargs."""

    def __init__(self, delegate: Any) -> None:
        self._delegate = delegate

    def resolve(self, **request: Any) -> tuple[dict[str, Any] | None, tuple[DbtPublishIssue, ...]]:
        method = self._delegate.resolve
        parameters = inspect.signature(method).parameters
        modern_shape = "transport" in parameters or any(
            parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters.values()
        )
        selected = request if modern_shape else {key: request[key] for key in ("source", "sink", "strategy", "path")}
        capability, issues = cast(
            tuple[dict[str, Any] | None, tuple[Any, ...]],
            method(**selected),
        )
        return legacy_preview_capability(capability) if capability is not None else None, issues


class LegacyPreviewRouteCapabilities:
    """Expose the single explicitly unverified historical preview route."""

    def __init__(self, *, fingerprint: Callable[[dict[str, Any]], str], issue_factory: Callable[..., Any]) -> None:
        self._fingerprint = fingerprint
        self._issue_factory = issue_factory

    def resolve(self, **request: Any) -> tuple[dict[str, Any] | None, tuple[DbtPublishIssue, ...]]:
        source = str(request["source"]).lower()
        sink = str(request["sink"]).lower()
        strategy = str(request["strategy"]).lower()
        route_id = f"{source}:{sink}:{strategy}"
        if (source, sink) != ("mssql", "clickhouse"):
            return None, (
                self._issue_factory(
                    code="DPONE_DBT_ROUTE_NOT_CERTIFIED",
                    message=f"Legacy preview supports only mssql -> clickhouse, got {source} -> {sink}",
                    path=str(request["path"]),
                    remediation="Use the canonical compiler with current route capability evidence.",
                ),
            )
        return (
            legacy_preview_capability(
                {
                    "snapshot_id": self._fingerprint({"compatibility": PREVIEW_REASON}),
                    "route_id": route_id,
                    "support": "conditional",
                    "variant_id": f"{route_id}|legacy-preview",
                    "transport": request.get("transport") or "legacy_preview",
                    "schema_evolution": request.get("schema_evolution") or "legacy_preview",
                    "airflow_runtime_mode": request.get("airflow_runtime_mode") or "local_preview",
                }
            ),
            (),
        )
