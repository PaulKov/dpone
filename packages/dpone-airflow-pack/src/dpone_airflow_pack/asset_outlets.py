"""Optional data-aware scheduling outlets for dpone runtime tasks.

Workload catalogs may declare produced datasets under
``airflow.execution.outlets``. Legacy values are URI strings; this layer also
supports richer mapping items when build-time metadata wants to retain provenance.

For environment-neutral dbt release packs, MSSQL outlets may carry a logical
``asset_ref``. The deployment index then supplies
``mssql_asset_outlet_projection`` so parse-time materialization uses the
environment binding-set + registry without rewriting release bytes.

The pack builder copies ``airflow.execution`` verbatim into compact pack output,
and this module turns resolved URIs into Airflow Asset/Dataset objects at DAG parse time.

Version compatibility is handled lazily:

- Airflow 3.x — ``airflow.sdk.Asset``
- Airflow 2.4+ — ``airflow.datasets.Dataset``
- neither importable — outlets are skipped (packs stay parse-safe on minimal images)

Parse shield: only legacy authoring URIs rejected by a provider sanitizer
(for example compact ``mssql://schema/table``) are skipped at parse time.
Projected logical outlets fail closed on ``Asset()``/``Dataset()`` ``ValueError``
with ``DPONE_MSSQL_ASSET_OUTLET_PROJECTION_INVALID``. Missing deployment
projection for a logical ``asset_ref`` is fail-closed with
``DPONE_MSSQL_ASSET_OUTLET_PROJECTION_REQUIRED`` — never a silent green DAG.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal

from dpone_airflow_pack.init_fetch_contract import InitFetchProviderError
from dpone_airflow_pack.mssql_asset_ref_codec import asset_ref_sha256
from dpone_airflow_pack.mssql_asset_uri_codec import (
    MAX_MSSQL_ASSET_URI_CHARS,
    require_canonical_mssql_asset_uri,
)
from dpone_airflow_pack.mssql_outlet_projection_contract import (
    PROJECTION_INVALID,
    PROJECTION_REQUIRED,
)

_LOG = logging.getLogger(__name__)
_PARSE_REJECT_CODE = "DPONE_MSSQL_ASSET_URI_PARSE_REJECTED"
OutletProvenance = Literal["deployment_projection", "legacy_authoring"]


@dataclass(frozen=True, slots=True)
class OutletUriSpec:
    """One outlet URI with provenance used for fail-closed vs parse-shield."""

    uri: str
    provenance: OutletProvenance


def outlet_specs_from_pack(
    pack: Mapping[str, Any],
    *,
    mssql_asset_uri_by_ref: Mapping[str, str] | None = None,
) -> tuple[OutletUriSpec, ...]:
    """Extract outlet URI specs (with provenance) from a compact pack payload."""

    airflow_section = pack.get("airflow")
    if not isinstance(airflow_section, Mapping):
        return ()
    execution = airflow_section.get("execution")
    if not isinstance(execution, Mapping):
        return ()
    raw = execution.get("outlets")
    if not isinstance(raw, Sequence) or isinstance(raw, str | bytes):
        return ()
    specs: list[OutletUriSpec] = []
    for item in raw:
        spec = _outlet_spec(item, mssql_asset_uri_by_ref=mssql_asset_uri_by_ref)
        if spec is not None:
            specs.append(spec)
    return tuple(specs)


def outlet_uris_from_pack(
    pack: Mapping[str, Any],
    *,
    mssql_asset_uri_by_ref: Mapping[str, str] | None = None,
) -> tuple[str, ...]:
    """Extract declared outlet URIs from a compact pack payload.

    Logical ``asset_ref`` outlets require a complete projection map and raise
    stable activation codes instead of silently dropping Asset events.
    """

    return tuple(spec.uri for spec in outlet_specs_from_pack(pack, mssql_asset_uri_by_ref=mssql_asset_uri_by_ref))


def _outlet_spec(
    raw_item: object,
    *,
    mssql_asset_uri_by_ref: Mapping[str, str] | None,
) -> OutletUriSpec | None:
    if isinstance(raw_item, str):
        text = raw_item.strip()
        return OutletUriSpec(uri=text, provenance="legacy_authoring") if text else None
    if isinstance(raw_item, Mapping):
        asset_ref = raw_item.get("asset_ref")
        if isinstance(asset_ref, Mapping):
            uri = _resolve_asset_ref_uri(asset_ref, mssql_asset_uri_by_ref=mssql_asset_uri_by_ref)
            return OutletUriSpec(uri=uri, provenance="deployment_projection")
        text = str(raw_item.get("uri") or "").strip()
        return OutletUriSpec(uri=text, provenance="legacy_authoring") if text else None
    return None


def _resolve_asset_ref_uri(
    asset_ref: Mapping[str, Any],
    *,
    mssql_asset_uri_by_ref: Mapping[str, str] | None,
) -> str:
    digest = asset_ref_sha256(asset_ref)
    if digest is None:
        raise InitFetchProviderError(
            PROJECTION_INVALID,
            "mssql logical asset_ref is incomplete",
        )
    if mssql_asset_uri_by_ref is None:
        raise InitFetchProviderError(
            PROJECTION_REQUIRED,
            "mssql logical asset_ref outlets require mssql_asset_outlet_projection",
        )
    uri = str(mssql_asset_uri_by_ref.get(digest) or "").strip()
    if not uri:
        raise InitFetchProviderError(
            PROJECTION_REQUIRED,
            "mssql_asset_outlet_projection is missing a logical asset_ref",
        )
    try:
        return require_canonical_mssql_asset_uri(uri)
    except ValueError as exc:
        raise InitFetchProviderError(
            PROJECTION_INVALID,
            "mssql_asset_outlet_projection uri failed closed canonical validation",
        ) from exc


def build_asset_outlets(uris: Sequence[str] | Sequence[OutletUriSpec]) -> list[Any]:
    """Build Asset (Airflow 3) or Dataset (Airflow 2) objects for the URIs.

    Provenance controls failure mode:

    - ``deployment_projection`` + ``ValueError`` →
      ``DPONE_MSSQL_ASSET_OUTLET_PROJECTION_INVALID`` (block)
    - ``legacy_authoring`` + ``ValueError`` → parse-shield warning and skip
    """

    specs = _coerce_specs(uris)
    if not specs:
        return []
    asset_class = _asset_class()
    if asset_class is None:
        return []
    built: list[Any] = []
    for index, spec in enumerate(specs):
        if len(spec.uri) > MAX_MSSQL_ASSET_URI_CHARS and spec.uri.startswith("mssql://"):
            _reject_or_shield(spec, index=index, exc=ValueError("mssql Asset URI exceeds length bound"))
            continue
        if spec.provenance == "deployment_projection":
            try:
                require_canonical_mssql_asset_uri(spec.uri)
            except ValueError as exc:
                raise InitFetchProviderError(
                    PROJECTION_INVALID,
                    "projected mssql outlet failed closed URI validation before Asset()",
                ) from exc
        try:
            built.append(asset_class(spec.uri))
        except ValueError as exc:
            _reject_or_shield(spec, index=index, exc=exc)
    return built


def _coerce_specs(uris: Sequence[str] | Sequence[OutletUriSpec]) -> tuple[OutletUriSpec, ...]:
    specs: list[OutletUriSpec] = []
    for item in uris:
        if isinstance(item, OutletUriSpec):
            specs.append(item)
        else:
            text = str(item).strip()
            if text:
                specs.append(OutletUriSpec(uri=text, provenance="legacy_authoring"))
    return tuple(specs)


def _reject_or_shield(spec: OutletUriSpec, *, index: int, exc: Exception) -> None:
    if spec.provenance == "deployment_projection":
        raise InitFetchProviderError(
            PROJECTION_INVALID,
            "projected mssql outlet rejected by Airflow Asset()/Dataset() ValueError",
        ) from exc
    # Do not log the raw URI — packs may carry sensitive coordinates.
    _LOG.warning(
        "%s outlet_index=%s provenance=%s reason=provider_value_error",
        _PARSE_REJECT_CODE,
        index,
        spec.provenance,
    )


def _asset_class() -> Any | None:
    try:
        from airflow.sdk import Asset

        return Asset
    except Exception:  # noqa: BLE001 - Airflow 2.x fallback below.
        pass
    try:
        from airflow.datasets import Dataset

        return Dataset
    except Exception:  # noqa: BLE001 - keep packs parse-safe without Airflow.
        return None


__all__ = [
    "OutletUriSpec",
    "build_asset_outlets",
    "outlet_specs_from_pack",
    "outlet_uris_from_pack",
]
