"""Source-free v1 quality identities for the bounded full-refresh subset.

This module hashes configuration; it does not establish evidence authority.
Effective-plan records must come from the trusted, fenced capsule producer.
Only unchanged full-refresh semantics and recorded ordered schema observations
are supported. Extraction overrides, nested normalization and opaque transforms
need a separately versioned reconstruction contract and fail closed here.

The field registries below are exhaustive at the LoadConfig and option boundary.
Nested semantic documents are included in full, with no recursive exclusions.
Only source_options/sink_options use the same explicit option registry. No raw
configuration, query, credential or column name is returned by any public API.
"""

from __future__ import annotations

import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import fields
from hashlib import sha256
from types import MappingProxyType
from typing import cast

from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.runtime.quality_replay_contracts import canonical_json_bytes

CONTRACT_VERSION = "dpone.quality.replay.identity.v1"
_MAX_BYTES = 256 * 1024
_MAX_DEPTH = 32
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")

# Exclusions are individual names, never prefixes or arbitrary caller choices.
EXCLUDED_LOAD_FIELDS = MappingProxyType({"log_sample_rows": "diagnostic sample size"})
EXCLUDED_OPTIONS = MappingProxyType(
    {
        "_source_connector": "injected source client; logical source is bound by LoadConfig",
        "run_id": "attempt identity; original run is separately capsule-bound",
        "load_id": "attempt identity; original load is separately capsule-bound",
        "__dpone_load_identity": "injected attempt IDs and extraction timestamp",
        "__dpone_clickhouse_full_refresh_scheduler_identity_v1": "invocation is separately publication-bound",
        "__dpone_clickhouse_full_refresh_replay_v1": "injected replay result; never a proof producer",
        "password": "credential; logical connection reference remains bound",
        "token": "credential; logical connection reference remains bound",
        "access_token": "credential; logical connection reference remains bound",
        "secret_key": "credential; logical connection reference remains bound",
        "log_sample_rows": "diagnostic sample size",
    }
)

_LOAD_FIELDS = frozenset(
    "source_conn_id target_conn_id source_schema source_table target_schema target_table "
    "source_database target_database staging_schema staging_database staging_table load_strategy "
    "unique_key only_new_rows micro_batch_commit overwrite_type merge_policy duplicate_policy "
    "allow_non_recommended_policy mutations_sync partition dedup_expression dedup_target with_dedup "
    "custom_predicate portable_scope batch_size export_format compress_export reconciliation "
    "reconciliation_policy repair_authority_ref tech_schema options source_materialization_work_connection_ref "
    "source_materialization_work_database source_materialization_work_schema".split()
)
_SEMANTIC_OPTIONS = frozenset(
    "source_type sink_type __dpone_route_identity_v1_endpoint_types source_options sink_options "
    "query sql source_query custom_query source_custom_predicate custom_predicate columns column_mapping "
    "normalization lineage schema_contract schema_evolution physical_design quality acceptance "
    "merge_policy duplicate_policy partition allow_non_recommended_policy mutations_sync strategy_intelligence "
    "unique_key only_new_rows micro_batch_commit overwrite_type dedup_expression dedup_target with_dedup "
    "batch_size export_format compress_export reconciliation state runtime_storage type_fidelity native_transfer "
    "profile incremental_column date_column date_from date_to partition_by order_by ttl table_ttl "
    "clickhouse_engine clickhouse_partition_by clickhouse_order_by clickhouse_ttl clickhouse_bulk "
    "clickhouse_max_insert_block_size source_byte_budget max_source_bytes __dpone_source_byte_budget_v1 "
    "manifest_dir repo_root "
    "export_to_gcs gcs_format gcs_chunk_rows".split()
)
_RECORD_FIELDS = frozenset(
    {
        "contract_version",
        "admission_digest",
        "effective_config_digest",
        "source_schema_digest",
        "payload_schema_digest",
        "target_schema_digest",
        "effective_plan_digest",
    }
)


class QualityReplayIdentityError(ValueError):
    """Safe identity failure without input values or connector diagnostics."""

    blocks_committed_success = True

    def __init__(self, reason: str = "UNSUPPORTED") -> None:
        self.code = f"DPONE_REPLAY_QUALITY_EVIDENCE_{reason}"
        super().__init__(self.code)


def admission_digest(load_config: LoadConfig) -> str:
    """Hash complete supported semantics without source calls or repr fallbacks.

    Call after deterministic configuration preparation, before source extraction.
    The replay caller must use that same preparation boundary. Unknown fields,
    dynamic instance attributes and unsupported values reject capability use.
    """

    if type(load_config) is not LoadConfig:
        raise QualityReplayIdentityError()
    registered = _LOAD_FIELDS | EXCLUDED_LOAD_FIELDS.keys()
    if {field.name for field in fields(load_config)} != registered or set(vars(load_config)) != registered:
        raise QualityReplayIdentityError()
    if load_config.load_strategy is not LoadStrategy.FULL_REFRESH:
        raise QualityReplayIdentityError()
    if load_config.reconciliation or load_config.reconciliation_policy is not None or load_config.portable_scope:
        raise QualityReplayIdentityError()
    projection = {name: getattr(load_config, name) for name in _LOAD_FIELDS if name != "options"}
    projection["load_strategy"] = load_config.load_strategy.value
    projection["options"] = _options(load_config.options)
    return _digest({"contract_version": CONTRACT_VERSION, "configuration": _json_value(projection)})


def schema_digest(schema: object) -> str:
    """Bind ordered, nonempty (name, type) pairs without persisting raw names.

    Both the logical and physical types must be supplied by the original
    producer at their own boundaries; replay must never fetch source schema.
    """

    if type(schema) not in (list, tuple) or not schema:
        raise QualityReplayIdentityError()
    columns = []
    names = set()
    for column in cast(Sequence[object], schema):
        if type(column) not in (list, tuple):
            raise QualityReplayIdentityError()
        pair = cast(Sequence[object], column)
        if len(pair) != 2:
            raise QualityReplayIdentityError()
        name, kind = pair
        if type(name) is not str or not name or type(kind) is not str or not kind or name in names:
            raise QualityReplayIdentityError()
        names.add(name)
        columns.append([name, kind])
    return _digest({"contract_version": CONTRACT_VERSION, "columns": _json_value(columns)})


def prepare_effective_plan(
    admission_config: LoadConfig,
    effective_config: LoadConfig,
    *,
    source_schema: object,
    payload_schema: object,
    target_schema: object,
) -> dict[str, object]:
    """Freeze v1 provenance after original extraction and validated projection.

    No semantic config override is supported in v1. Transient injected clients
    and attempt IDs may differ. Schema digests record the trusted producer's
    observations; their authenticity comes only from the enclosing authority.
    """

    admission = admission_digest(admission_config)
    effective = admission_digest(effective_config)
    if admission != effective:
        raise QualityReplayIdentityError()
    record: dict[str, object] = {
        "contract_version": CONTRACT_VERSION,
        "admission_digest": admission,
        "effective_config_digest": effective,
        "source_schema_digest": schema_digest(source_schema),
        "payload_schema_digest": schema_digest(payload_schema),
        "target_schema_digest": schema_digest(target_schema),
    }
    return {**record, "effective_plan_digest": _digest(record)}


def validate_effective_plan(admission_config: LoadConfig, record: Mapping[str, object]) -> str:
    """Reconstruct unchanged configuration using trusted recorded schema inputs.

    This pure check grants no authority to a caller-provided mapping. The
    enclosing validator must independently check publication identity, current
    target generation/schema and capsule authority before accepting replay.
    """

    if type(record) is not dict or set(record) != _RECORD_FIELDS:
        raise QualityReplayIdentityError("INVALID")
    if record["contract_version"] != CONTRACT_VERSION:
        raise QualityReplayIdentityError("UNSUPPORTED")
    for name in _RECORD_FIELDS - {"contract_version"}:
        value = record[name]
        if type(value) is not str or not _DIGEST.fullmatch(value):
            raise QualityReplayIdentityError("INVALID")
    admission = admission_digest(admission_config)
    if record["admission_digest"] != admission or record["effective_config_digest"] != admission:
        raise QualityReplayIdentityError("MISMATCH")
    expected = _digest({name: value for name, value in record.items() if name != "effective_plan_digest"})
    if expected != record["effective_plan_digest"]:
        raise QualityReplayIdentityError("MISMATCH")
    return expected


def _options(options: object, depth: int = 0) -> dict[str, object]:
    if type(options) is not dict or depth > _MAX_DEPTH:
        raise QualityReplayIdentityError()
    projection: dict[str, object] = {}
    for key, value in options.items():
        if type(key) is not str:
            raise QualityReplayIdentityError()
        if key in EXCLUDED_OPTIONS:
            continue
        if key not in _SEMANTIC_OPTIONS:
            raise QualityReplayIdentityError()
        if key in {"query", "sql", "source_query", "custom_query"}:
            _validate_query(value)
        if key in {"source_options", "sink_options"}:
            projection[key] = _options(value, depth + 1)
        else:
            projection[key] = _json_value(value)
        if key == "normalization" and (not isinstance(value, dict) or value.get("enabled", False)):
            raise QualityReplayIdentityError()
    return projection


def _validate_query(value: object) -> None:
    """Reject external files and template execution without content authority."""

    if type(value) is str and value:
        return
    if type(value) is not dict or set(value) - {"mode", "sql"}:
        raise QualityReplayIdentityError()
    if value.get("mode", "inline") != "inline" or type(value.get("sql")) is not str or not value["sql"]:
        raise QualityReplayIdentityError()


def _json_value(value: object, depth: int = 0) -> object:
    """Bound plain JSON values, preserving every nested semantic field."""

    if depth > _MAX_DEPTH:
        raise QualityReplayIdentityError()
    if value is None or type(value) in (bool, int):
        return value
    if type(value) is str:
        if len(value) > _MAX_BYTES:
            raise QualityReplayIdentityError()
        return value
    if type(value) is float and math.isfinite(value):
        return value
    if type(value) in (list, tuple):
        sequence = cast(Sequence[object], value)
        if len(sequence) > _MAX_BYTES:
            raise QualityReplayIdentityError()
        return [_json_value(item, depth + 1) for item in sequence]
    if type(value) is dict:
        if len(value) > _MAX_BYTES or any(type(key) is not str for key in value):
            raise QualityReplayIdentityError()
        return {key: _json_value(item, depth + 1) for key, item in value.items()}
    raise QualityReplayIdentityError()


def _digest(value: object) -> str:
    try:
        encoded = canonical_json_bytes(value)
    except (TypeError, ValueError, UnicodeError, RecursionError) as exc:
        raise QualityReplayIdentityError() from exc
    if len(encoded) > _MAX_BYTES:
        raise QualityReplayIdentityError()
    return "sha256:" + sha256(encoded).hexdigest()
