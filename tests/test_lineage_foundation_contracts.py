from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime

from dpone.config import LoadStrategy
from dpone.contracts.technical_columns import (
    TechnicalColumnCatalog,
    TechnicalColumnRole,
    resolve_technical_column_name,
)
from dpone.runtime.artifacts import InMemoryRowsArtifact
from dpone.runtime.lineage import LineageIdentityService, LineageOptions, RowLineageEnricher
from dpone.runtime.sinks.base import LoadPayload


def test_technical_column_catalog_uses_canonical_dpone_namespace() -> None:
    catalog = TechnicalColumnCatalog()

    assert catalog.name(TechnicalColumnRole.LOADED_AT) == "__dpone__loaded_at"
    assert catalog.name(TechnicalColumnRole.UPDATED_AT) == "__dpone__updated_at"
    assert catalog.name(TechnicalColumnRole.DELETED_AT) == "__dpone__deleted_at"
    assert catalog.name(TechnicalColumnRole.XMIN) == "__dpone__xmin"
    assert catalog.name(TechnicalColumnRole.LOAD_ID) == "__dpone__load_id"
    assert catalog.name(TechnicalColumnRole.ROW_ID) == "__dpone__row_id"
    assert catalog.name(TechnicalColumnRole.VALID_FROM_AT) == "__dpone__valid_from_at"
    assert catalog.name(TechnicalColumnRole.VALID_TO_AT) == "__dpone__valid_to_at"

    assert all(definition.name.startswith("__dpone__") for definition in catalog.definitions())
    assert all(not definition.name.endswith("_dttm") for definition in catalog.definitions())
    assert all(not definition.name.endswith("_dtm") for definition in catalog.definitions())


def test_technical_column_catalog_resolves_legacy_names_only_in_legacy_compat_mode() -> None:
    assert resolve_technical_column_name("meta__load_dtm", naming="legacy_compat") == "__dpone__loaded_at"
    assert resolve_technical_column_name("meta__update_dtm", naming="legacy_compat") == "__dpone__updated_at"
    assert resolve_technical_column_name("meta__delete_dtm", naming="legacy_compat") == "__dpone__deleted_at"
    assert resolve_technical_column_name("meta__xmin", naming="legacy_compat") == "__dpone__xmin"
    assert resolve_technical_column_name("__dpone_deleted_marker", naming="legacy_compat") == "__dpone__deleted_marker"

    assert resolve_technical_column_name("meta__load_dtm", naming="canonical") == "meta__load_dtm"


def test_lineage_options_resolve_default_standard_plus_quarantine() -> None:
    options = LineageOptions.from_config(None)

    assert options.enabled is True
    assert options.preset == "standard"
    assert options.has_feature("identity")
    assert options.has_feature("row_identity")
    assert options.has_feature("quarantine")
    assert not options.has_feature("diagnostics")
    assert options.target_column_names() == (
        "__dpone__load_id",
        "__dpone__loaded_at",
        "__dpone__row_id",
        "__dpone__extracted_at",
    )


def test_lineage_options_support_bulk_standard_without_row_materialization_columns() -> None:
    options = LineageOptions.from_config({"preset": "bulk_standard"})

    assert options.enabled is True
    assert options.has_feature("run_identity")
    assert options.has_feature("identity")
    assert options.has_feature("extraction_time")
    assert not options.has_feature("row_identity")
    assert options.target_column_names() == (
        "__dpone__run_id",
        "__dpone__load_id",
        "__dpone__loaded_at",
        "__dpone__extracted_at",
    )


def test_lineage_options_support_composable_debug_and_hierarchy_features() -> None:
    options = LineageOptions.from_config(
        {
            "enabled": True,
            "preset": "standard",
            "features": {
                "hierarchy": True,
                "operations": True,
                "diagnostics": True,
                "quarantine": True,
            },
        }
    )

    assert options.target_column_names() == (
        "__dpone__load_id",
        "__dpone__loaded_at",
        "__dpone__row_id",
        "__dpone__extracted_at",
        "__dpone__parent_row_id",
        "__dpone__root_row_id",
        "__dpone__list_index",
        "__dpone__op",
        "__dpone__meta",
    )


def test_lineage_identity_lengths_and_row_id_determinism() -> None:
    service = LineageIdentityService()
    run_id = service.new_run_id()
    load_id = service.new_load_id()
    row = {"id": 42, "updated_at": "2026-06-04T12:00:00Z", "name": "Ada"}

    first = service.row_id(
        source_type="postgres",
        source_schema="public",
        source_table="orders",
        row=row,
        unique_key=["id"],
    )
    second = service.row_id(
        source_type="postgres",
        source_schema="public",
        source_table="orders",
        row=dict(reversed(list(row.items()))),
        unique_key=["id"],
    )

    assert len(run_id) == 26
    assert len(load_id) == 26
    assert len(first) == 64
    assert first == second


def test_lineage_identity_preserves_existing_json_native_digest_contract() -> None:
    service = LineageIdentityService()
    identity = {
        "source_type": "mysql",
        "source_schema": "sales",
        "source_table": "orders",
        "key": {"id": 42},
    }
    legacy_rendering = json.dumps(identity, sort_keys=True, separators=(",", ":"), default=str, ensure_ascii=False)

    assert (
        service.row_id(
            source_type="mysql",
            source_schema="sales",
            source_table="orders",
            row={"id": 42},
            unique_key="id",
        )
        == hashlib.sha256(legacy_rendering.encode("utf-8")).hexdigest()
    )


def test_lineage_identity_distinguishes_temporal_values_from_equal_strings() -> None:
    service = LineageIdentityService()

    date_id = service.row_id(
        source_type="api",
        source_schema="",
        source_table="events",
        row={"event_key": date(2026, 7, 30)},
        unique_key="event_key",
    )
    date_string_id = service.row_id(
        source_type="api",
        source_schema="",
        source_table="events",
        row={"event_key": "2026-07-30"},
        unique_key="event_key",
    )
    timestamp = datetime(2026, 7, 30, 12, 0, tzinfo=UTC)
    timestamp_id = service.row_id(
        source_type="api",
        source_schema="",
        source_table="events",
        row={"event_key": timestamp},
        unique_key="event_key",
    )
    timestamp_string_id = service.row_id(
        source_type="api",
        source_schema="",
        source_table="events",
        row={"event_key": str(timestamp)},
        unique_key="event_key",
    )

    assert date_id != date_string_id
    assert timestamp_id != timestamp_string_id


def test_lineage_identity_distinguishes_bytes_from_equal_string_rendering() -> None:
    service = LineageIdentityService()

    bytes_id = service.row_id(
        source_type="api",
        source_schema="",
        source_table="events",
        row={"event_key": b"abc"},
        unique_key="event_key",
    )
    string_id = service.row_id(
        source_type="api",
        source_schema="",
        source_table="events",
        row={"event_key": "b'abc'"},
        unique_key="event_key",
    )

    assert bytes_id != string_id


def test_non_json_lineage_identity_is_independent_of_mapping_order() -> None:
    service = LineageIdentityService()
    first = {"event_date": date(2026, 7, 30), "event_key": b"abc"}
    second = dict(reversed(list(first.items())))

    assert service.row_hash(first) == service.row_hash(second)


def test_row_lineage_enricher_adds_expected_columns_without_mutating_source_rows() -> None:
    service = LineageIdentityService()
    extracted_at = datetime(2026, 6, 4, 9, 30, tzinfo=UTC)
    loaded_at = datetime(2026, 6, 4, 9, 31, tzinfo=UTC)
    options = LineageOptions.from_config({"preset": "standard", "features": {"operations": True}})
    enricher = RowLineageEnricher(identity_service=service)
    rows = [{"id": 1, "name": "Ada", "__dpone__op": "update"}]

    enriched = list(
        enricher.enrich_rows(
            rows,
            options=options,
            run_id=service.new_run_id(),
            load_id=service.new_load_id(),
            source_type="postgres",
            source_schema="public",
            source_table="orders",
            unique_key=["id"],
            extracted_at=extracted_at,
            loaded_at=loaded_at,
        )
    )

    assert rows == [{"id": 1, "name": "Ada", "__dpone__op": "update"}]
    assert enriched[0]["__dpone__load_id"]
    assert enriched[0]["__dpone__row_id"]
    assert enriched[0]["__dpone__extracted_at"] == "2026-06-04T09:30:00+00:00"
    assert enriched[0]["__dpone__loaded_at"] == "2026-06-04T09:31:00+00:00"
    assert enriched[0]["__dpone__op"] == "update"


def test_row_lineage_fallback_identity_ignores_retry_specific_lineage_values() -> None:
    service = LineageIdentityService()
    options = LineageOptions.from_config({"preset": "standard"})
    enricher = RowLineageEnricher(identity_service=service)
    row = {"name": "Ada", "amount": "42.00"}

    first = list(
        enricher.enrich_rows(
            [row],
            options=options,
            run_id=service.new_run_id(),
            load_id=service.new_load_id(),
            source_type="api",
            source_schema="api",
            source_table="orders",
            extracted_at=datetime(2026, 6, 4, 9, 30, tzinfo=UTC),
            loaded_at=datetime(2026, 6, 4, 9, 31, tzinfo=UTC),
        )
    )[0]
    second = list(
        enricher.enrich_rows(
            [row],
            options=options,
            run_id=service.new_run_id(),
            load_id=service.new_load_id(),
            source_type="api",
            source_schema="api",
            source_table="orders",
            extracted_at=datetime(2026, 6, 4, 10, 30, tzinfo=UTC),
            loaded_at=datetime(2026, 6, 4, 10, 31, tzinfo=UTC),
        )
    )[0]

    assert first["__dpone__load_id"] != second["__dpone__load_id"]
    assert first["__dpone__row_id"] == second["__dpone__row_id"]


def test_row_lineage_enricher_remaps_load_payload_schema_and_rows() -> None:
    service = LineageIdentityService()
    options = LineageOptions.from_config({"preset": "minimal"})
    enricher = RowLineageEnricher(identity_service=service)
    payload = LoadPayload(
        artifact=InMemoryRowsArtifact([{"id": 1}]),
        schema=[("id", "bigint")],
    )

    enriched = enricher.enrich_payload(
        payload,
        options=options,
        run_id=service.new_run_id(),
        load_id=service.new_load_id(),
        source_type="api",
        source_schema="api",
        source_table="orders",
        unique_key=["id"],
    )

    assert enriched.schema == [
        ("id", "bigint"),
        ("__dpone__load_id", "varchar(26)"),
        ("__dpone__loaded_at", "timestamp"),
    ]
    assert list(enriched.artifact._rows)[0]["__dpone__load_id"]


def test_new_load_strategies_are_public_enum_values() -> None:
    assert LoadStrategy("snapshot_diff") is LoadStrategy.SNAPSHOT_DIFF
    assert LoadStrategy("scd2") is LoadStrategy.SCD2
    assert LoadStrategy("cdc_apply") is LoadStrategy.CDC_APPLY
    assert LoadStrategy("backfill") is LoadStrategy.BACKFILL
