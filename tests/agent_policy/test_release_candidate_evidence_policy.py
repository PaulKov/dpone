from __future__ import annotations

from tests.agent_policy._release_candidate_evidence_helpers import policy

from dpone.ops.live_certification_local import LOCAL_SERVICE_MARKER_MIN_PASSED


def test_service_marker_policy_covers_the_complete_live_workflow_inventory() -> None:
    """Keep release authority aligned with every fixture the workflow executes."""

    assert policy.JUNIT_CASES["service_markers"] == (
        "tests.integration.mysql.test_mysql_source_integration::test_mysql_connector_selects_and_exports_mssql_delimited",
        "tests.integration.postgres.test_postgres_connector_integration::test_postgres_connector_executes_queries_and_streams_rows",
        "tests.integration.postgres.test_postgres_connector_integration::test_postgres_connector_copy_from_iter_loads_csv_rows",
        "tests.integration.mssql.test_mssql_optional_integration::test_mssql_bcp_import_roundtrip",
        "tests.integration.mssql.test_mssql_optional_integration::test_mssql_verified_empty_payload_has_receipt_without_bcp",
        "tests.integration.mssql.test_mssql_optional_integration::test_mssql_real_bcp_reject_cleans_staging_and_preserves_target",
        "tests.integration.kafka.test_kafka_optional_integration::test_kafka_json_sink_and_source_round_trip",
        "tests.integration.kafka.test_kafka_optional_integration::test_schema_registry_client_can_be_created_when_url_is_configured",
    )
    assert LOCAL_SERVICE_MARKER_MIN_PASSED == len(policy.JUNIT_CASES["service_markers"])
