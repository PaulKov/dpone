"""Metadata-only domain selection shares runtime policy without credentials."""

from dataclasses import replace

import pytest

from dpone.adapters.composition_clickhouse_enrollment import (
    ClickHouseCompositionEnrollmentReader,
    clickhouse_domain_for_write,
    clickhouse_physical_domain,
)
from dpone.contracts.composition_identity import CompositionAdmissionError
from tests.test_composition_physical_enrollment import DATABASE, SERVICE, WRITE, connection


def test_metadata_only_domain_equals_runtime_domain():
    properties = connection().descriptor.properties
    assert clickhouse_domain_for_write(properties, WRITE) == clickhouse_physical_domain(SERVICE, DATABASE)
    assert ClickHouseCompositionEnrollmentReader._pins(connection(), WRITE)[0] == clickhouse_domain_for_write(
        properties, WRITE
    )


@pytest.mark.parametrize("change", ["missing", "unknown", "extra", "nil", "default", "service"])
def test_metadata_rejects_incomplete_or_invalid_authorities(change):
    properties = {
        "database": "data",
        "composition_service_id": SERVICE,
        "database_authorities": {"data": {"database_uuid": DATABASE}},
    }
    if change == "missing":
        del properties["database_authorities"]
    elif change == "unknown":
        properties["database_authorities"] = {"other": {"database_uuid": DATABASE}}
    elif change == "extra":
        properties["database_authorities"]["unused"] = {"database_uuid": DATABASE, "extra": True}
    elif change == "nil":
        properties["database_authorities"]["unused"] = {"database_uuid": "00000000-0000-0000-0000-000000000000"}
    elif change == "default":
        properties["database"] = "absent"
    else:
        properties["composition_service_id"] = "bad"
    with pytest.raises(CompositionAdmissionError):
        clickhouse_domain_for_write(properties, WRITE)


def test_metadata_target_coordinates_preserve_runtime_policy():
    for fields in ({"connector": "mssql"}, {"database": "other"}):
        write = replace(WRITE, **fields)
        with pytest.raises(CompositionAdmissionError):
            clickhouse_domain_for_write(connection().descriptor.properties, write)


def test_runtime_still_rejects_credential_database_mismatch():
    bound = connection()
    bound.credentials.database = "other"
    with pytest.raises(CompositionAdmissionError):
        ClickHouseCompositionEnrollmentReader._pins(bound, WRITE)
