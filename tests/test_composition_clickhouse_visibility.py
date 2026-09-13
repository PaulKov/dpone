"""Global visibility must come from the same independently authenticated observer."""

import json
from types import SimpleNamespace

import pytest

from dpone.adapters.composition_clickhouse_visibility import ClickHouseCatalogVisibility
from dpone.contracts.composition_activation import CompositionAdmissionError

SERVICE = "11111111-1111-1111-1111-111111111111"


def response(**changes):
    values = {"observer": "protected_admin", "service": SERVICE, "revokes": 0, "complete": 1}
    values.update(changes)
    return json.dumps(
        {
            "meta": [
                {"name": name, "type": "String" if name in {"observer", "service"} else "UInt64"} for name in values
            ],
            "data": [list(values.values())],
            "rows": 1,
        }
    ).encode()


class Http:
    def __init__(self, body):
        self.body, self.requests = body, []

    def request(self, **kwargs):
        self.requests.append(kwargs)
        return SimpleNamespace(body=self.body)


def test_visibility_queries_originals_each_time():
    http = Http(response())
    observer = ClickHouseCatalogVisibility(http=http, service_id=SERVICE, username="protected_admin")
    original = observer()
    assert json.loads(original)["observer"] == "protected_admin"
    assert observer() == original
    assert len(http.requests) == 2
    assert b"system.grants" in http.requests[0]["payload"]
    assert b"currentUser()" in http.requests[0]["payload"]
    assert b"CHECK GRANT" not in http.requests[0]["payload"]


@pytest.mark.parametrize(
    "changes",
    [
        {"revokes": 1},
        {"complete": 0},
        {"observer": "worker"},
        {"service": "22222222-2222-2222-2222-222222222222"},
        {"complete": True},
        {"revokes": "01"},
    ],
)
def test_incomplete_or_foreign_observation_refused(changes):
    with pytest.raises(CompositionAdmissionError, match="snapshot_catalog_visibility"):
        ClickHouseCatalogVisibility(http=Http(response(**changes)), service_id=SERVICE, username="protected_admin")()


@pytest.mark.parametrize("body", [b"{}", b"not json", b" " * 8193])
def test_bad_or_oversized_original_refused(body):
    with pytest.raises(CompositionAdmissionError, match="snapshot_catalog_visibility"):
        ClickHouseCatalogVisibility(http=Http(body), service_id=SERVICE, username="protected_admin")()


def test_volatile_query_statistics_do_not_change_observed_authority():
    http = Http(response())
    observer = ClickHouseCatalogVisibility(http=http, service_id=SERVICE, username="protected_admin")
    first = json.loads(response())
    first["statistics"] = {"elapsed": 0.001, "rows_read": 17, "bytes_read": 600}
    http.body = json.dumps(first).encode()
    original = observer()
    first["statistics"]["elapsed"] = 0.002
    first["data"][0][2:] = ["0", "1"]
    http.body = json.dumps(first).encode()
    assert observer() == original


def test_contract_produces_the_same_stable_original_as_adapter():
    from dpone.contracts.composition_snapshot_materialization import catalog_visibility_original

    original = catalog_visibility_original(response(), expected_service_id=SERVICE, expected_observer="protected_admin")
    observer = ClickHouseCatalogVisibility(http=Http(response()), service_id=SERVICE, username="protected_admin")
    assert original == observer()
    assert json.loads(original) == {
        "schema": "dpone.composition-catalog-visibility.v1",
        "observer": "protected_admin",
        "service": SERVICE,
        "revokes": 0,
        "complete": 1,
    }


@pytest.mark.parametrize("mutation", ["extra", "metadata", "row_count", "row_shape", "boolean_count", "duplicate_key"])
def test_contract_rejects_nonclosed_visibility_documents(mutation):
    from dpone.contracts.composition_snapshot_materialization import catalog_visibility_original

    document = json.loads(response())
    if mutation == "extra":
        document["caller_claim"] = True
    elif mutation == "metadata":
        document["meta"][3]["type"] = "String"
    elif mutation == "row_count":
        document["rows"] = 2
    elif mutation == "row_shape":
        document["data"][0].append("extra")
    elif mutation == "boolean_count":
        document["rows"] = True
    body = json.dumps(document).encode()
    if mutation == "duplicate_key":
        body = body[:-1] + b',"rows":1}'
    with pytest.raises(CompositionAdmissionError, match="snapshot_catalog_visibility"):
        catalog_visibility_original(body, expected_service_id=SERVICE, expected_observer="protected_admin")


def test_unavailable_http_original_is_rejected_by_contract():
    class Unavailable:
        def request(self, **_):
            raise RuntimeError("private transport failure")

    with pytest.raises(CompositionAdmissionError, match="snapshot_catalog_visibility"):
        ClickHouseCatalogVisibility(http=Unavailable(), service_id=SERVICE, username="protected_admin")()
