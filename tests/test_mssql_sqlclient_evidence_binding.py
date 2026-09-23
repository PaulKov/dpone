"""Bindings distinguish structural consistency from independently retained originals."""

from dataclasses import asdict, replace

import pytest

from dpone.contracts.mssql_sqlclient_evidence_binding import (
    decode_evidence_binding,
    encode_evidence_binding,
    validate_evidence_binding,
)
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.contracts.strict_json import canonical_json_bytes
from tests.mssql_sqlclient_evidence_fixtures import registration


def test_roundtrip_and_original_comparison():
    r = registration()
    b = r.binding
    assert decode_evidence_binding(encode_evidence_binding(b)) == b
    validate_evidence_binding(
        b, launch=r.launch, identity=b.identity, ownership=b.ownership, object_identity=b.object_identity
    )
    with pytest.raises(ValueError):
        validate_evidence_binding(
            b,
            launch=r.launch,
            identity=b.identity,
            ownership=replace(b.ownership, fence=2),
            object_identity=b.object_identity,
        )


@pytest.mark.parametrize(
    "path", [("identity", "ordinal"), ("ownership", "fence"), ("process", "pid"), ("object_identity", "object_id")]
)
def test_nested_bool_and_unknown_rejected(path):
    data = asdict(registration().binding)
    data[path[0]][path[1]] = True
    with pytest.raises(ValueError):
        decode_evidence_binding(canonical_json_bytes(data))
    b = registration().binding
    object.__setattr__(getattr(b, path[0]), path[1], True)
    with pytest.raises(ValueError):
        encode_evidence_binding(b)


@pytest.mark.parametrize("name", ["database", "schema", "table"])
def test_sql_utf16_boundary(name):
    b = registration().binding
    accepted = replace(b.identity, **{name: "😀" * 64})
    assert replace(b, identity=accepted, attempt_sha256=attempt_identity_digest(accepted))
    rejected = replace(b.identity, **{name: "😀" * 65})
    with pytest.raises(ValueError):
        replace(b, identity=rejected, attempt_sha256=attempt_identity_digest(rejected))
