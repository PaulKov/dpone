"""Manifest admission unit checks; synthetic hashes do not prove binary loading."""

import hashlib
from copy import deepcopy

import pytest

from dpone.adapters.mssql_sqlclient_installation import validate_manifest
from dpone.contracts.strict_json import canonical_json_bytes


def manifest():
    rows = [
        ("companion", "Apache.Arrow.dll", "managed_dependency"),
        ("companion", "Microsoft.Data.SqlClient.dll", "managed_dependency"),
        ("companion", "Worker.deps.json", "worker_deps"),
        ("companion", "Worker.dll", "worker_assembly"),
        ("companion", "Worker.runtimeconfig.json", "worker_runtimeconfig"),
        ("dotnet", "dotnet", "dotnet_host"),
        ("dotnet", "shared/Microsoft.NETCore.App/8.0.31/libcoreclr.so", "runtime_library"),
    ]
    return dict(
        schema_version=1,
        backend="mssql_sqlclient",
        platform="linux_arm64",
        runtime_version="8.0.31",
        sqlclient_version="7.0.2",
        arrow_version="23.0.0",
        files=[dict(origin=o, path=p, role=r, sha256="a" * 64) for o, p, r in rows],
    )


def bind(value):
    body = canonical_json_bytes(value)
    return body, hashlib.sha256(b"dpone.sqlclient.deployment.v1\0" + body).hexdigest()


def test_strict_manifest_expected_digest():
    value = manifest()
    assert validate_manifest(*bind(value)) == value
    with pytest.raises(ValueError):
        validate_manifest(bind(value)[0], "b" * 64)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda v: v.update(extra=1),
        lambda v: v.update(schema_version=True),
        lambda v: v["files"].reverse(),
        lambda v: v["files"].append(deepcopy(v["files"][-1])),
        lambda v: v["files"][0].update(path="../Apache.Arrow.dll"),
        lambda v: v["files"][0].update(path="a//b"),
        lambda v: v["files"][0].update(origin="dotnet"),
        lambda v: v["files"][0].update(role="arbitrary"),
        lambda v: v["files"][0].update(sha256="A" * 64),
        lambda v: v["files"][0].update(path="Other.dll"),
        lambda v: v["files"][2].update(role="managed_dependency"),
    ],
)
def test_manifest_refuses_changed_shape_even_with_recomputed_digest(mutation):
    value = manifest()
    mutation(value)
    with pytest.raises(ValueError):
        validate_manifest(*bind(value))


def test_duplicate_fields_refused():
    body, digest = bind(manifest())
    duplicate = body.replace(b'"schema_version":1', b'"schema_version":1,"schema_version":1')
    with pytest.raises(ValueError):
        validate_manifest(duplicate, digest)
