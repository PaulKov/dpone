"""Closed startup originals and actual protected-read boundary, without startup."""

from dataclasses import FrozenInstanceError
from hashlib import sha256
from pathlib import Path

import pytest

from dpone.app import composition_dispatcher_service_config as config
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.strict_json import canonical_json_bytes

DIGEST = "sha256:" + "a" * 64
IDENTIFIER = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"


def document():
    return {
        "schema": "dpone.composition-dispatcher-service.v2",
        "dispatcher_id": IDENTIFIER,
        "dispatcher_uid": 1200,
        "dispatcher_gid": 1201,
        "capture_custody": "dispatcher_owned_v1",
        "context_root": "/etc/dpone/context",
        "host_probe_socket": "/run/dpone/probe.sock",
        "supervisor_enrollment_sha256": DIGEST,
        "capture_root": "/var/lib/dpone/capture",
        "capture_root_identity": {"device": 1, "inode": 2, "uid": 1200, "gid": 1201, "mode": 448},
        "listen": {"address": "127.0.0.1", "port": 8443},
        "tls": {"certificate_file": "/etc/dpone/server.crt", "private_key_file": "/etc/dpone/server.key"},
        "bearer_file": "/etc/dpone/bearer",
        "accept_timeout_seconds": 30,
        "execution_timeout_seconds": 900,
        "max_concurrency": 64,
        "authorities": {
            DIGEST: {
                "context_sha256": DIGEST,
                "control_connection_ref": "control",
                "expected_control_service_id": IDENTIFIER,
                "control_schema": "dpone_control",
            }
        },
    }


def decode(value=None, **overrides):
    raw = canonical_json_bytes(document() if value is None else value)
    arguments = {"expected_sha256": "sha256:" + sha256(raw).hexdigest(), "bootstrap_uid": 1200, "bootstrap_gid": 1201}
    arguments.update(overrides)
    return config.decode_dispatcher_service_config(raw, **arguments)


def test_decoded_configuration_is_deeply_immutable():
    value = document()
    result = decode(value)
    assert result.capture_root_identity.mode == 0o700
    assert result.authorities[DIGEST].control_schema == "dpone_control"
    assert result.tls.private_key_file == Path("/etc/dpone/server.key")
    assert result.listen.port == 8443
    assert result.sha256 == result.configuration_sha256 == "sha256:" + sha256(result.document).hexdigest()
    with pytest.raises(FrozenInstanceError):
        result.dispatcher_uid = 0
    with pytest.raises(TypeError):
        result.authorities[DIGEST] = result.authorities[DIGEST]
    with pytest.raises(FrozenInstanceError):
        result.capture_root_identity.uid = 0
    value["authorities"][DIGEST]["control_schema"] = "changed"
    assert result.authorities[DIGEST].control_schema == "dpone_control"


@pytest.mark.parametrize("field", sorted(document()))
def test_missing_required_field(field):
    value = document()
    del value[field]
    with pytest.raises(CompositionAdmissionError):
        decode(value)


def test_lower_bounds_and_ipv6_are_supported():
    value = document()
    value.update(accept_timeout_seconds=1, execution_timeout_seconds=1, max_concurrency=1)
    value["listen"] = {"address": "::1", "port": 1}
    value["capture_root_identity"]["device"] = 0
    assert decode(value).listen.address == "::1"


@pytest.mark.parametrize("path", [(), ("listen",), ("tls",), ("capture_root_identity",), ("authorities", DIGEST)])
def test_every_object_is_closed(path):
    value = document()
    target = value
    for key in path:
        target = target[key]
    target["unexpected"] = "credential-secret"
    with pytest.raises(CompositionAdmissionError, match="dispatcher_service_config") as error:
        decode(value)
    assert "credential-secret" not in str(error.value)


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("schema", "dpone.composition-dispatcher-service.v1"),
        ("dispatcher_id", IDENTIFIER.upper()),
        ("dispatcher_uid", True),
        ("dispatcher_uid", 0),
        ("dispatcher_uid", 2147483648),
        ("dispatcher_gid", 1201.0),
        ("capture_custody", "worker_owned"),
        ("context_root", "/etc//dpone"),
        ("context_root", "relative"),
        ("capture_root", "/var/../capture"),
        ("host_probe_socket", "/run/./probe"),
        ("bearer_file", "/etc/bearer\n"),
        ("supervisor_enrollment_sha256", "a" * 64),
        ("accept_timeout_seconds", 31),
        ("accept_timeout_seconds", 0),
        ("execution_timeout_seconds", 900.0),
        ("execution_timeout_seconds", 901),
        ("max_concurrency", 65),
        ("max_concurrency", False),
        ("authorities", {}),
    ],
)
def test_invalid_fields(field, bad):
    value = document()
    value[field] = bad
    with pytest.raises(CompositionAdmissionError, match="dispatcher_service_config"):
        decode(value)


@pytest.mark.parametrize(
    ("section", "field", "bad"),
    [
        ("listen", "address", "localhost"),
        ("listen", "address", "0.0.0.0"),
        ("listen", "address", "::"),
        ("listen", "address", "fe80::1%eth0"),
        ("listen", "port", True),
        ("listen", "port", 65536),
        ("tls", "private_key_file", "/keys/../key"),
        ("capture_root_identity", "uid", 1201),
        ("capture_root_identity", "gid", 1200),
        ("capture_root_identity", "mode", 449),
        ("capture_root_identity", "device", False),
        ("capture_root_identity", "inode", 0),
    ],
)
def test_invalid_nested_fields(section, field, bad):
    value = document()
    value[section][field] = bad
    with pytest.raises(CompositionAdmissionError):
        decode(value)


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("context_sha256", "bad"),
        ("control_connection_ref", "https://host"),
        ("expected_control_service_id", IDENTIFIER.upper()),
        ("control_schema", "x; DROP TABLE x"),
    ],
)
def test_authority_bindings_are_validated(field, bad):
    value = document()
    value["authorities"][DIGEST][field] = bad
    with pytest.raises(CompositionAdmissionError):
        decode(value)


def test_catalog_limit_and_digest_keys():
    value = document()
    binding = value["authorities"][DIGEST]
    value["authorities"] = {"sha256:" + f"{n:064x}": binding for n in range(1024)}
    assert len(decode(value).authorities) == 1024
    value["authorities"][DIGEST] = binding
    with pytest.raises(CompositionAdmissionError):
        decode(value)
    value["authorities"] = {"invalid": binding}
    with pytest.raises(CompositionAdmissionError):
        decode(value)


@pytest.mark.parametrize("raw", [b"{}", b"{" * 1024 * 1024, b'{"schema":1,"schema":2}', b"\xff", b'{"x":NaN}'])
def test_malformed_and_bounded_originals(raw):
    with pytest.raises(CompositionAdmissionError):
        config.decode_dispatcher_service_config(
            raw, expected_sha256="sha256:" + sha256(raw).hexdigest(), bootstrap_uid=1200, bootstrap_gid=1201
        )


def test_original_digest_and_canonical_bytes_and_bootstrap_identity():
    with pytest.raises(CompositionAdmissionError):
        decode(expected_sha256=DIGEST)
    for override in ({"bootstrap_uid": 1202}, {"bootstrap_gid": 1202}, {"bootstrap_uid": True}):
        with pytest.raises(CompositionAdmissionError):
            decode(**override)
    raw = canonical_json_bytes(document()) + b"\n"
    with pytest.raises(CompositionAdmissionError):
        config.decode_dispatcher_service_config(
            raw, expected_sha256="sha256:" + sha256(raw).hexdigest(), bootstrap_uid=1200, bootstrap_gid=1201
        )


def test_loader_uses_protected_reader_only(monkeypatch):
    raw = canonical_json_bytes(document())
    calls = []

    class Files:
        def __init__(self, root, *, dispatcher_gid):
            calls.append((root, dispatcher_gid))

        def read(self, relative, *, max_bytes):
            calls.append((relative, max_bytes))
            return raw

    monkeypatch.setattr(config, "DispatcherContextFiles", Files)
    monkeypatch.setattr(config.sys, "platform", "linux")
    monkeypatch.setattr(config.os, "getresuid", lambda: (1200, 1200, 1200), raising=False)
    monkeypatch.setattr(config.os, "getresgid", lambda: (1201, 1201, 1201), raising=False)
    result = config.load_dispatcher_service_config(
        Path("/etc/dpone"),
        "startup/service.json",
        expected_sha256="sha256:" + sha256(raw).hexdigest(),
        bootstrap_uid=1200,
        bootstrap_gid=1201,
    )
    assert result.dispatcher_uid == 1200
    assert calls == [(Path("/etc/dpone"), 1201), ("startup/service.json", config.MAX_CONFIGURATION_BYTES)]


@pytest.mark.parametrize(
    ("platform", "uid", "gid"),
    [
        ("darwin", (1200,) * 3, (1201,) * 3),
        ("linux", (1200, 0, 1200), (1201,) * 3),
        ("linux", (1200,) * 3, (1201, 1201, 0)),
    ],
)
def test_loader_rejects_wrong_actual_identity_before_read(monkeypatch, platform, uid, gid):
    monkeypatch.setattr(config.sys, "platform", platform)
    monkeypatch.setattr(config.os, "getresuid", lambda: uid, raising=False)
    monkeypatch.setattr(config.os, "getresgid", lambda: gid, raising=False)
    monkeypatch.setattr(config, "DispatcherContextFiles", lambda *args, **kwargs: pytest.fail("unexpected read"))
    with pytest.raises(CompositionAdmissionError, match="dispatcher_service_config"):
        config.load_dispatcher_service_config(
            Path("/etc/dpone"), "startup/service.json", expected_sha256=DIGEST, bootstrap_uid=1200, bootstrap_gid=1201
        )


def test_actual_reader_rejects_unprotected_original(tmp_path, monkeypatch):
    # Real filesystem reader; process-identity seam is explicitly simulated.
    monkeypatch.setattr(config.sys, "platform", "linux")
    monkeypatch.setattr(config.os, "getresuid", lambda: (1200,) * 3, raising=False)
    monkeypatch.setattr(config.os, "getresgid", lambda: (1201,) * 3, raising=False)
    directory = tmp_path / "startup"
    directory.mkdir()
    raw = canonical_json_bytes(document())
    (directory / "service.json").write_bytes(raw)
    with pytest.raises(CompositionAdmissionError):
        config.load_dispatcher_service_config(
            tmp_path,
            "startup/service.json",
            expected_sha256="sha256:" + sha256(raw).hexdigest(),
            bootstrap_uid=1200,
            bootstrap_gid=1201,
        )


@pytest.mark.parametrize(
    "relative",
    [
        "service.json",
        "/startup/service.json",
        "startup/../service.json",
        "startup//service.json",
        "startup/./service.json",
    ],
)
def test_loader_rejects_noncanonical_relative_before_read(monkeypatch, relative):
    monkeypatch.setattr(config.sys, "platform", "linux")
    monkeypatch.setattr(config.os, "getresuid", lambda: (1200,) * 3, raising=False)
    monkeypatch.setattr(config.os, "getresgid", lambda: (1201,) * 3, raising=False)
    monkeypatch.setattr(config, "DispatcherContextFiles", lambda *args, **kwargs: pytest.fail("unexpected read"))
    with pytest.raises(CompositionAdmissionError):
        config.load_dispatcher_service_config(
            Path("/etc/dpone"), relative, expected_sha256=DIGEST, bootstrap_uid=1200, bootstrap_gid=1201
        )
