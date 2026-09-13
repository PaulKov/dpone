"""Protected dispatcher selection rejects authority substitution before resolution."""

import pytest

from dpone.contracts.composition_dispatcher_binding import CompositionDispatcherBinding
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.strict_json import canonical_json_bytes

DISPATCHER = "11223344-1122-4122-8122-112233445566"
SHA = "sha256:" + "a" * 64


def descriptor():
    return {
        "schema": "dpone.composition-dispatcher-binding.v1",
        "dispatcher_id": DISPATCHER,
        "connection_ref": "api-dispatcher",
        "service_configuration_sha256": SHA,
    }


def test_descriptor_roundtrip_preserves_exact_canonical_original():
    original = canonical_json_bytes(descriptor())
    binding = CompositionDispatcherBinding.from_document(original)
    assert binding.to_bytes() == original
    assert binding.connection_ref == "api-dispatcher"


@pytest.mark.parametrize(
    "field,value",
    [
        ("schema", "future"),
        ("dispatcher_id", "not-uuid"),
        ("connection_ref", "../secret"),
        ("connection_ref", "https://host"),
        ("service_configuration_sha256", "A" * 64),
        ("extra", "secret"),
    ],
)
def test_descriptor_rejects_open_or_foreign_shape(field, value):
    value_map = descriptor() | {field: value}
    with pytest.raises(CompositionAdmissionError, match="dispatcher_binding"):
        CompositionDispatcherBinding.from_document(canonical_json_bytes(value_map))


def test_descriptor_rejects_noncanonical_original():
    with pytest.raises(CompositionAdmissionError, match="dispatcher_binding"):
        CompositionDispatcherBinding.from_document(b" " + canonical_json_bytes(descriptor()))


def _staged(monkeypatch, tmp_path):
    from types import SimpleNamespace

    from dpone.app import composition_dispatcher_context as module
    from dpone.contracts.airflow_run_identity import AirflowDeploymentIdentity
    from dpone.runtime.credentials.runtime_context import RuntimeConnectionContext

    release, deployment, plan_sha = ("sha256:" + digit * 64 for digit in "bcd")
    identity = AirflowDeploymentIdentity(release, deployment, DISPATCHER)
    body = {
        "schema": "dpone.composition-dispatcher-context.v1",
        "runtime_authority_sha256": SHA,
        "deployment_identity": identity.to_dict(),
        "init_fetch_plan_b64": "pinned-plan",
        "init_fetch_plan_sha256": "sha256:" + "e" * 64,
        "plan_sha256": plan_sha,
    }
    entry = {"type": "clickhouse", "connection": {"composition_dispatcher": descriptor()}}
    runtime = RuntimeConnectionContext(
        "prod",
        {
            "bindings": {
                "target": {"connection_ref": "registry-target"},
                "api-dispatcher": {"connection_ref": "registry-api"},
            }
        },
        {"connections": {"registry-target": entry, "registry-api": {"type": "api"}}},
        {},
        SimpleNamespace(resolve=lambda *_: pytest.fail("credentials resolved during selection")),
        release_id=release,
        deployment_id=deployment,
        authority_subject_sha256=SHA,
    )
    calls = []
    runtime_loader = SimpleNamespace(load=lambda env: calls.append(env) or runtime)
    plan = SimpleNamespace(
        sources=SimpleNamespace(subject_sha256=plan_sha),
        writes=[SimpleNamespace(connector="clickhouse", kind="transfer", connection_ref="target")],
    )
    monkeypatch.setattr(
        module,
        "decode_runtime_init_fetch_plan",
        lambda *_: (
            SimpleNamespace(
                release_id=release,
                deployment_id=deployment,
                environment="prod",
                binding_set=SimpleNamespace(artifact_ref="runtime/context/binding-set.json"),
            ),
            "digest",
        ),
    )
    monkeypatch.setattr(module, "cache_relative_path", lambda _: tmp_path.__class__("runtime/context/binding-set.json"))
    monkeypatch.setattr(module, "reopen_composition_plan", lambda *_: plan)
    monkeypatch.setattr(module.DispatcherContextFiles, "read", lambda *_args, **_kwargs: canonical_json_bytes(body))
    monkeypatch.setattr(module.DispatcherContextFiles, "require_tree", lambda *_: tmp_path / "cache")
    from hashlib import sha256

    loader = module.StagedDispatcherContextLoader(
        root=tmp_path,
        dispatcher_gid=1234,
        dispatcher_id=DISPATCHER,
        configuration_sha256=SHA,
        staged_authorities={SHA: "sha256:" + sha256(canonical_json_bytes(body)).hexdigest()},
        runtime_loader=runtime_loader,
    )
    arguments = dict(
        dispatcher_id=DISPATCHER, configuration_sha256=SHA, plan_sha256=plan_sha, target_binding_ref="target"
    )
    return loader, arguments, body, runtime, calls


def test_staged_selection_preserves_parent_and_ignores_ambient(monkeypatch, tmp_path):
    from dpone.runtime.credentials.runtime_context import RUNTIME_CONNECTION_CONTEXT_ENV

    loader, arguments, body, _, calls = _staged(monkeypatch, tmp_path)
    monkeypatch.setenv(RUNTIME_CONNECTION_CONTEXT_ENV, "/foreign/secret")
    result = loader.load(SHA, **arguments)
    assert result.occurrence.activation_id == body["deployment_identity"]["activation_id"]
    assert result.occurrence.runtime_context_sha256 == SHA
    assert result.binding.connection_ref == "api-dispatcher"
    assert len(calls) == 1 and len(calls[0]) == 3
    assert "/foreign/secret" not in str(calls)
    assert "resolver" not in repr(result)


@pytest.mark.parametrize(
    "field,value",
    [
        ("dispatcher_id", "22334455-2233-4233-8233-223344556677"),
        ("configuration_sha256", "sha256:" + "f" * 64),
        ("plan_sha256", "sha256:" + "f" * 64),
        ("target_binding_ref", "foreign"),
    ],
)
def test_selection_rejects_foreign_request(monkeypatch, tmp_path, field, value):
    loader, arguments, _, _, _ = _staged(monkeypatch, tmp_path)
    with pytest.raises(CompositionAdmissionError, match="dispatcher_context_unverified"):
        loader.load(SHA, **(arguments | {field: value}))


def test_unstaged_authority_never_loads_runtime(monkeypatch, tmp_path):
    loader, arguments, _, _, calls = _staged(monkeypatch, tmp_path)
    with pytest.raises(CompositionAdmissionError):
        loader.load("sha256:" + "f" * 64, **arguments)
    assert calls == []


def test_tampered_parent_metadata_rejected_before_runtime(monkeypatch, tmp_path):
    loader, arguments, body, _, calls = _staged(monkeypatch, tmp_path)
    body["deployment_identity"]["activation_id"] = "22334455-2233-4233-8233-223344556677"
    with pytest.raises(CompositionAdmissionError):
        loader.load(SHA, **arguments)
    assert calls == []


@pytest.mark.parametrize("field", ["authority_subject_sha256", "release_id", "deployment_id", "environment"])
def test_runtime_must_match_every_pinned_coordinate(monkeypatch, tmp_path, field):
    loader, arguments, _, runtime, _ = _staged(monkeypatch, tmp_path)
    object.__setattr__(runtime, field, "foreign")
    with pytest.raises(CompositionAdmissionError):
        loader.load(SHA, **arguments)


def test_signed_dispatcher_must_match_local_configuration(monkeypatch, tmp_path):
    loader, arguments, _, runtime, _ = _staged(monkeypatch, tmp_path)
    runtime.connection_registry["connections"]["registry-target"]["connection"]["composition_dispatcher"][
        "service_configuration_sha256"
    ] = "sha256:" + "f" * 64
    with pytest.raises(CompositionAdmissionError):
        loader.load(SHA, **arguments)


def _files(monkeypatch, tmp_path):
    import os

    from dpone.adapters import composition_dispatcher_context_files as module

    # Platform seam only: test directories are owned by the developer. Keep real
    # no-follow opens, traversal, file kinds, group checks and mode enforcement.
    monkeypatch.setattr(
        module, "open_protected", lambda path, **_: os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    )
    original = module.os.fstat

    def root_stat(fd):
        info = list(original(fd))
        info[4] = 0
        return os.stat_result(info)

    monkeypatch.setattr(module.os, "fstat", root_stat)
    tmp_path.chmod(0o750)
    folder = tmp_path / "authority"
    folder.mkdir(mode=0o750)
    path = folder / "context.json"
    path.write_bytes(b"original")
    path.chmod(0o640)
    return module.DispatcherContextFiles(tmp_path, dispatcher_gid=os.getgid()), path


def test_protected_original_exact_bytes_and_bound(monkeypatch, tmp_path):
    files, _ = _files(monkeypatch, tmp_path)
    assert files.read("authority/context.json") == b"original"
    with pytest.raises(CompositionAdmissionError):
        files.read("authority/context.json", max_bytes=2)


@pytest.mark.parametrize("mode", [0o660, 0o642, 0o600])
def test_protected_original_rejects_unsafe_access(monkeypatch, tmp_path, mode):
    files, path = _files(monkeypatch, tmp_path)
    path.chmod(mode)
    with pytest.raises(CompositionAdmissionError):
        files.read("authority/context.json")


def test_protected_original_rejects_symlink_and_traversal(monkeypatch, tmp_path):
    files, path = _files(monkeypatch, tmp_path)
    path.unlink()
    path.symlink_to(tmp_path / "foreign")
    for relative in ("authority/context.json", "../foreign", "/foreign"):
        with pytest.raises(CompositionAdmissionError):
            files.read(relative)


def test_protected_original_rejects_foreign_group(monkeypatch, tmp_path):
    files, _ = _files(monkeypatch, tmp_path)
    files._gid += 1
    with pytest.raises(CompositionAdmissionError):
        files.read("authority/context.json")


def test_protected_tree_rejects_writable_descendant(monkeypatch, tmp_path):
    files, path = _files(monkeypatch, tmp_path)
    assert files.require_tree("authority") == path.parent
    path.chmod(0o660)
    with pytest.raises(CompositionAdmissionError):
        files.require_tree("authority")


def test_real_runtime_loader_verifies_staged_documents(monkeypatch, tmp_path):
    import json
    from hashlib import sha256
    from types import SimpleNamespace

    from dpone.app import composition_dispatcher_context as module
    from dpone.contracts.airflow_run_identity import AirflowDeploymentIdentity
    from dpone.runtime.credentials.runtime_context import (
        RUNTIME_INIT_FETCH_PLAN_B64_ENV,
        RUNTIME_INIT_FETCH_PLAN_SHA256_ENV,
        RuntimeConnectionContextLoader,
    )
    from tests.test_runtime_connection_context_loader import _replace_plan_descriptor, _runtime_context

    env, context_root = _runtime_context(tmp_path)
    registry_path = context_root / "connection-registry.json"
    registry = json.loads(registry_path.read_bytes())
    registry["connections"]["source-registry"]["type"] = "api"
    registry["connections"]["sink-registry"]["connection"]["composition_dispatcher"] = descriptor() | {
        "connection_ref": "source-main"
    }
    content = canonical_json_bytes(registry)
    registry_path.write_bytes(content)
    env = _replace_plan_descriptor(env, name="connection_registry", content=content)
    runtime_loader = RuntimeConnectionContextLoader(vault_reader_factory=lambda _: None)
    runtime = runtime_loader.load(env)
    authority = runtime.authority_subject_sha256
    identity = AirflowDeploymentIdentity(runtime.release_id, runtime.deployment_id, DISPATCHER)
    body = canonical_json_bytes(
        {
            "schema": "dpone.composition-dispatcher-context.v1",
            "runtime_authority_sha256": authority,
            "deployment_identity": identity.to_dict(),
            "init_fetch_plan_b64": env[RUNTIME_INIT_FETCH_PLAN_B64_ENV],
            "init_fetch_plan_sha256": env[RUNTIME_INIT_FETCH_PLAN_SHA256_ENV],
            "plan_sha256": SHA,
        }
    )
    monkeypatch.setattr(module.DispatcherContextFiles, "read", lambda *_: body)
    monkeypatch.setattr(module.DispatcherContextFiles, "require_tree", lambda *_: tmp_path)
    monkeypatch.setattr(
        module,
        "reopen_composition_plan",
        lambda *_: SimpleNamespace(
            sources=SimpleNamespace(subject_sha256=SHA),
            writes=[SimpleNamespace(connector="clickhouse", kind="transfer", connection_ref="sink-main")],
        ),
    )
    loader = module.StagedDispatcherContextLoader(
        root=tmp_path,
        dispatcher_gid=1234,
        dispatcher_id=DISPATCHER,
        configuration_sha256=SHA,
        staged_authorities={authority: "sha256:" + sha256(body).hexdigest()},
        runtime_loader=runtime_loader,
    )
    args = dict(dispatcher_id=DISPATCHER, configuration_sha256=SHA, plan_sha256=SHA, target_binding_ref="sink-main")
    result = loader.load(authority, **args)
    assert result.runtime.authority_subject_sha256 == authority
    assert result.occurrence.deployment_id == runtime.deployment_id
    registry_path.write_bytes(content + b" ")
    with pytest.raises(CompositionAdmissionError, match="dispatcher_context_unverified"):
        loader.load(authority, **args)


@pytest.mark.parametrize("directory", [False, True])
def test_staging_rejects_world_access(monkeypatch, tmp_path, directory):
    files, path = _files(monkeypatch, tmp_path)
    (path.parent if directory else path).chmod(0o755 if directory else 0o644)
    with pytest.raises(CompositionAdmissionError):
        files.read("authority/context.json")


def test_original_read_ignores_only_access_time_changes(monkeypatch, tmp_path):
    import os

    from dpone.adapters import composition_dispatcher_context_files as module

    files, _ = _files(monkeypatch, tmp_path)
    original, count = module.os.fstat, 0

    def accessed(fd):
        nonlocal count
        count += 1
        fields = list(original(fd))
        fields[7] += count
        return os.stat_result(fields)

    monkeypatch.setattr(module.os, "fstat", accessed)
    assert files.read("authority/context.json") == b"original"


def test_staging_rejects_foreign_owner(monkeypatch, tmp_path):
    import os

    from dpone.adapters import composition_dispatcher_context_files as module

    files, _ = _files(monkeypatch, tmp_path)
    original = module.os.fstat

    def foreign(fd):
        fields = list(original(fd))
        fields[4] = 1234
        return os.stat_result(fields)

    monkeypatch.setattr(module.os, "fstat", foreign)
    with pytest.raises(CompositionAdmissionError):
        files.read("authority/context.json")
