"""Pure source semantics: exact archive member and DAG inventory, without file I/O."""

import base64
import io
import json
import tarfile
from copy import deepcopy

import pytest

from dpone.contracts.dbt_contract_validation import sha256_bytes
from dpone.contracts.dbt_identifiers import dbt_workload_id
from dpone.contracts.dbt_release_workload_binding import (
    DbtDevEvidenceReleaseError,
    decode_dbt_workflow_dag,
    runtime_payload_member,
)
from dpone.gitops.airflow_compact_pack_bootstrap import RuntimePayloadBuilder
from dpone.services.dbt_release_workflow_reader import DbtDevEvidenceReleaseError as ServiceError


def _pack(*, name="manifest.yaml", repeated=False, link=False, pax=False):
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        for _ in range(2 if repeated else 1):
            member = tarfile.TarInfo(name)
            member.size = 4
            if link:
                member.type, member.linkname, member.size = tarfile.SYMTYPE, "elsewhere", 0
            if pax:
                member.pax_headers = {"comment": pax if isinstance(pax, str) else "untrusted"}
            archive.addfile(member, io.BytesIO(b"data"))
    body = buffer.getvalue()
    return {
        "runtime_payload": {
            "schema": "dpone.airflow-runtime-payload.v1",
            "archive": {
                "encoding": "base64",
                "format": "tar+gzip",
                "sha256": sha256_bytes(body),
                "bytes": len(body),
                "data": base64.b64encode(body).decode(),
            },
        }
    }


def _member(pack, **overrides):
    return runtime_payload_member(
        pack,
        **(
            {
                "expected_path": "manifest.yaml",
                "max_archive_bytes": 4096,
                "max_member_bytes": 4,
                "label": "transfer manifest",
            }
            | overrides
        ),
    )


def test_exact_member_at_limit_and_compatibility_error_identity():
    pack = _pack()
    assert _member(pack, max_archive_bytes=pack["runtime_payload"]["archive"]["bytes"]) == b"data"
    assert ServiceError is DbtDevEvidenceReleaseError


@pytest.mark.parametrize("alias", ["a" * 256, "model_" + "сложное имя" * 32], ids=["ascii", "mixed-unicode"])
def test_long_author_names_use_canonical_producer_archive_without_pax(tmp_path, alias):
    """Real ID/archive producers remain compatible with the strict source reader."""

    workload_id = dbt_workload_id("w" * 64, alias, "model.project." + alias)
    assert len(workload_id) <= 64
    path = f"_dbt/manifests/{workload_id}.yaml"
    body = f"name: {workload_id}\n".encode()
    payload = RuntimePayloadBuilder(repo_root=tmp_path, paths=(), generated_files={path: body}).build()
    assert len(path.encode()) < 100
    assert _member({"runtime_payload": payload.to_jsonable()}, expected_path=path, max_member_bytes=len(body)) == body


@pytest.mark.parametrize(
    "change", [{"name": "../manifest.yaml"}, {"name": "other.yaml"}, {"repeated": True}, {"link": True}, {"pax": True}]
)
def test_noncanonical_archive_member_fails_without_extraction(change):
    with pytest.raises(DbtDevEvidenceReleaseError):
        _member(_pack(**change))


@pytest.mark.parametrize(
    "change",
    [
        {"bytes": True},
        {"bytes": 0},
        {"bytes": 4097},
        {"data": "!invalid!"},
        {"sha256": "sha256:" + "0" * 64},
        {"format": "zip"},
        {"extra": 1},
    ],
)
def test_archive_contract_and_identity_cannot_be_resealed_by_outer_pack(change):
    pack = _pack()
    pack["runtime_payload"]["archive"].update(change)
    with pytest.raises(DbtDevEvidenceReleaseError):
        _member(pack)


def test_member_length_is_bounded_before_reading_body():
    with pytest.raises(DbtDevEvidenceReleaseError):
        _member(_pack(), max_member_bytes=3)


def test_noncanonical_base64_padding_is_rejected_even_for_identical_decoded_bytes():
    pack = _pack()
    archive = pack["runtime_payload"]["archive"]
    body = base64.b64decode(archive["data"])
    if len(body) % 3 == 0:
        body += b"\x00"  # Legal gzip trailing zero, to obtain base64 padding.
    archive.update(bytes=len(body), sha256=sha256_bytes(body), data=base64.b64encode(body).decode())
    assert _member(pack) == b"data"
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
    offset = -3 if archive["data"].endswith("==") else -2
    data = list(archive["data"])
    data[offset] = alphabet[alphabet.index(data[offset]) | 1]
    archive["data"] = "".join(data)
    assert base64.b64decode(archive["data"], validate=True) == body
    with pytest.raises(DbtDevEvidenceReleaseError, match="encoding"):
        _member(pack)


def test_expanded_metadata_is_bounded_before_tar_parser_sees_it(monkeypatch):
    pack = _pack(pax="x" * 100_000)

    def forbidden(*args, **kwargs):
        raise AssertionError("unbounded metadata reached the tar parser")

    monkeypatch.setattr(tarfile, "open", forbidden)
    with pytest.raises(DbtDevEvidenceReleaseError, match="expanded"):
        _member(pack)


@pytest.mark.parametrize("compressed", [b"not gzip", b"\x1f\x8b\x08\x00" + b"\x00" * 6 + b"\x07"])
def test_matching_outer_hash_does_not_turn_invalid_compression_into_internal_error(compressed):
    pack = _pack()
    pack["runtime_payload"]["archive"].update(
        bytes=len(compressed), sha256=sha256_bytes(compressed), data=base64.b64encode(compressed).decode()
    )
    with pytest.raises(DbtDevEvidenceReleaseError):
        _member(pack)


def test_pure_dag_projection_preserves_exact_workload_membership():
    dag = {
        "dag_id": "DAG__a__b__c",
        "source": {"workflow": "daily"},
        "nodes": [{"workload_id": "dbt__daily"}, {"workload_id": "transfer"}],
        "workflow_outcome": {
            "schema": "dpone.dbt-workflow-outcome.v1",
            "task_id": "outcome",
            "workflow_id": "daily",
            "expected_terminal_task_ids": ["transfer__outcome"],
        },
    }
    kwargs = {"dag_id": dag["dag_id"], "workload_packs": {"dbt__daily": {}, "transfer": {}}}
    workflow, result = decode_dbt_workflow_dag(json.dumps(dag).encode(), **kwargs)
    assert workflow == "daily" and result.workload_ids == ("dbt__daily", "transfer")
    for workload in ("dbt__daily", "foreign"):
        invalid = deepcopy(dag)
        invalid["nodes"][1]["workload_id"] = workload
        with pytest.raises(DbtDevEvidenceReleaseError):
            decode_dbt_workflow_dag(json.dumps(invalid).encode(), **kwargs)
