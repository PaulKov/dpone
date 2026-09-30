import json
from pathlib import Path

import pytest

from dpone.services.mssql_native_evidence_privacy import scan_mssql_native_shareable_artifacts


def _write(path: Path, value: object) -> Path:
    path.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")
    return path


def test_scanner_accepts_closed_opaque_evidence_and_pointer(tmp_path: Path) -> None:
    evidence = _write(tmp_path / "revision.json", {"subject": {"invocation_id": "a" * 64}})
    pointer = _write(tmp_path / "current.json", {"schema_version": 1, "sha256": "b" * 64})

    scan_mssql_native_shareable_artifacts((evidence, pointer), secret_needles=("private-endpoint", "secret"))


@pytest.mark.parametrize("private_key", ["host", "database", "table", "password", "file_path", "query"])
def test_scanner_rejects_private_coordinate_keys(tmp_path: Path, private_key: str) -> None:
    artifact = _write(tmp_path / "revision.json", {"subject": {private_key: "opaque"}})

    with pytest.raises(ValueError, match="private_key"):
        scan_mssql_native_shareable_artifacts((artifact,), secret_needles=())


def test_scanner_rejects_secret_in_serialized_bytes(tmp_path: Path) -> None:
    artifact = _write(tmp_path / "revision.json", {"diagnostic": "prefix-private-endpoint-suffix"})

    with pytest.raises(ValueError, match="private_value"):
        scan_mssql_native_shareable_artifacts((artifact,), secret_needles=("private-endpoint",))


def test_short_secret_does_not_match_inside_public_brand_name(tmp_path: Path) -> None:
    artifact = _write(tmp_path / "revision.json", {"kind": "dpone.certification"})

    scan_mssql_native_shareable_artifacts((artifact,), secret_needles=("dpone",))


def test_short_secret_is_rejected_as_complete_value(tmp_path: Path) -> None:
    artifact = _write(tmp_path / "revision.json", {"opaque": "dpone"})

    with pytest.raises(ValueError, match="private_value"):
        scan_mssql_native_shareable_artifacts((artifact,), secret_needles=("dpone",))
