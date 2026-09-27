"""Contract tests for a supervised native BCP launch."""

from dataclasses import replace
from hashlib import sha256
from pathlib import Path

import pytest

from dpone.contracts.mssql_native_writer import NativeStageWriteGrant, NativeStageWriteOutcome
from dpone.runtime.connectors.mssql_bcp_process import BcpSupervisedResult
from dpone.runtime.sinks.mssql_native_bcp_writer import MssqlNativeBcpWriter


def _grant(path: Path) -> NativeStageWriteGrant:
    return NativeStageWriteGrant(
        attempt_id="attempt",
        qualified_stage="[db].[stage].[owned]",
        file_path=path,
        expected_rows=2,
        encoded_bytes=path.stat().st_size,
        file_sha256=sha256(path.read_bytes()).hexdigest(),
        grant_token_sha256="a" * 64,
        proof_capability="bcp-supervised-stage-barrier-v1",
    )


def test_supervised_writer_launches_once_and_requires_positive_evidence(tmp_path: Path) -> None:
    path = tmp_path / "sealed.native"
    path.write_bytes(b"sealed")
    grant = _grant(path)
    launches = []
    rejects = tmp_path / "rejects.txt"

    def launch(_grant, _rejects):
        launches.append((_grant, _rejects))
        return BcpSupervisedResult("success", True, True, 2)

    writer = MssqlNativeBcpWriter(launch)
    outcome = writer.write(grant, rejects_path=rejects)
    assert outcome.positive_terminal is True
    assert len(launches) == 1
    with pytest.raises(ValueError, match="grant_already_launched"):
        writer.write(grant, rejects_path=rejects)
    with pytest.raises(ValueError, match="grant_already_launched"):
        writer.write(replace(grant, grant_token_sha256="b" * 64), rejects_path=rejects)


@pytest.mark.parametrize(
    "result",
    [
        BcpSupervisedResult("failure", False, True, None),
        BcpSupervisedResult("timeout", False, True, None),
        BcpSupervisedResult("lost_ack", False, True, None),
        BcpSupervisedResult("cleanup_failed", False, True, None),
        BcpSupervisedResult("custody_lost", False, False, None),
    ],
)
def test_supervised_writer_returns_nonpositive_process_outcome(tmp_path: Path, result: BcpSupervisedResult) -> None:
    path = tmp_path / "sealed.native"
    path.write_bytes(b"sealed")
    outcome = MssqlNativeBcpWriter(lambda grant, rejects: result).write(_grant(path), rejects_path=tmp_path / "rejects")
    assert outcome.positive_terminal is False
    assert outcome.classification == result.classification


def test_supervised_writer_rejects_vendor_count_without_returning_outcome(tmp_path: Path) -> None:
    path = tmp_path / "sealed.native"
    path.write_bytes(b"sealed")
    with pytest.raises(ValueError, match="vendor_count_mismatch"):
        MssqlNativeBcpWriter(lambda grant, rejects: BcpSupervisedResult("success", True, True, 1)).write(
            _grant(path), rejects_path=tmp_path / "rejects"
        )


def test_supervised_writer_rejects_rejects_and_file_drift(tmp_path: Path) -> None:
    path = tmp_path / "sealed.native"
    path.write_bytes(b"sealed")
    grant = _grant(path)
    rejects = tmp_path / "rejects"

    def launch(_grant, _rejects):
        rejects.write_text("bad row")
        return BcpSupervisedResult("success", True, True, 2)

    with pytest.raises(ValueError, match="rejects_not_empty"):
        MssqlNativeBcpWriter(launch).write(grant, rejects_path=rejects)
    path.write_bytes(b"changed")
    with pytest.raises(ValueError, match="file_identity"):
        MssqlNativeBcpWriter(lambda grant, rejects: pytest.fail("must not launch")).write(
            grant, rejects_path=tmp_path / "other"
        )


def test_writer_outcome_contract_rejects_unsupported_or_false_success() -> None:
    with pytest.raises(ValueError, match="writer_outcome"):
        NativeStageWriteOutcome("attempt", True, None, "success")
    with pytest.raises(ValueError, match="writer_outcome"):
        NativeStageWriteOutcome("attempt", False, 2, "success")
    with pytest.raises(ValueError, match="writer_outcome"):
        NativeStageWriteOutcome("attempt", False, None, "invented")


def test_grant_rejects_unquoted_stage_before_writer_launch(tmp_path: Path) -> None:
    path = tmp_path / "sealed.native"
    path.write_bytes(b"sealed")
    with pytest.raises(ValueError, match="writer_grant"):
        replace(_grant(path), qualified_stage="stage; DROP TABLE x")
