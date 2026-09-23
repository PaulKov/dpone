from pathlib import Path

from dpone.contracts import mssql_tds_api
from dpone.contracts.mssql_tds_worker import TdsAttemptState
from dpone.contracts.mssql_tds_worker_codec import decode_state
from dpone.metrics.import_graph import collect_internal_deps
from dpone.metrics.loc import iter_py_files


def test_tds_api_keeps_codec_dependencies_out_of_dto_modules() -> None:
    package = Path("src/dpone")
    dependencies = collect_internal_deps(
        package,
        module_files=list(iter_py_files(package)),
        package_name="dpone",
    )

    assert "dpone.contracts.mssql_tds_worker_codec" not in dependencies["dpone.contracts.mssql_tds_directory_model"]
    assert len(dependencies["dpone.contracts.mssql_tds_api.part_1"]) < 60


def test_tds_api_preserves_worker_codec_and_dto_identity() -> None:
    assert mssql_tds_api.decode_state is decode_state
    assert mssql_tds_api.TdsAttemptState is TdsAttemptState
