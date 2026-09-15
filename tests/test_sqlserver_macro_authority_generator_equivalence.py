"""The graph extraction must not change generated authority or CLI behavior."""

import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest
from tools.dbt_self_service import generate_sqlserver_macro_authority as producer
from tools.dbt_self_service import sqlserver_macro_authority_graph as graph

ROOT = Path(__file__).resolve().parents[1]


def test_generated_baseline_retains_complete_pre_extraction_bytes():
    generated = producer.generate_baseline().encode()
    assert generated == producer.OUTPUT_PATH.read_bytes()
    assert hashlib.sha256(generated).hexdigest() == "a2055fc9cb515b31b9efe952246600a9792fdc7e74735de7cd1cd339b99de457"


def test_graph_functions_and_policy_have_one_owner():
    for name in (
        "_framework_records",
        "_invocation_extension_records",
        "_macro_record",
        "_dispatch_protection",
        "_assert_acyclic",
        "_logical_name",
        "_mapping",
        "_text",
        "_is_non_empty_string_sequence",
        "_sha256",
        "TRUSTED_PACKAGES",
        "TRUSTED_ROOTS",
        "INVOCATION_EXTENSION_UNIQUE_IDS",
    ):
        assert getattr(producer, name) is getattr(graph, name)


def test_direct_script_works_outside_checkout_in_isolated_python(tmp_path):
    result = subprocess.run(
        [sys.executable, "-I", str(Path(producer.__file__)), "--check"], cwd=tmp_path, capture_output=True, check=False
    )
    assert (result.returncode, result.stdout, result.stderr) == (0, b"macro-authority baseline is current\n", b"")


def test_module_entrypoint_retains_check_result():
    result = subprocess.run(
        [sys.executable, "-m", "tools.dbt_self_service.generate_sqlserver_macro_authority", "--check"],
        cwd=ROOT,
        capture_output=True,
        check=False,
    )
    assert (result.returncode, result.stdout, result.stderr) == (0, b"macro-authority baseline is current\n", b"")


@pytest.mark.parametrize("existing", [None, "stale"])
def test_check_missing_or_stale_output_retains_exit_and_does_not_write(tmp_path, monkeypatch, capsys, existing):
    output = tmp_path / "baseline.py"
    if existing is not None:
        output.write_text(existing)
    monkeypatch.setattr(producer, "ROOT", tmp_path)
    monkeypatch.setattr(producer, "OUTPUT_PATH", output)
    assert producer.main(["--check"]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "macro-authority baseline is stale: baseline.py\n"
    assert output.read_text() == existing if existing is not None else not output.exists()


def test_default_write_retains_exact_generated_bytes(tmp_path, monkeypatch, capsys):
    output = tmp_path / "baseline.py"
    monkeypatch.setattr(producer, "ROOT", tmp_path)
    monkeypatch.setattr(producer, "OUTPUT_PATH", output)
    expected = producer.generate_baseline().encode()
    assert producer.main([]) == 0
    assert output.read_bytes() == expected
    assert capsys.readouterr() == ("wrote baseline.py\n", "")


@pytest.mark.parametrize(
    "args",
    [
        ["--diff-output", "unused"],
        ["--candidate-manifest", "unused"],
        ["--check", "--candidate-manifest", "unused", "--diff-output", "unused"],
    ],
)
def test_invalid_cli_combinations_fail_before_generation(args, monkeypatch, capsys):
    def forbidden(*args, **kwargs):
        pytest.fail("invalid syntax reached generation")

    monkeypatch.setattr(producer, "generate_baseline", forbidden)
    monkeypatch.setattr(producer, "generate_candidate_diff", forbidden)
    with pytest.raises(SystemExit) as error:
        producer.main(args)
    assert error.value.code == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "error:" in captured.err


@pytest.mark.parametrize("failure", [OSError("unreadable"), ValueError("invalid"), UnicodeError("bad text")])
def test_generation_failure_retains_channels_and_exit(failure, monkeypatch, capsys):
    def fail():
        raise failure

    monkeypatch.setattr(producer, "generate_baseline", fail)
    assert producer.main(["--check"]) == 2
    assert capsys.readouterr() == ("", f"macro-authority generation failed: {failure}\n")


def test_write_failure_is_not_reclassified(tmp_path, monkeypatch):
    monkeypatch.setattr(producer, "OUTPUT_PATH", tmp_path / "missing" / "baseline.py")
    with pytest.raises(FileNotFoundError):
        producer.main([])


def test_unchanged_candidate_has_no_authority_delta():
    report = producer.generate_candidate_diff(producer.MANIFEST_PATH)
    for name in ("framework_macro_records", "invocation_extension_records"):
        assert report[name] == {"added": [], "removed": [], "changed": []}
    assert report["new_or_changed_execution_capable_macros"] == []
    assert report["trusted_root_paths_to_new_or_changed_execution_capable_macros"] == []
    assert report["baseline"] == report["candidate"]


@pytest.mark.parametrize(
    "mutation,message",
    [
        ("missing", "framework macro dependency is missing:"),
        ("cycle", "framework macro dependencies must be acyclic:"),
        ("duplicate", "depends_on.macros must not contain duplicates"),
        ("foreign", "protected dispatch candidate belongs to a foreign package:"),
    ],
)
def test_candidate_graph_rejections_keep_specific_diagnostics(tmp_path, mutation, message):
    manifest = json.loads(producer.MANIFEST_PATH.read_text())
    root = graph.TRUSTED_ROOTS[0]
    if mutation == "missing":
        del manifest["macros"][root]
    elif mutation == "cycle":
        manifest["macros"][root]["depends_on"]["macros"].append(root)
    elif mutation == "duplicate":
        dependencies = manifest["macros"][root]["depends_on"]["macros"]
        dependencies.append(dependencies[0])
    else:
        name = "sqlserver__get_create_table_as_sql"
        unique_id = "macro.foreign." + name
        manifest["macros"][unique_id] = {
            "unique_id": unique_id,
            "name": name,
            "package_name": "foreign",
            "resource_type": "macro",
            "macro_sql": "",
            "depends_on": {"macros": []},
        }
    path = tmp_path / "candidate.json"
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError) as error:
        producer.generate_candidate_diff(path)
    assert message in str(error.value)
