"""Public native starter flags do not reinterpret legacy recipe options."""

import pytest

from dpone.cli.parser import build_parser


def test_native_starter_parser_requires_explicit_inputs_and_defaults_apply():
    args = build_parser().parse_args(
        ["init", "dbt", "demo", "--profiles", "policy.yml", "--profile", "local", "--workflow", "orders"]
    )
    assert args.init_target == "dbt" and args.dbt_path == "demo"
    assert args.dbt_profiles == "policy.yml" and args.dbt_profile == "local"
    assert args.dbt_workflow == "orders" and args.dbt_dry_run is False


def test_format_inheritance_and_explicit_dry_run():
    args = build_parser().parse_args(
        [
            "init",
            "--format",
            "json",
            "dbt",
            "demo",
            "--profiles",
            "policy.yml",
            "--profile",
            "local",
            "--workflow",
            "orders",
            "--dry-run",
        ]
    )
    assert args.format == "json" and args.dbt_dry_run


@pytest.mark.parametrize("option", ["--profiles", "--profile", "--workflow"])
def test_required_dbt_option_missing(option):
    argv = ["init", "dbt", "demo", "--profiles", "policy.yml", "--profile", "local", "--workflow", "orders"]
    index = argv.index(option)
    del argv[index : index + 2]
    with pytest.raises(SystemExit) as error:
        build_parser().parse_args(argv)
    assert error.value.code == 2


def test_inherited_profile_remains_pipeline_scoped():
    parser = build_parser()
    pipeline = parser.parse_args(["init", "--profile", "recipe@1.0.0", "pipeline", "orders"])
    assert pipeline.profile == "recipe@1.0.0"
    with pytest.raises(SystemExit) as error:
        parser.parse_args(["init", "--profile", "local", "dbt", "demo"])
    assert error.value.code == 2


@pytest.mark.parametrize(
    "options",
    [["--recipe", "example"], ["--route", "mssql:clickhouse:full_refresh"], ["--airflow"], ["--source-type", "mssql"]],
)
@pytest.mark.parametrize("before", [False, True])
def test_foreign_options_reject_in_both_positions(options, before, capsys):
    base = ["dbt", "demo", "--profiles", "policy.yml", "--profile", "local", "--workflow", "orders"]
    with pytest.raises(SystemExit) as error:
        build_parser().parse_args(["init", *options, *base] if before else ["init", *base, *options])
    assert error.value.code == 2
    assert f"dbt does not accept: {options[0]}" in capsys.readouterr().err


def test_target_format_overrides_inherited_format():
    args = build_parser().parse_args(
        [
            "init",
            "--format",
            "json",
            "dbt",
            "demo",
            "--profiles",
            "policy.yml",
            "--profile",
            "local",
            "--workflow",
            "orders",
            "--format",
            "md",
        ]
    )
    assert args.format == "md"
