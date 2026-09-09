from __future__ import annotations

import ast
import shlex
from collections.abc import Mapping
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
DOCS_ROOT = ROOT / "docs"
SOURCE_SINK_DOCS = tuple(sorted((DOCS_ROOT / "source-sink").glob("*.md")))


def _fenced_blocks(path: Path, language: str) -> list[str]:
    blocks: list[str] = []
    in_block = False
    current: list[str] = []
    fence = f"```{language}"
    for line in path.read_text(encoding="utf-8").splitlines():
        if line == fence and not in_block:
            in_block = True
            current = []
            continue
        if line == "```" and in_block:
            in_block = False
            blocks.append("\n".join(current))
            continue
        if in_block:
            current.append(line)
    return blocks


def _scheduler_loader_examples(path: Path) -> list[str]:
    blocks = [path.read_text(encoding="utf-8")] if path.suffix == ".py" else _fenced_blocks(path, "python")
    return [
        block
        for block in blocks
        if ("load_dpone_dags(" in block or "load_and_acknowledge_dpone_dags(" in block) and "globals()" in block
    ]


def _is_load_report_assignment(node: ast.Assign) -> bool:
    return (
        len(node.targets) == 1
        and isinstance(node.targets[0], ast.Name)
        and node.targets[0].id in {"load_report", "loaded"}
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Name)
        and node.value.func.id in {"load_dpone_dags", "load_and_acknowledge_dpone_dags"}
    )


def _is_load_report_fatal_guard(node: ast.If) -> bool:
    direct = (
        isinstance(node.test, ast.Attribute)
        and isinstance(node.test.value, ast.Name)
        and node.test.value.id == "load_report"
        and node.test.attr == "fatal"
    )
    combined = (
        isinstance(node.test, ast.Attribute)
        and node.test.attr == "fatal"
        and isinstance(node.test.value, ast.Attribute)
        and node.test.value.attr == "report"
        and isinstance(node.test.value.value, ast.Name)
        and node.test.value.value.id == "loaded"
    )
    return direct or combined


def _yaml_mappings(path: Path) -> list[Mapping[str, object]]:
    mappings: list[Mapping[str, object]] = []
    for block in _fenced_blocks(path, "yaml"):
        value = yaml.safe_load(block)
        if isinstance(value, Mapping):
            mappings.append(value)
    return mappings


def _copy_paste_manifest(path: Path) -> Mapping[str, object]:
    for value in _yaml_mappings(path):
        if value.get("kind") == "dpone.batch.v1" and {"defaults", "schemas", "quality"} <= value.keys():
            return value
    raise AssertionError(f"copy/paste manifest not found in {path}")


def _markdown_section(path: Path, heading: str) -> str:
    content = path.read_text(encoding="utf-8")
    section = content.split(heading, 1)[1]
    return section.split("\n## ", 1)[0]


def test_docs_markdown_files_do_not_contain_cyrillic_text() -> None:
    offenders: list[str] = []
    for path in sorted(DOCS_ROOT.rglob("*.md")):
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if any("\u0400" <= char <= "\u04ff" for char in line):
                offenders.append(f"{path.relative_to(ROOT)}:{line_number}: {line.strip()}")
                break

    assert offenders == []


def test_airflow_provider_python_examples_use_deployment_scoped_cached_refs() -> None:
    examples = "\n".join(_fenced_blocks(DOCS_ROOT / "airflow-provider-api.md", "python"))

    assert "cached://workloads/load_orders?release=" not in examples
    assert "cached://dags/orders_daily?release=" not in examples
    assert (
        "cached://deployments/sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        "/workloads/load_orders" in examples
    )
    assert (
        "cached://deployments/sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        "/dags/orders_daily" in examples
    )


def test_airflow_scheduler_loader_examples_fail_closed_without_external_io() -> None:
    paths = [
        DOCS_ROOT / "airflow-pack-provider.md",
        DOCS_ROOT / "airflow-self-service.md",
        DOCS_ROOT / "airflow-provider-api.md",
        DOCS_ROOT / "airflow-provider-cache-migration.md",
        DOCS_ROOT / "getting-started" / "first-airflow-dag.md",
        ROOT / "packages" / "dpone-airflow-pack" / "README.md",
        ROOT / "examples" / "airflow-self-service-pilot" / "dags" / "dpone_dags.py",
    ]

    for path in paths:
        examples = _scheduler_loader_examples(path)
        assert examples, path
        for example in examples:
            tree = ast.parse(example)
            assignments = [node for node in ast.walk(tree) if isinstance(node, ast.Assign)]
            guards = [node for node in ast.walk(tree) if isinstance(node, ast.If)]
            combined_calls = [
                node
                for node in ast.walk(tree)
                if isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "load_and_acknowledge_dpone_dags"
            ]
            loader_calls = combined_calls

            assert len(loader_calls) == 1, path
            assert any(_is_load_report_assignment(node) for node in assignments), path
            assert any(
                keyword.arg == "index_path"
                and not (isinstance(keyword.value, ast.Constant) and keyword.value.value is None)
                for keyword in loader_calls[0].keywords
            ), path
            assert any(_is_load_report_fatal_guard(node) for node in guards), path
            assert "load_report.errors[0].get" in example or "loaded.report.errors[0].get" in example, path
            assert "DPONE_AIRFLOW_INDEX_INVALID" in example, path
            assert "raise RuntimeError" in example, path
            keywords = {keyword.arg for keyword in combined_calls[0].keywords}
            assert {"ack_path", "ack_root"} <= keywords, path
            assert not any(
                forbidden in example
                for forbidden in (
                    "requests.",
                    "urllib.",
                    "boto3.",
                    "Variable.get(",
                    "BaseHook.get_connection(",
                    "create_engine(",
                )
            ), path


def test_airflow_provider_cache_migration_guide_covers_safe_upgrade_and_rollback() -> None:
    path = DOCS_ROOT / "airflow-provider-cache-migration.md"
    content = path.read_text(encoding="utf-8")

    required = [
        "dpone_airflow_pack",
        "airflow.providers.dpone",
        "dpone init project --airflow",
        "latest/pack-index.json",
        "current/airflow-index.json",
        "DeprecationWarning",
        "`bytes`",
        "cache-recovery-plan",
        "cache-recovery-apply",
        "Rollback",
        "airflow-pack-provider.md",
        "airflow-provider-api.md",
        "airflow-self-service.md",
    ]

    assert [marker for marker in required if marker not in content] == []


def test_airflow_self_service_architecture_docs_are_in_mkdocs_nav() -> None:
    mkdocs = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")
    required_nav_paths = [
        "airflow-self-service-architecture.md",
        "airflow-provider-api.md",
        "route-certification-matrix.md",
        "adr/0006-authority-and-canonical-ir.md",
        "adr/0007-release-set-and-deployment-set.md",
        "adr/0008-airflow-parse-safe-provider.md",
        "adr/0009-artifact-delivery-and-cache-materializer.md",
        "adr/0010-binding-set-and-credential-resolvers.md",
        "adr/0011-canonical-fingerprints.md",
        "adr/0012-vault-runtime-authentication.md",
        "adr/0013-reproducible-rerun-and-retention.md",
    ]

    missing = [path for path in required_nav_paths if path not in mkdocs]

    assert missing == []


def test_route_certification_docs_separate_connector_and_route_claims() -> None:
    route_page = (DOCS_ROOT / "route-certification-matrix.md").read_text(encoding="utf-8")
    source_sink = (DOCS_ROOT / "source-sink-matrix.md").read_text(encoding="utf-8")
    connector = (DOCS_ROOT / "connector-certification.md").read_text(encoding="utf-8")

    assert "source, sink, strategy, transport, schema-evolution mode" in route_page
    assert "Missing evidence is never converted to `PASS`" in route_page
    assert "route-certification-matrix.md" in source_sink
    assert "route-certification-matrix.md" in connector


def test_first_airflow_dag_docs_have_copy_paste_golden_path() -> None:
    page = DOCS_ROOT / "getting-started" / "first-airflow-dag.md"
    content = page.read_text(encoding="utf-8")
    marker = "## Golden path commands"

    assert marker in content
    section = _markdown_section(page, marker)
    golden_blocks = [
        block
        for block in _fenced_blocks(page, "bash")
        if block in section and "dpone init pipeline orders_daily" in block
    ]
    assert len(golden_blocks) == 1
    golden_commands = [line.strip() for line in golden_blocks[0].splitlines() if line.strip().startswith("dpone ")]

    assert golden_commands == [
        "dpone init project --airflow",
        "dpone init pipeline orders_daily --recipe mssql-to-clickhouse-incremental",
        "dpone check orders_daily",
        "dpone airflow preview orders_daily",
        "dpone test orders_daily",
    ]
    assert "successful offline golden path" in section
    assert "dpone run pipelines/orders_daily --sample 1000 --target temporary" not in section
    assert "dpone airflow explain" not in section
    assert content.index("dpone airflow explain orders_daily") > content.index("## Diagnose")


def test_first_airflow_dag_labels_optional_platform_gated_sample() -> None:
    page = DOCS_ROOT / "getting-started" / "first-airflow-dag.md"
    optional_sample = _markdown_section(page, "## Optional: Request a platform-gated safe sample")
    prose = " ".join(optional_sample.split())

    assert "dpone run orders_daily --sample 1000 --target temporary" in optional_sample
    assert "exit code `3`" in prose
    assert "does not read or write source or target data" in prose
    assert "not a successful sample execution" in prose


def test_airflow_docs_distinguish_preview_rehearsal_canary_and_certification_lanes() -> None:
    architecture = " ".join((DOCS_ROOT / "airflow-self-service-architecture.md").read_text(encoding="utf-8").split())
    migration = " ".join((DOCS_ROOT / "airflow-provider-cache-migration.md").read_text(encoding="utf-8").split())
    runner_pack = " ".join((DOCS_ROOT / "gitops-airflow-runner-pack.md").read_text(encoding="utf-8").split())

    assert "preview v1" in architecture
    assert "local safe-sample handoff" in architecture
    assert "strict-v2 non-production executable rehearsal" in architecture
    assert "production parse canary" in architecture
    assert "does not establish production certification" in architecture

    assert "non-production executable rehearsal" in migration
    assert "production parse canary" in migration
    assert "must not be triggered" in migration

    assert "Advanced compatibility path (not the default self-service plane)." in runner_pack
    assert "indexed-v2 self-service plane" in runner_pack


def test_first_airflow_dag_docs_state_truthful_fifth_command_boundary() -> None:
    first_dag = " ".join((DOCS_ROOT / "getting-started" / "first-airflow-dag.md").read_text(encoding="utf-8").split())
    architecture = " ".join((DOCS_ROOT / "airflow-self-service-architecture.md").read_text(encoding="utf-8").split())
    blocker = " ".join(
        (DOCS_ROOT / "errors" / "DPONE_SAFE_SAMPLE_CERTIFIED_COPY_EXECUTOR_NOT_CONFIGURED.md")
        .read_text(encoding="utf-8")
        .split()
    )

    assert "The five-command sequence is the complete successful offline golden path." in first_dag
    assert "The optional live sample is platform-gated" in first_dag
    assert "The first five commands are the successful offline preview v1 golden path" in architecture
    assert "The live sample is a separate platform-gated request" in architecture
    assert "safe-sample-execution-plan.json" in first_dag
    assert (
        "- evidence: .dpone-cache/safe-sample-runs/orders_daily/<run-id>/safe-sample-runtime-execution.json"
        in first_dag
    )
    assert "`safe_sample.runtime_handoff.plan_path`" in first_dag
    assert "offline five-command journey" in blocker
    assert "not a successful sample execution" in blocker
    assert "Journey success for a first-time author" not in blocker
    assert "final golden-path command" not in first_dag


def test_airflow_docs_keep_preview_v1_separate_from_executable_v2() -> None:
    page = DOCS_ROOT / "getting-started" / "first-airflow-dag.md"
    first_dag = page.read_text(encoding="utf-8")
    architecture = (DOCS_ROOT / "airflow-self-service-architecture.md").read_text(encoding="utf-8")
    first_dag_prose = " ".join(first_dag.split())
    architecture_prose = " ".join(architecture.split())

    for prose in (first_dag_prose, architecture_prose):
        assert "loader entry point" in prose
        assert "current-pointer convention" in prose
        assert "cache layout" in prose
        assert "does not share the executable production schema contract" in prose
        assert "platform build regenerates" in prose
        assert "immutable executable v2" in prose
        assert "never edit or promote preview v1 in place" in prose

    assert "same index contract as production" not in first_dag_prose

    lane_diagrams = [
        block
        for block in _fenced_blocks(page, "mermaid")
        if "Preview v1" in block and "Platform build" in block and "executable v2" in block
    ]
    assert len(lane_diagrams) == 1
    assert "Offline golden-path success" in lane_diagrams[0]


def test_first_airflow_dag_labels_json_as_abridged_and_links_generated_reference() -> None:
    content = (DOCS_ROOT / "getting-started" / "first-airflow-dag.md").read_text(encoding="utf-8")

    assert "All JSON snippets on this page are abridged." in content
    assert "../reference/airflow-public-contracts.md" in content
    assert "../reference/gitops-schema-catalog.md" in content


def test_self_service_evidence_docs_match_unverified_exit_contract() -> None:
    evidence = (DOCS_ROOT / "airflow-self-service-evidence.md").read_text(encoding="utf-8")
    prose = " ".join(evidence.split())

    assert "| No external evidence | `UNVERIFIED` | `1` |" in evidence
    assert "| Four valid user sessions | usability `UNVERIFIED` | `1` |" in evidence
    assert "| `UNVERIFIED` with `--allow-unverified` | `UNVERIFIED` | `0` |" in evidence
    assert "`--allow-unverified` changes only the process exit code" in prose


def test_source_sink_guides_use_canonical_run_and_quality_contracts() -> None:
    for path in SOURCE_SINK_DOCS:
        content = path.read_text(encoding="utf-8")
        manifest = _copy_paste_manifest(path)
        quality = manifest["quality"]
        defaults = manifest["defaults"]
        assert isinstance(defaults, Mapping), path
        source = defaults["source"]
        sink = defaults["sink"]

        assert "dpone batch run" not in content, path
        expected_run = f"dpone run examples/source-sink/{path.stem}.yaml"
        assert expected_run in content, path
        assert isinstance(quality, Mapping), path
        assert isinstance(quality.get("gates"), list) and quality["gates"], path
        assert "checks" not in quality, path

        for endpoint in (source, sink):
            assert isinstance(endpoint, Mapping), path
            options = endpoint.get("options")
            assert not isinstance(options, Mapping) or "quality" not in options, path

        strategy = sink.get("strategy")
        strategy_mode = strategy.get("mode") if isinstance(strategy, Mapping) else None
        if isinstance(strategy_mode, str) and strategy_mode.startswith("incremental"):
            blocking_min_rows = [
                gate
                for gate in quality["gates"]
                if isinstance(gate, Mapping)
                and gate.get("type") == "min_rows"
                and gate.get("severity", "error") == "error"
                and gate.get("threshold") == 1
            ]
            assert blocking_min_rows == [], path


def test_managed_ux_uses_canonical_runtime_quality_gates() -> None:
    path = DOCS_ROOT / "managed-ux.md"
    content = path.read_text(encoding="utf-8")
    quality_examples = [value["quality"] for value in _yaml_mappings(path) if "quality" in value]

    assert quality_examples, path
    for quality in quality_examples:
        assert isinstance(quality, Mapping), path
        assert isinstance(quality.get("gates"), list) and quality["gates"], path
        assert "checks" not in quality, path
    assert "It never writes deprecated manifest `quality.checks`." in content


def test_acceptance_examples_use_top_level_quality_policy() -> None:
    paths = (*SOURCE_SINK_DOCS, DOCS_ROOT / "load-governance.md")
    top_level_acceptance_examples = 0

    for path in paths:
        for value in _yaml_mappings(path):
            for endpoint_name in ("source", "sink"):
                endpoint = value.get(endpoint_name)
                options = endpoint.get("options") if isinstance(endpoint, Mapping) else None
                endpoint_quality = options.get("quality") if isinstance(options, Mapping) else None
                assert not (isinstance(endpoint_quality, Mapping) and "acceptance" in endpoint_quality), path

            quality = value.get("quality")
            if isinstance(quality, Mapping) and isinstance(quality.get("acceptance"), Mapping):
                top_level_acceptance_examples += 1

    assert top_level_acceptance_examples > 0


def test_route_lifecycle_docs_distinguish_staged_and_legacy_finalization() -> None:
    for path in SOURCE_SINK_DOCS:
        manifest = _copy_paste_manifest(path)
        defaults = manifest["defaults"]
        assert isinstance(defaults, Mapping), path
        sink = defaults["sink"]
        assert isinstance(sink, Mapping), path
        sink_type = sink.get("type")
        content = path.read_text(encoding="utf-8")
        algorithm_section = _markdown_section(path, "## Runtime algorithm")
        algorithm = algorithm_section.split("```mermaid", 1)[1].split("```", 1)[0].lower()
        quality_position = algorithm.index("quality")

        if sink_type == "clickhouse":
            finalization_position = algorithm.index("finaliz")
            assert "pre_finalize" in content, path
            assert quality_position < finalization_position, path
        elif sink_type == "kafka":
            publication_position = algorithm.index("publish")
            assert "legacy_post_finalize" in content, path
            assert publication_position < quality_position, path
            assert "cannot retract delivered messages" in content, path
        else:
            finalization_position = algorithm.index("finaliz")
            assert "legacy_post_finalize" in content, path
            assert finalization_position < quality_position, path


def test_clickhouse_to_mssql_does_not_recommend_unimplemented_not_null_gate() -> None:
    content = (DOCS_ROOT / "source-sink" / "clickhouse-to-mssql.md").read_text(encoding="utf-8")

    assert "not_null" not in content


def test_load_governance_documents_quality_failure_and_recovery_contract() -> None:
    content = (DOCS_ROOT / "load-governance.md").read_text(encoding="utf-8")
    required_contract_tokens = {
        "DPONE_QUALITY_GATES_FAILED",
        "--format json",
        "stdout",
        "quality_gates",
        "pre_finalize",
        "legacy_post_finalize",
        "checkpoint",
        "recovery",
    }
    missing_tokens = {token for token in required_contract_tokens if token not in content}

    assert missing_tokens == set()
    assert any("`1`" in line and "quality" in line.lower() for line in content.splitlines())


def test_first_airflow_dag_diagnostics_link_out_without_legacy_manifest_detour() -> None:
    path = DOCS_ROOT / "getting-started" / "first-airflow-dag.md"
    diagnose = _markdown_section(path, "## Diagnose")

    assert "DPONE_LEGACY_CONNECTION_CONFIG_FOUND" not in diagnose
    assert "dpone fix " not in diagnose
    assert "../airflow-self-service.md#diagnostics-and-error-catalog" in diagnose
    assert "../airflow-cache-sync.md" in diagnose


def test_airflow_cache_publish_runbook_is_exact_and_scope_bound() -> None:
    text = (DOCS_ROOT / "airflow-cache-sync.md").read_text(encoding="utf-8")
    section = text.split("Publish that exact release/deployment pair.", maxsplit=1)[1]
    publish_example = section.split("Run materialization as a deployment step", maxsplit=1)[0]

    assert "--publication-mode exact" in publish_example
    assert "--expected-registry-scope-id" in publish_example
    assert "DPONE_ARTIFACT_REGISTRY_SCOPE_ID" in publish_example
    assert ":?set the protected endpoint-bound registry scope" in publish_example
    assert "endpoint-bound registry authority" in (DOCS_ROOT / "cli-reference.md").read_text(encoding="utf-8")


def test_airflow_cache_protected_ci_publish_example_matches_cli_contract() -> None:
    from dpone.cli.main import build_parser

    text = (DOCS_ROOT / "airflow-cache-sync.md").read_text(encoding="utf-8")
    section = text.split("The protected CI writer publishes only from passed promotion evidence:", maxsplit=1)[1]
    shell_block = section.split("```bash", maxsplit=1)[1].split("```", maxsplit=1)[0]
    commands = [
        command.strip()
        for command in shell_block.replace("\\\n", " ").split("\n\n")
        if command.strip().startswith("dpone ")
    ]

    assert len(commands) == 2
    parsed = [build_parser().parse_args(shlex.split(command)[1:]) for command in commands]
    assert parsed[0].status_output == ".ci/out/dpone_desired_state_prepare_error.json"
    assert parsed[1].status_output == ".ci/out/dpone_desired_state_publish_error.json"


ADVANCED_BEGINNER_DISCLAIMER = "Advanced path (not the beginner journey)"


def test_airflow_pipeline_glossary_defines_core_nouns() -> None:
    page = DOCS_ROOT / "getting-started" / "airflow-pipeline-glossary.md"
    content = page.read_text(encoding="utf-8")
    for noun in ("pipeline", "workload", "DAG", "pack"):
        assert noun.lower() in content.lower()


def test_airflow_ux_p2_route_framing_contracts() -> None:
    """P2: route/recipe/connection_ref glossary, First DAG bridge, BRIDGE_PAGES."""

    bridge_marker = "Start from your route (source → sink)"
    glossary = (DOCS_ROOT / "getting-started" / "airflow-pipeline-glossary.md").read_text(encoding="utf-8")
    for heading in ("## route", "## recipe", "## connection_ref"):
        assert heading in glossary, heading

    first_dag = (DOCS_ROOT / "getting-started" / "first-airflow-dag.md").read_text(encoding="utf-8")
    assert first_dag.index("MSSQL → ClickHouse") < first_dag.index("## Golden path commands")
    for recipe_ref in (
        "mssql-to-clickhouse-incremental",
        "mssql-to-clickhouse-full-refresh",
        "postgres-to-clickhouse-incremental",
        "postgres-to-clickhouse-full-refresh",
    ):
        assert recipe_ref in first_dag, recipe_ref

    bridge_pages = (
        DOCS_ROOT / "airflow-self-service.md",
        DOCS_ROOT / "index.md",
        DOCS_ROOT / "getting-started" / "quickstart.md",
        DOCS_ROOT / "getting-started" / "examples-gallery.md",
        DOCS_ROOT / "airflow-recipes.md",
        DOCS_ROOT / "source-sink-matrix.md",
    )
    for path in bridge_pages:
        assert bridge_marker in path.read_text(encoding="utf-8"), path


def test_first_airflow_dag_what_to_commit_guides_beginners() -> None:
    content = (DOCS_ROOT / "getting-started" / "first-airflow-dag.md").read_text(encoding="utf-8")
    assert "What to commit" in content
    assert "pipelines/" in content
    assert "domains/" in content
    assert "do not edit" in content.lower()


def test_advanced_airflow_pages_carry_beginner_disclaimer() -> None:
    paths = [
        DOCS_ROOT / "airflow-self-service-selectors.md",
        DOCS_ROOT / "gitops-workload-catalog.md",
        DOCS_ROOT / "airflow-self-service-advanced-cjm.md",
        ROOT / "examples" / "airflow-self-service-pilot" / "README.md",
    ]
    for path in paths:
        assert ADVANCED_BEGINNER_DISCLAIMER in path.read_text(encoding="utf-8"), path


def test_mkdocs_nav_discloses_airflow_advanced_away_from_beginners() -> None:
    mkdocs = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")
    assert "getting-started/airflow-pipeline-glossary.md" in mkdocs
    assert "airflow-self-service-advanced-cjm.md" in mkdocs
    getting_started = mkdocs.split("Getting started:", 1)[1].split("\n  - ", 1)[0]
    assert "gitops-workload-catalog.md" not in getting_started
    assert "airflow-self-service-selectors.md" not in getting_started
    core_concepts = mkdocs.split("Core concepts:", 1)[1].split("\n  - ", 1)[0]
    assert "gitops-workload-catalog.md" not in core_concepts
    assert "airflow-self-service-selectors.md" not in core_concepts
    assert "Advanced Airflow:" in mkdocs


def test_domain_first_ci_compares_before_promoting_candidate_baseline() -> None:
    page = DOCS_ROOT / "domain-first-discovery-ci.md"
    section = _markdown_section(page, "## Atomic CI handoff")
    commands = "\n".join(block for block in _fenced_blocks(page, "bash") if block in section)

    impact = "dpone workload impact"
    baseline = '--baseline "$baseline"'
    current = '--current "$candidate"'
    approval = '"$DPONE_WORKLOAD_IMPACT_APPROVER" \\'
    promotion = "dpone workload promote"
    assert impact in commands
    assert baseline in commands
    assert current in commands
    assert promotion in commands
    assert approval in commands
    assert 'approval_mode="bootstrap"' in commands
    assert 'approval_mode="change"' in commands
    assert '"$approval_evidence"' in commands
    assert '"$candidate"' in commands
    assert commands.index(impact) < commands.index(promotion)
    assert commands.index(current) < commands.index(promotion)
    assert commands.index(approval) < commands.index(promotion)
    assert "absent or rejecting approver fails closed" in section
    assert "bootstrap and later changes" in section
    assert "not a script trusted from the candidate pull request" in " ".join(section.split())
    assert "Omitting" in section
    assert "keeps interactive rediscovery compatibility" in section
    normalized = " ".join(section.split())
    assert "CI must not use it for baseline promotion" in normalized


_WIDE_CERT_ROUTES: tuple[tuple[str, str, str], ...] = (
    ("postgres", "bigquery", "landing_postgres_to_bq.batch.yaml"),
    ("postgres", "postgres", "landing_postgres_to_postgres.batch.yaml"),
    ("postgres", "mssql", "landing_postgres_to_mssql.batch.yaml"),
    ("postgres", "clickhouse", "landing_postgres_to_clickhouse.batch.yaml"),
    ("postgres", "kafka", "landing_postgres_to_kafka.batch.yaml"),
    ("mysql", "bigquery", "landing_mysql_to_bigquery.batch.yaml"),
    ("mysql", "postgres", "landing_mysql_to_postgres.batch.yaml"),
    ("mysql", "mssql", "landing_mysql_to_mssql.batch.yaml"),
    ("mysql", "clickhouse", "landing_mysql_to_clickhouse.batch.yaml"),
    ("mysql", "kafka", "landing_mysql_to_kafka.batch.yaml"),
    ("mssql", "bigquery", "landing_mssql_to_bigquery.batch.yaml"),
    ("mssql", "postgres", "landing_mssql_to_postgres.batch.yaml"),
    ("mssql", "mssql", "landing_mssql_to_mssql.batch.yaml"),
    ("mssql", "clickhouse", "landing_mssql_to_clickhouse.batch.yaml"),
    ("mssql", "kafka", "landing_mssql_to_kafka.batch.yaml"),
)


def test_wide_cert_guides_expose_self_service_golden_path() -> None:
    """Phase C: copy-paste doctor/plan/type-matrix/run CJM on certified routes."""

    for source, sink, landing in _WIDE_CERT_ROUTES:
        stem = f"{source}-to-{sink}"
        guide = DOCS_ROOT / "source-sink" / f"{stem}.md"
        content = guide.read_text(encoding="utf-8")
        example = f"examples/source-sink/{stem}.yaml"
        assert "## Self-service golden path" in content, stem
        assert "dpone doctor --profile local" in content, stem
        assert f"dpone plan {example} --format md" in content, stem
        assert f"dpone schema type-matrix --source {source} --sink {sink} --format md" in content, stem
        assert f"dpone run {example}" in content, stem
        assert f"examples/batch/{landing}" in content, stem
        assert "plan <manifest>" not in content, stem


def test_wide_cert_landing_batch_examples_exist() -> None:
    for _source, _sink, landing in _WIDE_CERT_ROUTES:
        path = ROOT / "examples" / "batch" / landing
        assert path.is_file(), path
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert isinstance(payload, Mapping)
        assert payload.get("kind") == "dpone.batch.v1"


def test_examples_gallery_lists_mysql_wide_cert_routes() -> None:
    gallery = (DOCS_ROOT / "getting-started" / "examples-gallery.md").read_text(encoding="utf-8")
    assert "## MySQL wide-certified flows" in gallery
    assert "wide vendor-live" in gallery
    for flow in (
        "MySQL -> BigQuery",
        "MySQL -> Postgres",
        "MySQL -> MSSQL",
        "MySQL -> ClickHouse",
        "MySQL -> Kafka",
    ):
        assert flow in gallery, flow


def test_examples_gallery_lists_mssql_wide_cert_routes() -> None:
    gallery = (DOCS_ROOT / "getting-started" / "examples-gallery.md").read_text(encoding="utf-8")
    assert "## MSSQL wide-certified flows" in gallery
    assert "wide vendor-live" in gallery
    for flow in (
        "MSSQL -> BigQuery",
        "MSSQL -> Postgres",
        "MSSQL -> MSSQL",
        "MSSQL -> ClickHouse",
        "MSSQL -> Kafka",
    ):
        assert flow in gallery, flow
