"""Documentation contracts for supervised composition operations."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
GUIDE = DOCS / "guides" / "composition-supervisor-kubernetes.md"

_CELLS = (
    "sqlserver_dbt_v1",
    "postgres_mssql_full_refresh_v1",
    "mssql_clickhouse_full_refresh_v1",
)
_JOURNEY_HEADINGS = (
    "## Administrator provisioning",
    "## Data-engineer compose and build",
    "## Operator cache-sync and desired-state",
    "## DAG triggering",
    "## Evidence inspection",
    "## Blocked retry diagnosis",
    "## Tombstone retention",
)
_SUPERVISOR_FLAGS = (
    "--composition-supervisor-pvc",
    "--composition-child-uid-start",
    "--composition-child-gid-start",
    "--composition-child-identity-count",
)
_SECURITY_TOKENS = (
    "runAsUser: 0",
    "readOnlyRootFilesystem: true",
    "CHOWN",
    "FOWNER",
    "DAC_READ_SEARCH",
    "SETUID",
    "SETGID",
    "KILL",
    "ReadWriteMany",
    "tmpfs",
)
_FALSE_CERTIFICATION = (
    "offline tests certify the route",
    "offline tests are route certification",
    "Batch ETL supported from a narrow smoke",
    "skipped live check is PASS",
    "skipped live checks as PASS",
)
_HUBS = (
    DOCS / "composition-activation-contract.md",
    DOCS / "airflow-cache-sync-promotion.md",
    DOCS / "airflow-pack-provider.md",
    DOCS / "compatibility.md",
)


def _fenced_blocks(path: Path, language: str) -> tuple[str, ...]:
    blocks: list[str] = []
    current: list[str] | None = None
    opening = f"```{language}"
    for line in path.read_text(encoding="utf-8").splitlines():
        if current is None and line == opening:
            current = []
        elif current is not None and line == "```":
            blocks.append("\n".join(current))
            current = None
        elif current is not None:
            current.append(line)
    return tuple(blocks)


def _guide() -> str:
    return GUIDE.read_text(encoding="utf-8")


def _guide_examples(*languages: str) -> str:
    return "\n".join("\n".join(_fenced_blocks(GUIDE, language)) for language in languages)


def test_composition_supervisor_guide_has_purpose_audience_and_journey() -> None:
    text = _guide()
    opening = "\n".join(text.splitlines()[:12])
    assert "**Purpose.**" in opening
    assert "**Audience.**" in opening
    missing = [heading for heading in _JOURNEY_HEADINGS if heading not in text]
    assert missing == []


def test_composition_supervisor_guide_examples_cover_pvc_ranges_and_security() -> None:
    bash = "\n".join(_fenced_blocks(GUIDE, "bash"))
    examples = _guide_examples("bash", "yaml")

    missing_flags = [flag for flag in _SUPERVISOR_FLAGS if flag not in bash]
    assert missing_flags == []
    assert "1000000000" in bash
    assert "1000000" in bash
    assert "dpone-composition-supervisor" in examples

    missing_security = [token for token in _SECURITY_TOKENS if token not in examples]
    assert missing_security == []


def test_composition_supervisor_docs_example_cache_sync_authority() -> None:
    bash = "\n".join(
        (
            "\n".join(_fenced_blocks(GUIDE, "bash")),
            "\n".join(_fenced_blocks(DOCS / "airflow-cache-sync-promotion.md", "bash")),
        )
    )
    cli = (DOCS / "cli-reference.md").read_text(encoding="utf-8")

    assert "--workspace-authority-connection-ref" in bash
    assert "dpone airflow cache-sync" in bash
    assert "--workspace-authority-connection-ref" in cli


def _section(text: str, heading: str) -> str:
    start = text.index(heading)
    rest = text[start + len(heading) :]
    next_heading = rest.find("\n## ")
    return heading + (rest if next_heading < 0 else rest[:next_heading])


def test_composition_supervisor_guide_names_all_three_cells() -> None:
    text = _guide()
    missing = [cell for cell in _CELLS if f"`{cell}`" not in text]
    assert missing == []


def test_composition_docs_state_shipped_pack_exec_reachability() -> None:
    guide = _guide()
    contract = (DOCS / "composition-activation-contract.md").read_text(encoding="utf-8")
    dag_trigger = _section(guide, "## DAG triggering")
    diagnosis = _section(guide, "## Blocked retry diagnosis")
    live_status = _section(guide, "## Live status")

    assert "`sqlserver_dbt_v1`" in dag_trigger
    assert "`CompositionDbtExecutionRoot`" in dag_trigger
    assert "parent context" in dag_trigger
    assert "`composition_ordinary_worker_unavailable`" in dag_trigger
    assert "`postgres_mssql_full_refresh_v1`" in dag_trigger
    assert "`mssql_clickhouse_full_refresh_v1`" in dag_trigger
    assert "fail closed" in dag_trigger.lower() or "fail-closes" in dag_trigger.lower()
    assert "three-cell trigger campaign" in dag_trigger.lower()
    assert "not ready" in dag_trigger.lower() or "is not ready" in dag_trigger.lower()

    assert "| `composition_native_worker_unavailable`" in diagnosis
    assert "| `composition_ordinary_worker_unavailable`" in diagnosis

    assert "`UNVERIFIED`" in live_status
    assert "never PASS" in live_status or "never `PASS`" in live_status
    ready_claims = (
        "three-cell trigger campaign is ready",
        "three-cell trigger campaign as ready",
        "ready three-cell trigger campaign",
    )
    combined = "\n".join((guide, contract)).lower()
    stale_ready = [claim for claim in ready_claims if claim in combined]
    assert stale_ready == []

    assert "installed roots apply these checks when pack-exec reaches them" in contract
    assert "ordinary/ClickHouse pack-exec still fail-closes" in contract
    assert "The installed workers apply these checks" not in contract
    assert "`composition_ordinary_worker_unavailable`" in contract
    assert "`sqlserver_dbt_v1`" in contract
    assert "parent context" in contract

    operations = (DOCS / "release-composition-operations.md").read_text(encoding="utf-8")
    assert "Default public composition activation is unavailable" not in operations
    assert "guides/composition-supervisor-kubernetes.md" in operations
    assert "`composition_ordinary_worker_unavailable`" in operations
    assert "`UNVERIFIED`" in operations


def test_composition_supervisor_guide_documents_commit_unknown_and_tombstones() -> None:
    text = _guide()
    examples = _guide_examples("bash", "python", "text")

    assert "`COMMIT_UNKNOWN`" in text
    assert "reconcile_unknown" in text
    assert "not automatically deleted" in text.lower() or "no automatic tombstone" in text.lower()
    assert "RETIRED" in text
    assert any("COMMIT_UNKNOWN" in block for block in (examples, text))


def test_composition_supervisor_guide_marks_live_status_unverified() -> None:
    text = _guide()
    combined = "\n".join(path.read_text(encoding="utf-8") for path in (GUIDE, *_HUBS))

    assert "`UNVERIFIED`" in text
    assert "never PASS" in text or "never `PASS`" in text
    assert "offline tests" in text.lower()
    assert "route certification" in text.lower()
    stale = [claim for claim in _FALSE_CERTIFICATION if claim.lower() in combined.lower()]
    assert stale == []
    assert "narrow smoke" in text.lower() or "narrow smoke" in combined.lower()


def test_composition_supervisor_guide_is_linked_from_existing_docs() -> None:
    target = "guides/composition-supervisor-kubernetes.md"
    missing = [path.name for path in _HUBS if target not in path.read_text(encoding="utf-8")]
    assert missing == []


def test_composition_activation_contract_describes_shipped_public_factory() -> None:
    text = (DOCS / "composition-activation-contract.md").read_text(encoding="utf-8")

    assert "build_composition_activation_coordinator" in text
    assert "--workspace-authority-connection-ref" in text
    for cell in _CELLS:
        assert f"`{cell}`" in text
    assert "`UNVERIFIED`" in text
    assert "Public composition activation and actual worker execution\nremain unavailable" not in text
