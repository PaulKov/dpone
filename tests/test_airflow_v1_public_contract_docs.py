from __future__ import annotations

from pathlib import Path

from dpone.adapters.yaml_pyyaml import PyYamlCodec
from dpone.services.docs.airflow_public_contracts import (
    is_airflow_public_contract_reference_in_sync,
    load_public_contract_baseline,
    render_airflow_public_contract_reference,
)

ROOT = Path(__file__).parents[1]


def test_generated_airflow_public_contract_reference_is_current() -> None:
    baseline = load_public_contract_baseline(
        ROOT / "docs" / "airflow-self-service-public-contracts-v1.yaml",
        yaml_codec=PyYamlCodec(),
    )
    doc = ROOT / "docs" / "reference" / "airflow-public-contracts.md"

    assert is_airflow_public_contract_reference_in_sync(doc, baseline=baseline)
    rendered = render_airflow_public_contract_reference(baseline)
    assert "Beginner command contract" in rendered
    assert "Successful flat offline preview path" in rendered
    assert "Successful domain-first offline preview path" in rendered
    assert "Optional platform-gated safe sample" in rendered
    assert "`dpone init project --airflow`" in rendered
    assert "`dpone init pipeline <name> --recipe mssql-to-clickhouse-incremental --airflow`" in rendered
    assert "`dpone check <target>`" in rendered
    assert "`dpone airflow preview <pipeline>`" in rendered
    assert "`dpone run <path> --sample 1000 --target temporary`" in rendered
    assert "`airflow.providers.dpone`" in rendered
    assert "`apache-airflow-providers-dpone`" in rendered


def test_reference_page_explains_that_the_baseline_is_not_generated_from_code() -> None:
    text = (ROOT / "docs" / "reference" / "airflow-public-contracts.md").read_text(encoding="utf-8")

    assert "never rewrites the reviewed baseline" in text
    assert "dpone docs check-airflow-public-contracts" in text
