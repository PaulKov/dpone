from __future__ import annotations

from pathlib import Path

from dpone.services.docs.manifest_schema_reference import (
    MANIFEST_SCHEMA_REF_END,
    MANIFEST_SCHEMA_REF_START,
    is_manifest_schema_reference_doc_in_sync,
    render_manifest_schema_reference_block,
    sync_manifest_schema_reference_doc,
)

ROOT = Path(__file__).parents[1]
SCHEMA_ROOT = ROOT / "src" / "dpone" / "schema"


def test_generated_reference_contains_every_public_schema() -> None:
    rendered = render_manifest_schema_reference_block(SCHEMA_ROOT)

    assert MANIFEST_SCHEMA_REF_START in rendered
    assert MANIFEST_SCHEMA_REF_END in rendered
    assert "Registered schemas: **18**." in rendered
    for name in (
        "capability-discovery.schema.json",
        "etl-batch-manifest.schema.json",
        "etl-config.schema.json",
        "etl-flow-fragment-manifest.schema.json",
        "etl-flow-manifest.schema.json",
        "recipe-catalog.schema.json",
        "recipe.schema.json",
        "profile.schema.json",
        "component.schema.json",
        "pipeline-summary.schema.json",
        "selectors.schema.json",
        "test-manifest.schema.json",
        "catalog-bundle.schema.json",
        "catalog-trust-policy.schema.json",
        "catalog-bundle-verification.schema.json",
        "extension-check-receipt.schema.json",
        "extension-conformance-request.schema.json",
        "extension-conformance.schema.json",
    ):
        assert name in rendered
    assert "`dpone.test.v1`" in rendered


def test_manifest_schema_reference_sync_roundtrip(tmp_path: Path) -> None:
    doc = tmp_path / "manifest-schemas.md"
    doc.write_text(
        f"# Manifest schemas\n\n{MANIFEST_SCHEMA_REF_START}\nold\n{MANIFEST_SCHEMA_REF_END}\n",
        encoding="utf-8",
    )

    changed, _ = sync_manifest_schema_reference_doc(doc, schema_root=SCHEMA_ROOT)

    assert changed is True
    assert is_manifest_schema_reference_doc_in_sync(doc, schema_root=SCHEMA_ROOT) is True
