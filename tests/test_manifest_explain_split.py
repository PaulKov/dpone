from __future__ import annotations

from pathlib import Path

from dpone.manifest.explain import compute_deep_patch, explain_manifest, explain_why


def test_explain_manifest_and_why_work_for_batch_manifest(tmp_path: Path) -> None:
    manifest = tmp_path / "root.batch.yaml"
    manifest.write_text(
        """
kind: dpone.batch.v1

vars:
  layer: landing
  src_system: monolite
  src_database: db1

defaults:
  source:
    type: postgres
    connection_id: pg_conn
  sink:
    type: bigquery
    connection_id: bq_conn
    strategy:
      mode: full_refresh

naming:
  sink_dataset: "{{ layer }}__{{ src_system }}__{{ src_database }}"
  sink_table: "{{ src_schema }}__{{ src_table }}"
  process_name: "{{ src_schema }}__{{ src_table }}"

schemas:
  public:
    tables:
      - table: t1
      - table: t2
        depends_on:
          - "#public.t1"
""".lstrip(),
        encoding="utf-8",
    )

    res = explain_manifest(manifest, selector="public.t2", include_snapshots=True)

    assert res.process_name == "public__t2"
    assert res.vars["src_system"] == "monolite"
    assert res.final_config["sink"]["table"]["schema"] == "landing__monolite__db1"
    assert res.final_config["sink"]["table"]["name"] == "public__t2"
    assert res.config_origin["depends_on[0]"] == "table.depends_on:public.t2"

    why = explain_why(res, "sink.table.schema")
    assert why.exists is True
    assert why.final_value == "landing__monolite__db1"
    assert why.naming is not None
    assert why.naming["naming_key"] == "sink_dataset"
    assert why.timeline is not None


def test_compute_deep_patch_detects_append_and_replace() -> None:
    patch, removed = compute_deep_patch(
        {"depends_on": [{"path": "a.yaml"}], "sink": {"strategy": {"mode": "full_refresh"}}},
        {
            "depends_on": [{"path": "a.yaml"}, {"path": "b.yaml"}],
            "sink": {"strategy": {"mode": "incremental_merge"}},
        },
    )

    assert removed == []
    assert patch["depends_on"] == [{"path": "b.yaml"}]
    assert patch["sink"]["strategy"]["mode"] == "incremental_merge"
