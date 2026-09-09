# Airflow pipeline glossary

Short noun map for the Airflow self-service path. Start with
[First Airflow DAG](first-airflow-dag.md). Use the
[advanced Data Engineer CJM](../airflow-self-service-advanced-cjm.md) only when
you need GitOps multi-pipeline governance.

Day-one framing: pick a **route** (source → sink), scaffold it with a **recipe**,
edit a **pipeline**. See [source → sink matrix](../source-sink-matrix.md).

## route

The day-1 intent noun: **source + sink + load strategy** (for example MSSQL →
ClickHouse with incremental merge). A route is what you choose from the
[source → sink matrix](../source-sink-matrix.md). It is not a file you edit.
Ops and certification may later attest the same route identity — preview and
safe sample alone do **not** certify a route.

## recipe

A platform or built-in template (recipe ref / slug) that scaffolds a chosen
**route** into an editable **pipeline**. Built-in refs encode source-to-sink,
for example `mssql-to-clickhouse-incremental`. See
[Recipes, profiles, and components](../airflow-recipes.md) and
[First Airflow DAG](first-airflow-dag.md).

## connection_ref

A logical endpoint alias used in pipeline authoring (source/sink credentials).
Resolved at workload start; it is **not** the route itself. See
[Connections and credentials](../connections.md).

## pipeline

The user-facing unit you create and edit. For beginners this is usually
`pipelines/<id>/pipeline.yaml` from `dpone init pipeline`. You check, preview,
and sample a **pipeline**.

## workload

The GitOps / delivery identity behind a pipeline: catalog entry, pack pin,
selectors, and credential scope. Machine fields keep names such as
`workload_id`, `workloads:` in domain YAML, and `cached://workloads/...`.

On the beginner 1:1:1 path, one pipeline maps to one catalog workload and one
default DAG id. You do not need to author a separate “workload” object on day
one. Scaffolded `domains/<domain>.yaml` registers your pipeline under the
catalog key `workloads:` — commit it, do not edit it as a beginner.

## DAG

The Airflow graph the provider loads from generated artifacts (via
`dags/dpone.py`). Beginners preview a DAG; they do not write Airflow Python.

## pack

A compact, pinned delivery artifact that Airflow consumes. Preview materializes
a local pack so the provider can validate the release/index contract. Packs
are build outputs — not hand-edited sources.

## Related enums and scopes

| Term | Where you see it | Meaning for beginners |
| --- | --- | --- |
| `resolution_scope: workload_start` | credential runtime | Resolve credentials once per workload attempt |
| `cached://workloads/...` | provider / URI examples | Immutable pack reference (do not invent paths) |
| Workload selectors | advanced path only | Filter many pipelines in CLI/build; not day one |
