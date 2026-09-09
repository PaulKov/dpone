# Advanced Data Engineer CJM (Airflow)

> **Advanced path (not the beginner journey).** If you are creating your first
> Airflow pipeline, start at [First Airflow DAG](getting-started/first-airflow-dag.md)
> instead.

This journey is for a Data Engineer who already completed the beginner five
commands once and needs governed multi-pipeline / GitOps delivery. Noun map:
[Airflow pipeline glossary](getting-started/airflow-pipeline-glossary.md).

## When to use this path

Use advanced only when you need one or more of:

- selectors across many pipelines (`--select` / `--exclude`);
- GitOps workload set / domain catalog ownership beyond scaffold defaults;
- `dpone workload init` into an existing platform catalog;
- the pilot layout as a migration reference;
- authoring-mode migration or self-service evidence kits.

Do **not** start here for first preview. Preview does not require selectors.

## Prerequisites

1. Completed [First Airflow DAG](getting-started/first-airflow-dag.md) once.
2. Understand that machine identity stays `workload_id` / pack URIs even when
   user docs say **pipeline**.
3. Platform ownership of connection registries and credential runtime is clear.

## Journey

1. Confirm the beginner pipeline still checks and previews.
2. Read [Workload selectors](airflow-self-service-selectors.md) before using
   `--select` on `dpone check` / `dpone airflow preview`.
3. Use the [GitOps workload catalog](gitops-workload-catalog.md) when CI needs a
   declarative multi-workload pack plane.
4. Prefer `dpone workload init` only when shipping into an existing governed
   catalog (see [Airflow self-service hub](airflow-self-service.md#advanced)).
5. Treat the pilot layout under `examples/airflow-self-service-pilot/` as a
   reference, not day-one scaffolding.
6. For evidence / certification kits, follow
   [Airflow self-service evidence](airflow-self-service-evidence.md).

## Non-goals

- Replacing the beginner First DAG path.
- Renaming schema fields or URIs to “pipeline”.
- Claiming industrial production GO without current release evidence.

## Recovery

If preview fails on an advanced selection, run `dpone airflow explain` for the
selected id, then narrow `--select` or return to a single pipeline id.
