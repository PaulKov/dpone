# Declarative Airflow pilot layout (advanced)

> **Advanced path (not the beginner journey).** If you are creating your first
> Airflow pipeline, start at
> [First Airflow DAG](../../docs/getting-started/first-airflow-dag.md)
> instead.

This directory is an **advanced GitOps reference**, not the beginner five-command
path. Follow the
[advanced Data Engineer CJM](../../docs/airflow-self-service-advanced-cjm.md)
after First Airflow DAG. Hub context:
[Airflow self-service](../../docs/airflow-self-service.md).

The pilot mirrors a domain-style migration: a short `dags:` block in GitOps, one
loader file, and no hand-written per-pipeline DAG module.

## Layout

```text
examples/airflow-self-service-pilot/
  dags/dpone_dags.py
  dpone_workloads/gitops/gitops.yaml
  dpone_workloads/gitops/domains/marketing.yaml
  workloads/marketing/dpone/manifests/sample_web_sync.yaml
  workloads/marketing/dpone/sql/sample_web_sync.sql
```

## Usage

1. Copy the tree into your Airflow DAG repository.
2. Replace the placeholder SQL with a real extract before any production deploy.
3. Point CI at `dpone_workloads/gitops/gitops.yaml` for pack + dag-spec builds.
4. Delete legacy `DAG__marketing__*.py` modules once the declarative path is verified.
5. Ensure the loader receives an explicit `index_path` for the deployment index in
   production (the example uses the local helper form for illustration).

## Operator materials

English guidance lives in this README and the linked docs above. Localized
operator checklists remain available as optional Russian companions
(`RUNBOOK.ru.md`, `MR_TEMPLATE.ru.md`, `DATAOPS_CHECKLIST.ru.md`) and are not
required for the public beginner journey.
