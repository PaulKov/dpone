# GitOps workload catalog

All workload and environment coordinates below are synthetic examples.

> **Advanced path (not the beginner journey).** If you are creating your first
> Airflow pipeline, start at [First Airflow DAG](getting-started/first-airflow-dag.md)
> instead.

`dpone gitops workloads` is the high-level GitOps facade for large Airflow
repositories. It lets teams describe many workloads declaratively, resolve
environment/domain/source overrides, and generate one compact Airflow pack per
workload.

Use it when `.dpone/gitops` and CI started accumulating repeated generated
files or long chains of low-level `dpone gitops airflow ...` commands. See the
[advanced Data Engineer CJM](airflow-self-service-advanced-cjm.md) for when this
belongs in your journey.

## Workload set

```yaml
gitops:
  version: 1

  defaults:
    runner: airflow
    resources_profile: safe_worker
    outcome_mode: xcom_then_gate

  environments:
    dev:
      namespace: airflow-example-dev
      runner_policy: advisory
    prod:
      namespace: airflow-example-prod
      runner_policy: certified_only

  source_types:
    mssql:
      resources_profile: throughput

  sources:
    mssql_analytics_reporting:
      source_type: mssql
      snapshot_profile: weak_worker_chunks

  includes:
    - path: gitops/domains/**/*.yaml
    - path: gitops/environments/**/*.yaml
    - manifest_glob: manifests/**/*.yaml
      infer_workload: true
```

Catalog files are included by the root workload set. Directory names are only
organization; semantics come from typed fields such as `domain`, `environment`,
`defaults`, and `workloads`.

```yaml
domain: interchange
defaults:
  owner: data-office
  labels: [interchange]

workloads:
  inter_ch_example_customer_directory:
    manifest: ../../manifests/mssql/interchange/example_customer_directory.yaml
    source: mssql_analytics_reporting
    schedule: "0 6 * * *"
    resources_profile: safe_worker
```

## Override precedence

Effective config is resolved in this order:

```text
global < environment < runner < source_type < source < sink_type < sink < domain < workload < manifest-local < CLI
```

Every effective value carries provenance, so `workloads explain` can show where
the value came from.

## Compact Airflow runner contract

See [gitops-airflow-runner-contract.md](gitops-airflow-runner-contract.md) for the
full industrial contract (`airflow.runner`: placement, embed assets, connection
binds, reconcile-time validation).

```yaml
environments:
  prod:
    namespace: airflow-example
    airflow:
      runner:
        placement:
          node_selector:
            dedicated: datawarehouse
          tolerations:
            - key: dedicated
              operator: Equal
              value: datawarehouse
              effect: NoSchedule
        embed_assets:
          - path: certs/RootCA.pem
            bind:
              connection_id: clickhouse_example
              query_key: ca_cert
      connection_projection:
        mode: unsafe_airflow_env
        connection_ids:
          - clickhouse_example
```

`dpone gitops airflow reconcile` writes placement into `pod_spec`, embeds assets
into the inline archive, and expands `bind` / `workspace_query_overrides` into
`connection_projection.query_overrides`.

## Commands

```bash
dpone gitops workloads list \
  --workload-set dpone_workloads/gitops.yaml \
  --env dev

dpone gitops workloads explain inter_ch_example_customer_directory \
  --workload-set dpone_workloads/gitops.yaml \
  --env dev

dpone gitops airflow pack \
  --workload inter_ch_example_customer_directory \
  --workload-set dpone_workloads/gitops.yaml \
  --env dev \
  --output-path .dpone/gitops/airflow/inter_ch_example_customer_directory/airflow-pack.json

dpone gitops airflow reconcile \
  --workload-set dpone_workloads/gitops.yaml \
  --changed-files-file changed.txt \
  --env dev \
  --output-dir .dpone/gitops

dpone gitops airflow reconcile \
  --workload-set dpone_workloads/gitops.yaml \
  --all-workloads \
  --env dev \
  --output-dir .dpone/parse-fixture/gitops

dpone gitops gitlab render-child-pipeline \
  --workload-set dpone_workloads/gitops.yaml \
  --changed-files-file changed.txt \
  --env dev \
  --output .dpone/gitops/child.yml
```

`airflow pack` writes one self-contained `airflow-pack.json` with workload
identity, effective config, runtime command, pod spec, optional connection
projection policy, XCom sidecar pinning, outcome gate, runtime evidence paths,
and pack fingerprint. Airflow DAG files should use the thin pack facade instead
of reading many generated files such as `kpo-kwargs.json`, `pod-spec.yaml` or
`run-spec.json`.

```python
from dpone_airflow_pack import build_dpone_gitops_task_group_from_pack

build_dpone_gitops_task_group_from_pack(
    "/opt/airflow/dags/.dpone/gitops/airflow/inter_ch_example_customer_directory/airflow-pack.json"
)
```

## CI pattern

For small repositories, one CI job can run `dpone gitops airflow reconcile`.
For large repositories, use `dpone gitops gitlab render-child-pipeline` to
generate one child job per affected workload. This keeps `.gitlab-ci.yml`
short while preserving parallel validation.

Use changed-file selection for merge-request feedback. Use the explicit
`--all-workloads` mode only for deployment-shaped fixtures and immutable
release builds that must contain the complete catalog. It cannot be combined
with `--changed-files` or `--changed-files-file`. Both compact packs and DAG
specs are written below the selected `--output-dir`; `selection_mode` in the
JSON evidence records which algorithm was used.

For the exact output tree, evidence fields, exit codes, path safety, retry and
cleanup procedure, see [Airflow reconcile and recovery](gitops-airflow-reconcile.md).

The low-level commands such as `run-spec`, `runtime-profile`, `pod-contract`,
`artifact-index`, and `preflight` remain available for debugging and
compatibility. New repositories should prefer the workload catalog facade.

## Guardrails

- All evidence paths are repo-relative.
- Secret values must not appear in workload catalogs or generated packs.
- Production policy can be tightened by environment defaults; weakening it
  should be explicit and reviewable.
- Airflow scheduler parse should read static pack JSON only and must not run
  GitOps rendering at import time.
