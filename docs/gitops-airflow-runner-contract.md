## Compact Airflow runner contract

Environment and domain defaults configure Kubernetes placement and runner-local
assets through a single `airflow.runner` block. This mirrors how mature
platforms split concerns:

| Platform | Placement | Runner-local files | Secret/path rewrite |
| --- | --- | --- | --- |
| **Airflow Helm chart** | `tolerations` / `nodeSelector` in values per env | git-sync volume on workers | Connections in K8s Secret / Vault |
| **Argo Workflows** | `podSpecPatch` / affinity per template | `artifacts` / init containers | Parameter + volume mounts |
| **Airbyte** | worker node pools / tolerations | connector-specific mounts in job pod | secret hydration at runtime |
| **dpone compact pack** | `runner.placement` → `pod_spec` | `runner.embed_assets` → inline tarball | `bind` + `workspace_query_overrides` → `query_overrides` |

KPO pods created by compact packs **do not** mount git-synced DAG folders. Any TLS
root, JDBC driver, or license file referenced by a connection URI must therefore
be declared as a runner embed asset.

```yaml
environments:
  prod:
    namespace: airflow-example
    airflow:
      runner:
        workspace_root: /workspace/repo
        placement:
          node_selector:
            dedicated: datawarehouse
          tolerations:
            - key: dedicated
              operator: Equal
              value: datawarehouse
              effect: NoSchedule
            - key: dedicated
              operator: Equal
              value: datawarehouse
              effect: NoExecute
        embed_assets:
          - path: certs/RootCA.pem
            bind:
              connection_id: clickhouse_example
              query_key: ca_cert
      connection_projection:
        mode: unsafe_airflow_env
        connection_ids:
          - clickhouse_example
          - mssql_example
```

`dpone gitops airflow reconcile` applies the contract to every compact pack:

- `runner.placement` is written into `pod_spec.spec.nodeSelector` and
  `pod_spec.spec.tolerations`.
- `runner.embed_assets` / legacy `runner.embed_paths` are bundled into the inline
  workload tarball at `/workspace/repo/...`.
- `bind` and legacy `workspace_query_overrides` expand repo-relative paths into
  absolute runner paths in `connection_projection.query_overrides`.
- For `connection_projection.mode: kubernetes_secret_volume`, the compiler
  closes the operator projection over the selected workload. It keeps only
  refs used by compiled `source`, `sink`, `state`, `bigquery_proxy`, top-level
  runtime object-storage authorities, and the active
  `source.options.native_transfer.snapshot.columnar_fast_path.object_storage.runtime_access`
  authority (or its `source.options.columnar_fast_path` compatibility fallback).
  The nested runtime alias follows runtime precedence: `connection_id`, then
  `connection_ref`. Current `clickhouse_read_access` connection and presigned
  modes render ClickHouse probe inputs but do not resolve a second Python-side
  credential, so they do not add a projection dependency. An unrelated
  connection declared in the same domain is not read when this workload runs.
  This closure claim does not extend to the legacy `unsafe_airflow_env` mode.
- Selection tries exact logical `connection_ref`, resolved registry ref, then
  physical Airflow `connection_id`. The first matching tier must identify one
  entry; a shared physical-only match fails with
  `DPONE_AIRFLOW_CONNECTION_PROJECTION_REF_AMBIGUOUS` instead of selecting
  another workload's entry. URI overrides may be keyed by any of those aliases.
  Packs normalize relevant overrides to the physical ID consumed by the
  operator. Several logical refs may share that ID; their entries and distinct
  runtime mount paths remain intact. Conflicting alias-specific overrides for
  one shared physical credential fail at pack build.
- A compiled runtime ref with no matching projection entry fails pack build
  with `DPONE_AIRFLOW_CONNECTION_PROJECTION_REF_MISSING`; reconcile never emits
  a partially usable pack.
- Closure runs only when process compilation produced an exact dependency set.
  An exact empty set emits no operator projection (`{}`). Unmaterialized packs
  and the legacy process-plan compatibility fallback preserve their unresolved
  projection for diagnostics instead of misclassifying it as connection-free.
- Missing embed assets fail reconcile with `runner_embed_asset_missing` instead
  of a silent runtime TLS failure inside the pod.

Legacy flat keys (`airflow.node_selector`, `airflow.tolerations`,
`airflow.runner_embed_paths`) remain supported for existing workload sets.
