# ADR 0008: Airflow parse-safe provider

## Status

Accepted.

> Amended by [ADR 0024](0024-airflow-executable-init-fetch-wire-boundary.md).
> The bounded loader accepts v1 only for `local_preview`; strict executable
> `init_fetch` requires `dpone.airflow-deployment-index.v2`.
>
> Amended for the exact-cache Kubernetes topology: the canonical parser cache
> root is `/opt/airflow/.dpone-cache`, it is mounted read-only, and loader ACK
> evidence is written to a separate confined volume.

## Context

Airflow DAG parsing must be deterministic and fast. It must not parse dpone manifests, call external systems, read secrets, or refresh caches.

## Decision

The canonical Airflow import is `airflow.providers.dpone`. The recommended loader is:

```python
from airflow.providers.dpone import load_and_acknowledge_dpone_dags

loaded = load_and_acknowledge_dpone_dags(
    globals(),
    index_path="/opt/airflow/.dpone-cache/current/airflow-index.json",
    ack_path="/opt/airflow/.dpone-ack/loader-ack.json",
    ack_root="/opt/airflow/.dpone-ack",
)
if loaded.report.fatal:
    error_code = (
        loaded.report.errors[0].get("code", "DPONE_AIRFLOW_INDEX_INVALID")
        if loaded.report.errors
        else "DPONE_AIRFLOW_INDEX_INVALID"
    )
    raise RuntimeError(
        f"{error_code}: dpone Airflow deployment index could not be loaded"
    )
```

The loader reads one supported `airflow-index.json`: v1 for non-runnable
`local_preview`, or v2 for strict executable `init_fetch`. It resolves only
`cache://` artifacts inside the configured cache root, validates sizes and
checksums, and returns `LoadReport`.
The same bounded resolution surface is available through `CacheResolver` for
escape-hatch code that needs to resolve logical `cached://dags/<dag_id>` /
`cached://workloads/<workload_id>` refs or the strict pinned
`cached://deployments/<deployment_id>/dags/<dag_id>` /
`cached://deployments/<deployment_id>/workloads/<workload_id>` refs without
scanning the repository or touching external systems. Query-form
`release`/`deployment` pins remain a compatibility path, but public examples use
the deployment-scoped form.

The combined loader holds one shared cache lease through parse and ACK. The
cache remains immutable to the parser; only the separate acknowledgement root
is writable. Omitting `ack_root` keeps the historical cache-local `status/`
API compatible, but that topology is not recommended for Kubernetes.

`dpone init project --airflow` also owns the migration path for generated
`dags/dpone.py` files. It upgrades only an exact SHA-256 fingerprint from the
allowlist of historical dpone-generated loaders. The replacement uses a
compare-and-swap guard against the fingerprint observed during planning.
Canonical bytes are a no-op on later runs. Unknown bytes, including comments or
whitespace edits to an otherwise generated loader, remain a conflict and are
never overwritten. Fresh scaffolds and successful upgrades both use the same
canonical template.

## Consequences

- Airflow parse path is bounded.
- Verified cache bytes are read-only to the parser, while ACK evidence has a
  separate, confined write boundary.
- A missing, malformed, or contract-invalid deployment index is visible as an
  Airflow DAG import error instead of a successful parse with zero dpone DAGs.
- Generated-loader migration is deterministic and idempotent; maintainers rerun
  `dpone init project --airflow` instead of hand-editing the loader.
- User-owned loader variants require manual review and remain byte-for-byte
  unchanged.
- Existing `dpone_airflow_pack` provider facade APIs remain as compatibility
  exports and emit one `DeprecationWarning` per process when accessed from the
  legacy root namespace.
- Invalid DAG specs are isolated according to provider policy.
