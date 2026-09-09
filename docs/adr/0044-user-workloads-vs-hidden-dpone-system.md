# ADR 0044: User workloads vs committed `.dpone` system vs generated gitops

## Status

Accepted.

## Context

Domain-first authoring already places pipelines under
`workloads/<domain>/pipelines/**`, but multi-task Airflow DAG wiring still lived
in `dpone_workloads/gitops/domains/**`. That dual source of truth forced domain
engineers into a platform-owned tree and contradicted the self-service CJM.

Generated packs and dag-specs already use gitignored `.dpone/gitops/`. Platform
loader Python and docker/config must remain committed so Airflow git-sync and CI
can load them, but they must not look like user authoring.

## Decision

1. **User SoT** is only `workloads/<domain>/**`:
   - `ownership.yaml`
   - `pipelines/<pipeline_id>/`
   - `dags/<dag_id>.yaml` (`dpone.domain-dag.v1`)
   - optional `airflow/native/` for handwritten DAGs
2. **Committed system SoT** is under `.dpone/{platform,docker,config,registry}/`.
   Dot-prefix means “not for beginners”, not “gitignore everything”.
3. **Generated artifacts** remain under gitignored `.dpone/gitops/` (and optional
   `.dpone/generated/`).
4. **`dpone_workloads/` is removed** after migration. During dual-read, colocated
   DAG fingerprints win when equal; unequal fingerprints are blockers.
5. Discovery and reconcile consume a unified IR; they do not treat domain
   catalogs as the long-term authoring surface.

## Consequences

- Beginner/docs CJM never sends users into `.dpone/` or `dpone_workloads/`.
- CODEOWNERS can separate `workloads/<domain>/` from `.dpone/**`.
- Airflow loader path remaps to `.dpone/platform/airflow_dags/`.
- ADR 0031 remains valid for pipeline discovery; this ADR adds colocated DAG
  authoring and the three-way `.dpone` split.
- Compatibility requires a dual-read window before fail-closed removal of legacy
  catalogs.

## Links

- Feature design: `docs/feature-design-domain-colocated-dags-hidden-system-v1.md`
- Related: [ADR 0031](0031-domain-first-self-service-selection.md),
  [ADR 0008](0008-airflow-parse-safe-provider.md),
  [ADR 0009](0009-artifact-delivery-and-cache-materializer.md)
