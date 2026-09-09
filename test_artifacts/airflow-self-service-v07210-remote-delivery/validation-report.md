# v0.72.10 Airflow remote delivery validation

Date: 2026-07-16

Scope: immutable release/deployment publication, bounded pinned
materialization, local descriptor-anchored registry proof, parse-safe provider,
guarded activation, and public CLI/schema/documentation contracts.

## Deterministic proof

- `publish-created.json`: PASS. The fresh-wheel publisher created all 10
  release/deployment objects with completion markers last.
- `publish-no-op.json`: PASS. An identical retry compared all 10 objects and
  created none.
- `materialize.json`: PASS. Nine declared objects were downloaded and verified;
  immutable release/deployment trees were created and `activated` remained
  `false`.
- `wheel-provider-smoke.json`: PASS. A guarded `cache-sync` activated the exact
  deployment, then Python 3.12.12 with Airflow 3.2.0 loaded exactly
  `orders_daily` through `custom-cache/current/airflow-index.json` with no
  parse errors. Provider discovery succeeded and the activated custom cache
  root resolved release artifacts correctly.
- `package-sha256.txt`: SHA-256 inventory for all root, provider, and native
  wheel/sdist artifacts used by the smoke. The root wheel was installed with
  the Azure extra, including `azure-identity` and `azure-storage-blob`.

## Validation gates

| Gate | Status | Result |
| --- | --- | --- |
| Focused remote delivery, registry, cache materializer | PASS | 166 tests |
| Provider index/loader and parse SLO | PASS | 55 tests |
| Self-service CLI, schema, lazy imports | PASS | 104 tests |
| Real Airflow loader and parse SLO | PASS | 7 tests |
| Full non-live suite | PASS | 4526 passed, 472 skipped |
| Ruff lint and format | PASS | repository-wide |
| mypy | PASS | 506 source files |
| Import rules | PASS | no violations |
| Layer metrics | PASS | cross-layer ratio 0.300 |
| Module-size budget | PASS | no new warnings; 10 pre-existing warnings |
| Architecture fitness | PASS | average clustering 0.179 |
| Workflow security | PASS | 0 errors, 0 warnings |
| Generated references and compatibility | PASS | in sync |
| Documentation and strict MkDocs build | PASS | 424 files, 1752 local links |
| Root/provider/native builds and Twine | PASS | six distributions |
| Fresh-wheel base-help parse safety | PASS | no Airflow/Vault/cloud SDK imports |
| Fresh-wheel provider discovery and pinned load | PASS | Airflow 3.2.0 |

## Security regressions

- Concurrent replacement of the local registry root cannot redirect a
  conditional create outside the descriptor-pinned tree or split objects and
  completion markers between operations.
- Every local registry root component is opened without following symlinks;
  symlinked ancestors and noncanonical/empty remote roots fail closed.
- Download limits are enforced while bytes are written, and an oversize partial
  destination is removed.
- Secret-shaped logical connection IDs, including modern `sk-proj` forms, fail
  as input errors without echo. Vault failures also redact connection inputs
  and backend exception text at the provider boundary.
- Empty and schema-only releases fail before publication.
- Azure workload identity uses the account-scoped URI and lazy
  `WorkloadIdentityCredential`; accountless Azure input fails before SDK I/O.
- `cache-materialize` exposes no Airflow/Vault logical resolver options.
- Private materializer staging paths are not copied into public structured
  error details.

## Live certification

- S3 conditional-create/materialization: **UNVERIFIED**. No approved live
  credentials/environment were available.
- GCS conditional-create/materialization: **UNVERIFIED**. No approved live
  credentials/environment were available.
- Azure Blob conditional-create/materialization: **UNVERIFIED**. No approved
  live credentials/environment were available.
- Airflow/Kubernetes init-fetch/cache-sync/KPO lifecycle: **UNVERIFIED**. No
  approved cluster was available.

These statuses are not treated as passes. Two independent final reviews found
no remaining findings after the security and provider-path fixes. The change
is ready for code review and CI, but not for a production-certification claim
until the corresponding live evidence is produced from the exact reviewed
commit.
