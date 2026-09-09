# Airflow observability correlation v1 validation

- Date: `2026-07-16`
- Branch: `codex/airflow-self-service-roadmap`
- Base commit: `a8d64e25`
- Target release: `v0.73.0`
- Specification: `docs/feature-design-airflow-observability-correlation-v1.md`
- ADR: `docs/adr/0020-airflow-observability-correlation.md`

## Result

Local implementation and non-live validation: **PASS**.

Approved Airflow/Kubernetes/OpenLineage-backend/OTel-backend live
certification: **UNVERIFIED**. No approved live environment or credentials were
used, so this report does not claim production collector delivery or pod-level
route certification.

Fresh-context subagent review: **UNVERIFIED**. The reviewer orchestrator
returned `agent thread limit reached`; stale reviewer IDs were `not_found`.
Automated gates and the integrator review are recorded below, but they are not
misrepresented as an independent review.

## Public contract

- Added strict, bounded, non-secret `dpone.airflow-correlation.v1`.
- Added an optional `correlation` section to
  `gitops.airflow_evidence_bundle`.
- Added `--airflow-evidence-bundle` to existing OpenLineage and metrics export
  commands; omitting it preserves existing behavior.
- OpenLineage uses a deterministic UUIDv5 only when correlation is present and
  preserves the dpone run ID in the dpone facet.
- OTel receives stable resource and attempt data-point attributes.
- Prometheus receives no correlation label.
- Airflow provider parse behavior and package imports are unchanged.

## Validation

| Check | Status | Evidence |
| --- | --- | --- |
| Focused correlation/schema/evidence/export/CLI tests | PASS | all selected tests passed in the final focused run |
| Corrected optional-dependency regression subset | PASS | all previously failing tests passed after `uv sync --all-extras --group dev` |
| Full non-live suite | PASS | final run: `5014 passed, 476 skipped in 245.74s` |
| Ruff | PASS | `uv run ruff check .` |
| Ruff format | PASS | `3233 files already formatted` |
| Mypy | PASS | `586 source files` |
| Import rules | PASS | no architectural import violations |
| Architecture fitness | PASS | hard cross-layer ratio test and layer gate passed |
| Module size | PASS | no new debt; existing warnings unchanged |
| Generated references | PASS | `3/3 in sync` |
| Documentation links/contracts | PASS | `506 markdown files`, `1835` local links |
| Documentation language | PASS | 4 tests passed |
| MkDocs strict build | PASS | site built successfully |
| Compatibility registry | PASS | 19 entries; generated block in sync |
| Workflow security | PASS | 0 errors, 0 warnings |
| Root wheel/sdist build | PASS | `dpone-0.72.3` artifacts built |
| Native acceleration wheel/sdist build | PASS | `dpone_native_accel-0.72.3` artifacts built |
| Airflow pack wheel/sdist build | PASS | `dpone_airflow_pack-0.72.3` artifacts built |
| Twine metadata check | PASS | all artifacts in `dist/` passed |
| Provider package and parse SLO smoke | PASS | 12 tests passed |
| Approved live Airflow/Kubernetes correlation | UNVERIFIED | environment not approved/available |
| OpenLineage/OTel backend ingestion | UNVERIFIED | local artifacts only; no network exporter by design |
| Fresh-context reviewer | UNVERIFIED | agent thread limit |

The first full non-live run failed because the local environment lacked locked
optional test dependencies (`google-cloud-*`, `pyarrow`, and the editable
native acceleration package). This was an environment setup issue, not counted
as a product PASS. After restoring all project extras, the exact failing subset
and the full suite passed.

## Security and failure semantics

- Correlation and evidence inputs are bounded and reject symlinks.
- Unknown fields, invalid digests, DAG/image mismatches, digest tampering, and
  incomplete release evidence fail closed.
- No Vault path, secret, signed URL, row value, token, password hash, baggage,
  or synthetic trace/span ID is emitted.
- OpenLineage custom facet output validates against its pinned v1 JSON Schema.
- Correlation is never added to Prometheus labels.
- Exporters do no network I/O and provider parsing imports no observability
  SDK.

## Documentation and CJM

The five-command beginner path is unchanged. Correlation is an operator/CI
capability exposed through one optional argument on existing export commands.
User, provider, compatibility, lineage, observability, schema catalog, CLI
reference, troubleshooting, ADR, and backlog documentation are synchronized.

## Remaining risk

The local implementation is ready for review and CI, but production
certification remains blocked on an approved Airflow/Kubernetes run that
produces a real pod UID/image digest and on downstream OpenLineage/OTel backend
ingestion evidence. Those checks must remain `UNVERIFIED` until executed on the
exact release commit and environment.
