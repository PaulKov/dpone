# Development workspace delivery

Use development workspace delivery when one isolated Airflow deployment must
contain a complete native dbt workspace and ordinary flow or batch workloads.
The release can deliver and parse every artifact without granting every
workload permission to execute.

This contract is for platform adapters and advanced development environments.
Production publishing continues to require production route certification.

## Prerequisites

- an isolated nonproduction target and control plane;
- an exact source commit and a digest that identifies the source repository
  without exposing its URL;
- a platform verifier that authenticates the original policy and grant bytes;
- current validity and revocation state;
- explicit workload and source-byte limits;
- dbt projects that pass `dpone dbt workspace check`;
- ordinary workload packs whose connection projection contains aliases only.

Do not serialize repository URLs, credentials, connection values, private SQL,
or local paths into the release. The release embeds only a scope projection:
digests, bounded identifiers, limits and optional execution subjects. The full
verified receipt and protected target policy remain outside the artifact.

## Compile a development workspace

The verifier adapter must create `DevelopmentAuthorityReceipt` only after it
has authenticated the original bytes. Inject that receipt into the application
root:

```python
from pathlib import Path

from dpone.app.dbt_workspace_composition import build_dbt_workspace_service


def compile_workspace(*, workspace: Path, output: Path, verified_receipt) -> None:
    service = build_dbt_workspace_service(
        environment={},
        development_authority=verified_receipt,
    )
    report = service.compile(workspace, output_dir=output)
    if not report.passed:
        raise RuntimeError("development workspace compilation failed")
```

`verified_receipt` is a `DevelopmentAuthorityReceipt` returned by the platform's
signature and policy verifier. dpone deliberately does not accept an arbitrary
JSON authority file as a trusted CLI input.

The output contains:

```text
release-set.json
release-subjects.sha256
_dbt/dbt-source-snapshot.json
dags/
packs/
runtime/
schemas/
```

`release-set.json` uses `dpone.dbt-release-set.development.v1`. Runtime payloads
retain `dpone.dbt-airflow-self-service.v2`, so the artifact transport stays
compatible while authority remains distinct.

## Materialize and compose

Materialize the complete workspace through the authenticated Python boundary.
The public CLI has no verifier injection and therefore rejects this release
family. The materializer adds profile `development_workspace_delivery_v1`:

```python
from dpone.readiness.airflow_compact_pack_release import materialize_compact_pack_release

materialized = materialize_compact_pack_release(
    pack_root=compiled_root,
    cache_root=cache_root,
    xcom_sidecar_image=xcom_sidecar_image,
    development_authority=verified_receipt,
)
```

The Python composition request must use the same profile when ordinary
workloads are added:

```python
from dpone.contracts.development_delivery_authority import (
    DEVELOPMENT_COMPOSITION_PROFILE,
)
from dpone.contracts.release_composition import ReleaseCompositionRequest
from dpone.app.release_composition import build_release_composition_service

request = ReleaseCompositionRequest(
    native_root=native_release_root,
    expected_release_id=native_release_id,
    standalone_root=ordinary_source_root,
    expected_inventory_sha256=ordinary_inventory_sha256,
    output_dir=output_root,
    xcom_sidecar_image=xcom_sidecar_image,
    profile=DEVELOPMENT_COMPOSITION_PROFILE,
)
report = build_release_composition_service(
    development_authority=verified_receipt,
).compose(request)
```

The parent remains `dpone.release-set.v3`. Its promotion profile and native
child schema identify development authority; no constituent is relabeled as a
production release.

## Trusted target admission

Remote publication, installation and cache activation require separate
`DevelopmentTargetAdmission` values returned by the platform verifier. Each is
bound to one operation (`publish`, `materialize`, or `activate`), exact release
and deployment IDs, the protected target-policy digest, current time and
revocation epoch, exact target environment, and `non_production` trust tier.

```python
from dpone.runtime.airflow_artifact_delivery import (
    AirflowArtifactMaterializer,
    AirflowArtifactPublisher,
)
from dpone.runtime.deployment_cache import DeploymentCacheMaterializer

# The platform verifier authenticates original policy/grant/signature bytes.
# The current-target verifier reopens protected policy and revocation state at
# every operation boundary; it is a platform adapter, not bundle metadata.
AirflowArtifactPublisher(
    registry=registry,
    development_admission=verified_publish_admission,
    development_admission_verifier=current_target_verifier,
).publish(publish_request)

AirflowArtifactMaterializer(
    registry=registry,
    development_admission=verified_materialize_admission,
    development_admission_verifier=current_target_verifier,
).materialize(materialize_request)

DeploymentCacheMaterializer(
    cache_root,
    workspace_activation=physical_target_coordinator,
    development_admission=verified_activate_admission,
    development_admission_verifier=current_target_verifier,
).promote(deployment_dir, environment="development")
```

The embedded projection, deployment `environment`, database names, a CLI
`--environment` value, and copied or resealed files are not target authority.
The integrity-checked deployment must agree with the independently verified
target. A missing, unknown, expired, revoked, mismatched, or production-tier
target fails before registry writes, local installation, or current-pointer
mutation. Operation receipts are not interchangeable. If target state changes
after activation preparation starts, the failure can leave a sealed inactive
snapshot or `PREPARED` coordinator reservation; the error then reports
`state_may_have_changed: true` and `recovery_required: true`, while `current`
remains unchanged.
The verifier must compare the receipt with the current protected target-policy
digest and revocation epoch on every publish, materialize, promote, pointer
recovery, and audit-repair attempt. Reusing a previously valid receipt does not
bypass a later revocation or policy replacement.

Promotion and pointer recovery revalidate the complete sealed activation,
including every declared artifact size and digest, after coordinator
preparation and immediately before pointer commit. Audit-only repair repeats
that full validation after active-occurrence readback and before appending the
audit record. A protected-verifier availability failure is reported through
`DPONE_DEVELOPMENT_ACTIVATION_AUTHORITY_REQUIRED`; if preparation has already
started, the error also carries the recovery flags described above.

`AirflowArtifactPublisher` and `AirflowArtifactMaterializer` reapply the
workload and source-byte limits before the first registry write or local cache
installation. The public `dpone airflow publish` and `cache-materialize`
commands cannot inject target admission and therefore fail closed for this
family.

## Runtime status

Deployment activation is not workload execution permission. Cache activation
can select a DEV-only deployment only after exact target admission and physical
workspace coordination. The standard dpone workload runtime still deliberately
rejects development releases in this increment, even when the build authority
contains execution subjects. This prevents delivery or pointer activation from
becoming an implicit credential or writer grant.

A later runtime feature must inject a current externally verified receipt before
init-fetch, credential resolution or source access and bind it to the exact
workload and command kind. Until that protected entrypoint exists, the delivered
workspace is intentionally dormant and cannot be described as runnable.

## Ordinary flow constraints

Development composition accepts selector-scoped flow and batch processes,
public Airflow resources, alias-only connection projection, declarative SQL
dependencies, and separately scheduled SQL pre-hooks. It rejects:

- credential material in connection projection;
- arbitrary commands or custom runners;
- separately scheduled post-hooks;
- unbound dependencies and cross-constituent ownership;
- duplicate selectors, workload IDs, hook IDs, or logical writes.

The verifier rebuilds each selected process from the captured source archive
and compares executable semantics and fingerprints before publication.

## Diagnose and recover

| Symptom | Meaning | Recovery |
| --- | --- | --- |
| `DPONE_DEVELOPMENT_AUTHORITY_INVALID` | Receipt family, digest, scope, limit, time, or revocation check failed | Reverify original policy and grant bytes; issue a new bounded receipt when appropriate |
| `DPONE_DEVELOPMENT_AUTHORITY_REQUIRED` | Publish or materialize lacks an exact current operation receipt, or artifact and target differ | Reopen trusted target configuration and request a receipt for the exact release, deployment and operation; do not retry with a renamed environment |
| `DPONE_DEVELOPMENT_ACTIVATION_AUTHORITY_REQUIRED` | Cache promotion or recovery lacks exact current DEV target admission | Leave `current` unchanged; reverify policy, revocation and the non-production target before retrying the same immutable deployment |
| `DPONE_DEVELOPMENT_TARGET_FORBIDDEN` | A DEV-only release was projected for a production-tier target | Rebuild from reviewed source through the production compiler with current production route evidence |
| Development release rejected as production | The authority families were kept separate | Select the development composition profile; do not rewrite the release schema |
| Runtime rejects the development release | This delivery-only increment has no protected development runtime entrypoint | Keep the immutable release dormant; execution requires a separately approved runtime feature |
| Ordinary source verification fails | Captured bytes and rebuilt pack differ | Regenerate the source capture from the exact commit; do not edit generated artifacts |

An identical retry reuses the immutable release. Any source, projection,
command, limit, or authority change creates a new identity. If publication
durability is uncertain, inspect the existing content-addressed directory and
retry identical inputs; never overwrite it.

## Validation

Run the focused public checks without credentials:

```bash
uv run pytest -q \
  tests/test_development_delivery_authority.py \
  tests/test_development_target_admission.py \
  tests/test_development_remote_delivery_authority.py \
  tests/test_dbt_workspace_cache_activation_saga.py \
  tests/test_dbt_workspace_release_assembly.py \
  tests/test_dbt_compact_wire_v2.py \
  tests/test_release_composition_delivery.py \
  tests/test_release_composition_ordinary.py
```

These checks prove deterministic assembly, authority separation, immutable
materialization, mixed composition, selector closure, and fail-closed runtime
admission with synthetic fixtures. They do not certify a live data route.

Continue with [compact workspace delivery](dbt-compact-delivery.md), the
[dbt operations runbook](dbt-self-service-runbook.md), and
[ADR 0066](adr/0066-development-workspace-batch-federation.md).
