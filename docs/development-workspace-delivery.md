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
or local paths into the authority receipt. The public receipt stores only
digests, bounded identifiers, limits and optional execution subjects.

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

Remote registry publication and cache materialization use the same injected
receipt boundary. `AirflowArtifactPublisher` and `AirflowArtifactMaterializer`
reject both a direct development release and a composed development parent when
the receipt is absent or differs from the embedded projection. They reapply the
workload and source-byte limits before the first registry write or local cache
installation. The public `dpone airflow publish` and `cache-materialize`
commands cannot inject a receipt and therefore fail closed for this family.

## Runtime status

Delivery is not execution permission. The standard dpone runtime deliberately
rejects development releases in this increment, even when the delivery receipt
contains execution subjects. This prevents artifact delivery from becoming an
implicit credential or writer grant.

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
| Development release rejected as production | The authority families were kept separate | Select the development composition profile; do not rewrite the release schema |
| Runtime says current external authority is required | Delivery succeeded, but the protected entrypoint did not receive a current verified receipt | Configure the platform verifier adapter and retry the same immutable release |
| Runtime says workload execution is not authorized | The exact runtime or pre-hook subject is absent | Add only the required subject through the external grant process and rebuild the release |
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
