# Workspace promotion: audit preparation and verification

Status: **unreleased**. These commands prepare and compare a multi-project audit
mirror. They do not yet provide complete workspace compilation, production
rollout or live certification. Existing singleton commands remain unchanged.

## Who uses this

The platform promotion workflow runs these commands after accepting a complete
immutable DEV release. Analysts do not maintain a central project list, supply
trust hashes manually, or run promotion for each project separately. Project
membership comes from the release-bound source inventory, not the Git diff.

SQL stays in immutable release artifacts. The audit mirror is review material,
not an execution source; Airflow tasks fetch their release-pinned sources.

## Prerequisites

- The complete compiled v2 dbt release, including `release-set.json`, all source
  artifacts, `_dbt/dbt-source-snapshot.json` v2 and `release-subjects.sha256`.
- A Git checkout for the audit mirror. For first use, the mirror, snapshot and
  descriptor destinations must all be absent. Subsequent updates require valid
  v3 ownership metadata matching the existing layout and snapshot.
- Independently authenticated DEV release and complete campaign evidence. CI
  supplies deployment, evidence subject, producer, source commit and both campaign
  identities. This CLI does not replace signature verification.

Missing project B artifacts fail the whole preparation. Do not rebuild the release
from the current checkout or remove project B from an inventory to make it pass.

## Prepare the complete review mirror

The following environment variables must be populated by the trusted promotion
workflow from its authenticated acceptance outputs. They are not database
credentials or values guessed from the mutable mirror.

```bash
dpone dbt workspace prepare-prod-mirror \
  --root "$AUDIT_CHECKOUT" \
  --compiled-root "$COMPILED_RELEASE" \
  --mirror-root audit/dbt-workspace \
  --source-snapshot-path .dpone/dbt/source-snapshot.json \
  --descriptor-path .dpone/dbt/promotion.json \
  --expected-release-id "$ACCEPTED_RELEASE_ID" \
  --dev-deployment-id "$ACCEPTED_DEV_DEPLOYMENT_ID" \
  --dev-evidence-ref "$ACCEPTED_EVIDENCE_REF" \
  --dev-evidence-subject-sha256 "$ACCEPTED_EVIDENCE_SUBJECT_SHA256" \
  --dev-evidence-artifact-name "$ACCEPTED_EVIDENCE_ARTIFACT_NAME" \
  --dev-evidence-producer-workflow "$ACCEPTED_EVIDENCE_PRODUCER_WORKFLOW" \
  --dev-evidence-source-commit "$ACCEPTED_EVIDENCE_SOURCE_COMMIT" \
  --dev-evidence-set-id "$ACCEPTED_EVIDENCE_SET_ID" \
  --dev-evidence-campaign-request-sha256 "$ACCEPTED_CAMPAIGN_REQUEST_SHA256" \
  --format json
```

The [prepare report](schemas/dbt/dpone.dbt-workspace-mirror-prepare.v1.schema.json)
lists every project in source-inventory order and the promotion fingerprint.
Each project appears at `audit/dbt-workspace/<project_path>`. Identical repeated
preparation returns `no_op: true`; source drift inside an established bot-owned
mirror can be repaired. No command implicitly adopts an existing author-owned
directory, even when its bytes happen to match the candidate.

## Verify without changing repository files

```bash
dpone dbt workspace verify-promotion \
  --root "$AUDIT_CHECKOUT" \
  --descriptor .dpone/dbt/promotion.json \
  --release-set "$COMPILED_RELEASE/release-set.json" \
  --format json
```

The [verification report](schemas/dbt/dpone.dbt-workspace-promotion-verification.v1.schema.json)
retains every pinned project row, including failed or unavailable observations.
A missing/unreadable project has a null observed digest, never a fabricated
success. A readable but changed project reports its actual observed digest.
Unexpected aggregate files fail the overall report even if every project row
matches. Ignored/generated files within a project also fail its row.

Exit `0` means preparation succeeded or source comparison passed. Exit `2` means
validation/usage failure; internal errors use exit `5`. JSON goes to stdout.
Source-comparison failures retain the complete report; errors that prevent
loading the candidate use the existing `dpone.error.v1` envelope. Text diagnostics
go to stderr. The default root is the current directory; no database connection
or application runtime context is needed.

**Source comparison is not promotion authorization.** A consistently rehashed
descriptor can still contain an untrusted DEV identity. The platform gate must
authenticate the evidence and compare all independently expected fields. Python
callers have an explicit reviewed-comparison entrypoint:

```python
from pathlib import Path

from dpone.app.dbt_promotion_composition import (
    build_dbt_workspace_promotion_verification_service,
)
from dpone.contracts.dbt_workspace_promotion import DbtWorkspacePromotionDescriptor


def verify_authenticated_promotion(
    checkout: Path,
    compiled: Path,
    authenticated_expected: DbtWorkspacePromotionDescriptor,
) -> bool:
    report = build_dbt_workspace_promotion_verification_service().verify_reviewed(
        repository_root=checkout,
        compiled_root=compiled,
        descriptor_path=".dpone/dbt/promotion.json",
        expected_release_id=authenticated_expected.release_id,
        expected_descriptor=authenticated_expected,
    )
    return report.passed
```

The expected descriptor must come from authenticated acceptance inputs, **not**
by loading the same mutable descriptor being checked. The service does not
authenticate its caller's expected object. Cryptographic workflow integration
and environment activation remain separate mandatory rollout gates.

## Recovery and concurrent access

Preparation acquires one exclusive repository lock, recovers any prior valid
transaction journal, then checks ownership before even deciding on a no-op.
All projects are staged before active output changes. The journal has at most
three replacements: the entire mirror, snapshot and descriptor.

A verifier holds the same lock but never runs recovery. If a transaction is
pending, finish/retry the authorized installer before verifying. Do not delete
the journal or its backups. Competing readers/writers fail promptly rather than
reading a mixed release. Recovery by an installer can restore prior files before
its new ownership check; a rejected *new* installation is not a promise that
an older interrupted transaction was left unrecovered.

Rollback restores output content, not an exact snapshot of empty parent
directories. Ordinary Git file modes are accepted without chmod; symlinks,
executable audit files, overlapping destinations and reserved control paths are
rejected. If ownership metadata was manually removed or corrupted, restore the
reviewed metadata or use an explicitly authorized migration—there is no force
adoption switch.

## Validation and next steps

Offline tests cover two actual bundled SQL models, complete evidence finalization,
ownership rejection, idempotency, project removal, every install/backup crash
boundary, read/write contention, all-row reporting, malformed nested metadata,
CLI output and legacy compatibility. These are not live SQL Server or Airflow
certification. Database writes are not an all-project transaction; rolling back
code does not undo previously committed rows.

Next: [workspace source contracts](dbt-workspace-source-verification.md),
[complete feature design](feature-design-dbt-multi-project-release.md), and
[promotion/rollback operations](dbt-self-service-promotion.md).
