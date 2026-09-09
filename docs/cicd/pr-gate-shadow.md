# PR Gate shadow

`PR Gate shadow` is a diagnostic CI context under issue #512. It is not a
required check and cannot authorize a merge: the required contexts resolved
from the canonical active policy remain the sole authority.

## Audience, prerequisites, and non-goals

This page is for a pull-request contributor, CI operator, or security reviewer
who needs to inspect diagnostic evidence. Contributors need no configuration or
credential: GitHub creates the producer attempt from a pull request. The
default-branch auditor uses its read-only Actions token; users must never add a
personal token, rerun it from a local checkout, or treat a receipt as branch
protection. Creating a required context, changing protection, publishing a
release, or executing pull-request bytes in the auditor is out of scope.

The producer derives a closed plan from an exact PR identity and changed paths.
The version-one route policy is closed and requires `default_route: "full"`.
Unknown input, an identity error, cancellation, or a missing provider job is
reported as `UNVERIFIED`; it never means that work can be skipped. Each run
attempt writes a create-only JSON claim. Claims are producer-controlled and
remain untrusted until the separately delivered PR4B auditor verifies them.
The auditor runs only from the default branch, reads bounded provider data, and
never checks out or executes PR bytes. It accepts a claim only after two equal
current observations of the exact PR/merge-ref tuple and a final revalidation
immediately before writing its create-only receipt. A missing, moved, closed or
ambiguous tuple is `UNVERIFIED`, never a pass.

For local, fixture-only inspection:

```bash
uv run python tools/ci/change_plan.py --event event.json --policy policy.json --output plan.json
uv run python tools/ci/gate_evaluator.py --plan plan.json --jobs jobs.json --output claims.json
uv run python tools/ci/audit_pr_gate_shadow.py --event audit-fixture.json --output audit-receipt.json
```

The output paths must not already exist. A source change needs a new PR head;
a provider rerun gets a distinct attempt-specific artifact and does not replace
an earlier claim.

The contracts are closed and versioned: [producer claims schema](../schemas/cicd/pr-gate-shadow-evidence-v1.schema.json),
[audit receipt schema](../schemas/cicd/pr-gate-shadow-audit-v1.schema.json), and
[trusted bundle schema](../schemas/cicd/ci-shadow-bundle-v1.schema.json).

For diagnosis and recovery, see [the PR Gate shadow runbook](pr-gate-shadow-runbook.md).
For the future reconciler's diagnostic prerequisite, see
[PR Gate shadow capacity](pr-gate-shadow-capacity.md) and its
[capacity runbook](pr-gate-shadow-capacity-runbook.md).
