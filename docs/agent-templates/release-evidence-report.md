# Release evidence: <version>

- Frozen commit: `<sha>`
- Requested operation: readiness audit | authorized publication | retrospective verification | scoped certification
- Release type: patch | minor | major
- Source tag and annotated tag object: <tag / object SHA, or not yet created>
- Controller commit: <reviewed SHA and actual run SHA, or N/A with reason>
- Controller run ID / attempt: <identity or not dispatched>
- Publication authorization: <reference, or not requested>
- Auditor: <name/agent>
- Started: <timestamp>
- Readiness decision: GO | NO-GO | N/A (observation only)
- Publication observation: PASS | FAIL | SKIP | UNVERIFIED | N/A (not published)

## Scope

Changed areas, connectors, strategies, schemas, integrations, packages, and
explicit N/A rationale.

Name the current publisher contract from `docs/release.md`. Separate source
readiness, ordinary PyPI publication, and route/image/production certification.
Do not make GitHub Release creation, unrelated live campaigns, or unfinished
CI-shadow backlog mandatory for a PyPI-only observation. Do not waive an
applicable source gate because it is not implemented by the controller.

## Status rules

`PASS`, `FAIL`, `SKIP`, `N/A`, and `UNVERIFIED` have the meanings defined in
`docs/agent-release-protocol.md`. Mocked, stale, or skipped evidence is not PASS.

## Gate summary

| Gate | Scope / applicability | Status | Blocking | Evidence |
|---|---|---|---:|---|
| R1 CLI correctness and UX | | | | |
| R2 run CLI/Python parity | | | | |
| R3 hierarchical identity | | | | |
| R4 source-to-sink × strategy | | | | |
| R5 contracts and guardrails | | | | |
| R6 docs and CJM | | | | |
| R7 Airflow and dbt | | | | |
| R8 packaging/security/supply chain | | | | |
| R9 recovery/observability/performance | | | | |

## Detailed evidence

Repeat for every check:

```yaml
id:
gate:
status:
required: true
requirement_source:
applicability:
commit:
environment:
command_or_workflow:
started_at:
finished_at:
observed_result:
artifact:
owner:
risk:
blocking_decision:
notes:
```

For R8 include the exact `agent_pr_merge_receipt.json` workflow run and artifact
IDs, its `integration_commit_sha`, source artifact/archive digests, and the
preserved `source-agent-pr-receipt.zip`. Reconcile that commit with
`release_identity.json` and `exact_commit_checks.json`. Missing immutable source
evidence is `UNVERIFIED` and blocks `GO`.

## Publication observation

For each of `dpone`, `dpone-native-accel`, `dpone-airflow-pack`, and
`apache-airflow-providers-dpone`, retain original wheel/sdist filenames and
hashes. Record controller run URL/attempt, artifact IDs/names/provider digests,
retention/expiry, original distribution ZIP, `release-manifest.json`, and
`verify-published` logs/conclusion. That job does not upload a separate JSON
receipt. Never substitute source-workflow or locally rebuilt archives.

For retrospective verification attach `retro_pypi_verification.json` and
the same observation's `fresh_install.log` when the install stage ran, exact
command/exit code, observation time, and actual PASS/FAIL/UNVERIFIED result.
Use a fresh output directory per observation; never pair a replaced receipt
with a leftover install transcript. The verifier requires a successful overall
publisher run; upload-job success alone does not qualify. Its retained-wheel install is not public Simple API
resolver proof. Missing original evidence stays unverified; do not republish.
Publication observation does not retroactively prove source-readiness gates.

## Failures, skips, and unverifiable items

| Item | Status | Reason | Owner | Remediation | Recheck |
|---|---|---|---|---|---|
| | | | | | |

## Compatibility and migration

Public-contract changes, deprecations, migration evidence, and rollback plan.

## Residual risks

Non-blocking risks only. A blocking risk cannot be accepted by wording alone.

## Decision

State readiness and publication observation separately, with exact identities,
rationale, residual risks, and invalidation conditions. A GO recommendation
does not authorize uploads or provider changes. A changed source/controller
revision requires affected evidence to be re-evaluated.
