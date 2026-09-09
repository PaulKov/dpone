# Developer Data Product Remediation

The remediation pack is intentionally split into small provider-neutral modules.
Generic readiness code must not import ClickHouse, database clients, Airflow,
SCM SDKs, catalog SDKs, ticketing SDKs, notification SDKs, cloud billing SDKs,
or object-store SDKs.

## Components

| Component | Responsibility |
| --- | --- |
| `DataProductRemediationOptions` | Immutable manifest config for mode, profile and closeout policy. |
| `FailureSignalClassifier` | Converts Trust Center blockers and blocked evidence refs into normalized failure signals. |
| `DataProductRemediationPlanner` | Builds owner-routed actions, command templates and expected evidence. |
| `DataProductRemediationGate` | Fails closed when a plan has unmapped signals or unactionable actions. |
| `DataProductRemediationCloseout` | Verifies fresh follow-up evidence for every expected artifact kind. |
| `RemediationRenderer` | Renders Markdown runbooks and closeout reports. |
| `DataProductRemediationFacade` | Thin file-IO facade for CLI commands. |
| `RemediationExecutionOptions` | Immutable nested config for execution mode, allowlists, locks and idempotency. |
| `RemediationExecutionPlanner` | Converts remediation actions into allowlisted argv steps with resolved parameters. |
| `RemediationExecutionRunner` | Emits dry-run receipts or executes prevalidated steps through an injected executor. |
| `RemediationExecutionCertifier` | Verifies executed steps against fresh expected evidence. |
| `RemediationExecutionRenderer` | Renders execution reports for operators and reviewers. |
| `DataProductRemediationExecutionFacade` | Thin file-IO facade and local executor composition root. |

## Extension Rules

Add new repair classes by extending the deterministic remediation catalog in
`data_product_remediation_support.py`. A catalog entry must define:

- the source domain;
- expected evidence kind and allowed statuses;
- at least one safe handoff command template;
- whether fresh evidence is required for closeout;
- owner routing behavior.

Do not generate free-form repair commands. If a target-specific repair already
exists, link to that command and its required evidence instead of duplicating
target logic.

Execution extension rules:

- keep readiness modules free of `subprocess`, database clients, schedulers,
  SCM SDKs and catalog SDKs;
- add command templates to the remediation catalog only when the command is a
  deterministic dpone workflow with a typed evidence artifact;
- keep allowlists prefix-based and manifest-controlled;
- never pass command strings to a shell; execute only argv tuples produced by
  `RemediationExecutionPlanner`;
- represent execution output with bounded snippets and digests, not full logs.

## Failure Semantics

- `plan` status `ready` means every source blocker has an actionable remediation
  action. It does not mean the release is safe.
- `gate` status `allowed` means the remediation plan is actionable for the
  profile.
- `closeout` status `allowed` means fresh expected evidence exists and has an
  allowed status.
- `execution plan` status `ready` means every remediation action has an
  allowlisted argv step. It does not mean anything has run.
- `execution run` status `dry_run` means no command was executed. Strict
  certification blocks dry-runs; advisory turns that into a warning.
- `execution certificate` status `certified` means the run executed and fresh
  expected evidence is present.
- `advisory` converts blockers to warnings; `prod_strict` and `regulated` keep
  fail-closed behavior.
