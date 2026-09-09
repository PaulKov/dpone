# Feature design standard

Status: normative

This standard defines the minimum information required before implementing a
material dpone feature. Its purpose is to make architecture, product value,
failure behavior, and validation understandable before code creates sunk cost.

## When a full specification is required

Use `docs/agent-templates/feature-design-spec.md` when a change introduces or modifies:

- public CLI, Python API, manifest/schema, or artifact behavior;
- connector capabilities, source-to-sink routes, or load strategies;
- state, checkpoint, identity, schema evolution, reconciliation, or evidence;
- Airflow, dbt, GitOps, or another orchestration integration;
- a cross-layer dependency or reusable extension point;
- a first-time-user journey or operational workflow;
- a migration, deprecation, or breaking change.

A docs typo, isolated regression with an existing contract, or behavior-neutral
internal refactor may use a concise plan. The agent must still state contract
impact, tests, and risks.

## Approval states

A specification uses one of these states:

- `DRAFT`: incomplete; implementation prohibited;
- `RESEARCHED`: algorithm, alternatives, and evidence are present;
- `APPROVED`: maintainer approved implementation;
- `IMPLEMENTED`: code and docs landed, validation linked;
- `SUPERSEDED`: replaced by another spec or ADR.

No production implementation begins before `APPROVED` unless the maintainer
explicitly authorizes an exception.

## Required algorithm description

The specification MUST describe behavior precisely enough that another engineer
can implement or test it without guessing:

1. input acquisition and validation;
2. normalization and capability negotiation;
3. planning and deterministic identity derivation;
4. execution and transaction boundaries;
5. state/checkpoint and evidence ordering;
6. retries, backoff, resume, replay, cancellation, and rollback;
7. concurrency, ordering, partitioning, and resource limits;
8. failure classification, user-facing messages, and operator recovery;
9. artifacts, observability, and audit trail;
10. output and compatibility behavior.

Include pseudocode or a state diagram for non-trivial flows. State what happens
for empty input, null/missing values, partial failure, timeout, duplicate delivery,
process crash, schema drift, and unsupported capabilities when relevant.

## Architecture section

The design identifies:

- existing components reused;
- new components and their single responsibility;
- ports/interfaces and concrete adapters;
- dependency direction and composition root;
- data models and public schemas;
- compatibility shims and migration path;
- module-size and import-graph impact;
- alternatives considered and why rejected;
- ADR requirement.

The design must be systematic, not tied to one customer or one connector. It
must explain how the abstraction behaves for at least two realistic variations,
or why no abstraction is introduced yet.

## Product and UX section

Define personas and an end-to-end customer journey:

- discovery and prerequisites;
- first successful configuration;
- first successful run;
- understanding output and artifacts;
- diagnosing a common failure;
- recovery and safe retry;
- production operation and upgrade.

Specify CLI help, Python examples, error messages, defaults, output formats,
file overwrite/atomicity, and non-interactive behavior where applicable.

## Market research protocol

Research is capability-specific, not brand-scorecard theater. Select relevant
comparators from:

- dlt;
- Informatica;
- Airbyte;
- Fivetran;
- Pentaho;
- Microsoft SSIS;
- gusty;
- Astronomer Cosmos;
- Apache Beam.

For each relevant comparator record:

| Field | Requirement |
|---|---|
| Capability | Exact feature being compared |
| Product/version | Version, edition, or hosted/open-source context |
| Source | Current official primary documentation |
| Checked date | Date the source was verified |
| Observed design | Fact supported by the source |
| Strength | What works well for users or operators |
| Limitation | Constraint relevant to dpone's goal |
| Adopt | Pattern dpone will use and why |
| Reject | Pattern dpone will not use and why |

Mark a system `N/A` with a reason when it does not implement the relevant layer.
Do not force comparison between, for example, a managed ELT service and a DAG
factory on an irrelevant axis.

## Demonstrating that dpone is ahead

"Industrial standard" and "better" are hypotheses until measured. Every claimed
advantage must define:

```yaml
axis:                  # e.g. deterministic descendant identity after replay
scenario:              # exact workload and failure condition
baseline:              # comparator/version or current dpone behavior
metric:                # correctness, latency, steps, recovery time, etc.
target:                # numeric or binary acceptance threshold
procedure:             # reproducible test/benchmark/usability study
artifact:               # machine-readable evidence path
limitations:           # what the claim does not establish
```

Prefer correctness and operator-recovery evidence over feature-count claims.
Claims in public docs must remain scoped to the measured scenario.

## Validation and rollout

The design includes unit, contract, integration, live-certification, performance,
security, migration, and documentation coverage as applicable. It defines feature
flags or compatibility modes, telemetry, rollback criteria, and post-release
verification.

Implementation is complete only when the approved algorithm, public contract,
tests, artifacts, user journey, runbook, architecture documentation, and release
impact are all reconciled.
