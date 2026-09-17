# ADR 0066: Separate development delivery from workload execution

- Status: Accepted
- Date: 2026-09-17

## Context

A complete development deployment may contain a native dbt workspace and
ordinary flow or batch workloads. Production-native releases require qualified
route evidence, while the existing synthetic authority is intentionally limited
to isolated generated fixtures. Neither authority can be relabeled to admit
shared read-only development sources or ordinary business-shaped workloads.

Treating release membership as execution permission would also make dormant dbt
workloads executable merely because their artifacts must be delivered with the
deployment. That coupling is unsafe and prevents a selected ordinary workload
from being delivered without granting every native workload access to credentials.

## Decision

Add a closed development authority and release family defined by the approved
[development workspace and batch federation specification](../feature-specs/development-workspace-batch-federation.md).
The approved specification SHA-256 is
`6a466d62f898f8cbe434e6cb361a4b8caea5f2c3ef71ce35b8d1b18e18b4b2ed`.

Development delivery authority permits an immutable deployment to contain the
complete verified workspace. It does not permit any workload to execute. The
standard runtime remains fail-closed until a separately approved protected
entrypoint can verify current authority before init-fetch or credential access.

Production, development and isolated-synthetic documents have distinct authority
schemas or profiles. Development and production dbt releases share the stable V2
artifact wire because their artifact serialization is identical; authority
dispatch is based on the release schema and never falls back between families.
A production reader continues to require production route certification. The
synthetic family keeps its fixture, isolation and size constraints unchanged.

Ordinary development composition admits selector-scoped flow and batch packs,
public Airflow resource requests and limits, alias-only connection projection,
declarative SQL dependencies and separately scheduled SQL pre-hooks. It rejects
credential material, arbitrary commands, custom runners, separate post-hooks
and cross-constituent dependencies. Exact source bytes, rebuilt executable
semantics and fingerprints remain mandatory.

## Consequences

Operators can install and parse a complete development workspace while dormant
native and ordinary workloads remain unable to acquire runtime authority. No
development workload executes in this increment. Delivery evidence cannot be
reported as execution evidence.

The feature adds an explicit authority path through compilation, release
assembly, composition and cache delivery. Older readers
reject the new documents. Existing production and synthetic release identities,
validation results and runtime behavior do not change.

Connection aliases and public scheduling resources become identity-bearing
ordinary-pack semantics. Physical connection details and credentials remain
deployment-owned inputs and never enter source archives or release metadata.

## Privacy boundary

The public implementation, documentation and tests use neutral terminology and
synthetic fixtures only. Organization names, private repositories, internal
hosts, schemas, SQL, issue identifiers, operational evidence and source data are
outside this repository. Downstream integrations supply private configuration
without changing the generic public contract.

## Rollout and rollback

Ship the compiler, runtime, Airflow pack, provider and accelerator as one exact
version set. First validate pure contracts, deterministic pack reconstruction,
authenticated remote delivery and negative runtime boundaries with isolated
public fixtures. A separately approved runtime feature and live execution
evidence are required before claiming route support.

Rollback disables new development admissions and selects the previous immutable
deployment. It does not reinterpret development artifacts as production or
synthetic releases, revoke already recorded outcomes, retry uncertain writes or
grant dormant workloads execution permission.

Publication remains governed by the release controller. This ADR authorizes
implementation of the approved contract; it does not authorize a package upload
or a production qualification claim.

Target admission is a separate authority from the embedded release projection.
Every development publication, installation, activation and activation recovery
must present a current operation-specific receipt bound to the exact release,
deployment, protected target environment and `non_production` tier. The receipt
identifies the independently trusted target policy and current revocation epoch.
At each operation boundary an injected current-target verifier reopens those two
protected values; a receipt issued before revocation or policy replacement is
rejected. Activation recovery includes audit-only repair before coordinator
readback or durable audit mutation.
An artifact environment label, database name, CLI option or copied descriptor is
never sufficient authority. Unknown or production-tier targets fail before
external writes or pointer mutation. Production promotion requires a new
production-certified build from reviewed source; the development artifact is
never relabelled or promoted in place.

## Related contracts

- [Architecture decisions](../adr-index.md)
- [Workspace release source authority](0052-dbt-workspace-release-source-authority.md)
- [Verified release composition](0058-verified-release-composition.md)
- [Synthetic composition authority](0060-nonproduction-composition-authority.md)
- [Development workspace and batch federation](../feature-specs/development-workspace-batch-federation.md)
