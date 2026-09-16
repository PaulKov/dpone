# Physical plan membership before enrollment

`MssqlPhysicalPlanMembershipReader` is a read-only source consumer for physical
plan enrollment. It acquires retained originals itself, checks the complete
selected model set, and compares the plan with the retained manifest. It does not
create an enrollment, connect to SQL Server, register a graph policy, or authorize
execution.

Compose it with concrete `NativeOriginalVerifier` and
`NativeProjectDocumentReader` instances, the existing `ConfinedReleaseFileReader`,
an absolute immutable `release_root`, and an explicit `max_release_bytes` bound.
Use the same root and release bound as the verifier. Keep the verifier alive until
the call finishes. The existing catalog policy reader independently verifies full
policy and registration selections; membership then reacquires originals and
compares both observations. This deliberately performs two bounded acquisitions.
Any surrounding operation deadline must cover both; this reader creates no fresh
time allowance or retry loop.

Call `require_plan_membership(refs, registration=registration, plan_set=plan_set)`
after enrollment's real bound original reader has acquired and canonically decoded
the retained plan, and before enrollment mutation. A caller-created plan or
registration remains a comparison claim. The method accepts no caller manifest,
resolved-source DTO, proof callback, or replacement policy result.

The reader binds release bytes and source inventory, locates the execution pack's
exact indexed runtime trio, and reads the manifest and selection lock at their
registered descriptor sizes. Existing per-kind ceilings remain 16 MiB for dbt
manifests and 1 MiB for selection locks. It checks actual byte count and SHA256,
then validates the retained manifest through `DbtArtifactReader.read_payload`
and its vendored official schema. Error-severity issues reject admission. The
reader retains that artifact's ordered column contracts alongside the raw strict
mapping, then reevaluates the graph through the existing finite policy registry. The native
plan itself must fit the registration's metadata bound.

Membership includes every selected MODEL ID, including upstream models, rather
than only publish roots. Each must use `dpone_managed_table`. Checks retain declared
column order, exact canonical SQL types and NOT NULL semantics, graph identity,
resolved database/schema/alias, selected physical filegroup name and resource
bounds. The base invocation target is checked against catalog policy separately;
model/spec comparison uses `execution.profile`, the resolved model target. Custom
schemas may legitimately differ from the base invocation schema. Layout comes from `config.meta.dpone.publish.model_storage`; omission uses
the approved `rowstore_none` default. Other SQL Server layout controls cannot
substitute for it. The normalized intent corroborates publish-root layout; it does
not assign a layout to upstream models.

Failures propagate before enrollment can mutate SQL. The new payload-free
`PhysicalPlanMembershipError.code` values are:

| Code | Meaning |
| --- | --- |
| `DPONE_PHYSICAL_PLAN_MEMBERSHIP_INVALID` | Source/plan membership or compared identities differ. |
| `DPONE_PHYSICAL_PLAN_MANIFEST_INVALID` | The retained manifest has a supported-artifact schema error. |
| `DPONE_PHYSICAL_PLAN_MANAGED_SOURCE_REQUIRED` | A selected model uses another materialization. |
| `DPONE_PHYSICAL_PLAN_COLLATION_UNAVAILABLE` | A character column lacks a producer-owned authoritative collation projection. |

Existing original acquisition, catalog policy and graph errors retain their own
diagnostics. No error is converted into an empty successful membership result.

The current finite registry admits the legacy graph only, so a complete managed
source cannot yet pass this reader. Actual managed materialization, graph policy,
selection producer and generated macro authority must exist before that becomes
possible. Pure noncharacter comparison tests establish comparison behavior only;
they do not establish managed source admission or execution qualification.

Character columns require the exact `native_execution.physical_collation.name`
from the authenticated selected policy. Missing selection fails explicitly;
every character column must match the same selected name. Collation cannot be
borrowed from the expected plan, a database default or unapproved manifest
metadata. This establishes the expected value only: actual SQL availability,
pre-reservation observation and helper-output agreement remain unverified. See
[physical collation policy](dbt-mssql-physical-collation.md).
The caller's generation/attempt ownership, immutable storage
retention, live filegroup IDs, namespace collisions, session/transaction state and
commit proof remain responsibilities of the original/enrollment/discovery consumers.


Activation ownership is an explicit upstream prerequisite, not a result of this
reader. `ResolvedNativeOriginals` and `NativeGenerationOriginalSubject` expose no
activation ID. The existing `native_generation_mssql_owner.physical_owner` producer
checks the **retained generation request's** activation, running attempt and guard
against protected ACTIVE/HELD database rows in the caller's transaction. It does
not by itself compare the caller's plan with that retained request. Enrollment must
compare the complete `plan_set.workspace_attempt` and `plan_set.guard` with the
authenticated retained generation request before invoking membership or mutating
enrollment. Plan-original subject/digest verification alone does not prove this
equality. Composed activation coverage remains unverified until that upstream
comparison and its mismatch regression pass independent review. The earlier design
trace's expectation of direct activation comparison in this reader is superseded
by this responsibility split; no private verifier fields are inspected.

Continue with [pre-generation discovery](dbt-mssql-physical-discovery.md) and
[managed admission transport](dbt-mssql-managed-admission.md). These components
remain distinct from [catalog observation](dbt-mssql-physical-catalog-acquisition.md)
and do not themselves enable the complete installed workflow.
