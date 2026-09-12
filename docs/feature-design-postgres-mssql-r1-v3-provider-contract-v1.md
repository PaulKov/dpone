
> Historical provenance: “source record NNN” names a privately archived original, not a Git ref, executable task grant or current validation result. Outcomes attached to these references retain their original historical scope. Current execution requires a separate genuine public authority chain.

<!-- Private migration draft: public bindings and approval transfer are PENDING. -->

# Feature design: PostgreSQL → MSSQL R1 V3 provider contract bundle

> Migration scope: this document is retained from source development. Historical approval, acceptance, exception, commit and evidence statements below apply to that source context; they do not establish current migration approval, activation, certification or passing validation. Current candidate status is tracked separately.

Audience: dpone maintainers navigating the independently owned R1 provider
contracts. This document is a status-bearing historical index, not an
implementation specification.

- Status: RESEARCHED
- Owner: dpone maintainers
- Issue: PostgreSQL → MSSQL Industrial Integration V7 / R1
- Target release: R1
- Last verified: 2026-09-07
- Depends on: `MssqlR1PhysicalSchemaDescriptorV1` at its implemented exact commit

## Executive summary

The SQL-free physical descriptor intentionally models tables, procedures,
resources, locks, transitions and replay semantics, but it does not own four
different concerns required by an executable SQL Server provider:

1. certificate and installation security policy;
2. migration observation and fail-closed disposition;
3. target-specific binding-module instantiation and attestation;
4. deterministic SQL statement planning and rendered-bundle identity.

This umbrella indexes the companion specifications and records only their
dependency boundaries. It does not add an implementable aggregate bundle. The
approved security-authority V2 amendment changes the core inventory to 20
tables; each child must independently pin the resulting exact physical
descriptor and prove its own anti-splice closure.

Exact child specifications own the implementable field algebra. The umbrella
remains `RESEARCHED` until all children are approved. Current implementation
and blocker status is maintained in the
[provider implementation map](developer-postgres-mssql-r1-v3-provider-implementation.md):

- [shared install security](feature-design-postgres-mssql-r1-v3-provider-security-contract-v1.md) — `APPROVED`;
- [security authority amendment V2](feature-design-postgres-mssql-r1-v3-provider-security-authority-amendment-v2.md) — `APPROVED`;
- [selected-relation source schema authority](feature-design-postgres-mssql-r1-source-schema-authority-green-v5.md) — `APPROVED`, implemented at `source record 009` with hermetic `local_pass` Evidence V3 head `source record 010`; activation remains blocked and vendor-live remains `UNVERIFIED`;
- [migration observation and admission V2](feature-design-postgres-mssql-r1-v3-provider-migration-contract-v2.md) — `APPROVED`, implementation absent, reviewed task contract and frozen RED required;
- [binding mapping/pack authority V2](feature-design-postgres-mssql-r1-v3-provider-binding-contract-v2.md) — `APPROVED`, implemented with hermetic `local_pass`; activation and vendor-live remain blocked/`UNVERIFIED`;
- [renderer registry/evidence](feature-design-postgres-mssql-r1-v3-provider-renderer-contract-v1.md) — `RESEARCHED`;
- [exact SQL-template catalog](feature-design-postgres-mssql-r1-v3-provider-sql-template-catalog-v1.md) — `RESEARCHED`;
- aggregate/report integration — pending child specification.

Each linked child owns its candidate algebra according to that child's stated
status; only an `APPROVED` child may authorize production implementation. Where
this umbrella differs, it is never an alternate authority. No production code
may implement a prose-only umbrella field.

The measurable future outcome is one separately specified provider authority
from which a renderer, installer and catalog attestor can operate without
inventing security, migration, binding or statement-order conventions.

## Personas and customer journey

| Persona | Goal | Current pain | Success signal |
|---|---|---|---|
| Provider implementer | Render and install one exact schema | Parent specification leaves several rules in prose | Every executable choice resolves to one typed contract leaf |
| Security reviewer | Prove signer and permission lifecycle | Signer names do not describe creation and private-key handling | Closed install-security digest and mutation tests |
| Incident responder | Distinguish replay, install and block | Partial inventory can reach an ambiguous branch | One fail-closed migration decision for every observation vector |
| Data-product operator | Bind modules to one registered target | Stable module pack and dynamic stage names conflict | Target-only compiler inputs and fresh pack attestation |

Journey:

1. Integrator follows the implementation map and exact child statuses.
2. Each approved child is implemented and independently validated.
3. A future aggregate child pins the complete reviewed outputs and defines all
   cross-digest equations.
4. Its future generated report exposes only canonical semantic coordinates and
   digests.
5. Any mismatch blocks before database mutation; no umbrella fallback exists.

## Scope

### In scope

- Status-bearing navigation to child contracts.
- Dependency and delivery boundaries between those contracts.
- Historical context explaining why the earlier bundle was decomposed.

### Non-goals

- Concrete 20/29/6 descriptor data.
- SQL rendering, catalog SQL, ODBC/pyodbc, credentials or database I/O.
- Installation or migration execution.
- Live binding-module compilation or attestation.
- Public manifest, CLI, ports, composition or activation changes.
- Generic SQL AST, policy language or provider plugin framework.

### Assumptions and constraints

- The core descriptor is implemented and byte-stable before child tasks start.
- This index imposes no model, codec, normalization, domain, field-order or
  decode rules. Each child and the future aggregate specification own those
  rules.
- Secret handling and canonical-field exclusions are likewise child-owned.

## Public contract

There is no public CLI, Python import or manifest change. These are internal,
unreleased contracts. Compatibility shims are forbidden. A later public-output
specification owns plan/status JSON paths, Markdown projection and blocker UX.

Each child owns its exact hermetic evidence schema and status vocabulary. The
future aggregate child will define the complete provider report only after all
inputs exist; this umbrella supplies no report fields or producer. No live
status may be inferred from documentation. Live remains `UNVERIFIED`.

## Child authority boundaries

This umbrella defines no canonical fields or algorithms. Current authority is
partitioned across status-bearing children:

| Child | Status in this revision | Owns |
|---|---|---|
| Physical descriptor R2 | `APPROVED` / implemented | Physical resources, schema and procedure semantics |
| Security V2 | `APPROVED` / implemented | Principals, signer lifecycle and permission closure |
| Selected-relation source schema | `APPROVED` / implemented / `local_pass` | Exact implementation `source record 009` and Evidence V3 head `source record 010`; activation remains blocked and vendor-live remains `UNVERIFIED` |
| Binding V2 | `APPROVED` / implemented / `local_pass` | Pure portable source-target module pack at `PENDING_PUBLIC_COMMIT_BINDING`; activation blocked, vendor-live `UNVERIFIED`; see [ADR 0070](adr/source-history/0070-r1-provider-binding-v2-clustering-exception.md) |
| Migration V2 | `APPROVED`; implementation absent; task contract required | Live observation, disposition, installer UoW and replay |
| Renderer | `RESEARCHED`; V2 required | Renderer algebra and resolved statement evidence |
| Exact SQL catalog | `RESEARCHED` | Concrete reviewed SQL template bytes |
| Provider aggregate | specification absent | Cross-child composition, digest, report and admission |

A child may import only dependencies documented in its own specification.
Migration and Renderer consume Binding independently and never import one
another. SQL catalog feeds Renderer. A future provider aggregate compares the
complete Migration and Renderer/catalog outputs without this umbrella inventing
an intermediate digest or model.

The rejected architecture patterns remain: opaque JSON extension fields,
generic SQL AST/plugin frameworks for one provider, dynamic stage identifiers
inside stable modules, reverse adapter dependencies, and umbrella fallback
semantics.

## Market comparison

External ETL products are `N/A` for this internal canonical-contract algebra.
No product-facing superiority claim is made. dlt, Informatica, Airbyte,
Fivetran, Pentaho, SSIS, gusty, Astronomer Cosmos and Apache Beam become
relevant only in route/runtime or certification specifications.

## Measurable differentiation

```yaml
axis: authority-navigation ambiguity
scenario: maintainer selects the implementation authority for one provider layer
baseline: umbrella retained stale pseudo-models alongside child specifications
metric: executable fields or algorithms defined only by this umbrella
target: 0
procedure: documentation contract review plus link/status validation
artifact: documentation check output for the exact commit
limitations: navigation evidence, not provider correctness or vendor-live evidence
```

## Security, privacy and operations

This umbrella contains no canonical payload or generated artifact. Each child
owns secret handling, redaction, retention and operational recovery for its
scope. No activation or live status can be derived from an umbrella link.

## Test and certification plan

| Layer | Scenario | Expected artifact |
|---|---|---|
| Documentation | child links, exact statuses, no stale umbrella algebra | docs checks PASS |
| Architecture | documented import/delivery DAG is acyclic | fresh review |
| Child contracts | owned by each linked specification | child evidence only |
| Live | N/A for this index | `UNVERIFIED`, never PASS |

## Documentation plan

Keep the implementation map as the maintainer entry point and this umbrella as
a research history/index. A future aggregate child owns its reference and
report documentation. No first-success or public matrix claim changes until
the physical provider and live route are approved.

## Rollout and rollback

This index has no runtime rollout. Child releases remain independently
activation-blocked. A future aggregate child defines its own compatibility and
rollback after its contract exists.

## Agent execution plan

| Role | Owned paths | Forbidden | Dependency |
|---|---|---|---|
| child implementer | paths in one approved child task contract | every sibling/shared path | approved child |
| test certifier | read-only child/evidence review | all writes | exact child commit |
| integrator | this index, implementation map and future aggregate specification | child implementation modules | fresh reviews |

One path-scoped task contract must enumerate disjoint files and exact checks
for each child. Final composition requires its own later approved specification
and task contract.

## Approval checklist

- [x] User problem and CJM are clear.
- [x] This umbrella defines no implementation algorithm or failure contract.
- [x] Public/internal navigation boundaries are explicit.
- [x] Architecture and alternatives are justified.
- [x] Market comparison is correctly `N/A` for the pure internal scope.
- [x] Navigation differentiation is measurable.
- [x] Index validation and no-runtime rollout are explicit.
- [x] Path ownership and integration plan are conflict-safe.
- [ ] Every required child is independently approved and implemented.
- [ ] A provider aggregate child is separately specified and approved.
