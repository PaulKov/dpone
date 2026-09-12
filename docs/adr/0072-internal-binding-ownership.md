# ADR 0072: Canonical ownership of internal Binding models

Status: accepted for the private migration under the maintainer's approval to complete functional decomposition. This decision does not waive architecture budgets, transfer historical authority, or authorize publication.

## Context

Binding V2's public-contract section explicitly makes new models internal and excludes compatibility-facade/package-root reexports. The persisted pack encoding is a durable contract. Treating every private source module path as an already-published import created needless ownership boundaries and obscured aggregate reader policy.

## Decision

`dpone.contracts.mssql_r1_v3_binding_modules` owns portable target mapping, stage discriminators/projections/invocations and module-core derivation. `mssql_r1_v3_binding_pack` owns whole-pack decoding and mismatch classification. Signer, permissions and validation retain their distinct owners. The unpublished `binding_enums`, `binding_mapping` and `binding_stage` modules are removed; internal consumers import canonical owners directly.

Class and enum names, fields/order, exact type identity, domain strings, canonical bytes/digests and failure precedence remain unchanged. The private canonical base is shared because both stage models supply their own encoding and the prior bases have identical digest behavior. No existing public-baseline import, manifest, CLI, transaction or activation behavior changes.

## Evidence compatibility

The earlier evidence-2/layout-2 profile described the five-file owner inventory.
Its original records and the older eight-file layout-1 schema remain immutable
in the private migration archive. Later owner refinements are recorded in
[ADR 0073](0073-typed-contract-runtime-ownership.md).

Current public evidence uses evidence-4, authority version 2 and layout 4. The
version-4 protocol domain binds the exact public integration base and all 15
producer, schema, test, recipe and fixture dependencies. Public tests exercise
explicitly synthetic legacy layouts 1, 2 and 3; these are new records with new
identities, not byte-identical historical evidence. Mixed layouts and authority
versions are rejected. Current evidence requires a genuine public source chain;
historical reports and synthetic test histories cannot supply that authority.

The closed 246 behavioral case registry and existing model-field inventory are unchanged. Supplemental ownership tests live outside that frozen behavioral case denominator. Producer source-path inventory is updated; generated evidence is never hand-edited to claim PASS.

## Validation and consequences

Verify concrete model AST/canonical behavior, golden/roundtrip and rejection tests, runtime annotation resolution, both schema versions and mixed-version rejection. Then use canonical module-size, import/layer/graph checks and Docker regressions. The refactor improves ownership and measured clustering, but global clustering and source-content clearance remain separate gates.
