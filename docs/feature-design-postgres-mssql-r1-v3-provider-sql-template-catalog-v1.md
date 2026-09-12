<!-- Private migration draft: public bindings and approval transfer are PENDING. -->

# Feature design: PostgreSQL → MSSQL R1 V3 SQL-template catalog

> Migration scope: this document is retained from source development. Historical approval, acceptance, exception, commit and evidence statements below apply to that source context; they do not establish current migration approval, activation, certification or passing validation. Current candidate status is tracked separately.

- Status: RESEARCHED
- Owner: dpone maintainers
- Issue: PostgreSQL → MSSQL Industrial Integration V7 / R1
- Target release: R1
- Last verified: 2026-09-05
- Depends on: approved renderer algebra specification plus approved and
  implemented concrete physical descriptor, security, migration and binding
  contracts at exact commits
- Amended by:
  `docs/feature-design-postgres-mssql-r1-v3-provider-security-authority-amendment-v2.md`.
  The receipt entries remain non-executable until their dedicated V2 resource
  coordinates and ABI are pinned.

## Outcome and gate

This child owns the exact 27-key SQL Server statement-template catalog required
by the R1 V3 provider renderer. It freezes only SQL that is not already owned by
an exact descriptor/query/module authority. For passthrough keys the catalog
owns composition and the exact `(source arm, field ordinal, leaf kind, use)`
policy only; exact per-instance source refs/digests belong to renderer statement
rules and are never copied into the singleton catalog entry.

This revision is intentionally not implementation-ready. On the verified commit
there is no production concrete 20-table/29-procedure/6-binding descriptor and
therefore no exact receipt coordinates, result ABI or production definition
bytes to pin. Synthetic descriptor bytes from tests and V2/experimental V3 SQL
must not become catalog golden vectors.

Activation remains blocked until:

```text
renderer algebra/model contract APPROVED
→ non-renderer dependency specs APPROVED
→ non-renderer dependency implementations pinned
→ exact concrete descriptor available
→ every fixed skeleton and intrinsic source policy frozen here
→ per-instance refs pinned through the exact statement registry
→ fresh architecture/security/test review
→ maintainer changes catalog status to APPROVED
→ renderer and catalog implementation under one approved scoped task
```

There is no manifest, CLI or public Python API change.

## Authority rules

The catalog follows the renderer's three composition arms:

```text
fixed_render
  catalog owns exact SQL skeleton bytes and typed placeholder ABI

complete_intrinsic_passthrough
  source authority owns the complete SQL bytes; catalog owns the key,
  composition classification and exact intrinsic source policy; the statement
  registry owns and resolves the exact per-instance source reference

fixed_render_matches_intrinsic
  catalog owns a generic typed skeleton; identifier resolution must reproduce
  the fully resolved source-owned secret template byte-for-byte
```

No intrinsic fragment is concatenated into a catalog skeleton. No free text is
rendered as an identifier, permission or SQL token. Identifier quoting follows
one strict schema-2 validated bracket-quoting function and doubles `]`. Scalar
values in parameterized mode remain DB-API parameters; approved stable metadata
uses the renderer's strict typed-literal grammar. Secret substitution is allowed only for the
single approved certificate-password placeholder and executable secret-bearing
bytes are never retained, hashed, logged or returned.

Catalog ordering is closed-rule enum order, followed by permission-scope enum
order. Non-permission rules have one NULL-scope key. Each permission rule has
four keys: database, schema, object and column.

## Exact 27-key inventory

`F`, `P` and `M` mean `fixed_render`, `complete_intrinsic_passthrough` and
`fixed_render_matches_intrinsic`.

| # | Template key | Composition | Byte authority |
|---:|---|---|---|
| 1 | `migration_query_v1 / NULL` | P | exact migration observation query |
| 2 | `create_schema_v1 / NULL` | F | catalog skeleton |
| 3 | `create_table_v1 / NULL` | P | physical table definition |
| 4 | `create_index_v1 / NULL` | P | physical index definition |
| 5 | `create_trigger_v1 / NULL` | P | physical trigger definition |
| 6 | `create_procedure_v1 / NULL` | P | physical procedure definition |
| 7 | `add_extended_property_v1 / NULL` | F | catalog skeleton and typed text parameter |
| 8 | `shared_create_certificate_v1 / NULL` | M | catalog skeleton equals resolved security template |
| 9 | `shared_create_certificate_user_v1 / NULL` | F | catalog skeleton |
| 10 | `shared_grant_permission_v1 / database` | F | database permission skeleton |
| 11 | `shared_grant_permission_v1 / schema` | F | schema permission skeleton |
| 12 | `shared_grant_permission_v1 / object` | F | object permission skeleton |
| 13 | `shared_grant_permission_v1 / column` | F | column permission skeleton |
| 14 | `shared_sign_module_v1 / NULL` | M | catalog skeleton equals resolved security template |
| 15 | `shared_remove_private_key_v1 / NULL` | F | non-secret catalog skeleton |
| 16 | `binding_create_certificate_v1 / NULL` | M | catalog skeleton equals resolved binding template |
| 17 | `binding_create_certificate_user_v1 / NULL` | F | catalog skeleton |
| 18 | `binding_module_create_v1 / NULL` | P | instantiated binding module definition |
| 19 | `binding_permission_grant_v1 / database` | F | database permission skeleton |
| 20 | `binding_permission_grant_v1 / schema` | F | schema permission skeleton |
| 21 | `binding_permission_grant_v1 / object` | F | object permission skeleton |
| 22 | `binding_permission_grant_v1 / column` | F | column permission skeleton |
| 23 | `binding_sign_module_v1 / NULL` | M | catalog skeleton equals resolved binding template |
| 24 | `binding_remove_private_key_v1 / NULL` | F | non-secret catalog skeleton |
| 25 | `attestation_query_v1 / NULL` | P | exact attestation query definition |
| 26 | `receipt_probe_v1 / NULL` | F | catalog query and result ABI |
| 27 | `receipt_append_v1 / NULL` | F | catalog insert and parameter ABI |

The shared and binding permission entries remain distinct keys even if a future
approved revision proves some payload bytes identical.

Both create-certificate keys bind every varying non-secret input before the
equality check: certificate identifier, owner identifier, subject, start date
and expiry date, followed by the one untouched secret placeholder. Shared
signers may therefore have different subjects and binding signers may contain
their resolved UUID without weakening the singleton catalog skeleton.

## Placeholder and parameter ABI

The approved child revision must freeze a parsed, nonrecursive token grammar:

- distinct identifier, scalar and closed-token namespaces;
- contiguous three-digit ordinals;
- every declared placeholder occurs exactly once;
- no undeclared braces, repeated token or recursive replacement;
- exact terminal LF, NFC UTF-8 and no NUL/CR;
- exact DB-API positional marker order after rendering.

Every parameter position binds an exact physical target coordinate and SQL type
descriptor from the concrete descriptor. The minimum required coordinates are:

```text
receipt probe:
  input: installation_effect_key
  output: row_count, stored_request_digest,
          stored_payload_bytes, stored_payload_digest

receipt append:
  input: effect_key, request_digest, payload_bytes, payload_digest

extended property:
  input: exact bounded Unicode property value
```

Executable authority is carried by each catalog entry's ordered
`MssqlR1ApprovedParameterAbiV1` tuple: contiguous position, placeholder,
binding/scalar kinds, closed coordinate selector, exact SQL type payload and
input direction. Renderer evidence contains only a resolved byte-identical
projection. For receipt probe the catalog also carries the fixed typed result
ABI payload; source-owned observation/attestation result ABIs remain in their
exact query definitions. Evidence never invents either input or result ABI.

The probe returns exactly one storage-observation row. For count zero or two or
more, stored fields are NULL. Only count one returns stored values. SQL does not
receive current expected digests and does not decide `exact|mismatch`; the
application compares the observation with the admitted authority. Append is one
immutable INSERT and relies on the exact unique effect-key constraint. Neither
statement may invent another authority table: both must resolve the approved
twentieth `dpone_provider_install_receipt_v3` resource.

## Permission and secret grammar

Permission templates consume exactly one approved uppercase permission keyword
and one closed grant-option token. Expected executable edges require
`effect=GRANT`. `DENY` belongs only to negative attestation. `False` maps to an
empty suffix; `True` maps to exact ` WITH GRANT OPTION`. Binding edges are always
false. Permission scope selects one exact template and has no nullable coordinate
slot or wider-scope fallback.

Certificate creation and module signing use one ephemeral password placeholder.
The catalog first resolves and quotes typed certificate/module identifiers, then
requires equality with the fully identifier-resolved security/binding template.
Only the secret is substituted during execution. Private-key removal is the
non-secret statement `ALTER CERTIFICATE <typed identifier> REMOVE PRIVATE KEY`
under exact catalog formatting; it contains no password placeholder.

Module creation precedes signature creation because changing a signed module
removes its signature. Private-key removal occurs only after every required
signature succeeds. Reinstall observes exact public-key-only state and performs
no DDL, regrant or resign operation.

## Source ownership and forbidden reuse

The approved statement registry references these source authorities by exact
payload and digest; the catalog records only their exact intrinsic source
policies:

- migration observation `query_definition.utf8_bytes`;
- physical table/index/trigger/procedure definition payloads;
- instantiated binding-module definition bytes;
- attestation query definitions;
- security/binding create-certificate and add-signature templates.

Forbidden sources include:

- synthetic `synthetic ... definition` test fixtures;
- V2 target-authority DDL;
- experimental V3 stage SQL with schema-wide grants or `EXECUTE AS OWNER`;
- generic identifier quoting without prior schema-2 validation;
- deserialized rendered evidence as executable authority.

## Transaction and execution boundary

Catalog statements execute only after fresh rendering from admitted canonical
inputs on one installer-owned physical session:

```text
autocommit OFF
→ XACT_ABORT ON
→ SERIALIZABLE
→ BEGIN
→ exact transaction-owned schema lock
→ observations and receipt precheck
→ admitted mutations
→ stable attestation
→ target-local immutable receipt
→ COMMIT
```

There are no `GO` separators, `EXECUTE AS OWNER`, dynamic SQL from free text or
cross-database receipt writes. Parameterized execution requires a new typed
provider boundary; the existing raw `.execute(sql)` mutation compatibility path
is not sufficient and is not silently reused.

## Tests and evidence

Hermetic gates:

- exact 27-key count/order and 19+4+4 coverage;
- composition arm, intrinsic source-policy closure and per-statement source-reference
  resolution;
- golden fixed skeleton bytes/digests;
- strict placeholder and parameter ABI;
- four permission scopes, permission allowlist and grant-option cases;
- secret placeholder position/count and secret-free removal;
- source-owned intrinsic/template differential equality;
- receipt probe zero/one/multiple storage rows;
- receipt append uniqueness and lost-response replay;
- malformed/trailing/casefold/union mutation registry;
- module size, import rules and architecture-fitness non-regression.

Live SQL Server evidence is mandatory before provider activation:

- compile every rendered statement on the exact SQL Server 2022 CU profile;
- verify transaction rollback and lost-commit recovery;
- prove certificate/user/signature/private-key lifecycle;
- execute positive and negative permission tests;
- compare exact catalog observations and receipt row types;
- prove secrets absent from dpone artifacts/logging/telemetry.

Unavailable, mocked or skipped live checks remain `UNVERIFIED`, never `PASS`.

## Normative vendor sources

- [CREATE SCHEMA (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-schema-transact-sql?view=sql-server-ver16)
- [sp_addextendedproperty](https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-addextendedproperty-transact-sql?view=sql-server-ver16)
- [CREATE CERTIFICATE (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-certificate-transact-sql?view=sql-server-ver16)
- [CREATE USER (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-user-transact-sql?view=sql-server-ver16)
- [ADD SIGNATURE (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/statements/add-signature-transact-sql?view=sql-server-ver16)
- [ALTER CERTIFICATE (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/statements/alter-certificate-transact-sql?view=sql-server-ver16)
- [GRANT database permissions](https://learn.microsoft.com/en-us/sql/t-sql/statements/grant-database-permissions-transact-sql?view=sql-server-ver16)
- [GRANT schema permissions](https://learn.microsoft.com/en-us/sql/t-sql/statements/grant-schema-permissions-transact-sql?view=sql-server-ver16)
- [GRANT object permissions](https://learn.microsoft.com/en-us/sql/t-sql/statements/grant-object-permissions-transact-sql?view=sql-server-ver16)

## Approval checklist

- [x] Exact 27-key identity and order are fixed.
- [x] Byte ownership and three composition arms are explicit.
- [x] Private-key removal is non-secret.
- [x] Permission and receipt semantic ABI are closed.
- [ ] Exact non-renderer dependency implementation commits are pinned.
- [ ] Concrete descriptor coordinates and production intrinsic bytes are pinned.
- [ ] Every fixed skeleton, placeholder and parameter/result type is frozen.
- [ ] Golden payload/digest vectors are recorded.
- [ ] Fresh architecture, security and test reviews approve the exact revision.
- [ ] Maintainer changes status to `APPROVED`.
