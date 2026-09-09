# `DPONE_POSTGRES_SOURCE_AUTHORITY_INVALID`

The environment registry contains an incomplete, malformed, open, or ambiguous
PostgreSQL source authority document. The contract is deliberately closed and
requires exactly one supported version and profile, topology, canonical
database and principals, and a non-empty finite relation map. Version 1 also
requires a physical cluster identifier and timeline; version 2 requires
`verification_profile: catalog_identity` and forbids those unverified fields.

## Fix

Re-run the read-only discovery queries as the deployment runtime principal and
replace the invalid editable registry document with reviewed canonical values.
Relation-map keys must exactly equal `schema.relation`; OIDs are positive
32-bit catalog OIDs; the system identifier is a decimal unsigned 64-bit value;
and ASCII-case-fold-equivalent relation keys are forbidden. Version 2 is the
least-privilege read-only profile; version 1 is the enhanced physical-cluster
profile. Never delete an
unknown field merely to silence CI without first reconciling the registry
producer and generated schema.

See the
[signed source authority schema and least-privilege procedure](../source-sink/postgres-to-mssql.md#signed-postgresql-source-authority),
then run `dpone check --connections` again.
