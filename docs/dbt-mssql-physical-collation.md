# Physical collation policy

For platform engineers preparing managed SQL Server models, this optional v4
policy field records the exact expected collation of every character column.
It is a policy foundation: managed preparation and SQL availability are
**UNVERIFIED** until the separate signed observation and execution dependencies
are implemented and qualified.

## Select the expectation

Add this fragment inside the selected profile's existing `native_execution`
object, then regenerate and authenticate the complete retained policy through
the existing [native composition flow](native-generation-execution.md):

```yaml
physical_collation:
  name: Latin1_General_100_BIN2
```

The example is a lexical selection, not a claim that this name exists in your SQL
instance. Choose the exact catalog spelling for the intended model database.
The closed object requires only `name`: an ASCII letter followed by at most 127
ASCII letters, digits or underscores. Nulls, extra keys, whitespace, SQL syntax,
Unicode and `database_default` in any case reject. There is no default, trimming
or case normalization. No CLI override is introduced.

Omission preserves existing policy bytes and noncharacter behavior; the v4 schema
identifier does not change. Adding the field changes complete policy bytes and
therefore their digest: retain newly generated originals, never edit old evidence.
An entirely noncharacter plan does not need a collation observation just because
this field is present.

## Observe and recover

Membership derives each `char`, `varchar`, `nchar` and `nvarchar` column's expected
collation from the authenticated selected profile. Every such column in every
selected model must match the same exact name; all other columns keep `None`.
Retained column order, supported types and enforced nullability still must match.
A caller plan or a manifest metadata extension cannot supply missing authority.

`DPONE_PHYSICAL_PLAN_COLLATION_UNAVAILABLE` means the character expectation lacks
an admissible selection. Its `remediation` attribute tells Python consumers to
select `native_execution.physical_collation.name` and verify availability before
reservation. Correct the authored policy, regenerate originals, then retry
preparation. A mixed or differently spelled plan rejects with
`DPONE_PHYSICAL_PLAN_MEMBERSHIP_INVALID`; correct and regenerate the complete plan.
These pure checks perform no SQL, reservation, writes or cleanup.

## Developer boundary and qualification

The real membership reader keeps `NativeOriginalVerifier` and
`NativeProjectDocumentReader` authentication, indexed manifest acquisition,
official schema validation and resolved target comparison. The pure membership
contract receives that authenticated profile; direct calls with mappings prove
comparison only. `require_physical_collation_name` validates representation and
uses the same canonical lexical validator as the
[candidate renderer](dbt-mssql-physical-rendering.md). Renderer SQL bytes and
existing errors remain compatible.

Before any generation reservation, a separate bounded signed observer must prove
exact name spelling and byte length under the pinned model database and current
registration. The proposed `physical_discover_character_namespace_v1` response
adds one field to the existing discovery 18 fields through a new 19-field entry;
the old response and signature inventory remain unchanged. This wrapper captures
the old procedure with `INSERT EXEC`: consume the wrapper directly through the
client, **never capture it with another `INSERT EXEC`**. Its SQL implementation,
structural regression, deployment identity and live evidence are separate pending
dependencies, not delivered by this policy change.

Build must compare the actual helper output's ordered types, nullability and
collations before candidate creation/load. Keep authenticated compiled SQL
unchanged; never append implicit `COLLATE`, coerce values or relax helper checks.
Candidate DDL does not prove helper agreement. No managed route, generation
authority, availability receipt or complete preparation is enabled here.

Focused offline tests are `tests/test_dbt_native_policy_v4.py`,
`tests/test_dbt_mssql_physical_collation.py`,
`tests/test_dbt_mssql_physical_plan_membership.py` and
`tests/test_dbt_mssql_physical_rendering.py`. Pure comparison and synthetic retained
acquisition tests are not live certification. Continue with the
[dbt overview](dbt.md) for the supported end-to-end user journey.
