# dbt-dpone-sqlserver (preparatory)

This optional Python distribution registers the actual dbt adapter type
`dpone_sqlserver` through dbt's normal AdapterPlugin loader. It is separate from
the `dbt-dpone` macro package. Version `0.1.0.dev0` is a development artifact, not
a qualified release. No distribution publication or managed route is certified.

Ordinary SQL Server behavior is inherited from the pinned dbt-sqlserver adapter.
Existing `sqlserver` profiles and qualification identities are unchanged. The
single new Jinja method, `adapter.dpone_physical_protocol_v1(operation,
parameters)`, is replaced with a no-I/O `None` result during parsing. Execution
currently fails closed before reading credentials or opening a cursor: the
authenticated platform factory, recorder delivery and driver qualification are
not yet integrated. Do not enable managed macros based on method presence.

## Platform build and migration

Build with `uv build packages/dbt-dpone-sqlserver`; validate the resulting wheel
with `twine check`. Install the exact reviewed base dpone wheel containing the
transport primitives together with this wheel into the qualified runtime image.
The declared dpone version range is packaging compatibility, **not** proof that
a published base wheel contains the preparatory primitives. Record exact source,
wheel and dependency hashes before qualification. No base dpone import/help path
loads dbt or this plugin. The supported Python interval is 3.11–3.12.

Operators must produce a new trusted profile, toolchain, source snapshot and
managed registration for `dpone_sqlserver`. Never relabel legacy `sqlserver`
hashes or fall back to an ordinary adapter inside a failed managed attempt.
Analysts receive the generated profile; there are no new private budget flags,
connection handles, environment recipes or transaction controls to configure.

## Internal boundaries and remaining work

The internal strict helper owns the raw cursor on an already materialized
same-thread handle, validates exact attach fields before conversion, bounds
incremental fetches and rejects every non-None terminal `nextset` result. Only
attach has an implemented producer/codec. Bind, transaction/namespace/helper
guards, catalog and receipt operations reject before cursor creation. Failure
poisons the attempt; no reconnect, transaction management, retry, rollback proof,
publication receipt or all-session settlement is supplied.

The private B1 packet is canonical, bounded and immutable but is not authority
by construction. Its one-use POSIX launch lease yields exactly one read-only,
anonymous inherited descriptor and a reserved environment addition. The existing
runner remains the sole owner of Popen, output draining, deadline waits and
process-supervisor cleanup. The application must retain and authenticate the
profile directory and actual policy/original/registration chain. A random file
name or caller-created packet is never admission evidence.

The driver query timeout is applied to the existing connection before cursor
creation, matching pyodbc's actual API. It rounds remaining absolute time up to
whole seconds and does not prove subsecond cancellation. Exact SQL Server driver
extra-set/late-error/cancel/close semantics and process termination remain
UNVERIFIED until approved Linux live tests. A successful mock or anonymous-file
test is not a managed route, source release or recovery certificate.
