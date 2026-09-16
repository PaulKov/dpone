# Finite source bridge provisioning

Audience: platform deployment owners and independent reviewers. This is a
source-only privileged adapter, not an enabled physical dbt provider or a model
admission result. Base: 5be70905bd36ee2791de901e157aebbc059c6ece; real SQL producer
dependency: a015eb6 (cherry-picked as baf1aec).

## Prepare and call

Authenticate the actual selected upstream policy, profile, toolchain, control,
capacity, program, macro and package originals using the external platform
authority before calling this adapter. Registration hashes and representational
validation do not authenticate those bytes. No new original kind or callback
supplies that authority. Exclude concurrent privileged DDL during provisioning
and registration insertion.

Install the immutable registration schema through its existing migration. Place
the same dedicated certificate (including a usable private key) in both registered
databases. Obtain its expected public bytes from the authenticated deployment
input and compare with `CERTENCODED`; do not trust an arbitrary observed certificate
as its own expected identity. The factory returns fresh bounded privileged model
connections and opens protected private keys when needed. Never put passwords or
private key bytes in registration, this API, artifacts or logs.

Construct `MssqlPhysicalSourceSchemaProvisioner` with `connection_factory`, actual
`admission_sql: bytes`, `certificate_name`, `certificate_public_bytes: bytes`, and
`certificate_user`. Call `apply(registration)` only after the preparation above.
The constructor accepts no authentication callback. The registration object is
the existing closed codec value. The SQL producer accepts coordinates only.

## Installed state and limitations

The adapter requires SQL-login users with exact retained IDs/SIDs, no role
memberships or broad permissions, disabled guest/trustworthy/chaining, dbo local
ownership chains, and full privileged catalog visibility. Existing finite native
object permissions remain separate. Dedicated observers are checked but receive
no source entry grant. Shared observers use their existing metadata/build mapping.
SQL2022's default public `VIEW ANY COLUMN ENCRYPTION KEY DEFINITION` and
`VIEW ANY COLUMN MASTER KEY DEFINITION` permissions are allowed as finite metadata
observation rights. The first isolated live run observed both defaults and rejected
them before module creation; the correction preserves those normal defaults
without allowing EXECUTE, CONTROL, impersonation or broader administrative rights.

Only `physical_require_source_v1` is signed; only
`physical_control_require_source_v1` is countersigned. Control certificate users
receive only helper EXECUTE; metadata/build receive only model entry EXECUTE.
Definition comparisons are exact UTF-16 bytes and lengths, with null EXECUTE AS,
expected ANSI settings and finite signature/grant inventories. Existing changed
definitions are rejected without replacing them. Private keys are never serialized.

Provisioning owns one DDL transaction across the registered same-instance
databases. It revisits both inventories before commit. The concrete registration
store runs only after that commit succeeds and independently reads back its row.
After ambiguous DDL commit, registration does not start; reconcile actual inventory
with privileged observation before another attempt. The adapter does not retry
DDL or invent a new registration UUID. Exact existing definitions may be reused.

## Evidence and remaining work

PASS: initial red test failed on missing implementation; green tests use the real
deterministic producer and exercise finite inventory guards, commit/rollback,
definition reuse and no-registration-on-failure. Unit doubles test lifecycle only,
not SQL engine permissions. PASS: focused Ruff and mypy.

UNVERIFIED here: SQL2022 actual permissions, certificate signing behavior, caller
identity positives/negatives, and independently authenticated retained originals.
The bridge owner owns that separate isolated live fixture. No corporate environment
is used. Independent non-author review remains required before completion/merge.

Documentation impact: integrator should link this bounded preparation/recovery
contract into the physical adapter CJM and record the additive API in CHANGELOG.
No existing runtime route, CLI or registration wire representation changes.
