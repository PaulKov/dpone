# Isolated enrollment SQL qualification

Use this fixture to assess the five enrollment/session procedures and their SQL
permissions in an owned, disposable SQL Server instance. It does not qualify a
managed dbt execution route. See [enrollment behavior](dbt-mssql-physical-enrollment.md)
for the public contract.

The prerequisite provenance is **SYNTHETIC**. The fixture retains actual canonical
plan, command and reservation bytes in an exact-version memory store, binds their
complete identities through `MssqlNativeOriginalBindings`, and obtains actual
workspace P, generation G and executor bindings through the existing SQL APIs.
Archive verification and the protected registration/catalog-binding lifecycle run
against actual fixture originals. Physical model membership is authored by the
isolated fixture owner; a complete managed source producer and runtime registry
are unavailable. No membership success method is patched, and the Python enroller
is not claimed to have passed. Fixed SQL endpoints test precisely this approved
SQL/permission boundary after real original acquisition.

## Prerequisites and execution

The parent coordinator owns Docker execution and fresh independent review. Run
serially on its approved frozen runner with Python 3.12.14, ODBC Driver 18
(observed qualification profile 18.06), and SQL Server 2022 CU26, engine build
16.0.4265.3. The approved engine image is
`mcr.microsoft.com/mssql/server@sha256:ba4c8329f48fb8f02e1416be6a930ebfd71268caee78aa985f3af4315e457c89`.
Record the actual runner content identity; a mutable tag is insufficient evidence.
Do not run these cells concurrently with another enrollment fixture: the fixed
master connection-certificate/login names require exclusive instance ownership.

Provide `DPONE_NATIVE_SQL_TEST_HOST` and `DPONE_NATIVE_SQL_TEST_PASSWORD` through
the approved environment without printing them. The SQL fixture creates random
owned databases and SQL-authenticated METADATA, BUILD and OBSERVER logins. The
initial master certificate and login names must be absent. It verifies CU26 and
uses the existing finite `emulated-host-correctness-v1` connection/setup/read
budgets (15/60/120 seconds); those are correctness budgets, not performance claims.

From the exact reviewed checkout inside that runner:

```bash
DPONE_RUN_PHYSICAL_ENROLLMENT_LIVE=1 \
DPONE_PHYSICAL_CATALOG_TEST_PROFILE=emulated-host-correctness-v1 \
PYTHONPATH=src:. python -B -m pytest \
  tests/test_dbt_mssql_physical_enrollment_live.py \
  -q -o addopts='' --junitxml=/tmp/physical-enrollment-live.xml
```

The test parameterization runs both `same_database` and `two_database`. Without
explicit live opt-in, five pure prerequisite/wire checks run and both SQL cells report
SKIP. Missing approved connection inputs also report SKIP, never PASS.

## Observable result and recovery

Each SQL cell first requires actual METADATA enrollment, fresh independent read,
identical replay, and complete successful BUILD attachment before any negative
drill. The attachment row survives disconnect; subsequent valid attachment is
therefore a duplicate rejection, never another positive baseline. The provisioner checks all five definitions, E/C signatures,
certificate users and exact permission inventories; replay must preserve prior
source module signatures, grants and runtime DENYs. Tests then check ordinary
role rejection, protected-table denial, unavailable direct DMV capability, changed
payload/original bindings, wrong plan/model/source identity, namespace collision,
missing C signature, durable BUILD attachment, duplicate attachment, closed G,
and terminal P. Rejection checks require SQL error numbers and exact custom DPONE symbolic codes and unchanged enrollment
and session rows plus every inherited native control table, including P/G,
capacity and original bindings. Only the exact CREATE/DROP view DDL epoch change
is allowed across each intentional namespace setup/cleanup operation; all other
custody facts must remain identical. Attachment verifies exact ordered column names from the actual cursor against the
canonical `dpone_managed_columns('attach')` wire, complete executor and selected
model bytes
and survives the actual BUILD connection closing.

JUnit includes environment observations, expanded module/source hashes, certificate
thumbprints, retained original digests and explicit qualification limits. Nested discovery inventory is labeled
as a prerequisite observation after actual fixture G admission; discovery itself
does not qualify G or enrollment. Record
the tested Git SHA and runner/image identities alongside it. Setup or baseline
failure blocks subsequent negative-case certification; preserve that failure as
FAIL rather than bypassing it. Cleanup runs on failures and removes only the
fixture-owned databases/logins and new master certificate/login. If cleanup fails,
the parent must remove its disposable container before another attempt.

Transaction bind/reset/receipt behavior, full managed source membership, genuine
dbt execution, launch uncertainty, performance, and production readiness remain
UNVERIFIED or unavailable. This fixture supplies no production or release GO.
