# SQL Server managed admission transport

The package includes source-only admission helpers. They do not enable a managed
materialization, provision SQL procedures, register authority, execute model SQL,
produce receipts, or certify a route. Existing publishing behavior is unchanged.

The protected model-database installation must supply these exact endpoints:

| Macro | Fixed SQL endpoint | Required caller state |
| --- | --- | --- |
| `dpone_mssql_attach(model_unique_id)` | `[dpone_physical].[physical_attach_session_v1]` | Outside the model transaction |
| `dpone_mssql_bind_transaction(attach)` | `[dpone_physical].[physical_bind_transaction_v1]` | Inside the new model transaction |
| `dpone_mssql_require_transaction(attach, transaction)` | `[dpone_physical].[physical_require_transaction_v1]` | Inside the same bound transaction |

`attach` and `transaction` are the complete dictionaries returned by the first
and second helpers. Attach returns the frozen 15-column projection; bind and
require return the frozen 12-column projection. The helpers preserve the original
`executor_json` and `plan_json` text and compare request, projection, nested
executor, model and transaction identities. No result dictionary is independent
execution authority or commit evidence.

The trusted caller supplies the closed `__dpone_managed` variable containing only
`generation_id`, `invocation_id`, `runtime_registration_id`, and
`plan_set: {locator, sha256}`. These values locate protected enrollment; callers
cannot provide a database/schema override, replacement executor, or plan payload.
All endpoints use the fixed `dpone_physical` schema. The protected registration
must pin the model database and reject another local schema before reservation.

Each helper returns `none` without database activity when dbt `execute` is false.
During execution it calls dbt's `run_query(sql)`, whose actual statement macro uses
`auto_begin=false`. The caller must begin the model transaction through dbt before
bind. Helpers never open a connection, begin a model transaction, commit, retry,
or reconnect. They reject malformed inputs before issuing SQL, and reject missing,
extra, reordered, NULL, unsupported-version or inconsistent result data.

Transport validation checks canonical UUID/digest spelling, Unicode scalar and
UTF-8 string bounds, SQL integer bounds, closed nested object fields, identifier
width/control characters, duplicate JSON keys, and native JSON byte/depth/token
limits before decoding. It does not independently recompute SHA-256 digests,
derived object names, SQL type normalization, database-collation equivalence, or
canonical original bytes. Protected SQL must authenticate the full retained
canonical executor/plan bytes, their digests, all derived names and domain
semantics, enrollment, source predicates, actual connection identity and current
transaction. Native limits are ceilings; registered tighter metadata bounds are
also enforced by that server. The macro projection cannot replace those checks.

A missing endpoint or driver error propagates. A timeout or lost acknowledgement
is an unknown outcome, including attach which may already have committed its
child-session registration. Do not rerun attach, reconnect and continue, create a
replacement generation, or interpret an absent observation as proof of no commit.
Stop dispatch and use the approved parent settlement/reconciliation workflow.

Full managed execution remains unavailable until the real protected SQL producer,
materialization algorithm, complete receipt codec/producer, independent observer,
and generated authority qualification are supplied. Local Jinja tests prove the
transport boundary only; they do not prove SQL session enforcement, transaction
atomicity, receipt correctness or live certification.
