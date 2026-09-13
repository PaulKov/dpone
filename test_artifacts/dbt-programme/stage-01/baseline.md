# Stage 01: dbt configuration and execution baseline

Observed source: `46830976b214262c7772800523e832a5a6f6d78f` (0.79.2),
equal to the local `origin/master` at task start on 2026-09-13.
The worktree was clean and detached before the baseline. Subsequent changes are
identified separately in the validation report. Synthetic data only; no live
database, corporate source, credential backend, release or production action.

This report is design input, not certification. Its current-code observations
are independent of the proposed changes in [design-draft.md](design-draft.md).
The coordinator granted bounded analysis, five-page documentation corrections
and the isolated interval fix; shared semantic files remain coordinator-owned.

## Execution map

1. `adapters/dbt_publish_artifact_reader.py:124` reads resolved dbt database,
   schema and alias into `DbtModelArtifact.relation`
   (`contracts/dbt_publish_models.py:78`).
2. `manifest/dbt_publish_profiles.py:72` validates the closed policy before
   mapping source/sink options. Current ordinary publishing uses policy v3 and
   `dbt-sqlserver-1.11-core-1.12-certified` (Core 1.12.3, adapter 1.11.1).
3. `services/dbt_publish_compiler.py:297` negotiates strategy-specific route
   capability using profile certification coordinates. It does not infer route
   certification from a native-transfer mode setting.
4. `services/dbt_publish_model_compiler.py:85` copies admitted options, overlays
   generated extraction-window options and supplies schema/physical/lineage
   policy. Source identity is dbt-resolved. Target author overrides precede
   profile defaults. MSSQL sink pure projection carries source model database;
   that observation alone does not prove route support.
5. `services/dbt_project_artifacts.py:146` emits each transfer manifest under
   `_dbt/manifests/`, then ordinary strict Airflow transfer packs. A separate
   `DbtWorkflowReleasePlan` builds the transformation execution pack with one
   logical source database/schema per workflow. It has no transfer option map.
6. `readiness/dbt_airflow_execution_pack.py:83` invokes `dpone dbt execute-pack`
   for the transformation. Sibling transfer tasks use `dpone run`.
7. `runtime/bootstrap_runner.py:93` hydrates ordinary endpoints and supplies
   `NativeTransferRuntimeService` to ETL. MSSQL → ClickHouse native execution
   already exists. The explicit `native_runtime_factory` guard at line 58 is for
   ClickHouse → MSSQL `mssql_native`/`bounded_stream`, a different route.
8. Semantic-refresh V2 emits non-executable templates instead of ordinary
   transfer manifests. Release-v3 composition is another authority: ADR 0059
   keeps activation closed until every physical writer is admitted and fenced.

All code paths above are relative to `src/dpone/` at the observed source SHA.

## Capability and gap matrix

| Input or capability | Observed baseline | Evidence / consequence |
|---|---|---|
| `native_transfer: {mode: auto}` | Accepted, preserved | `profile-baseline.json`, registry/compiler probe |
| `native_transfer: {}` | Accepted | No richer capability implied |
| `mode: required` or `off` | Rejected | Closed dbt policy enum; new design required |
| `native_transfer.wire` / `.execution` | Rejected | Runtime options exist below the authoring boundary |
| source fetch-size / TLS options | Rejected | Profile does not own connection TLS |
| sink `load_governance.audit` | Accepted, preserved | Other compiler-added options are separate policy |
| sink `settings` / `clickhouse_bulk` | Rejected | No sink tuning surface in current dbt profile |
| source database/schema/alias | Preserved in manifest | `warehouse`, `mart`, `events_view` |
| parsed MSSQL schema label | `warehouse.mart` | Canonical compatibility normalization; database remains explicit |
| ClickHouse target | `schema=reporting`, `name=events_target` | Here schema means ClickHouse physical database |
| old toolchain under v3 | Rejected | Active docs incorrectly prescribed it before this patch |
| dbt TLS booleans | Accepted | String `yes` rejected by renderer |
| dbt missing schema | `DPONE_DBT_PROFILE_INVALID` | Missing schema in example runtime registry is a follow-up gap |
| dbt mismatched schema | `DPONE_DBT_TARGET_IDENTITY_MISMATCH` | Fails before rendering/execution |
| secure CH connector → bulk HTTP | Defaults false / 8123 | Explicit transport settings work; inheritance is not established |
| secure CH connector → bulk native TCP | Defaults false / 9000 | Both native builders agree; explicit values work |
| secure CH connector → client mode | Inherits true / 9440 in probe | Different omission semantics from HTTP/native TCP |
| custom CA | Retained on connector; HTTP forwards; native drops | Bulk credential contracts lack CA field; no handshake run |
| compiled lower interval bound | Leaves `{{timestamp}}` after runtime binding | Confirmed isolated bug; fixed separately |
| composition activation | Fail closed at this baseline | Existing native execution does not activate composition |

Source admission is `contracts/dbt_publish_schema_contract_policy.py:154–194`.
V1 bytes are frozen by `tests/test_dbt_publish_schema_contracts.py`; shared
schema helpers must not widen historical policy versions accidentally.

## Authority and precedence

- Model identity is the resolved manifest's database/schema/alias; do not
  reconstruct schema using adapter defaults or concatenate it again.
- Model target overrides beat profile target defaults. The runtime must still
  prove the configured relation belongs to the deployment's physical target.
- Compiler-generated source window options overlay the copied source profile;
  physical/schema/readiness policy overlays the copied sink profile. This is
  established precedence, not permission to inject arbitrary profile options.
- `BindingCredentialResolver` owns one resolved connection snapshot.
  `connection.host/port/database/schema/secure` are registry-owned and cannot
  be supplied through credential maps (`binding_resolver.py:263–303`).
- The dbt renderer uses credential driver/TLS fields before descriptor fallback:
  driver, encrypt, trust_cert. Values for TLS must be booleans. Fixed adapter
  policy remains pyodbc, retries=1, login timeout=15, query timeout=process−300.
- Ordinary MSSQL connector construction also reads `additional_params` with
  `trust_server_certificate`/`TrustServerCertificate` spellings. Arbitrary
  adapter-option equivalence across dbt, control SQL and BCP is unproven.
- Ordinary `dpone run` interval CLI values override `DPONE_INTERVAL_*` env
  values (`services/interval_context.py:46`). `dbt execute-pack` has only a
  relative pack path and JSON format; scheduler intervals are required and
  become the two locked `dpone_data_interval_*` vars. Setting interval env alone
  does not create release, deployment, attempt or connection authority.

## Scope handoff to successor stages

| Interface input | Required distinction / consumer |
|---|---|
| Execution family | native ordinary publishing, semantic-refresh V2, composition parent |
| Relation | source database/schema/table plus connection ref; CH database/table; resolved physical identity separate |
| Extraction scope | whole snapshot or explicit UTC half-open window; exact predicate and lookback |
| Partition scope | physical expression, evaluated type, complete-partition proof; never infer completeness from column alone |
| Empty behavior | V2 empty window does not exchange/advance generation; composition whole empty snapshot replaces with empty |
| Transfer options | admitted source wire/execution vs sink ingest settings; route certification coordinates separate |
| Connection snapshot | credential-safe driver/TLS/port/CA projection and precedence, endpoint identity, resolver version |
| Publication/recovery | finalizer capability, owner/attempt, journal and commit/evidence/checkpoint order; stage 03 owns analysis |
| Physical/resources | layout, partition limits and bounded execution; PR 48 owns independent DDA limits |
| Diagnostics | machine-readable effective configuration and unavailable metrics; no authority from diagnostic output |

Stage 03 specifically identified that forwarding `toYYYYMM(event_date)` to a
partition replacement while extracting a daily window could erase other days.
The brace fix restores the existing window; it neither fixes nor widens this
separate partition-completeness contract. No empty/vanished partition behavior
is changed here. Durable identity collision hypotheses remain UNVERIFIED.

## Approval coverage and external overlap

- APPROVED existing toolchain migration:
  `docs/feature-design-dbt-1-12-sqlparse-security-upgrade-v1.md`.
- Existing native self-service authority: ADR 0034; workspace child authority:
  ADR 0052. Composition foundation: ADR 0059 and
  `docs/feature-specs/composition-activation-execution.md`; approval of a
  foundation does not establish missing concrete execution/certification.
- Coordinator granted only active docs corrections and the isolated brace fix,
  each via a validated narrow task contract. Profile expansion is not approved.
- `pr-overlap.json` records public GitHub metadata, not integrated code.
  PR 42 (`4ce797dfe52dd00cfdc79b525317c0dde9a44e40`) and PR 43
  (`a04c00ed48b3adc01919e122c6f1a9b18da4f088`) overlap composition execution.
  PR 48 (`36e90087bff89631db65a3a47702112803261c3a`) overlaps DDA limits and
  `native_transfer_execution.py`. These are coordinator reservations.
  None is taken as current-master authority or permission to cherry-pick.

## Reproduction and limits

`probe_profile.py` produces admission, compiler, interval and renderer JSON.
`profile-baseline.json` was produced before the one-line implementation change;
its interval status is FAIL. `probe_tls.py` blocks sockets/subprocesses and
produces `tls-observations.json`. Its PASS means current behavior reproduced,
including the projection gaps, not secure live execution.

The test specialist executed 233 selected existing tests: 230 passed in a
cached minimal Python 3.12.11/pytest 9.1.1 environment; three isolated-import
cases failed because `python -I` ignored PYTHONPATH. Only those three were
rerun in the frozen editable project environment (pytest 9.0.3), and passed.
One real-dbt-parse test was deliberately deselected at baseline. This is
agent-reported evidence retained in `validation.md`, not a fabricated full raw
suite log. Live certification is SKIP. No performance claim is made.

The baseline is ready for design review. See `validation.md` for the separate
correction checks, risks and candidate readiness.
