# Independent scoped provisioner review

Reviewed author commit `9162eb3` (integrated here as `43d9ce3`) against its
`a015eb6` dependency. Reviewer implemented the separate source reader/SQL modules,
not the provisioner. This is a scoped review, not a fresh-context review of the
entire composed feature.

Verdict: PASS for the installation code scope; actual SQL2022 catalog/signature
behavior remains UNVERIFIED until the isolated live fixture runs.

No confirmed blocking findings. The adapter compares literal observed database
pins and requires a privileged same-instance connection, disabled guest/chaining/
TRUSTWORTHY, exact preinstalled public certificate identity and usable private
keys. It installs the deterministic CREATE definitions without ALTER/repair,
countersigns only the control helper, signs only the model entry, grants the
certificate user only helper EXECUTE, verifies complete finite inventories, then
commits. Registration storage starts only after the verified DDL commit; any
installation/commit failure prevents registration and rolls back/closes.

Principal catalog checks retain actual SQL login/user ID/SID mappings, reject
database/server role membership and broad authority, and preserve existing finite
native object EXECUTE grants. Existing native table DENYs are not altered. Source
hash authentication remains an explicit upstream provisioning precondition; no
callback or hash-shaped value is labeled proof. The composition owner must retain
the verified expansion/signature inventory externally for operational evidence.

Verification: the combined source/provisioner unit suite passed (37 tests at
`8626d6e`); own source mypy, import rules, repository Ruff and exact module-size
checks passed. Those mocks test lifecycle and rendering, not SQL semantics.

Follow-up review: `9cef31379f9308c675ff4e6cd7c1771506083ab8`, integrated as
`6dad14f`, PASS. The live fixture confirmed SQL2022 public defaults include
`VIEW ANY COLUMN ENCRYPTION KEY DEFINITION` and
`VIEW ANY COLUMN MASTER KEY DEFINITION`. The correction adds only those two
metadata-visibility permissions to the finite allowlist, preserving EXECUTE,
DML, CONTROL and impersonation restrictions. The reviewer reran all 15
provisioner tests successfully. Runtime SQL/signing qualification still depends
on the separate full live matrix.

Live review amendment: the first SQL2022 installation exposed incorrect
certificate-principal identity comparisons. Principal SIDs match
`sys.certificates.sid`, not `thumbprint`; only `sys.crypt_properties` uses the
thumbprint. This blocks certificate-user verification and also makes the original
extra-certificate-user/server-login checks incomplete. The provisioner owner is
correcting all three locations. Newly created certificate users also carry a
default CONNECT grant; its handling must preserve the intended helper-only
permission boundary. Until the corrective commit and live matrix pass, overall
provisioner integration was NOT READY despite the earlier unit/static PASS.

Corrective follow-up: `f3015b47b8731d434c72e41f6ecd69d39564a131`, integrated as
`fb84b39`, PASS on independent diff review and 15 rerun provisioner tests. All
three principal comparisons now use certificate SID; cryptographic signature
comparisons retain thumbprints. CONNECT is revoked only within the new-user
creation branch in the DDL transaction. Existing extra grants are rejected,
never silently repaired. These confirmed code findings are resolved; final
runtime acceptance remains dependent on the live matrix.

Runtime-signature integration follow-up:
`04c166281d629ed538498876f0c7cf011b62dc18`, integrated as `4cb5067`, PASS.
Both actual certificate thumbprints are acquired only after expected public-byte
and actual database-pin verification. The installer requires exactly one binary
20-byte value per database and equality before deterministic module expansion or
DDL. Missing, ambiguous, malformed or different results fail without installation
or registration. No Python-computed fingerprint or private-key extraction is
used. All 49 combined reader/renderer/provisioner tests passed independently after
integration. The paired runtime signature predicate is a separately reviewed
source change, motivated by the observed SQL2022 countersignature-removal case.

Subsequent live evidence prevents qualification of that intermediate commit:
ordinary signed runtime contexts could not see their cryptographic catalog rows,
so the added exact self-check also rejected valid positive calls. Expected token
continuity alone was experimentally insufficient: a foreign-only countersignature
also retained the entry certificate token after fresh connections/recompilation.
No token-only fallback or broader runtime grants were introduced. The integrator
authorized a disposable probe of VIEW DEFINITION on each exact module for its
certificate-mapped user only; any production permission-contract adjustment must
be explicit and independently reviewed after that measured result.

Measured correction follow-up:
`1410d7853fe594cb010377ff192071bf02737cb5`, integrated as `f9a18be`, PASS
for the scoped implementation review. The approved correction grants the model
certificate user VIEW DEFINITION only on the entry; the control certificate user
has EXECUTE and VIEW DEFINITION only on the helper. Same-database installation
uses the exact three-grant union, after both objects exist. Bidirectional EXCEPT
checks cover permission class, object, minor ID, permission and grant state;
roles, ownership, other certificate principals and server logins remain rejected.
No runtime/public/database/schema/certificate metadata grants are added. All 51
combined focused tests passed independently. The full live matrix remains the
separate required proof that the signed contexts can inspect the exact inventory
while ordinary runtime callers retain no direct helper privilege.
