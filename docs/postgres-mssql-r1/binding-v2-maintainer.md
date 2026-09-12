
> Historical provenance: “source record NNN” names a privately archived original, not a Git ref, executable task grant or current validation result. Outcomes attached to these references retain their original historical scope. Current execution requires a separate genuine public authority chain.

<!-- Private migration draft: public bindings and approval transfer are PENDING. -->

# Binding V2 maintainer guide

> Historical development evidence below describes source-side work. It does not certify this migration candidate. Public commit binding and the current binding inventory remain pending; no passing artifact is implied by a historical test count.

This guide is for dpone framework maintainers integrating the internal
PostgreSQL to MSSQL R1 V3 provider. Binding V2 is a pure compiler for portable
contract bytes. It is not a CLI command, manifest surface, public Python API or
self-service activation path. The route remains activation-blocked and
vendor-live behavior remains `UNVERIFIED`.

Start with the [provider implementation map](../developer-postgres-mssql-r1-v3-provider-implementation.md).
The normative behavior is defined by the [Binding V2 specification](../feature-design-postgres-mssql-r1-v3-provider-binding-contract-v2.md).

## Inputs

Obtain all six inputs from their accepted composition or verification
factories. Do not construct physical names, identifiers or authority bytes from
user input:

1. physical descriptor R2;
2. shared Security V2 profile;
3. Binding signer lifecycle policy;
4. selected-relation source-schema authority from the verified PostgreSQL
   snapshot issuer;
5. rotation-stable target authority;
6. registered-target catalog from the same target binding.

The factory exact-validates types, individually canonical-round-trips every
authority, derives the pack and then independently decodes and re-derives the
finished bytes.

## Restricted invocation

These imports are internal and intentionally absent from package exports:

```python
from dpone.contracts.mssql_r1_v3_binding_pack import (
    MssqlR1BindingModulePackFactoryV2,
)
from dpone.contracts.mssql_r1_v3_binding_validation import (
    MssqlR1BindingContractErrorV2,
)

factory = MssqlR1BindingModulePackFactoryV2()
try:
    pack = factory.create(
        physical_descriptor=descriptor,
        shared_security_profile=security_profile,
        binding_signer_lifecycle_policy=binding_signer_lifecycle_policy,
        source_schema_authority=source_schema_authority,
        stable_target_authority=stable_target,
        target_catalog=target_catalog,
    )
except MssqlR1BindingContractErrorV2 as exc:
    diagnostic = {
        "reason": exc.reason,
        "recovery_class": exc.recovery_class,
        "correlation_id": caller_owned_correlation_id,
    }
else:
    diagnostic = {
        "binding_digest": pack.digest.hex(),
        "module_count": len(pack.portable_identity.ordered_modules),
        "status": "activation_blocked",
    }
    restricted_pack_bytes = pack.canonical_bytes
```

A successful R1 pack has `module_count=6`. Observable success is limited to
the digest, module count and `activation_blocked` status. Observable failure is
limited to the stable reason, recovery class and a caller-owned correlation ID.
Never log or display the complete pack, physical identifiers, authority
payloads, credentials or secrets.

## Retry and handoff

Compilation is deterministic, side-effect-free and performs no I/O. After the
recovery action indicated by `recovery_class`, retry with a newly verified,
complete set of six authorities. Do not patch canonical bytes or reuse a pack
across a changed source or target generation.

Only the complete `pack.canonical_bytes` value is a valid downstream handoff.
Pass it through the restricted internal channel to future Migration V2 and
Renderer V2 implementations; a digest is diagnostic identity, not a substitute
payload. Those consumers remain separately specified, unimplemented and
activation-blocked.

## Historical source evidence and status

The accepted internal implementation is
`PENDING_PUBLIC_COMMIT_BINDING`. Its immutable candidate evidence
is `binding-inventory.json` (pending public commit binding; artifact not produced for this candidate)
at evidence head `source record 026`, SHA-256
`188ccc52c73c0c2f099de234fbbee519fe80fe5eeadbf0e6560195abe80889fc`.

The 269 executable nodes include contract, mutation-inventory and semantic
inventory tests. The 246 semantic cases are the closed reader/factory phase-pair
universe inside that larger executable set; these numbers describe different
denominators and must not be conflated. All are hermetic `PASS`, while
certification remains `UNVERIFIED`.

Next work is governed independently by the
[approved Migration V2 specification](../feature-design-postgres-mssql-r1-v3-provider-migration-contract-v2.md)
and [Renderer research contract](../feature-design-postgres-mssql-r1-v3-provider-renderer-contract-v1.md).
Neither Migration V2 approval nor the Renderer research document authorizes
activation.
