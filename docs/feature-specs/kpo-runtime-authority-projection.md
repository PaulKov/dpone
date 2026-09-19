# Feature design: strict KPO runtime-authority projection

- Status: APPROVED
- Owner: dpone maintainers
- Issue: TBD
- Target release: 0.81.4
Last verified: 2026-09-19

## Executive summary

Protected development plans already require a current image-installed runtime
authority adapter, but the strict KubernetesPodOperator pod has no closed way to
receive that adapter's external configuration. This extension lets deployment
owners name one Kubernetes Secret key. The provider projects only that reference
into a fixed read-only volume mounted by both `runtime-init-fetch` and
`runtime-pack-exec`; secret values never enter release artifacts, plans, logs,
XCom, or operator arguments.

## Personas and customer journey

| Persona | Goal | Current pain | Success signal |
|---|---|---|---|
| Platform operator | Configure the external authority once per deployment | The image adapter cannot read deployment-owned configuration | Both runtime processes see the same fixed read-only file |
| Security reviewer | Prevent pack/DAG authors from replacing the source | Generic KPO overrides could otherwise become an authority path | The source is deployment-owned, closed, and absent from `operator_overrides` |
| Data developer | Receive a safe failure for incomplete setup | Missing runtime configuration currently fails inside a private adapter | Projection fails before operator creation with a stable public error |

The operator supplies a Secret name and key while building a protected
development deployment. The immutable index carries only those Kubernetes
coordinates. At DAG parse, the provider validates the closed record, adds one
provider-owned volume, mounts it read-only in the init and base containers, and
sets `DPONE_RUNTIME_AUTHORITY_PATH` to the fixed projected file. Missing,
malformed, or misplaced configuration rejects the deployment or pod composition.

## Scope

### In scope

- One vendor-neutral `kubernetes_secret_volume` source with bounded Secret name
  and key.
- Projection only when `development_authority_required` is literally true.
- A fixed volume name, directory, file name, and path environment variable.
- Mapping and Kubernetes-client object pod representations.
- Closed schemas, strict parser validation, operator-override isolation, tests,
  CLI help, reference documentation, and upgrade notes.

### Non-goals

- Defining an authority vendor, payload format, endpoint, identity provider, or
  credential structure.
- Reading or validating Secret values in the scheduler.
- Supporting caller-selected mount paths, environment-variable values,
  ConfigMaps, inline payloads, or `operator_overrides` injection.
- Claiming live Kubernetes certification.

## Public contract

`dpone airflow build` accepts
`--runtime-authority-secret-name` and optional
`--runtime-authority-secret-key` (default `authority.json`). Protected
development deployments require the complete reference. Ordinary and production
indexes reject the authority field. Deployment/index v4 add the required
closed `runtime_authority` object; runtime plan v4 keeps its existing explicit
`development_authority_required: true` marker and contains no Secret value.

The pod contract owns:

```text
volume: dpone-runtime-authority
mount: /run/secrets/dpone/runtime-authority (read-only)
file: /run/secrets/dpone/runtime-authority/authority
env: DPONE_RUNTIME_AUTHORITY_PATH
```

Existing ordinary v2/v3 indexes and plans are byte-compatible. Existing
development deployment-build callers must add the Secret reference; rollback
uses an earlier exact package set and deployment index.

## Detailed algorithm and failure semantics

1. Deployment build detects the existing development-authority release marker.
2. It requires and validates one Secret source without reading its value.
3. The closed deployment/index v4 pair publishes the reference symmetrically;
   the deployment-set v2/v3 contracts and their identity bytes remain unchanged.
4. The parse-safe provider requires the source iff development authority is
   required; any unknown field, invalid DNS name/key, or ordinary-plan source is
   rejected before operator construction.
5. Strict pod composition creates exactly one Secret volume, adds read-only
   mounts to init and base containers, and injects only the fixed path value.
6. Both processes independently invoke the existing image-installed adapter.
7. Retry reconstructs a pod from the same immutable reference. Kubernetes owns
   Secret retrieval; dpone never logs or persists the value.

Empty input, partial CLI pairs, unknown source modes, invalid names/keys,
missing protected-plan configuration, and attempted override collisions fail
closed. Production and ordinary no-authority plans receive no volume, mount, or
path variable.

## Architecture and alternatives

| Component | Change | Responsibility |
|---|---|---|
| Deployment projection | Extend | Validate and publish non-secret Secret coordinates |
| Provider context/parser | Extend | Enforce source/plan co-presence and closed fields |
| Strict pod composer | Extend | Own deterministic volume, mounts, and path env |
| Runtime authority adapter | Unchanged | Interpret the mounted file using private policy |

An inline value or environment-variable payload is rejected because it leaks
through scheduler metadata and pod specs. A configurable mount path is rejected
because it creates collision and override surfaces. ConfigMap support is not
included because authority configuration may contain credentials; a Secret
reference is the safe universal Kubernetes primitive. No new ADR is required:
this is a bounded projection of the existing ADR 0068 authority boundary, not a
new authority or dependency direction.

The existing Airbyte/Fivetran/Cosmos comparison in
`development-runtime-authority.md` remains applicable; this extension makes no
new market superiority claim. The implementation adds one small provider DTO
and pod helper and must remain within existing module and import-graph budgets.

## Test, documentation, rollout, and evidence plan

Hermetic tests cover mapping/object pod specs, init/base mounts, missing and
invalid references, ordinary/production paths, override rejection, canonical
schema behavior, and absence of secret values in serialized artifacts. Focused
tests run first, followed by the repository change-aware checks and full
non-live suite. Live Kubernetes certification is `UNVERIFIED` unless separately
run in an approved environment.

Public Airflow provider documentation explains prerequisites, exact CLI usage,
observable pod shape, safe failures, recovery, and rollback. Rollout upgrades
core, Airflow pack, provider, and runtime image together, creates the Secret in
the deployment namespace, then rebuilds the immutable development deployment.

## Approval checklist

- [x] User problem and journey are clear.
- [x] Public behavior, compatibility, and failure semantics are explicit.
- [x] Secret values and vendor-specific behavior remain out of scope.
- [x] Test, documentation, rollout, and rollback plans are defined.
- [x] Maintainer request explicitly authorizes this implementation.
