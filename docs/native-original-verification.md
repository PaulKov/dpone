# Native original verification

Native preflight binds a complete platform policy to an immutable project archive
and an exact pinned workspace activation. It does not run dbt, prepare or activate
a workspace, construct a generation writer, or qualify a runtime.

The Python components described here are an incremental implementation. Policy v4
and generated intent v3 are accepted only by these explicit native components.
The general CLI/compiler schema registry still rejects the new versions. Native
bootstrap, authoring-v2 normalization, registered runtime qualification and the
complete generation-to-delivery route remain unfinished.

## Capture complete policy bytes

`dpone.adapters.native_project_documents.NativeProjectDocuments` owns archive
capture and reading through the existing project bundle operations. The previous
`dpone.runtime.native_project_documents.NativeProjectDocuments` import remains a
compatibility re-export of the same class. Pure full-policy, selection and generated
intent decisions belong to `dpone.contracts.native_project_documents`. Supply a
validated normalized `DbtPublishIntent` and the complete canonical policy-v4 bytes.
The native producer validates the selected profile, workflow, model layout and
strategy. It adds these generated files only to an owned extracted snapshot:

- `dpone/native-policy.json`: the exact selected policy bytes;
- `dpone/native-intent.json`: canonical intent v3 containing the complete policy
  member's relative path, SHA256 and byte count, plus explicit `model_storage`.

The project must explicitly include `dpone` in its dbt `asset-paths`. For example:

```yaml
asset-paths: [dpone]
```

Preserve any existing asset paths. The producer does not edit `dbt_project.yml` or
other source files. Missing asset admission fails after archive verification;
existing generated members must match exactly rather than being overwritten.
The returned archive is extracted and verified again to prove both files actually
participated in capture. Ordinary bundle capture and existing intent-v2 bytes
retain their behavior.

A policy identity covers the full canonical document, including every profile.
A reconstructed subsection or parsed YAML mapping cannot reproduce that original
identity. Floats, duplicate JSON keys, noncanonical bytes, unknown native fields,
invalid locators, and exceeded bounds fail before execution capabilities are used.
An inline storage authority is digest-bound, but decoding it does not verify an
S3 deployment or authorize access to an arbitrary provider.

## Resolve immutable originals

`NativeOriginalsRefV1` contains a local projection root and three `OriginalRef`
values for release, deployment and selected workload pack. Paths are local
acquisition inputs, never hashed authority coordinates. The application must also
supply the existing pinned `AirflowDeploymentIdentity`.

Use `build_native_original_verifier` from
`dpone.app.native_originals_composition` to construct the production capabilities.
Supply an explicit cache root, environment, logical control connection reference,
control schema, SQL connection/statement timeouts, policy/archive byte bounds,
and package environment. Construction does not read files or resolve credentials.
The factory shares one `DeploymentCacheWorkspaceActivationInputs` instance between
source acquisition, the verifier and the existing activation coordinator.

Consume the result while its verifier is alive:

```python
with verifier:
    originals = verifier.resolve(original_refs)
    # originals.project_directory remains owned for this context's lifetime.
    # Pass verified originals to separately qualified native application logic.
```

Verifier instances are confined to one invocation and are not thread-safe. Do not
call `resolve` and `close` concurrently. Closing the verifier removes its own
extracted directories. Do not retain the
returned pathname after close or treat a pathname as a lifetime isolation lease.
The future execution bootstrap must separately protect its execution roots.

The verifier performs these checks in order:

1. Read deployment bytes from the activation projection and release/pack bytes
   from the explicitly configured immutable release root. Check complete hashes,
   computed content identities and the pinned release/deployment coordinates.
2. Check canonical workload descriptor membership and the pack fingerprint.
   Acquire complete sources through the existing source reader, matching their
   inventory and workload hashes to the same release. Every declared project
   archive must fit the configured archive ceiling before complete-source
   acquisition starts, including archives belonging to other workflows.
3. Select exactly one project/workflow owner through the verified DAG. Transfer
   packs use their actual manifest membership and transfer-write owner; workload
   spelling does not define ownership.
4. Match the runtime authority's complete release/deployment hashes, environment
   and identity. Read, extract and verify the selected project archive. Check the
   intent and full policy against their exact inventory members and selected
   workflow. Confined readers reject symlink/path escape and changed bytes.
5. Invoke the pinned ACTIVE callback only after successful offline preflight.
   Compare its returned request and receipt to the earlier source inventory,
   runtime authority, environment and pinned occurrence.

Release and deployment content IDs are distinct from full-byte SHA256 values:
release content identity deliberately excludes provenance. Both checks are
required. Metadata reads use the smaller of the configured archive bound and the
existing 8 MiB workload metadata ceiling; project reads use the explicit archive
bound. Policy and intent additionally obey the existing native JSON ceiling.

## Read the pinned activation

An activation projection contains deployment files, not its sibling release
archives. The factory constructs the release root from its configured cache and
pinned release ID. It never derives a trusted cache by walking projection parents.

The immutable release, deployment and pack do not contain an activation UUID or
previous deployment. A mutable current pointer could refer to a newer activation
of the same deployment, so it cannot supply a pinned invocation's authority.

After preflight, the callback reads the durable activation row by the pinned UUID,
checks ACTIVE state and exact source/runtime coordinates, and retains its actual
previous deployment value. Missing, duplicate, partial, foreign, PREPARED,
RETIRING or RETIRED rows fail closed. It then calls the existing coordinator's
`require_active`, which independently validates the complete request and guards.
Neither lookup prepares or activates a workspace. Query/connection bounds for the
new lookup are explicit; existing coordinator reads retain their own policy.

## Evidence and remaining work

Focused checks exercise real project archives and release/source readers,
policy-member substitution, confined reads, transfer ownership, pinned activation
mismatches, directory lifetime and the callback's actual lookup composition.
The callback tests use a SQL connection test double; they do not certify a live
native activation route. No production credentials or endpoints are needed.

Independent actual SQL evidence for source admission is described in
[native source admission closure](native-source-admission-closure.md). The earlier
recorder checks are described in [trusted dbt invocation](native-generation-execution.md).
Those component results do not establish installed native route qualification,
three-replica publication, or release readiness. Architecture gates and the
programme's unfinished runtime, authoring and delivery work remain applicable.
