# Exact-SHA compatibility candidate

PR5A creates a local, diagnostic inventory for the three dpone compatibility
wheels. It does not execute a wheel, authenticate a GitHub artifact, authorize a
merge or release, or change any required CI check. Those authority decisions
remain deliberately deferred to PR5B and later stages.

## Create a candidate manifest

Build exactly these three wheels into a new directory: `dpone`,
`dpone-airflow-pack`, and `apache-airflow-providers-dpone`. Then write the
manifest to a **new** path outside that directory:

```bash
uv run python tools/ci/build_candidate_manifest.py \
  --dist dist/compatibility-wheels \
  --output test_artifacts/compatibility-candidate.json
```

Success is silent and writes one canonical JSON document. Its three entries are
filename-sorted and bind filename, normalized distribution and version, byte
size, and SHA-256. `inventory_digest` is the SHA-256 of the canonical document
without that field. The public shape is defined by
[`compatibility-candidate-v1.schema.json`](../schemas/cicd/compatibility-candidate-v1.schema.json).

The output is create-new only. A retry needs a fresh `--output` path; never edit
or reuse an old manifest. This preserves a producer attempt's artifact identity.

## Recovery

`CANDIDATE_MANIFEST_UNVERIFIED` means no candidate was produced. Delete no
existing manifest. Repair the source directory and run the command again with a
new output name.

- Wrong count, an extra file, symlink, directory, malformed wheel, version
  mismatch, or changed input: rebuild a clean three-wheel directory.
- Existing output: retain it and choose a distinct output path.
- I/O failure: investigate the destination volume; a partially created file is
  removed only when this invocation created it.

There is no hosted-live success claim in PR5A. A default-branch, authenticated
producer and verifier are later work; until then the manifest is local evidence
only.
