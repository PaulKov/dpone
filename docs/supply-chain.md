# Supply-chain evidence

`dpone supply-chain attest` creates release evidence for OSS and internal CI:

- SPDX-like SBOM JSON;
- CycloneDX-like SBOM JSON;
- SLSA/in-toto-inspired provenance JSON;
- optional local HMAC signature envelope;
- one attestation bundle with checksums.

This command is dependency-light and works without external signing services.
For public releases, the release workflow pairs it with GitHub Artifact
Attestations and verifies the GitHub attestation receipts before publishing.
The scoped release posture is documented in
[SLSA release self-assessment](supply-chain-slsa.md).

## Contents

- [Quickstart](#quickstart)
- [Artifacts](#artifacts)
- [Signing model](#signing-model)
- [CI pattern](#ci-pattern)
- [Runbook](#runbook)
- [Related docs](#related-docs)

## Quickstart

```bash
uv build

subjects=()
for artifact in dist/*.whl dist/*.tar.gz; do
  subjects+=(--subject "$artifact")
done

uv run dpone supply-chain attest \
  --project-root . \
  --output-dir test_artifacts/supply-chain/current \
  --release vX.Y.Z \
  "${subjects[@]}" \
  --repository https://github.com/PaulKov/dpone \
  --commit-sha "$GITHUB_SHA" \
  --builder-id "github-actions:$GITHUB_RUN_ID" \
  --signing-key "$DPONE_LOCAL_ATTESTATION_KEY" \
  --signing-key-id github-actions \
  --format json
```

## Artifacts

| File | Purpose |
| --- | --- |
| `sbom.spdx.json` | SPDX-like dependency inventory generated from `pyproject.toml`. |
| `sbom.cyclonedx.json` | CycloneDX-like component inventory generated from `pyproject.toml`. |
| `provenance.intoto.json` | Release subjects, SHA-256 digests, repo, commit, builder, and release metadata. |
| `signature.hmac-sha256.json` | Local HMAC signature envelope for deterministic CI evidence. |
| `supply_chain_attestation.json` | Bundle index with artifact paths and checksums. |
| `supply_chain_attestation.md` | Human-readable release evidence summary. |

## Signing model

The built-in signature is a local HMAC-SHA256 envelope. It proves that the same
CI environment with access to the same secret signed a specific provenance
digest. It is useful for internal evidence chains and deterministic testing.

It is not a public identity replacement for:

- GitHub artifact attestations;
- Sigstore/cosign;
- hardware-backed code signing;
- cloud KMS signing.

Use external identity-backed signing for public release trust, and attach the
dpone bundle as additional evidence.

## CI pattern

The following is a source-workflow/local attestation pattern, not the active
ordinary PyPI publisher. The external controller builds its own bytes and does
not run this HMAC/GitHub attestation sequence. See [Release](release.md) for
current authority and the original controller manifest/retrospective receipt.
Use this pattern only for an explicitly scoped supply-chain evidence task:

```bash
uv sync --all-extras
uv run pytest -m "not integration_live"
uv build
uv run twine check dist/*

subjects=()
for artifact in dist/*.whl dist/*.tar.gz; do
  subjects+=(--subject "$artifact")
done

uv run dpone supply-chain attest \
  --release "$GITHUB_REF_NAME" \
  "${subjects[@]}" \
  --repository "$GITHUB_SERVER_URL/$GITHUB_REPOSITORY" \
  --commit-sha "$GITHUB_SHA" \
  --builder-id "github-actions:$GITHUB_RUN_ID" \
  --signing-key "$DPONE_LOCAL_ATTESTATION_KEY" \
  --signing-key-id github-actions \
  --output-dir test_artifacts/supply-chain/current \
  --format json

mkdir -p test_artifacts/supply-chain/github-attestations
for artifact in dist/*.whl dist/*.tar.gz; do
  gh attestation verify "$artifact" \
    --repo "$GITHUB_REPOSITORY" \
    --source-ref "$GITHUB_REF" \
    --source-digest "$GITHUB_SHA" \
    --format json \
    > "test_artifacts/supply-chain/github-attestations/$(basename "$artifact").github-attestation.json"
done
```

Set `DPONE_LOCAL_ATTESTATION_KEY` as a release environment secret. Upload
`test_artifacts/supply-chain/current/` and
`test_artifacts/supply-chain/github-attestations/` as release evidence
artifacts within that evidence task. Do not add secrets or source execution to
the controller's artifact-only publish job to reproduce this pattern.

## Runbook

| Symptom | Likely cause | Action |
| --- | --- | --- |
| `subjects.empty` | No `--subject` files were provided. | Run `uv build` first and pass wheel/sdist paths. |
| `subject.missing.<path>` | A subject path does not exist. | Check glob expansion and working directory in CI. |
| `signature.missing_key` | No local signing key was provided. | Set `DPONE_LOCAL_ATTESTATION_KEY` or run external Sigstore/GitHub attestation separately. |
| `gh attestation verify` fails | GitHub attestation has not propagated, the artifact digest changed, or the source repo/ref/digest does not match. | Preserve the original bytes and repeat read-only verification after propagation. Investigate identity mismatch; do not rerun publication or rebuild an original artifact as replacement evidence. |
| SBOM misses a dependency | Dependency is not declared in `pyproject.toml`. | Add dependency metadata or extend the SBOM reader for lockfile evidence. |
| Public users cannot verify trust | HMAC is local-only evidence. | Add GitHub artifact attestation or Sigstore/cosign in the release workflow. |

## Related docs

- [Developer supply-chain guide](developer-supply-chain.md)
- [SLSA release self-assessment](supply-chain-slsa.md)
- [Developer CI/CD guide](developer-ci-cd.md)
- [Release](release.md)
- [Production readiness](production-readiness.md)
