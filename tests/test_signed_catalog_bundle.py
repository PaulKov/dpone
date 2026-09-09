from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import datetime
from pathlib import Path

import pytest
import yaml
from jsonschema import Draft7Validator

from dpone._compat import UTC
from dpone.cli import main as cli_main
from dpone.contracts.blob_signature import BlobSignatureVerification
from dpone.contracts.catalog_bundle import (
    CatalogBundleBuildRequest,
    CatalogBundleError,
    CatalogBundleVerifyRequest,
    sha256_bytes,
)
from dpone.services.catalog_bundle_builder import CatalogBundleBuilder
from dpone.services.catalog_bundle_verification import CatalogBundleVerificationService


class _Verifier:
    def __init__(self, result: BlobSignatureVerification | None = None) -> None:
        self.result = result or BlobSignatureVerification.verified("3.0.4")
        self.calls = 0

    def verify_blob(self, **kwargs: object) -> BlobSignatureVerification:
        del kwargs
        self.calls += 1
        return self.result


class _MutatingVerifier(_Verifier):
    def __init__(self, payload_path: Path) -> None:
        super().__init__()
        self._payload_path = payload_path

    def verify_blob(self, **kwargs: object) -> BlobSignatureVerification:
        content = self._payload_path.read_bytes()
        self._payload_path.write_bytes(content.replace(b"postgres.internal", b"postgres.changed"))
        return super().verify_blob(**kwargs)


def _write_yaml(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _registry(root: Path) -> Path:
    path = root / "platform/registries/prod.yaml"
    if path.exists():
        return path
    _write_yaml(
        path,
        {
            "schema": "dpone.connection-registry.v1",
            "environment": "prod",
            "connections": {
                "pg_prod": {
                    "type": "postgres",
                    "connection": {"host": "postgres.internal", "database": "dwh"},
                    "credentials": {
                        "resolver": "vault_kv",
                        "mount": "kv",
                        "kv_version": 2,
                        "path": "dpone/prod/credentials/pg_source",
                        "fields": {"username": "username", "password": "password"},
                        "version_policy": "latest",
                        "resolution_scope": "workload_start",
                    },
                }
            },
        },
    )
    return path


def _build(root: Path):
    source = _registry(root)
    return CatalogBundleBuilder().build(
        CatalogBundleBuildRequest(
            project_root=root.as_posix(),
            kind="connection_registry",
            source=source.relative_to(root).as_posix(),
            bundle_root=".dpone/catalog-bundles",
            publisher_id="data-platform-connections",
            environment="prod",
        )
    )


def _digest(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _recipe_catalog(root: Path) -> Path:
    component = root / "platform/recipes/component.yaml"
    profile = root / "platform/recipes/profile.yaml"
    recipe = root / "platform/recipes/recipe.yaml"
    catalog = root / "platform/recipes/catalog.yaml"
    _write_yaml(
        component,
        {
            "schema": "dpone.component.v1",
            "id": "load-orders",
            "version": "1.0.0",
            "owner": "data-platform",
            "status": "stable",
            "processes": [
                {
                    "name": {"$context": "pipeline_id"},
                    "source": {
                        "type": "mssql",
                        "connection_ref": {"$param": "source_connection_ref"},
                        "table": {"schema": "dbo", "name": "orders"},
                    },
                    "sink": {
                        "type": "clickhouse",
                        "connection_ref": {"$param": "sink_connection_ref"},
                        "table": {"schema": "analytics", "name": "orders"},
                        "strategy": {"mode": "full_refresh"},
                    },
                }
            ],
        },
    )
    _write_yaml(
        profile,
        {
            "schema": "dpone.profile.v1",
            "id": "defaults",
            "version": "1.0.0",
            "owner": "data-platform",
            "status": "stable",
            "values": {},
            "locked_parameters": [],
        },
    )
    _write_yaml(
        recipe,
        {
            "schema": "dpone.recipe.v1",
            "id": "orders",
            "version": "1.0.0",
            "owner": "data-platform",
            "status": "stable",
            "description": "Orders route",
            "domain": "sales",
            "parameter_schema": {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "source_connection_ref": {
                        "type": "string",
                        "default": "mssql_prod",
                        "x-dpone-format": "connection_ref",
                    },
                    "sink_connection_ref": {
                        "type": "string",
                        "default": "clickhouse_prod",
                        "x-dpone-format": "connection_ref",
                    },
                },
                "required": ["source_connection_ref", "sink_connection_ref"],
            },
            "override_allowlist": ["source_connection_ref", "sink_connection_ref"],
            "default_profile_ref": "defaults@1.0.0",
            "profiles": [
                {
                    "ref": "defaults@1.0.0",
                    "artifact_ref": profile.relative_to(root).as_posix(),
                    "sha256": _digest(profile),
                }
            ],
            "components": [
                {
                    "ref": "load-orders@1.0.0",
                    "artifact_ref": component.relative_to(root).as_posix(),
                    "sha256": _digest(component),
                }
            ],
        },
    )
    _write_yaml(
        catalog,
        {
            "schema": "dpone.recipe-catalog.v1",
            "catalog_id": "data-platform",
            "artifacts": [
                {
                    "kind": "recipe",
                    "ref": "orders@1.0.0",
                    "artifact_ref": recipe.relative_to(root).as_posix(),
                    "sha256": _digest(recipe),
                }
            ],
        },
    )
    _write_yaml(
        root / "dpone.yaml",
        {
            "schema": "dpone.project.v1",
            "authoring": {
                "primary_source_policy": "one_per_pipeline",
                "recipe_catalog": {
                    "path": catalog.relative_to(root).as_posix(),
                    "trusted_catalog_ids": ["data-platform"],
                },
            },
        },
    )
    return catalog


def _verification_request(
    root: Path,
    bundle_dir: str,
    *,
    kind: str = "connection_registry",
    publisher_id: str = "data-platform-connections",
) -> CatalogBundleVerifyRequest:
    trusted_root = root / "platform/trust/trusted-root.json"
    sigstore_bundle = root / "platform/trust/catalog.sigstore.json"
    policy = root / "platform/trust/catalog-policy.yaml"
    trusted_root.parent.mkdir(parents=True, exist_ok=True)
    trusted_root.write_bytes(b'{"trusted":"root"}\n')
    sigstore_bundle.write_bytes(b'{"mediaType":"sigstore"}\n')
    _write_yaml(
        policy,
        {
            "schema": "dpone.catalog-trust-policy.v1",
            "policy_id": "prod-catalogs",
            "allowed_kinds": [kind],
            "allowed_publishers": [publisher_id],
            "allowed_environments": ["prod"],
            "certificate_identity": "https://github.com/acme/platform/.github/workflows/catalog.yml@refs/heads/main",
            "certificate_oidc_issuer": "https://token.actions.githubusercontent.com",
            "trusted_root_sha256": sha256_bytes(trusted_root.read_bytes()),
            "verifier": {
                "minimum_version": "3.0.4",
                "maximum_version_exclusive": "4.0.0",
                "timeout_seconds": 30,
            },
        },
    )
    return CatalogBundleVerifyRequest(
        bundle_dir=bundle_dir,
        sigstore_bundle=sigstore_bundle.as_posix(),
        policy=policy.as_posix(),
        trusted_root=trusted_root.as_posix(),
        output=(root / ".dpone/catalog-verification/receipt.json").as_posix(),
    )


def test_connection_registry_bundle_is_content_addressed_and_idempotent(tmp_path: Path) -> None:
    first = _build(tmp_path)
    second = _build(tmp_path)

    assert first.status == "created"
    assert second.status == "no_op"
    assert first.bundle_id == second.bundle_id
    manifest = json.loads(Path(first.manifest_path).read_text())
    assert manifest["bundle_id"] == first.bundle_id
    assert manifest["entrypoint"] == "payload/connection-registry.yaml"
    assert Path(first.bundle_dir, "_SUCCESS").read_text().strip() == first.bundle_id
    schema = json.loads(
        (Path(__file__).parents[1] / "src/dpone/schema/catalog-bundle.schema.json").read_text(encoding="utf-8")
    )
    Draft7Validator.check_schema(schema)
    Draft7Validator(schema).validate(manifest)


def test_recipe_bundle_contains_and_validates_the_complete_pinned_closure(tmp_path: Path) -> None:
    source = _recipe_catalog(tmp_path)
    build = CatalogBundleBuilder().build(
        CatalogBundleBuildRequest(
            project_root=tmp_path.as_posix(),
            kind="recipe_catalog",
            source=source.relative_to(tmp_path).as_posix(),
            bundle_root=".dpone/catalog-bundles",
            publisher_id="data-platform-recipes",
        )
    )
    manifest = json.loads(Path(build.manifest_path).read_text())

    assert build.artifacts == 4
    assert {item["logical_id"] for item in manifest["artifacts"]} == {
        "catalog",
        "recipe/orders@1.0.0",
        "profile/defaults@1.0.0",
        "component/load-orders@1.0.0",
    }
    result = CatalogBundleVerificationService(blob_verifier=_Verifier()).verify(
        _verification_request(
            tmp_path,
            build.bundle_dir,
            kind="recipe_catalog",
            publisher_id="data-platform-recipes",
        )
    )
    assert result.decision == "verified"
    assert result.verified_artifacts == 4


def test_registry_bundle_rejects_inline_secret_values(tmp_path: Path) -> None:
    path = _registry(tmp_path)
    payload = yaml.safe_load(path.read_text())
    payload["connections"]["pg_prod"]["connection"]["password"] = "must-not-enter-bundle"
    _write_yaml(path, payload)

    with pytest.raises(CatalogBundleError) as exc:
        _build(tmp_path)

    assert exc.value.code == "DPONE_CATALOG_CONTENT_INVALID"
    assert "must-not-enter" not in str(exc.value)


def test_catalog_verification_checks_signature_and_semantics(tmp_path: Path) -> None:
    build = _build(tmp_path)
    verifier = _Verifier()
    result = CatalogBundleVerificationService(
        blob_verifier=verifier,
        clock=lambda: datetime(2026, 7, 16, 12, tzinfo=UTC),
    ).verify(_verification_request(tmp_path, build.bundle_dir))

    assert result.decision == "verified"
    assert result.code == "DPONE_CATALOG_BUNDLE_VERIFIED"
    assert result.bundle_id == build.bundle_id
    assert result.verified_artifacts == 1
    assert verifier.calls == 1


def test_catalog_verification_rejects_payload_mutation_before_cosign(tmp_path: Path) -> None:
    build = _build(tmp_path)
    manifest = json.loads(Path(build.manifest_path).read_text())
    payload = Path(build.bundle_dir) / manifest["entrypoint"]
    payload.write_bytes(payload.read_bytes() + b"# mutation\n")
    verifier = _Verifier()
    result = CatalogBundleVerificationService(blob_verifier=verifier).verify(
        _verification_request(tmp_path, build.bundle_dir)
    )

    assert result.decision == "invalid"
    assert result.code == "DPONE_CATALOG_BUNDLE_INTEGRITY_FAILED"
    assert verifier.calls == 0


def test_catalog_verification_rejects_duplicate_manifest_keys_before_cosign(tmp_path: Path) -> None:
    build = _build(tmp_path)
    manifest_path = Path(build.manifest_path)
    content = manifest_path.read_text(encoding="utf-8")
    manifest_path.write_text(content.replace('"schema":', '"schema": "duplicate",\n  "schema":', 1), encoding="utf-8")
    verifier = _Verifier()

    result = CatalogBundleVerificationService(blob_verifier=verifier).verify(
        _verification_request(tmp_path, build.bundle_dir)
    )

    assert result.decision == "invalid"
    assert result.code == "DPONE_CATALOG_BUNDLE_INTEGRITY_FAILED"
    assert verifier.calls == 0


def test_catalog_verification_rechecks_payload_changed_during_cosign(tmp_path: Path) -> None:
    build = _build(tmp_path)
    manifest = json.loads(Path(build.manifest_path).read_text())
    payload = Path(build.bundle_dir) / manifest["entrypoint"]
    verifier = _MutatingVerifier(payload)

    result = CatalogBundleVerificationService(blob_verifier=verifier).verify(
        _verification_request(tmp_path, build.bundle_dir)
    )

    assert result.decision == "invalid"
    assert result.code == "DPONE_CATALOG_BUNDLE_INTEGRITY_FAILED"
    assert result.verified_artifacts == 0
    assert verifier.calls == 1


def test_catalog_verification_rejects_unlisted_and_symlink_payloads(tmp_path: Path) -> None:
    build = _build(tmp_path)
    extra = Path(build.bundle_dir) / "payload/extra.yaml"
    extra.write_text("extra: true\n", encoding="utf-8")
    verifier = _Verifier()
    result = CatalogBundleVerificationService(blob_verifier=verifier).verify(
        _verification_request(tmp_path, build.bundle_dir)
    )
    assert result.code == "DPONE_CATALOG_BUNDLE_INTEGRITY_FAILED"
    assert verifier.calls == 0

    extra.unlink()
    manifest = json.loads(Path(build.manifest_path).read_text())
    payload = Path(build.bundle_dir) / manifest["entrypoint"]
    payload.unlink()
    payload.symlink_to(tmp_path / "platform/registries/prod.yaml")
    second_request = _verification_request(tmp_path, build.bundle_dir)
    second_request = replace(
        second_request,
        output=(tmp_path / ".dpone/catalog-verification/symlink.json").as_posix(),
    )
    result = CatalogBundleVerificationService(blob_verifier=verifier).verify(second_request)
    assert result.code == "DPONE_CATALOG_BUNDLE_INTEGRITY_FAILED"
    assert verifier.calls == 0


def test_catalog_verification_distinguishes_unavailable_verifier(tmp_path: Path) -> None:
    build = _build(tmp_path)
    verifier = _Verifier(BlobSignatureVerification.unverified("verifier_unavailable"))
    result = CatalogBundleVerificationService(blob_verifier=verifier).verify(
        _verification_request(tmp_path, build.bundle_dir)
    )

    assert result.decision == "unverified"
    assert result.code == "DPONE_CATALOG_VERIFIER_UNAVAILABLE"
    receipt = json.loads(Path(_verification_request(tmp_path, build.bundle_dir).output).read_text(encoding="utf-8"))
    schema = json.loads(
        (Path(__file__).parents[1] / "src/dpone/schema/catalog-bundle-verification.schema.json").read_text(
            encoding="utf-8"
        )
    )
    Draft7Validator.check_schema(schema)
    Draft7Validator(schema).validate(receipt)


def test_catalog_bundle_build_cli_is_discoverable_and_machine_readable(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source = _registry(tmp_path)
    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "supply-chain",
                "catalog-bundle-build",
                "--project-root",
                tmp_path.as_posix(),
                "--kind",
                "connection_registry",
                "--source",
                source.relative_to(tmp_path).as_posix(),
                "--bundle-root",
                ".dpone/catalog-bundles",
                "--publisher-id",
                "data-platform-connections",
                "--environment",
                "prod",
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert exc.value.code == 0
    assert payload["schema"] == "dpone.catalog-bundle-build.v1"
    assert payload["status"] == "created"


def test_catalog_bundle_build_cli_uses_validation_exit_without_secret_echo(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source = _registry(tmp_path)
    payload = yaml.safe_load(source.read_text())
    payload["connections"]["pg_prod"]["connection"]["password"] = "must-not-leak"
    _write_yaml(source, payload)
    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "supply-chain",
                "catalog-bundle-build",
                "--project-root",
                tmp_path.as_posix(),
                "--kind",
                "connection_registry",
                "--source",
                source.relative_to(tmp_path).as_posix(),
                "--bundle-root",
                ".dpone/catalog-bundles",
                "--publisher-id",
                "data-platform-connections",
                "--environment",
                "prod",
                "--format",
                "json",
            ]
        )

    output = capsys.readouterr().out
    assert exc.value.code == 1
    assert "DPONE_CATALOG_CONTENT_INVALID" in output
    assert "must-not-leak" not in output
