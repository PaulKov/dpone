from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.airflow_cache_promotion_test_support import (
    assert_no_promotion_side_effects,
    load_json,
    write_cache_fixture,
    write_json,
)


def test_repeated_exact_promotion_has_distinct_persisted_activation_ids(
    tmp_path: Path,
) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = write_cache_fixture(cache_root)
    activation_ids = iter(
        (
            "11111111-1111-4111-8111-111111111111",
            "22222222-2222-4222-8222-222222222222",
        )
    )
    materializer = DeploymentCacheMaterializer(
        cache_root,
        activation_id_factory=lambda: next(activation_ids),
    )

    first = materializer.promote(fixture.deployment, environment="dev")
    second = materializer.promote(fixture.deployment, environment="dev")

    pointer = load_json(cache_root / "current-pointer.json")
    audits = [
        json.loads(line)
        for line in (cache_root / "current-pointer-audit.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert first.activation_id == "11111111-1111-4111-8111-111111111111"
    assert second.activation_id == "22222222-2222-4222-8222-222222222222"
    assert pointer["activation_id"] == second.activation_id
    assert [item["activation_id"] for item in audits[-2:]] == [
        first.activation_id,
        second.activation_id,
    ]


def test_promotion_uses_authoritative_activation_id_without_local_generation(
    tmp_path: Path,
) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = write_cache_fixture(cache_root)
    activation_id = "33333333-3333-4333-8333-333333333333"
    materializer = DeploymentCacheMaterializer(
        cache_root,
        activation_id_factory=lambda: (_ for _ in ()).throw(AssertionError("local generation used")),
    )

    current = materializer.promote(
        fixture.deployment,
        environment="dev",
        activation_id=activation_id,
    )

    assert current.activation_id == activation_id
    assert load_json(cache_root / "current-pointer.json")["activation_id"] == activation_id


def test_promotion_rejects_non_v4_factory_activation_id(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = write_cache_fixture(cache_root)

    with pytest.raises(
        DeploymentCacheError,
        match="activation_id must be a canonical UUIDv4",
    ) as exc:
        DeploymentCacheMaterializer(
            cache_root,
            activation_id_factory=lambda: "11111111-1111-1111-8111-111111111111",
        ).promote(fixture.deployment, environment="dev")

    assert exc.value.code == "DPONE_CURRENT_POINTER_ACTIVATION_ID_INVALID"
    assert_no_promotion_side_effects(cache_root)


def test_promotion_rejects_non_v4_authoritative_activation_id_before_mutation(
    tmp_path: Path,
) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = write_cache_fixture(cache_root)
    release_dir = cache_root / "releases" / fixture.release_id.replace(":", "-", 1)
    modes = {
        path.relative_to(release_dir).as_posix(): path.stat().st_mode for path in (release_dir, *release_dir.rglob("*"))
    }

    with pytest.raises(DeploymentCacheError, match="activation_id must be a canonical UUIDv4") as exc:
        DeploymentCacheMaterializer(cache_root).promote(
            fixture.deployment,
            environment="dev",
            activation_id="11111111-1111-1111-8111-111111111111",
        )

    assert exc.value.code == "DPONE_CURRENT_POINTER_ACTIVATION_ID_INVALID"
    assert_no_promotion_side_effects(cache_root)
    assert {
        path.relative_to(release_dir).as_posix(): path.stat().st_mode for path in (release_dir, *release_dir.rglob("*"))
    } == modes


def test_promotion_rejects_invalid_authoritative_activation_id(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = write_cache_fixture(cache_root)

    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheMaterializer(cache_root).promote(
            fixture.deployment,
            environment="dev",
            activation_id="not-a-uuid",
        )

    assert exc.value.code == "DPONE_CURRENT_POINTER_ACTIVATION_ID_INVALID"
    assert_no_promotion_side_effects(cache_root)


def test_audit_repair_preserves_legacy_pointer_without_activation_id(
    tmp_path: Path,
) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = write_cache_fixture(cache_root)
    materializer = DeploymentCacheMaterializer(cache_root)
    materializer.promote(fixture.deployment, environment="dev")
    pointer_path = cache_root / "current-pointer.json"
    pointer = load_json(pointer_path)
    pointer.pop("activation_id")
    write_json(pointer_path, pointer)
    (cache_root / "current-pointer-audit.jsonl").unlink()

    repaired = materializer.repair_audit(
        environment="dev",
        recovery_actor="ci://recovery",
        expected_current_deployment_id=fixture.deployment_id,
    )

    assert repaired.activation_id is None
    assert "activation_id" not in repaired.to_dict()
    assert "activation_id" not in load_json(pointer_path)
    audit = json.loads((cache_root / "current-pointer-audit.jsonl").read_text(encoding="utf-8"))
    assert "activation_id" not in audit
