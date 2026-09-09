"""Read the dependency-free authority baseline sealed in release verifiers."""

from __future__ import annotations

import hashlib
import hmac
import importlib.util
import json
import re
import stat
import sys
from pathlib import Path
from typing import Any, NamedTuple

SCHEMA = "dpone.release_authority_baseline.v1"
POLICY_PATH = ".agents/policy/github-branch-protection.yml"
POLICY_FILENAME = "github-branch-protection.yml"
BASELINE_FILENAME = "github-branch-protection.release-authority.json"
MAX_FILE_BYTES = 1024 * 1024
_DIGEST = re.compile(r"^[0-9a-f]{64}$")


def _load_sibling(module_name: str, filename: str) -> Any:
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


live_ruleset = _load_sibling("dpone_release_authority_live_ruleset", "governance_live_ruleset.py")


class ReleaseAuthorityBaseline(NamedTuple):
    """Reviewed authority semantics independent from mutable CI receipts."""

    ruleset_id: int
    policy_sha256: str
    policy_projection_sha256: str
    integration_ids: dict[str, int]
    projection: dict[str, Any]
    privileged: dict[str, Any]

    @property
    def context_names(self) -> tuple[str, ...]:
        """Return the exact canonical required-check name set."""

        return tuple(sorted(self.integration_ids))

    @property
    def full_projection(self) -> dict[str, Any]:
        """Return observable and privileged ruleset fields as one projection."""

        return {
            **self.projection,
            "bypass_actors": self.privileged["bypass_actors"],
            "version_id": self.privileged["version_id"],
        }


def load_sibling_release_authority(anchor: Path) -> ReleaseAuthorityBaseline:
    """Load the two fixed authority files sealed beside an entrypoint."""

    directory = anchor.parent
    return load_release_authority_baseline(
        directory / BASELINE_FILENAME,
        policy_path=directory / POLICY_FILENAME,
    )


def load_release_authority_baseline(
    path: Path,
    *,
    policy_path: Path,
) -> ReleaseAuthorityBaseline:
    """Validate a closed baseline and bind it to the exact reviewed YAML bytes."""

    payload = _json_object(_read_regular(path, label="release authority baseline"))
    expected_fields = {
        "schema",
        "policy_path",
        "policy_sha256",
        "ruleset_id",
        "required_check_producers",
        "ruleset_projection",
        "privileged_baseline",
        "policy_projection_sha256",
    }
    if set(payload) != expected_fields or payload.get("schema") != SCHEMA:
        raise ValueError("release authority baseline schema is invalid")
    if payload.get("policy_path") != POLICY_PATH:
        raise ValueError("release authority baseline policy path is invalid")
    policy_sha256 = _sha256(payload.get("policy_sha256"), label="policy")
    observed_policy_sha256 = hashlib.sha256(_read_regular(policy_path, label="closed branch policy")).hexdigest()
    if not hmac.compare_digest(policy_sha256, observed_policy_sha256):
        raise ValueError("release authority baseline does not bind the closed branch policy")
    ruleset_id = _positive_int(payload.get("ruleset_id"), label="ruleset id")
    integration_ids, producers = _producers(payload.get("required_check_producers"))
    projection = _projection(payload.get("ruleset_projection"), ruleset_id=ruleset_id, producers=producers)
    privileged = _privileged(payload.get("privileged_baseline"), projection=projection)
    received_digest = _sha256(payload.get("policy_projection_sha256"), label="policy projection")
    full_projection = {
        **projection,
        "bypass_actors": privileged["bypass_actors"],
        "version_id": privileged["version_id"],
    }
    expected_digest = hashlib.sha256(_canonical_json(full_projection)).hexdigest()
    if not hmac.compare_digest(received_digest, expected_digest):
        raise ValueError("release authority baseline projection digest is invalid")
    return ReleaseAuthorityBaseline(
        ruleset_id=ruleset_id,
        policy_sha256=policy_sha256,
        policy_projection_sha256=received_digest,
        integration_ids=integration_ids,
        projection=projection,
        privileged=privileged,
    )


def _read_regular(path: Path, *, label: str) -> bytes:
    try:
        before = path.lstat()
        if path.is_symlink() or not stat.S_ISREG(before.st_mode) or not 0 < before.st_size <= MAX_FILE_BYTES:
            raise ValueError(f"{label} must be one bounded regular file")
        raw = path.read_bytes()
        after = path.lstat()
    except OSError as exc:
        raise ValueError(f"{label} is unavailable") from exc

    def identity(value: Any) -> tuple[int, ...]:
        return (value.st_dev, value.st_ino, value.st_mode, value.st_size, value.st_mtime_ns)

    if len(raw) != before.st_size or identity(before) != identity(after):
        raise ValueError(f"{label} changed while it was read")
    return raw


def _json_object(raw: bytes) -> dict[str, Any]:
    try:
        payload = json.loads(raw.decode("utf-8", "strict"), object_pairs_hook=_unique_object)
    except (UnicodeError, json.JSONDecodeError, ValueError) as exc:
        raise ValueError("release authority baseline must be unique-key UTF-8 JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError("release authority baseline must be one JSON object")
    return payload


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _producers(raw: Any) -> tuple[dict[str, int], list[dict[str, Any]]]:
    if not isinstance(raw, list) or not raw:
        raise ValueError("release authority required-check producers are unavailable")
    producers: list[dict[str, Any]] = []
    integration_ids: dict[str, int] = {}
    for item in raw:
        if not isinstance(item, dict) or set(item) != {"context", "integration_id"}:
            raise ValueError("release authority required-check producer is invalid")
        context = item.get("context")
        if not isinstance(context, str) or not context or context != context.strip() or context in integration_ids:
            raise ValueError("release authority required-check context is invalid")
        integration_id = _positive_int(item.get("integration_id"), label="integration id")
        integration_ids[context] = integration_id
        producers.append({"context": context, "integration_id": integration_id})
    expected = sorted(producers, key=lambda item: str(item["context"]))
    if producers != expected:
        raise ValueError("release authority required-check producers are not canonical")
    return integration_ids, producers


def _projection(raw: Any, *, ruleset_id: int, producers: list[dict[str, Any]]) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError("release authority ruleset projection is unavailable")
    try:
        canonical = live_ruleset.observable_ruleset_projection(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError("release authority ruleset projection is invalid") from exc
    if canonical != raw or canonical.get("id") != ruleset_id:
        raise ValueError("release authority ruleset projection is not canonical")
    checks = canonical.get("required_status_checks")
    if not isinstance(checks, dict) or set(checks) != {"checks", "strict"} or checks.get("checks") != producers:
        raise ValueError("release authority projection required checks differ from its baseline")
    matching_rules = [rule for rule in canonical.get("rules", []) if rule.get("type") == "required_status_checks"]
    if len(matching_rules) != 1:
        raise ValueError("release authority projection required-check rule is ambiguous")
    parameters = matching_rules[0].get("parameters")
    if not isinstance(parameters, dict) or parameters.get("required_status_checks") != producers:
        raise ValueError("release authority projection rule producers differ from its baseline")
    if parameters.get("strict_required_status_checks_policy") != checks.get("strict"):
        raise ValueError("release authority projection strict policy is inconsistent")
    return canonical


def _privileged(raw: Any, *, projection: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(raw, dict) or set(raw) != {"bypass_actors", "updated_at", "version_id"}:
        raise ValueError("release authority privileged baseline is invalid")
    actors = raw.get("bypass_actors")
    if not isinstance(actors, list):
        raise ValueError("release authority bypass actors are invalid")
    normalized: list[dict[str, Any]] = []
    for actor in actors:
        if not isinstance(actor, dict) or set(actor) != {"actor_id", "actor_type", "bypass_mode"}:
            raise ValueError("release authority bypass actor is invalid")
        actor_id = _positive_int(actor.get("actor_id"), label="bypass actor id")
        actor_type, bypass_mode = actor.get("actor_type"), actor.get("bypass_mode")
        if not isinstance(actor_type, str) or not actor_type or not isinstance(bypass_mode, str) or not bypass_mode:
            raise ValueError("release authority bypass actor is invalid")
        normalized.append({"actor_id": actor_id, "actor_type": actor_type, "bypass_mode": bypass_mode})
    normalized.sort(key=lambda item: (item["actor_id"], item["actor_type"], item["bypass_mode"]))
    if actors != normalized:
        raise ValueError("release authority bypass actors are not canonical")
    updated_at = live_ruleset.canonical_ruleset_updated_at(raw.get("updated_at"))
    if updated_at is None or updated_at != projection.get("updated_at"):
        raise ValueError("release authority privileged revision is invalid")
    return {
        "bypass_actors": normalized,
        "updated_at": updated_at,
        "version_id": _positive_int(raw.get("version_id"), label="ruleset version id"),
    }


def _positive_int(raw: Any, *, label: str) -> int:
    if isinstance(raw, bool) or not isinstance(raw, int) or raw <= 0:
        raise ValueError(f"release authority {label} must be a positive integer")
    return raw


def _sha256(raw: Any, *, label: str) -> str:
    if not isinstance(raw, str) or _DIGEST.fullmatch(raw) is None:
        raise ValueError(f"release authority {label} SHA-256 is invalid")
    return raw


def _canonical_json(payload: Any) -> bytes:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")


__all__ = [
    "BASELINE_FILENAME",
    "POLICY_FILENAME",
    "ReleaseAuthorityBaseline",
    "load_release_authority_baseline",
    "load_sibling_release_authority",
]
