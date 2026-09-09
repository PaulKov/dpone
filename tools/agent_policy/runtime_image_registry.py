"""Compose runtime-image policy, registry transport, and durable evidence."""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
import re
import stat
from pathlib import Path
from types import ModuleType
from typing import Any

if __package__:
    from .runtime_image_publication_journal import (
        MAX_JOURNAL_BYTES,
        PublicationJournal,
        PublicationJournalError,
        journal_path_for,
        publication_from_completed_journal,
    )
    from .runtime_image_registry_transport import (
        AuthenticatedOciRegistry,
        HttpResponse,
        RegistryCommand,
        RegistryLookup,
    )
else:
    from runtime_image_publication_journal import (  # type: ignore[no-redef]
        MAX_JOURNAL_BYTES,
        PublicationJournal,
        PublicationJournalError,
        journal_path_for,
        publication_from_completed_journal,
    )
    from runtime_image_registry_transport import (  # type: ignore[no-redef]
        AuthenticatedOciRegistry,
        HttpResponse,
        RegistryCommand,
        RegistryLookup,
    )

TOKEN_ENV = re.compile(r"^[A-Z_][A-Z0-9_]{0,127}$")
__all__ = (
    "AuthenticatedOciRegistry",
    "HttpResponse",
    "RegistryCommand",
    "RegistryLookup",
    "reconcile_certified_image",
    "run_cli",
)


def _observation(item: Any) -> dict[str, Any]:
    return {
        key: value
        for key, value in (
            ("state", item.state.value.upper()),
            ("digest", item.digest),
            ("version", item.version),
            ("error_code", item.error_code),
        )
        if value is not None
    }


def _lookup(policy: ModuleType, registry: Any, reference: str, *, include_version: bool) -> Any:
    return policy.classify_manifest_lookup(registry.lookup(reference, include_version=include_version))


def _read_bounded_journal(path: Path) -> Any:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise PublicationJournalError("PUBLICATION_JOURNAL_INVALID")
            chunks: list[bytes] = []
            remaining = MAX_JOURNAL_BYTES + 1
            while remaining:
                chunk = os.read(descriptor, remaining)
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            content = b"".join(chunks)
        finally:
            os.close(descriptor)
        if not content or len(content) > MAX_JOURNAL_BYTES:
            raise PublicationJournalError("PUBLICATION_JOURNAL_INVALID")

        def object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
            result: dict[str, Any] = {}
            for key, value in pairs:
                if key in result:
                    raise PublicationJournalError("PUBLICATION_JOURNAL_INVALID")
                result[key] = value
            return result

        return json.loads(content, object_pairs_hook=object_pairs)
    except PublicationJournalError:
        raise
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PublicationJournalError("PUBLICATION_JOURNAL_INVALID") from exc


def _replay_completed_journal(
    policy: ModuleType,
    path: Path,
    *,
    candidate_digest: str,
    certification_sha256: str,
) -> Any:
    payload = publication_from_completed_journal(
        _read_bounded_journal(path),
        candidate_digest=candidate_digest,
        certification_sha256=certification_sha256,
        policy=policy,
    )
    return policy.validate_publication(payload)


def _postcondition_matches(
    *,
    policy: ModuleType,
    role: str,
    decision: str,
    before: Any,
    after: Any,
    image: str,
    digest: str,
    version: str,
    trusted_latest_certification: Any,
) -> bool:
    if after.state is not policy.ManifestState.PRESENT:
        return False
    if decision == policy.AliasDecision.PRESERVED_NEWER.value:
        return after == before and policy._certification_binds(
            trusted_latest_certification,
            image=image,
            digest=after.digest,
            version=after.version,
        )
    if after.digest != digest:
        return False
    return role != "latest" or after.version == version


def reconcile_certified_image(
    *,
    policy: ModuleType,
    certification: Any,
    certification_sha256: str,
    registry: Any,
    journal: Any | None = None,
    trusted_latest_certification: Any = None,
) -> Any:
    """Reconcile aliases with a durable intent/transition boundary around writes."""

    policy._text(certification_sha256, policy.CANONICAL_DIGEST)
    image, digest, version = certification.image, certification.digest, certification.version
    aliases = (
        ("version", f"{image}:{version}"),
        ("sha", f"{image}:sha-{certification.source_commit_sha[:12]}"),
        ("latest", f"{image}:latest"),
    )
    transitions: list[dict[str, Any]] = []
    for role, reference in aliases:
        include_version = role == "latest"
        before = _lookup(policy, registry, reference, include_version=include_version)
        decision = (
            policy.decide_latest(
                candidate_version=version,
                candidate_digest=digest,
                observed=before,
                observed_version=before.version,
                candidate_image=image,
                trusted_latest_certification=trusted_latest_certification,
            )
            if include_version
            else policy.decide_fixed_tag(expected_digest=digest, observed=before)
        )
        after, blocker = before, decision.blocker_code
        decision_value = decision.decision.value
        if decision.write_required:
            if journal is None:
                blocker = "PUBLICATION_JOURNAL_REQUIRED"
                decision_value = policy.AliasDecision.BLOCKED.value
            else:
                journal.prepare_mutation(
                    role=role,
                    reference=reference,
                    expected_digest=digest,
                    before=_observation(before),
                    decision=decision_value,
                )
                precondition = _lookup(policy, registry, reference, include_version=include_version)
                if precondition != before:
                    after = precondition
                    blocker = "ALIAS_PRECONDITION_CHANGED"
                    decision_value = policy.AliasDecision.BLOCKED.value
                else:
                    command = registry.create_alias(reference, digest)
                    after = _lookup(policy, registry, reference, include_version=include_version)
                    if not getattr(command, "ok", False):
                        blocker = policy._safe_code(
                            getattr(command, "error_code", None),
                            "ALIAS_CREATE_FAILED",
                        )
                    elif not _postcondition_matches(
                        policy=policy,
                        role=role,
                        decision=decision_value,
                        before=before,
                        after=after,
                        image=image,
                        digest=digest,
                        version=version,
                        trusted_latest_certification=trusted_latest_certification,
                    ):
                        blocker = "ALIAS_POSTCONDITION_FAILED"
        elif blocker is None:
            if journal is None:
                blocker = "PUBLICATION_JOURNAL_REQUIRED"
                decision_value = policy.AliasDecision.BLOCKED.value
            else:
                after = _lookup(policy, registry, reference, include_version=include_version)
                if not _postcondition_matches(
                    policy=policy,
                    role=role,
                    decision=decision_value,
                    before=before,
                    after=after,
                    image=image,
                    digest=digest,
                    version=version,
                    trusted_latest_certification=trusted_latest_certification,
                ):
                    blocker = "ALIAS_POSTCONDITION_FAILED"
                    decision_value = policy.AliasDecision.BLOCKED.value
        transition = {
            "role": role,
            "reference": reference,
            "before": _observation(before),
            "decision": decision_value,
            "after": _observation(after),
            "status": "FAIL" if blocker else "PASS",
        }
        if blocker:
            transition["blocker_code"] = policy._safe_code(blocker, "PROMOTION_FAILED")
        if journal is not None:
            journal.record_transition(transition)
        transitions.append(transition)
        if blocker:
            break
    status = "PASS" if len(transitions) == 3 and all(item["status"] == "PASS" for item in transitions) else "FAIL"
    partial = status == "FAIL" and any(item["decision"] != "BLOCKED" for item in transitions)
    receipt = policy.validate_publication(
        {
            "schema": policy.PUBLICATION_SCHEMA,
            "status": status,
            "outcome": "PUBLISHED" if status == "PASS" else "PARTIAL_ALIAS_PENDING" if partial else "BLOCKED",
            "candidate_digest": digest,
            "certification_sha256": certification_sha256,
            "transitions": transitions,
        },
        trusted_latest_certification=trusted_latest_certification,
    )
    if journal is not None:
        journal.complete(receipt)
    return receipt


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate evidence and promote certified runtime-image aliases.")
    commands = parser.add_subparsers(dest="command", required=True)
    certify = commands.add_parser("certify", help="Construct PASS from bounded raw evidence.")
    for option in "image version release-tag source-ref source-repository signer-workflow digest platform run-id run-attempt".split():
        certify.add_argument(f"--{option}", required=True)
    certify.add_argument("--source-commit", dest="source_commit_sha", required=True)
    paths = (
        "dockerfile dockerignore context-root context-manifest-output sbom provenance-verification "
        "sbom-verification checks-root output"
    )
    for option in paths.split():
        certify.add_argument(f"--{option}", required=True, type=Path)
    validate = commands.add_parser("validate-certification", help="Validate certification against release identity.")
    validate.add_argument("--certification", "--input", dest="certification", required=True, type=Path)
    promote = commands.add_parser("promote", help="Promote aliases from one exact PASS certification.")
    promote.add_argument("--certification", required=True, type=Path)
    for command in (validate, promote):
        command.add_argument("--image", required=True)
        command.add_argument("--version", required=True)
        command.add_argument("--digest", required=True)
        command.add_argument("--source-commit", "--source-commit-sha", dest="source_commit_sha", required=True)
        command.add_argument("--source-ref", required=True)
    promote.add_argument("--actor", required=True)
    promote.add_argument("--token-env", required=True)
    promote.add_argument("--output", required=True, type=Path)
    return parser


def run_cli(policy: ModuleType, argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    producer = importlib.import_module(
        f"{__package__}.runtime_image_certification" if __package__ else "runtime_image_certification"
    )
    try:
        if args.command == "certify":
            values = vars(args).copy()
            values.pop("command")
            output = values.pop("output")
            rendered = producer.produce_and_write(policy, producer.CertificationInputs(**values), output)
            print(rendered, end="")
            return 0
        expected = policy.CertificationIdentity(
            args.image, args.version, args.digest, args.source_commit_sha, args.source_ref
        )
        payload, raw = producer.read_bounded_json(args.certification, 1_048_576, "JSON_INVALID", include_raw=True)
        certification = policy.validate_certification(payload, expected=expected)
        rendered = json.dumps(certification.to_payload(), indent=2, sort_keys=True) + "\n"
        if args.command == "promote":
            journal_path = journal_path_for(args.output)
            if args.output.exists() or args.output.is_symlink():
                raise policy.ContractError("PUBLICATION_OUTPUT_INVALID")
            certification_sha256 = f"sha256:{hashlib.sha256(raw).hexdigest()}"
            if journal_path.exists() or journal_path.is_symlink():
                receipt = _replay_completed_journal(
                    policy,
                    journal_path,
                    candidate_digest=certification.digest,
                    certification_sha256=certification_sha256,
                )
                rendered = json.dumps(receipt.to_payload(), indent=2, sort_keys=True) + "\n"
                producer.write_new(args.output, rendered)
                print(rendered, end="")
                return 0 if receipt.status == "PASS" else 3
            policy.require_promotable_certification(certification)
            if TOKEN_ENV.fullmatch(args.token_env) is None or not (token := os.environ.get(args.token_env)):
                raise policy.ContractError("TOKEN_ENV_UNAVAILABLE")
            journal = PublicationJournal(
                path=journal_path,
                candidate_digest=certification.digest,
                certification_sha256=certification_sha256,
            )
            receipt = reconcile_certified_image(
                policy=policy,
                certification=certification,
                certification_sha256=certification_sha256,
                registry=AuthenticatedOciRegistry(image=args.image, actor=args.actor, token=token),
                journal=journal,
            )
            rendered = json.dumps(receipt.to_payload(), indent=2, sort_keys=True) + "\n"
            producer.write_new(args.output, rendered)
            print(rendered, end="")
            return 0 if receipt.status == "PASS" else 3
        print(rendered, end="")
        return 0
    except (OSError, ValueError) as exc:
        code = getattr(exc, "code", "COMMAND_IO_FAILED")
        error = {"error": {"code": policy._safe_code(code, "COMMAND_FAILED")}, "status": "FAIL"}
        print(json.dumps(error, indent=2, sort_keys=True))
        return 2
