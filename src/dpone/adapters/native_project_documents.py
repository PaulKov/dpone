"""Read generated native documents through bounded confined file capabilities."""

from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from typing import Any

from dpone.contracts.dbt_project_bundle import DbtProjectBundle
from dpone.contracts.dbt_publish_schema_contract_v4 import require_native_document_selection, validate_native_policy_v4
from dpone.contracts.native_delivery import NativePolicyDocumentRef
from dpone.contracts.native_policy_document import verify_native_policy_member
from dpone.contracts.native_project_documents import (
    NATIVE_INTENT_MEMBER,
    NATIVE_POLICY_MEMBER,
    decode_native_project_intent,
)
from dpone.ports.dbt_release_files import ConfinedReleaseFileReader


class NativeProjectDocumentReader:
    """Authenticate captured document bytes before any execution capability use."""

    def __init__(self, *, read_file: ConfinedReleaseFileReader, max_policy_bytes: int) -> None:
        if type(max_policy_bytes) is not int or max_policy_bytes <= 0:
            raise ValueError("native documents require an explicit positive metadata bound")
        self._read = read_file
        self._maximum = max_policy_bytes

    def read(self, project_root: Path, bundle: DbtProjectBundle) -> tuple[bytes, dict[str, Any]]:
        """Authenticate native members against an already verified archive inventory.

        Callers must extract/verify the archive first. This method independently
        binds every consumed member's actual bytes to the supplied inventory.
        """
        if type(bundle) is not DbtProjectBundle:
            raise ValueError("native documents require an exact verified bundle inventory")
        bundle.__post_init__()
        intent_bytes = self._member(project_root, bundle, NATIVE_INTENT_MEMBER)
        intent = decode_native_project_intent(intent_bytes, max_bytes=self._maximum)
        descriptor = NativePolicyDocumentRef(**intent["native_policy_document"])
        if descriptor.path != NATIVE_POLICY_MEMBER:
            raise ValueError("native intent names an unsupported generated policy member")
        policy = self._member(project_root, bundle, descriptor.path)
        verify_native_policy_member(policy, descriptor, bundle, max_bytes=self._maximum)
        validated = validate_native_policy_v4(policy, max_bytes=self._maximum)
        require_native_document_selection(
            validated,
            profile=intent["profile"],
            workflow=intent["workflow"],
            layout=intent["model_storage"],
            strategy_mode=intent["strategy"]["mode"],
        )
        return policy, intent

    def _member(self, root: Path, bundle: DbtProjectBundle, relative: str) -> bytes:
        matches = tuple(item for item in bundle.files if item.path == relative)
        if len(matches) != 1 or matches[0].bytes > self._maximum:
            raise ValueError("native generated member is absent or exceeds its admitted bound")
        payload = self._read(root, relative, max_bytes=self._maximum)
        if len(payload) != matches[0].bytes or "sha256:" + sha256(payload).hexdigest() != matches[0].sha256:
            raise ValueError("native generated member differs from the verified archive inventory")
        return payload
