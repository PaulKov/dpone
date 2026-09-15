"""Capture and read native documents through verified archive and file ports."""

from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from dpone.contracts.dbt_project_bundle import DbtProjectBundle, DbtProjectBundleArtifact
from dpone.contracts.dbt_publish_models import DbtPublishIntent
from dpone.contracts.native_delivery import NativePolicyDocumentRef
from dpone.contracts.native_project_documents import (
    NATIVE_INTENT_MEMBER,
    NATIVE_POLICY_MEMBER,
    decode_native_project_intent,
    generate_native_project_intent,
    validate_native_project_policy,
)
from dpone.ports.dbt_project_bundle import DbtProjectBundleOperations
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
        validate_native_project_policy(policy, intent, descriptor, bundle, max_bytes=self._maximum)
        return policy, intent

    def _member(self, root: Path, bundle: DbtProjectBundle, relative: str) -> bytes:
        matches = tuple(item for item in bundle.files if item.path == relative)
        if len(matches) != 1 or matches[0].bytes > self._maximum:
            raise ValueError("native generated member is absent or exceeds its admitted bound")
        payload = self._read(root, relative, max_bytes=self._maximum)
        if len(payload) != matches[0].bytes or "sha256:" + sha256(payload).hexdigest() != matches[0].sha256:
            raise ValueError("native generated member differs from the verified archive inventory")
        return payload


class NativeProjectDocuments:
    """Capture one native intent and full selected policy without editing sources.

    The project must explicitly admit the ``dpone`` asset path. Generated files
    are added only to an owned extracted snapshot. No project configuration,
    source code, package environment or legacy archive behavior is rewritten.
    This producer performs no runtime qualification, credentials or dbt dispatch.
    """

    def __init__(
        self,
        *,
        bundles: DbtProjectBundleOperations,
        read_file: ConfinedReleaseFileReader,
        max_policy_bytes: int,
    ) -> None:
        if type(max_policy_bytes) is not int or max_policy_bytes <= 0:
            raise ValueError("native documents require an explicit positive metadata bound")
        self._bundles = bundles
        self._read = read_file
        self._documents = NativeProjectDocumentReader(read_file=read_file, max_policy_bytes=max_policy_bytes)
        self._maximum = max_policy_bytes

    def build(
        self,
        project_root: Path,
        *,
        policy: bytes,
        intent: DbtPublishIntent,
        model_storage: str = "rowstore_none",
    ) -> DbtProjectBundleArtifact:
        """Return an immutable archive containing exact canonical generated members."""
        intent_bytes = generate_native_project_intent(
            policy,
            intent,
            model_storage=model_storage,
            max_bytes=self._maximum,
        )
        original = self._bundles.build(project_root)
        with TemporaryDirectory(prefix="dpone-native-documents-") as temporary:
            snapshot = Path(temporary) / "project"
            self._bundles.extract(original.archive, snapshot)
            self._bundles.verify(original.archive, snapshot)
            for relative, payload in ((NATIVE_POLICY_MEMBER, policy), (NATIVE_INTENT_MEMBER, intent_bytes)):
                path = snapshot / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                if path.exists() or path.is_symlink():
                    if self._read(snapshot, relative, max_bytes=self._maximum) != payload:
                        raise ValueError("existing native project member differs from selected generated bytes")
                else:
                    with path.open("xb") as output:
                        output.write(payload)
            produced = self._bundles.build(snapshot)
            # Re-extract the actual returned archive; checking files before its
            # producer runs would not prove generated members were admitted.
            roundtrip = Path(temporary) / "roundtrip"
            bundle = self._bundles.extract(produced.archive, roundtrip)
            self._bundles.verify(produced.archive, roundtrip)
            observed_policy, observed_intent = self.read(roundtrip, bundle)
            if observed_policy != policy or observed_intent != decode_native_project_intent(
                intent_bytes,
                max_bytes=self._maximum,
            ):
                raise ValueError("captured native documents differ from generated originals")
            return produced

    def read(self, project_root: Path, bundle: DbtProjectBundle) -> tuple[bytes, dict[str, Any]]:
        """Authenticate the generated members of an already verified archive."""
        return self._documents.read(project_root, bundle)
