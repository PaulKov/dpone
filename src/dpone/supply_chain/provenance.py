"""Build provenance evidence for release artifacts."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from dpone.supply_chain.checksums import sha256_file


@dataclass(frozen=True, slots=True)
class ProvenanceArtifact:
    path: str
    subject_count: int


class ProvenanceService:
    """Build an in-toto/SLSA-inspired provenance JSON document."""

    def build(
        self,
        *,
        output_dir: str | Path,
        release: str,
        subjects: list[str | Path] | tuple[str | Path, ...],
        repository: str,
        commit_sha: str,
        builder_id: str,
    ) -> ProvenanceArtifact:
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        subject_payload = [
            {
                "name": str(Path(subject)),
                "digest": {"sha256": sha256_file(subject)},
            }
            for subject in subjects
        ]
        payload = {
            "_type": "https://in-toto.io/Statement/v1",
            "predicateType": "https://slsa.dev/provenance/v1",
            "subject": subject_payload,
            "predicate": {
                "buildDefinition": {
                    "buildType": "https://github.com/PaulKov/dpone/build/release",
                    "externalParameters": {"release": release, "repository": repository, "commit": commit_sha},
                },
                "runDetails": {
                    "builder": {"id": builder_id},
                    "metadata": {"finishedOn": datetime.now(UTC).isoformat()},
                },
            },
        }
        path = out / "provenance.intoto.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return ProvenanceArtifact(path=str(path), subject_count=len(subject_payload))
