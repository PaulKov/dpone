"""Local signature envelope helpers for supply-chain evidence."""

from __future__ import annotations

import hmac
import json
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path

from dpone.supply_chain.checksums import sha256_file


@dataclass(frozen=True, slots=True)
class SignatureArtifact:
    path: str
    signed_path: str
    signature: str


class LocalHMACSignatureService:
    """Create a local HMAC-SHA256 signature envelope.

    This is useful for deterministic internal CI evidence. Public release
    identity should still use GitHub artifact attestations or Sigstore/cosign.
    """

    def sign_file(
        self,
        *,
        path: str | Path,
        output_dir: str | Path,
        key: str,
        key_id: str,
    ) -> SignatureArtifact:
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        source = Path(path)
        digest = sha256_file(source)
        signature = hmac.new(key.encode("utf-8"), digest.encode("utf-8"), sha256).hexdigest()
        payload = {
            "algorithm": "HMAC-SHA256",
            "key_id": key_id,
            "signed_path": str(source),
            "signed_sha256": digest,
            "signature": signature,
        }
        signature_path = out / "signature.hmac-sha256.json"
        signature_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return SignatureArtifact(path=str(signature_path), signed_path=str(source), signature=signature)
