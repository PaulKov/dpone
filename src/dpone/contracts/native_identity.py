"""Validated native original references; identity alone grants no authority."""

from __future__ import annotations

from dataclasses import dataclass
from unicodedata import category

from dpone.contracts.dbt_contract_validation import DbtPublishingError, require_digest, require_relative

Digest = str
_ERROR_CODE = "DPONE_NATIVE_IDENTITY_INVALID"


@dataclass(frozen=True, slots=True)
class OriginalRef:
    """Exact locator/digest pair that consumers must independently authenticate.

    Locators are canonical relative POSIX paths, at most 4096 UTF-8 bytes, with
    no control characters. Validation performs no filesystem or storage access.
    Unicode is preserved without normalization; equivalent-looking names can
    intentionally denote different originals.
    """

    locator: str
    sha256: Digest

    def __post_init__(self) -> None:
        if type(self.locator) is not str or type(self.sha256) is not str:
            raise DbtPublishingError(_ERROR_CODE, "original reference fields must be strings")
        require_relative(self.locator, "locator", _ERROR_CODE)
        require_digest(self.sha256, "sha256", _ERROR_CODE)
        try:
            encoded = self.locator.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise DbtPublishingError(_ERROR_CODE, "locator must contain Unicode scalars") from exc
        if self.locator == "." or len(encoded) > 4096 or any(category(character) == "Cc" for character in self.locator):
            raise DbtPublishingError(_ERROR_CODE, "locator exceeds its byte bound or contains controls")
