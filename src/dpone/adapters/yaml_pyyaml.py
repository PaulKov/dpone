from __future__ import annotations

from typing import Any

import yaml


class PyYamlCodec:
    def load(self, text: str) -> Any:
        return yaml.safe_load(text)

    def dump(self, obj: Any) -> str:
        # diff-friendly defaults
        return yaml.safe_dump(obj, sort_keys=False, allow_unicode=True)
