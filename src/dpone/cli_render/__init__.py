"""CLI renderers.

Renderers are responsible for converting internal objects into human-friendly
text output (tables, headings, YAML evidence blocks).

Design goals:
- keep command modules small (parse args -> compute -> render)
- keep rendering logic reusable and testable
- do not import runtime-heavy optional dependencies
"""

from __future__ import annotations
