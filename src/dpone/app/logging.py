from __future__ import annotations

import logging
import os


def setup_logging() -> logging.Logger:
    """Configure dpone logger.

    KISS:
      - no external logging framework
      - env-controlled level
    """

    level_raw = os.getenv("DPONE_LOG_LEVEL", "INFO").upper().strip()
    level = getattr(logging, level_raw, logging.INFO)

    logging.basicConfig(
        level=level,
        format="%(levelname)s: %(message)s",
    )

    return logging.getLogger("dpone")
