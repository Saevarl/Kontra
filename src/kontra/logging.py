# src/kontra/logging.py
"""
Logging utilities for Kontra.

Provides consistent, opt-in verbose logging across the codebase.
Logging is controlled by the KONTRA_VERBOSE environment variable.

Usage:
    from kontra.logging import get_logger
    logger = get_logger(__name__)

    logger.debug("This appears only when KONTRA_VERBOSE is set")
    logger.warning("This always appears but with more detail when KONTRA_VERBOSE is set")
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Optional

# Module-level flag for verbose mode
_verbose_mode: Optional[bool] = None


def is_verbose() -> bool:
    """Check if verbose mode is enabled via KONTRA_VERBOSE env var."""
    global _verbose_mode
    if _verbose_mode is None:
        _verbose_mode = bool(os.getenv("KONTRA_VERBOSE"))
    return _verbose_mode


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger configured for Kontra.

    When KONTRA_VERBOSE is set:
        - DEBUG and above messages are shown
        - Format includes module name and level

    When KONTRA_VERBOSE is not set:
        - Only WARNING and above are shown
        - Format is minimal
    """
    logger = logging.getLogger(name)

    # Only configure if not already configured
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)

        if is_verbose():
            logger.setLevel(logging.DEBUG)
            handler.setFormatter(
                logging.Formatter("[%(levelname)s] %(name)s: %(message)s")
            )
        else:
            logger.setLevel(logging.WARNING)
            handler.setFormatter(
                logging.Formatter("[%(levelname)s] %(message)s")
            )

        logger.addHandler(handler)
        logger.propagate = False

    return logger


def log_exception(
    logger: logging.Logger,
    msg: str,
    exc: Exception,
    level: int = logging.DEBUG,
) -> None:
    """
    Log an exception with appropriate detail level.

    In verbose mode: logs full exception details
    Otherwise: logs just the message (if level >= WARNING)
    """
    if is_verbose():
        logger.log(level, f"{msg}: {type(exc).__name__}: {exc}")
    elif level >= logging.WARNING:
        logger.log(level, msg)
