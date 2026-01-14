# src/kontra/scout/reporters/__init__.py
"""
Kontra Scout reporters for different output formats.
"""

from typing import Literal

from kontra.scout.types import DatasetProfile

from .json_reporter import render_json, render_llm
from .markdown_reporter import render_markdown
from .rich_reporter import render_rich


def render_profile(
    profile: DatasetProfile,
    format: Literal["rich", "json", "markdown", "llm"] = "rich",
) -> str:
    """
    Render a DatasetProfile to the specified format.

    Args:
        profile: The DatasetProfile to render
        format: Output format ("rich", "json", "markdown", "llm")

    Returns:
        Formatted string output
    """
    if format == "json":
        return render_json(profile)
    elif format == "markdown":
        return render_markdown(profile)
    elif format == "llm":
        return render_llm(profile)
    else:
        return render_rich(profile)


__all__ = ["render_profile", "render_json", "render_markdown", "render_rich", "render_llm"]
