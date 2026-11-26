"""Pytest configuration for the ``tests/fs`` suite."""

from typing import TYPE_CHECKING


if TYPE_CHECKING:
    import pytest


def pytest_configure(config: pytest.Config) -> None:
    """Register local markers used under ``tests/fs``."""
    config.addinivalue_line(
        "markers",
        "free_threading: multi-threaded stress tests for free-threaded Python (PEP 703)",
    )
