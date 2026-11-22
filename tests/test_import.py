"""Smoke test: the package imports and reports its version."""

from __future__ import annotations

import upathtools


def test_package_imports() -> None:
    """The package can be imported and exposes a version string."""
    assert isinstance(upathtools.__version__, str)
