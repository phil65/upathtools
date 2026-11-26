"""Run ``fsspec``'s own abstract compliance suite against ``upathtools.fs`` adapters.

Covers:
  1. ``to_fsspec(MemoryFS())``
  2. Round-trip ``to_fsspec(from_fsspec(fsspec.filesystem("memory", ...)))``

Note: In ``fsspec 2026.9.0``, ``fsspec.tests.abstract.mv`` defines two standalone
functions hardcoded to ``fsspec.filesystem("file")`` rather than an ``AbstractMvTests``
mixin class. If a future ``fsspec`` release exposes ``AbstractMvTests``, it is picked
up automatically via ``getattr``.
"""

import posixpath
from typing import TYPE_CHECKING, Any

import fsspec
from fsspec.tests.abstract import (
    AbstractCopyTests,
    AbstractFixtures,
    AbstractGetTests,
    AbstractOpenTests,
    AbstractPipeTests,
    AbstractPutTests,
)
import fsspec.tests.abstract.mv as fsspec_abstract_mv
import pytest

from upathtools.fs import LoopRunner, MemoryFS, from_fsspec, to_fsspec


if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from upathtools.fs import AsFsspecFileSystem


_AbstractMvTests: type[Any] = getattr(fsspec_abstract_mv, "AbstractMvTests", object)


class _CoreAbstractSuite(
    AbstractFixtures,
    AbstractCopyTests,
    AbstractGetTests,
    AbstractPutTests,
    AbstractOpenTests,
    AbstractPipeTests,
    _AbstractMvTests,
):
    """Shared fixtures for ``fsspec.tests.abstract`` runs over :class:`AsFsspecFileSystem`."""

    @pytest.fixture(scope="class")
    @classmethod
    def runner(cls) -> Iterator[LoopRunner]:
        with LoopRunner(name="test-fsspec-abstract") as loop_runner:
            yield loop_runner

    @pytest.fixture
    def fs_path(self) -> str:
        return "/root"

    @pytest.fixture
    def fs_join(self) -> Callable[..., str]:
        return posixpath.join

    @pytest.fixture
    def supports_empty_directories(self) -> bool:
        return True


class TestToFsspecMemoryFS(_CoreAbstractSuite):
    """Run ``fsspec.tests.abstract`` against ``to_fsspec(MemoryFS())``."""

    @pytest.fixture
    def fs(self, runner: LoopRunner, fs_path: str) -> AsFsspecFileSystem:
        adapted = to_fsspec(MemoryFS(), runner=runner)
        adapted.makedirs(fs_path, exist_ok=True)
        return adapted


class TestRoundTripFsspecMemory(_CoreAbstractSuite):
    """Run ``fsspec.tests.abstract`` against ``to_fsspec(from_fsspec(MemoryFileSystem))``."""

    @pytest.fixture
    def fs(self, runner: LoopRunner, fs_path: str) -> AsFsspecFileSystem:
        raw = fsspec.filesystem("memory", global_store=False, skip_instance_cache=True)
        raw.store.clear()
        raw.pseudo_dirs[:] = [""]
        adapted = to_fsspec(from_fsspec(raw), runner=runner)
        adapted.makedirs(fs_path, exist_ok=True)
        return adapted
