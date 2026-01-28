"""Async local filesystem with ripgrep-rs optimizations."""

from __future__ import annotations

from asyncio import get_running_loop
from functools import partial, wraps
from inspect import iscoroutinefunction
import json
import os
from pathlib import Path
import shutil
from typing import TYPE_CHECKING, Any, Literal, Required, overload

from fsspec.implementations.local import LocalFileSystem

from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath, GrepMatch
from upathtools.filesystems.base.file_objects import FileInfo


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable


class LocalFileInfo(FileInfo, total=False):
    """Info dict for local filesystem paths."""

    size: int
    created: float
    islink: bool
    mode: int
    uid: int
    gid: int
    mtime: float
    ino: int
    nlink: int
    destination: Required[bool]


# Windows path normalization for fsspec compatibility
_IS_WINDOWS = os.name == "nt"


def _normalize_path(path: str) -> str:
    """Normalize path separators to forward slashes (fsspec convention)."""
    return path.replace("\\", "/") if _IS_WINDOWS else path


class LocalPath(BaseUPath[LocalFileInfo]):
    """UPath implementation for local filesystem."""

    __slots__ = ()


def wrap[**P, R](func: Callable[P, R]) -> Callable[P, Awaitable[R]]:
    @wraps(func)
    async def run(*args: P.args, **kwargs: P.kwargs) -> R:
        loop = get_running_loop()
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(None, pfunc)

    return run


class AsyncLocalFileSystem(BaseAsyncFileSystem[LocalPath, LocalFileInfo], LocalFileSystem):
    """Async implementation of LocalFileSystem.

    This filesystem provides both async and sync methods. The sync methods are not
    overridden and use LocalFileSystem's implementation.

    The async methods run the respective sync methods in a threadpool executor.
    It also provides open_async() method that supports asynchronous file operations,
    using `aiofile`_.

    Note that some async methods like _find may call these wrapped async methods
    many times, and might have high overhead.
    In that case, it might be faster to run the whole operation in a threadpool,
    which is available as `_*_async()` versions of the API.
    eg: _find_async()/_get_file_async, etc.

    .. aiofile:
        https://github.com/mosquito/aiofile
    """

    mirror_sync_methods = False
    upath_cls = LocalPath

    _cat_file = wrap(LocalFileSystem.cat_file)  # type: ignore[assignment]
    _chmod = wrap(LocalFileSystem.chmod)
    _cp_file = wrap(LocalFileSystem.cp_file)  # type: ignore[assignment]
    _created = wrap(LocalFileSystem.created)
    _find_async = wrap(LocalFileSystem.find)
    _get_file_async = wrap(LocalFileSystem.get_file)
    _islink = wrap(LocalFileSystem.islink)
    _lexists = wrap(LocalFileSystem.lexists)
    _link = wrap(LocalFileSystem.link)
    _makedirs = wrap(LocalFileSystem.makedirs)  # type: ignore[assignment]
    _mkdir = wrap(LocalFileSystem.mkdir)  # type: ignore[assignment]
    _modified = wrap(LocalFileSystem.modified)
    # `mv_file` was renamed to `mv` in fsspec==2024.5.0
    # https://github.com/fsspec/filesystem_spec/pull/1585
    _mv = wrap(getattr(LocalFileSystem, "mv", None) or LocalFileSystem.mv_file)  # type: ignore[arg-type,assignment]
    _mv_file = _mv  # type: ignore[assignment]
    _pipe_file = wrap(LocalFileSystem.pipe_file)  # type: ignore[assignment]
    _put_file = wrap(LocalFileSystem.put_file)  # type: ignore[assignment]
    _read_bytes = wrap(LocalFileSystem.read_bytes)
    _read_text = wrap(LocalFileSystem.read_text)
    _rm = wrap(LocalFileSystem.rm)  # type: ignore[assignment]
    _rm_file = wrap(LocalFileSystem.rm_file)  # type: ignore[assignment]
    _rmdir = wrap(LocalFileSystem.rmdir)
    _touch = wrap(LocalFileSystem.touch)
    _symlink = wrap(LocalFileSystem.symlink)
    _write_bytes = wrap(LocalFileSystem.write_bytes)
    _write_text = wrap(LocalFileSystem.write_text)

    async def _info(self, path: str, **kwargs: Any) -> LocalFileInfo:
        """Get info for a single path."""
        loop = get_running_loop()
        return await loop.run_in_executor(None, partial(LocalFileSystem.info, self, path, **kwargs))  # type: ignore[return-value]

    @overload
    async def _ls(
        self,
        path: str,
        detail: Literal[True] = ...,
        **kwargs: Any,
    ) -> list[LocalFileInfo]: ...

    @overload
    async def _ls(
        self,
        path: str,
        detail: Literal[False],
        **kwargs: Any,
    ) -> list[str]: ...

    async def _ls(
        self,
        path: str,
        detail: bool = True,
        **kwargs: Any,
    ) -> list[LocalFileInfo] | list[str]:
        """List directory contents."""
        loop = get_running_loop()
        return await loop.run_in_executor(  # type: ignore[return-value]
            None, partial(LocalFileSystem.ls, self, path, detail=detail, **kwargs)
        )

    async def _get_file(
        self,
        src: str,
        dst: Any,
        **kwargs: Any,
    ) -> None:
        if not iscoroutinefunction(getattr(dst, "write", None)):
            src = self._strip_protocol(src)
            return await self._get_file_async(src, dst)

        async with await self.open_async(src, "rb") as fsrc:
            while True:
                buf = await fsrc.read(length=shutil.COPY_BUFSIZE)  # type: ignore[attr-defined]
                if not buf:
                    break
                await dst.write(buf)

    async def open_async(
        self,
        path: str,
        mode: str = "rb",
        **kwargs: Any,
    ) -> Any:
        import aiofile

        path = self._strip_protocol(path)
        if self.auto_mkdir and "w" in mode:
            await self._makedirs(self._parent(path), exist_ok=True)
        return await aiofile.async_open(path, mode, **kwargs)

    # -------------------------------------------------------------------------
    # ripgrep-rs optimized methods (optional fast path when available)
    # -------------------------------------------------------------------------

    @overload
    async def _find(
        self,
        path: str,
        maxdepth: int | None = None,
        withdirs: bool = False,
        *,
        detail: Literal[False] = False,
        **kwargs: Any,
    ) -> list[str]: ...

    @overload
    async def _find(
        self,
        path: str,
        maxdepth: int | None = None,
        withdirs: bool = False,
        *,
        detail: Literal[True],
        **kwargs: Any,
    ) -> dict[str, LocalFileInfo]: ...

    async def _find(
        self,
        path: str,
        maxdepth: int | None = None,
        withdirs: bool = False,
        *,
        detail: bool = False,
        **kwargs: Any,
    ) -> list[str] | dict[str, LocalFileInfo]:
        """Find files recursively, using ripgrep-rs for speed when available.

        Uses ripgrep-rs for both detail=False and detail=True modes.
        """
        from ripgrep_rs import files as rg_files, files_with_info

        loop = get_running_loop()
        abs_path = str(Path(self._strip_protocol(path)).resolve())

        if detail:
            # Use files_with_info for metadata - returns dict[str, FileInfo]
            rg_result = await loop.run_in_executor(
                None,
                partial(
                    files_with_info,
                    patterns=["*"],
                    paths=[abs_path],
                    hidden=kwargs.get("hidden", False),
                    no_ignore=kwargs.get("no_ignore", False),
                    include_dirs=withdirs,
                    absolute=True,
                ),
            )
            # Convert FileInfo objects to fsspec-compatible dicts
            # Normalize paths to forward slashes (fsspec convention)
            return {
                _normalize_path(p): LocalFileInfo(
                    name=info.name,
                    size=info.size,
                    type="directory" if info.type == "directory" else "file",
                    created=info.created,
                    islink=info.islink,
                    mode=info.mode,
                    uid=info.uid,
                    gid=info.gid,
                    mtime=info.mtime,
                    ino=info.ino,
                    nlink=info.nlink,
                    destination=False,  # Not a symlink destination
                )
                for p, info in rg_result.items()
            }

        # ripgrep-rs runs in a thread pool since it releases the GIL
        results = await loop.run_in_executor(
            None,
            partial(
                rg_files,
                patterns=["*"],
                paths=[abs_path],
                hidden=kwargs.get("hidden", False),
                no_ignore=kwargs.get("no_ignore", False),
                include_dirs=withdirs,
                absolute=True,  # Ensure absolute paths (for Windows)
            ),
        )
        # Normalize paths to forward slashes (fsspec convention)
        return [_normalize_path(p) for p in results]

    @overload
    async def _glob(
        self,
        path: str,
        maxdepth: int | None = None,
        *,
        detail: Literal[False] = False,
        **kwargs: Any,
    ) -> list[str]: ...

    @overload
    async def _glob(
        self,
        path: str,
        maxdepth: int | None = None,
        *,
        detail: Literal[True],
        **kwargs: Any,
    ) -> dict[str, LocalFileInfo]: ...

    async def _glob(
        self,
        path: str,
        maxdepth: int | None = None,
        *,
        detail: bool = False,
        **kwargs: Any,
    ) -> list[str] | dict[str, LocalFileInfo]:
        """Glob for files, using ripgrep-rs for speed when available.

        Uses ripgrep-rs for both detail=False and detail=True modes.
        Falls back to base implementation for complex glob patterns that ripgrep can't handle.
        """
        from glob import has_magic

        from ripgrep_rs import files as rg_files, files_with_info

        stripped = self._strip_protocol(path)
        if not has_magic(path):
            # No glob pattern - fall back to base implementation
            if detail:
                return await super()._glob(path, maxdepth=maxdepth, detail=True, **kwargs)
            return await super()._glob(path, maxdepth=maxdepth, detail=False, **kwargs)

        # Fast path: use ripgrep-rs for globs
        loop = get_running_loop()

        if "**" in stripped:
            # Recursive glob - split at first **
            # e.g., "/home/user/**/*.py" -> base="/home/user", pattern="**/*.py"
            idx = stripped.find("**")
            base = stripped[:idx].rstrip("/") or "."
            glob_pattern = stripped[idx:]
            depth = None  # No depth limit for ** patterns
        else:
            # Non-recursive glob - use max_depth=1
            # e.g., "/home/user/*.py" -> base="/home/user", pattern="*.py"
            p = Path(stripped)
            base = str(p.parent) if str(p.parent) != "." else "."
            glob_pattern = p.name
            depth = 1  # Only match in the specified directory

        abs_base = str(Path(base).resolve())
        if detail:
            # Use files_with_info for metadata - returns dict[str, FileInfo]
            rg_result = await loop.run_in_executor(
                None,
                partial(
                    files_with_info,
                    patterns=["*"],
                    paths=[abs_base],
                    globs=[glob_pattern],
                    hidden=kwargs.get("hidden", False),
                    no_ignore=kwargs.get("no_ignore", False),
                    max_depth=depth,
                    absolute=True,
                ),
            )
            # Convert FileInfo objects to fsspec-compatible dicts
            # Normalize paths to forward slashes (fsspec convention)
            return {
                _normalize_path(p): LocalFileInfo(
                    name=info.name,
                    size=info.size,
                    type="directory" if info.type == "directory" else "file",
                    created=info.created,
                    islink=info.islink,
                    mode=info.mode,
                    uid=info.uid,
                    gid=info.gid,
                    mtime=info.mtime,
                    ino=info.ino,
                    nlink=info.nlink,
                    destination=False,  # Not a symlink destination
                )
                for p, info in rg_result.items()
            }
        results = await loop.run_in_executor(
            None,
            partial(
                rg_files,
                patterns=["*"],
                paths=[abs_base],
                globs=[glob_pattern],
                hidden=kwargs.get("hidden", False),
                no_ignore=kwargs.get("no_ignore", False),
                max_depth=depth,
                absolute=True,  # Ensure absolute paths (for Windows)
            ),
        )
        # Normalize paths to forward slashes (fsspec convention)
        return [_normalize_path(p) for p in results]

    async def _grep(
        self,
        path: str,
        pattern: str,
        *,
        max_count: int | None = None,
        case_sensitive: bool | None = None,
        hidden: bool = False,
        no_ignore: bool = False,
        globs: list[str] | None = None,
        context_before: int | None = None,
        context_after: int | None = None,
        multiline: bool = False,
    ) -> list[GrepMatch]:
        """Search for pattern in files using ripgrep-rs.

        This provides a fast grep implementation using ripgrep's algorithms
        via the ripgrep-rs Python bindings.

        Args:
            path: Directory to search in.
            pattern: Regex pattern to search for.
            max_count: Maximum total number of matches to return.
            case_sensitive: Force case sensitivity (None = smart case).
            hidden: Search hidden files/directories.
            no_ignore: Don't respect .gitignore rules.
            globs: File patterns to include/exclude (e.g., ``['*.py', '!*_test.py']``).
            context_before: Lines of context before match.
            context_after: Lines of context after match.
            multiline: Enable multiline matching.

        Returns:
            List of GrepMatch objects with file, line number, and matched text.
        """
        from ripgrep_rs import search as rg_search

        loop = get_running_loop()
        abs_path = str(Path(self._strip_protocol(path)).resolve())

        # Build kwargs for ripgrep
        rg_kwargs: dict[str, Any] = {
            "patterns": [pattern],
            "paths": [abs_path],
            "hidden": hidden,
            "no_ignore": no_ignore,
            "json": True,  # Get structured output
        }
        if max_count is not None:
            rg_kwargs["max_count"] = max_count
        if case_sensitive is not None:
            rg_kwargs["case_sensitive"] = case_sensitive
        if globs:
            rg_kwargs["globs"] = globs
        if context_before is not None:
            rg_kwargs["before_context"] = context_before
        if context_after is not None:
            rg_kwargs["after_context"] = context_after
        if multiline:
            rg_kwargs["multiline"] = True

        # Run ripgrep in thread pool (ripgrep-rs releases the GIL)
        raw_results = await loop.run_in_executor(None, partial(rg_search, **rg_kwargs))

        # Parse JSON Lines output into GrepMatch objects
        matches: list[GrepMatch] = []
        for file_result in raw_results:
            for line in file_result.strip().split("\n"):
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    if obj.get("type") == "match":
                        data = obj["data"]
                        # Normalize path to forward slashes (fsspec convention)
                        match_path = _normalize_path(data["path"]["text"])
                        matches.append(
                            GrepMatch(
                                path=match_path,
                                line_number=data["line_number"],
                                text=data["lines"]["text"].rstrip("\n"),
                                submatches=[
                                    (sm["start"], sm["end"]) for sm in data.get("submatches", [])
                                ],
                                absolute_offset=data.get("absolute_offset", 0),
                            )
                        )
                except (json.JSONDecodeError, KeyError):
                    continue

        return matches


def register_async_local_fs() -> bool:
    """Register AsyncLocalFileSystem in fsspec registry, replacing morefs.

    This registers our optimized AsyncLocalFileSystem (with ripgrep-rs support)
    as the handler for the 'asynclocal' protocol, overriding the default morefs
    implementation.

    Returns:
        True if registration succeeded, False if upath is not available.
    """
    import fsspec
    from fsspec.implementations.local import make_path_posix
    from fsspec.utils import stringify_path
    from upath._flavour_sources import AbstractFileSystemFlavour
    from upath.registry import register_implementation

    # Register in fsspec registry (clobber=True to override morefs)
    fsspec.register_implementation(
        "asynclocal",
        "upathtools.filesystems.async_local_fs.AsyncLocalFileSystem",
        clobber=True,
    )

    class AsyncLocalFileSystemFlavour(AbstractFileSystemFlavour):
        __orig_class__ = "upathtools.filesystems.async_local.AsyncLocalFileSystem"
        protocol = ()
        root_marker = "/"
        sep = "/"
        local_file = True

        @classmethod
        def _strip_protocol(cls, path):  # type: ignore[override]
            path = stringify_path(path)
            if path.startswith("file://"):
                path = path[7:]
            elif path.startswith("file:"):
                path = path[5:]
            elif path.startswith("local://"):
                path = path[8:]
            elif path.startswith("local:"):
                path = path[6:]

            path = str(make_path_posix(path))
            if os.sep != "/":
                if path[1:2] == ":":
                    drive, path = path[:2], path[2:]
                elif path[:2] == "//":
                    if (index1 := path.find("/", 2)) == -1 or (
                        index2 := path.find("/", index1 + 1)
                    ) == -1:
                        drive, path = path, ""
                    else:
                        drive, path = path[:index2], path[index2:]
                else:
                    drive = ""

                return drive + (path.rstrip("/") or cls.root_marker)

            return path.rstrip("/") or cls.root_marker

        @classmethod
        def _parent(cls, path):  # type: ignore[override]
            path = cls._strip_protocol(path)
            if os.sep == "/":
                return path.rsplit("/", 1)[0] or "/"
            path_ = path.rsplit("/", 1)[0]
            if len(path_) <= 3 and path_[1:2] == ":":  # noqa: PLR2004
                return path_[0] + ":/"
            return path_

    register_implementation("asynclocal", LocalPath, clobber=True)
    return True


# Keep old name for backwards compatibility
register_flavour = register_async_local_fs


if __name__ == "__main__":
    import asyncio

    async def main():
        fs = AsyncLocalFileSystem()
        await fs._mkdir("test")
        ls = await fs._ls("")
        print(ls)

    asyncio.run(main())
