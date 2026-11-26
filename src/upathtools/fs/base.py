"""Async-native base filesystem abstraction for ``upathtools.fs``."""

from abc import ABC, abstractmethod
import asyncio
from bisect import bisect_right
import difflib
import errno
from pathlib import Path
import re
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Self, overload

from upathtools.fs._sync import FSSPEC_SYNC_VERSION, FsspecSyncNote
from upathtools.fs.batch import run_bounded
from upathtools.fs.cache import DirCache
from upathtools.fs.errors import FilesystemClosedError, UnsupportedOperation
from upathtools.fs.glob import compile_glob, has_magic
from upathtools.fs.paths import (
    join_path,
    normalize_path,
    parent_path,
    strip_protocol as strip_protocol_path,
)
from upathtools.fs.streams import DEFAULT_BLOCK_SIZE, AsyncBufferedReader, AsyncBufferedWriter
from upathtools.fs.types import FileInfo, GrepMatch, WriteMode


if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Sequence
    import os


__fsspec_sync__ = FsspecSyncNote(
    version=FSSPEC_SYNC_VERSION,
    upstream=(
        "fsspec/asyn.py:AsyncFileSystem._walk",
        "fsspec/asyn.py:AsyncFileSystem._find",
        "fsspec/asyn.py:AsyncFileSystem._glob",
        "fsspec/asyn.py:AsyncFileSystem._du",
    ),
    notes=(
        "Async-first rewrite with no metaclass cache, no sync mirroring, and no "
        "detail:bool polymorphism. exists/is_file/is_dir catch only FileNotFoundError "
        "and NotADirectoryError. walk() lists sibling directories concurrently via "
        "run_bounded(). find() on a single file returns [FileInfo] with full metadata. "
        "du() runs in a single pass over find(). copy() and move() follow pathlib.Path "
        "exact-target semantics with pre-flight conflict checks."
    ),
)

_BINARY_CHECK_BYTES = 8192
_DEFAULT_GREP_BATCH_SIZE = 64

# Primitives a backend provides natively. Derived helpers (copy, touch, grep over
# read_bytes, ...) work on every backend and are no capability; "move" and "grep" count
# when overridden because the override is a native (atomic / server-side) implementation.
_OPTIONAL_CAPABILITY_METHODS: tuple[str, ...] = (
    "write_bytes",
    "mkdir",
    "rmdir",
    "remove_file",
    "move",
    "grep",
)


def _matches_globs(entry: FileInfo, base_path: str, globs: Sequence[str]) -> bool:
    """Return True if ``entry`` matches the ripgrep-style ``globs`` filter list."""
    if base_path == "/":
        rel = entry.path.lstrip("/")
    elif entry.path.startswith(f"{base_path}/"):
        rel = entry.path[len(base_path) + 1 :]
    elif entry.path == base_path:
        rel = entry.name
    else:
        rel = entry.path.lstrip("/")

    has_positive = any(not g.startswith("!") for g in globs)
    included = not has_positive
    for raw in globs:
        negated = raw.startswith("!")
        pat = raw[1:] if negated else raw
        target = rel if "/" in pat else entry.name
        if compile_glob(pat).match(target) is not None:
            included = not negated
    return included


def _grep_text(
    path: str,
    text: str,
    regex: re.Pattern[str],
    *,
    ctx_before: int,
    ctx_after: int,
    multiline: bool,
    remaining: int | None,
) -> list[GrepMatch]:
    """Scan decoded file text and return matching :class:`GrepMatch` records."""
    lines = text.splitlines()
    if not lines:
        return []

    matches: list[GrepMatch] = []
    if not multiline:
        for idx, line in enumerate(lines):
            found = list(regex.finditer(line))
            if not found:
                continue
            before = tuple(lines[max(0, idx - ctx_before) : idx]) if ctx_before else ()
            after = tuple(lines[idx + 1 : idx + 1 + ctx_after]) if ctx_after else ()
            matches.append(
                GrepMatch(
                    path=path,
                    line_number=idx + 1,
                    line=line,
                    submatches=tuple((m.start(), m.end()) for m in found),
                    context_before=before,
                    context_after=after,
                )
            )
            if remaining is not None and len(matches) >= remaining:
                break
        return matches

    # Multiline mode: run regex across full text and map offsets to 1-based lines.
    raw_lines = text.splitlines(keepends=True)
    line_starts: list[int] = []
    offset = 0
    for raw_line in raw_lines:
        line_starts.append(offset)
        offset += len(raw_line)

    for m in regex.finditer(text):
        line_idx = bisect_right(line_starts, m.start()) - 1
        line_idx = max(0, min(line_idx, len(lines) - 1))
        line_start = line_starts[line_idx]
        line_text = lines[line_idx]
        col_start = m.start() - line_start
        col_end = min(m.end() - line_start, len(line_text))
        before = tuple(lines[max(0, line_idx - ctx_before) : line_idx]) if ctx_before else ()
        after = tuple(lines[line_idx + 1 : line_idx + 1 + ctx_after]) if ctx_after else ()
        matches.append(
            GrepMatch(
                path=path,
                line_number=line_idx + 1,
                line=line_text,
                submatches=((col_start, col_end),),
                context_before=before,
                context_after=after,
            )
        )
        if remaining is not None and len(matches) >= remaining:
            break
    return matches


class AsyncFS[TInfo: FileInfo = FileInfo](ABC):
    """Async-native, free-threading-safe base filesystem.

    Subclasses must implement the three read primitives:
      - :meth:`info`
      - :meth:`ls`
      - :meth:`read_bytes`

    Writable subclasses additionally override:
      - :meth:`write_bytes`
      - :meth:`mkdir`
      - :meth:`rmdir`
      - :meth:`remove_file`

    Capability detection:
      Because this base class defines default methods for all optional operations
      (raising :class:`UnsupportedOperation` for unimplemented mutations), the
      runtime-checkable protocols in :mod:`upathtools.fs.protocols` are intended
      for duck-typed non-``AsyncFS`` objects. On ``AsyncFS`` instances, inspect
      :attr:`capabilities` and :attr:`readonly` instead.
    """

    protocol: ClassVar[str | tuple[str, ...]] = "abstract"
    root_marker: ClassVar[str] = "/"
    sep: ClassVar[str] = "/"
    local_file: ClassVar[bool] = False
    _capabilities: ClassVar[frozenset[str]] = frozenset()

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls._capabilities = frozenset(
            name
            for name in _OPTIONAL_CAPABILITY_METHODS
            if getattr(cls, name, None) is not getattr(AsyncFS, name, None)
        )

    def __init__(
        self,
        *,
        batch_size: int | None = None,
        use_listings_cache: bool = False,
        listings_expiry_time: float | None = None,
        max_cached_paths: int | None = None,
    ) -> None:
        """Initialize base filesystem options.

        Args:
            batch_size: Default concurrency limit for derived batch/traversal
                operations, or ``None`` for unbounded concurrency.
            use_listings_cache: If ``True``, create a thread-safe :class:`DirCache`
                on :attr:`dircache` for backend implementations to use.
            listings_expiry_time: Optional TTL in seconds for cached listings.
            max_cached_paths: Optional LRU capacity for cached listings.

        Raises:
            ValueError: If ``batch_size`` is provided and ``<= 0``.
        """
        if batch_size is not None and batch_size <= 0:
            raise ValueError(f"batch_size must be a positive integer or None, got {batch_size}")
        self.batch_size = batch_size
        self.dircache: DirCache[TInfo] | None = (
            DirCache(expiry_time=listings_expiry_time, max_paths=max_cached_paths)
            if use_listings_cache
            else None
        )
        self._closed = False

    # --- Capability & lifecycle helpers ---

    @property
    def capabilities(self) -> frozenset[str]:
        """Native optional primitives this class overrides.

        Out of ``write_bytes``, ``mkdir``, ``rmdir``, ``remove_file``, ``move``, ``grep``.
        """
        return type(self)._capabilities

    @property
    def readonly(self) -> bool:
        """Return True when this filesystem does not support writing."""
        return "write_bytes" not in self.capabilities

    @property
    def closed(self) -> bool:
        """Return True if this filesystem instance has been closed."""
        return self._closed

    def _check_open(self) -> None:
        """Raise :class:`FilesystemClosedError` if this filesystem has been closed."""
        if self._closed:
            raise FilesystemClosedError(f"{type(self).__name__} is closed")

    async def aclose(self) -> None:
        """Release resources held by this filesystem instance. Idempotent."""
        if not self._closed:
            self._closed = True
            self.invalidate_cache()

    async def __aenter__(self) -> Self:
        self._check_open()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    def invalidate_cache(self, path: str | None = None) -> None:
        """Invalidate cached directory listings, if :attr:`dircache` is enabled."""
        if self.dircache is not None:
            self.dircache.invalidate(None if path is None else self._norm(path))

    # --- Path helpers (pure, class-level, overridable) ---

    @classmethod
    def strip_protocol(cls, path: str) -> str:
        """Strip this filesystem's own protocol prefix(es) and normalize ``path``.

        For backends with ``root_marker == "/"``, returned paths are always canonical
        absolute POSIX paths (e.g. ``"a/b"``, ``"/a/b/"``, and ``"proto://a/b"`` all
        normalize to ``"/a/b"``, and root normalizes to ``"/"``).
        """
        stripped = strip_protocol_path(path, cls.protocol, root_marker="")
        norm = normalize_path(stripped)
        if cls.root_marker == "/":
            if not norm:
                return "/"
            return norm if norm.startswith("/") else f"/{norm}"
        if cls.root_marker:
            if not norm:
                return cls.root_marker
            if not norm.startswith(cls.root_marker):
                return f"{cls.root_marker}{norm.lstrip('/')}"
            return norm
        return norm.lstrip("/")

    @classmethod
    def parent(cls, path: str) -> str:
        """Return the normalized parent directory of ``path``."""
        return parent_path(cls.strip_protocol(path), root_marker=cls.root_marker)

    @classmethod
    def kwargs_from_url(cls, url: str) -> dict[str, Any]:
        """Extract backend constructor keyword arguments encoded in ``url``."""
        return {}

    def _norm(self, path: str) -> str:
        """Normalize a path argument using :meth:`strip_protocol`."""
        return self.strip_protocol(path)

    # --- Required read primitives ---

    @abstractmethod
    async def info(self, path: str) -> TInfo:
        """Return normalized metadata for ``path``.

        Raises:
            FileNotFoundError: If ``path`` does not exist.
            NotADirectoryError: If a path component prior to the leaf is not a directory.
        """

    @abstractmethod
    async def ls(self, path: str) -> list[TInfo]:
        """List direct children of directory ``path``, sorted by ``entry.path``.

        Raises:
            FileNotFoundError: If ``path`` does not exist.
            NotADirectoryError: If ``path`` (or an ancestor) is not a directory.
        """

    @abstractmethod
    async def read_bytes(
        self,
        path: str,
        *,
        start: int | None = None,
        end: int | None = None,
    ) -> bytes:
        """Read bytes from file ``path``, optionally sliced by ``[start:end]``.

        ``start`` and ``end`` follow Python slice semantics (negative offsets count
        from the end of the file, out-of-range offsets are clamped, and
        ``start >= end`` returns ``b""``).

        Raises:
            FileNotFoundError: If ``path`` does not exist.
            IsADirectoryError: If ``path`` is a directory.
            NotADirectoryError: If an ancestor of ``path`` is not a directory.
        """

    # --- Optional mutation primitives (raise UnsupportedOperation by default) ---

    async def write_bytes(
        self,
        path: str,
        data: bytes | bytearray | memoryview,
        *,
        mode: WriteMode = "overwrite",
    ) -> None:
        """Write raw bytes to ``path``.

        Args:
            path: Target file path.
            data: Bytes-like payload to write.
            mode: ``"overwrite"`` to replace existing files or ``"create"`` to raise
                :class:`FileExistsError` if ``path`` already exists.
        """
        raise UnsupportedOperation(f"{type(self).__name__} does not support write_bytes")

    async def mkdir(
        self,
        path: str,
        *,
        parents: bool = True,
        exist_ok: bool = False,
    ) -> None:
        """Create directory ``path``."""
        raise UnsupportedOperation(f"{type(self).__name__} does not support mkdir")

    async def rmdir(self, path: str) -> None:
        """Remove an empty directory ``path``.

        Raises:
            OSError: With ``errno.ENOTEMPTY`` if the directory is not empty.
            NotADirectoryError: If ``path`` is not a directory.
            FileNotFoundError: If ``path`` does not exist.
        """
        raise UnsupportedOperation(f"{type(self).__name__} does not support rmdir")

    async def remove_file(self, path: str) -> None:
        """Remove a single file ``path``.

        Raises:
            IsADirectoryError: If ``path`` is a directory.
            FileNotFoundError: If ``path`` does not exist.
        """
        raise UnsupportedOperation(f"{type(self).__name__} does not support remove_file")

    # --- Derived read helpers ---

    async def ls_names(self, path: str) -> list[str]:
        """Return sorted canonical paths of direct children of ``path``."""
        return [entry.path for entry in await self.ls(path)]

    async def exists(self, path: str) -> bool:
        """Return True if ``path`` exists, propagating non-existence errors."""
        try:
            await self.info(path)
        except FileNotFoundError, NotADirectoryError:
            return False
        return True

    async def is_file(self, path: str) -> bool:
        """Return True if ``path`` exists and is a regular file."""
        try:
            return (await self.info(path)).is_file
        except FileNotFoundError, NotADirectoryError:
            return False

    async def is_dir(self, path: str) -> bool:
        """Return True if ``path`` exists and is a directory."""
        try:
            return (await self.info(path)).is_dir
        except FileNotFoundError, NotADirectoryError:
            return False

    async def size(self, path: str) -> int | None:
        """Return entry size in bytes from :meth:`info`."""
        return (await self.info(path)).size

    async def read_text(
        self,
        path: str,
        *,
        encoding: str = "utf-8",
        errors: str = "strict",
    ) -> str:
        """Read file ``path`` and decode it as text."""
        return (await self.read_bytes(path)).decode(encoding, errors=errors)

    async def write_text(
        self,
        path: str,
        text: str,
        *,
        encoding: str = "utf-8",
        errors: str = "strict",
        mode: WriteMode = "overwrite",
    ) -> None:
        """Encode ``text`` and write it to ``path``."""
        await self.write_bytes(path, text.encode(encoding, errors=errors), mode=mode)

    @overload
    async def read_many(
        self,
        paths: Sequence[str],
        *,
        limit: int | None = None,
        on_error: Literal["raise", "omit"] = "raise",
    ) -> dict[str, bytes]: ...

    @overload
    async def read_many(
        self,
        paths: Sequence[str],
        *,
        limit: int | None = None,
        on_error: Literal["return"],
    ) -> dict[str, bytes | Exception]: ...

    async def read_many(
        self,
        paths: Sequence[str],
        *,
        limit: int | None = None,
        on_error: Literal["raise", "omit", "return"] = "raise",
    ) -> dict[str, bytes] | dict[str, bytes | Exception]:
        """Concurrently read multiple files, bounded by ``limit`` or ``self.batch_size``.

        Args:
            paths: Sequence of paths to read.
            limit: Maximum concurrent reads (defaults to ``self.batch_size``).
            on_error: Error handling mode: ``"raise"`` propagates the first error,
                ``"omit"`` drops failed entries, and ``"return"`` stores the exception
                instance in the returned mapping.
        """
        if on_error not in ("raise", "omit", "return"):
            raise ValueError(f"Unsupported on_error mode {on_error!r}")

        effective_limit = self.batch_size if limit is None else limit
        if not paths:
            return {}

        if on_error == "raise":
            values = await run_bounded(
                (self.read_bytes(p) for p in paths),
                limit=effective_limit,
                return_exceptions=False,
            )
            return dict(zip(paths, values, strict=True))

        raw_results = await run_bounded(
            (self.read_bytes(p) for p in paths),
            limit=effective_limit,
            return_exceptions=True,
        )
        if on_error == "omit":
            out_bytes: dict[str, bytes] = {}
            for p, res in zip(paths, raw_results, strict=True):
                if isinstance(res, BaseException):
                    if not isinstance(res, Exception):
                        raise res
                    continue
                out_bytes[p] = res
            return out_bytes

        out_mixed: dict[str, bytes | Exception] = {}
        for p, res in zip(paths, raw_results, strict=True):
            if isinstance(res, BaseException) and not isinstance(res, Exception):
                raise res
            out_mixed[p] = res
        return out_mixed

    # --- Traversal & search ---

    async def walk(
        self,
        path: str,
        *,
        maxdepth: int | None = None,
        topdown: bool = True,
        on_error: Literal["omit", "raise"] | Callable[[OSError], None] = "omit",
    ) -> AsyncIterator[tuple[str, list[TInfo], list[TInfo]]]:
        """Walk a directory tree level-by-level, listing sibling directories concurrently.

        Args:
            path: Starting directory path.
            maxdepth: Maximum depth to descend (``1`` yields only ``path`` itself).
            topdown: When ``True``, yield parent directories before their children;
                when ``False``, yield children before their parents.
            on_error: How to handle ``OSError`` from :meth:`ls`: ``"omit"``,
                ``"raise"``, or a callback receiving the exception.

        Yields:
            Tuples of ``(dirpath, dirs, files)`` where ``dirs`` and ``files`` are
            sorted lists of :class:`FileInfo` entries.
        """
        if maxdepth is not None and maxdepth < 1:
            raise ValueError("maxdepth must be at least 1")
        if on_error not in ("omit", "raise") and not callable(on_error):
            raise ValueError(f"Unsupported on_error value {on_error!r}")

        async def _safe_ls(dirpath: str) -> list[TInfo] | OSError:
            try:
                return await self.ls(dirpath)
            except OSError as exc:
                if on_error == "raise":
                    raise
                return exc

        root = self._norm(path)
        frontier: list[str] = [root]
        depth = 1
        levels: list[list[tuple[str, list[TInfo], list[TInfo]]]] = []

        while frontier:
            results = await run_bounded(
                (_safe_ls(d) for d in frontier),
                limit=self.batch_size,
            )
            next_frontier: list[str] = []
            current_level: list[tuple[str, list[TInfo], list[TInfo]]] = []

            for dirpath, res in zip(frontier, results, strict=True):
                if isinstance(res, OSError):
                    if callable(on_error):
                        on_error(res)
                    continue
                dirs = [entry for entry in res if entry.is_dir]
                files = [entry for entry in res if not entry.is_dir]
                if topdown:
                    yield (dirpath, dirs, files)
                else:
                    current_level.append((dirpath, dirs, files))
                if maxdepth is None or depth < maxdepth:
                    next_frontier.extend(d.path for d in dirs)

            if not topdown and current_level:
                levels.append(current_level)
            frontier = next_frontier
            depth += 1

        if not topdown:
            for level in reversed(levels):
                for item in level:
                    yield item

    async def find(
        self,
        path: str,
        *,
        maxdepth: int | None = None,
        withdirs: bool = False,
    ) -> list[TInfo]:
        """Return sorted entries beneath ``path``.

        If ``path`` is a single file, returns ``[info]`` with full metadata.
        If ``path`` does not exist, returns ``[]``.
        """
        if maxdepth is not None and maxdepth < 1:
            raise ValueError("maxdepth must be at least 1")

        norm = self._norm(path)
        try:
            root_info = await self.info(norm)
        except FileNotFoundError, NotADirectoryError:
            return []

        if not root_info.is_dir:
            return [root_info]

        entries: list[TInfo] = [root_info] if withdirs else []
        async for _dirpath, dirs, files in self.walk(norm, maxdepth=maxdepth, topdown=True):
            if withdirs:
                entries.extend(dirs)
            entries.extend(files)

        entries.sort(key=lambda e: e.path)
        return entries

    async def find_paths(
        self,
        path: str,
        *,
        maxdepth: int | None = None,
        withdirs: bool = False,
    ) -> list[str]:
        """Return sorted canonical paths returned by :meth:`find`."""
        return [entry.path for entry in await self.find(path, maxdepth=maxdepth, withdirs=withdirs)]

    async def glob(
        self,
        pattern: str,
        *,
        maxdepth: int | None = None,
    ) -> list[TInfo]:
        """Match filesystem entries against a POSIX glob ``pattern``.

        Follows ``fsspec`` glob semantics:
          - ``*`` matches a single path component.
          - ``**`` matches zero or more path components.
          - ``?`` and ``[...]`` match single characters.
          - A pattern without wildcard characters returns ``[info]`` if it exists
            (and is a directory when ending with ``/``), else ``[]``.
          - A trailing ``/`` restricts matches to directories.
        """
        if maxdepth is not None and maxdepth < 1:
            raise ValueError("maxdepth must be at least 1")

        stripped_raw = strip_protocol_path(pattern, self.protocol, root_marker="")
        ends_with_sep = pattern.endswith("/") and stripped_raw != ""
        norm_pattern = self._norm(pattern)

        if not has_magic(norm_pattern):
            try:
                entry = await self.info(norm_pattern)
            except FileNotFoundError, NotADirectoryError:
                return []
            if ends_with_sep and not entry.is_dir:
                return []
            return [entry]

        # Validate/compile before doing any I/O.
        full_pattern = f"{norm_pattern}/" if ends_with_sep else norm_pattern
        regex = compile_glob(full_pattern)

        append_slash_to_dirname = (
            ends_with_sep or norm_pattern.endswith("/**") or norm_pattern == "**"
        )

        magic_indices = [idx for ch in ("*", "?", "[") if (idx := norm_pattern.find(ch)) >= 0]
        min_idx = min(magic_indices)

        if "/" in norm_pattern[:min_idx]:
            slash_idx = norm_pattern[:min_idx].rindex("/")
            root = norm_pattern[:slash_idx] or self.root_marker
            depth: int | None = norm_pattern[slash_idx + 1 :].count("/") + 1
        else:
            root = self.root_marker
            depth = norm_pattern.count("/") + 1

        if "**" in norm_pattern:
            if maxdepth is not None:
                idx_double_stars = norm_pattern.find("**")
                depth_double_stars = norm_pattern[idx_double_stars:].count("/") + 1
                assert depth is not None
                depth = depth - depth_double_stars + maxdepth
                if depth < 1:
                    return []
            else:
                depth = None

        candidates = await self.find(root, maxdepth=depth, withdirs=True)
        matched = [
            entry
            for entry in candidates
            if regex.match(
                f"{entry.path}/"
                if append_slash_to_dirname and entry.is_dir and not entry.path.endswith("/")
                else entry.path
            )
            is not None
        ]
        matched.sort(key=lambda e: e.path)
        return matched

    async def glob_paths(
        self,
        pattern: str,
        *,
        maxdepth: int | None = None,
    ) -> list[str]:
        """Return sorted canonical paths returned by :meth:`glob`."""
        return [entry.path for entry in await self.glob(pattern, maxdepth=maxdepth)]

    async def du(
        self,
        path: str,
        *,
        maxdepth: int | None = None,
        withdirs: bool = False,
    ) -> dict[str, int]:
        """Compute disk usage in a single pass over :meth:`find`."""
        entries = await self.find(path, maxdepth=maxdepth, withdirs=withdirs)
        return {entry.path: entry.size or 0 for entry in entries}

    # --- Derived mutation operations ---

    async def touch(self, path: str, *, truncate: bool = True) -> None:
        """Create an empty file at ``path``, or optionally truncate an existing one.

        Args:
            path: Target file path.
            truncate: If ``True``, an existing file is emptied. If ``False``, an
                existing file is left untouched.
        """
        if self.readonly:
            raise UnsupportedOperation(f"{type(self).__name__} does not support write_bytes")
        norm = self._norm(path)
        if truncate:
            await self.write_bytes(norm, b"", mode="overwrite")
            return
        try:
            existing = await self.info(norm)
        except FileNotFoundError:
            try:
                await self.write_bytes(norm, b"", mode="create")
            except FileExistsError:
                return
            return
        if existing.is_dir:
            raise IsADirectoryError(errno.EISDIR, "Is a directory", norm)

    async def remove(self, path: str, *, recursive: bool = False) -> None:
        """Remove a file or directory.

        Args:
            path: Target path to remove. Cannot be the filesystem root.
            recursive: When ``True``, recursively removes files first, then removes
                subdirectories bottom-up. When ``False``, removing a non-empty
                directory raises ``OSError(errno.ENOTEMPTY)``.

        Raises:
            ValueError: If ``path`` is the filesystem root.
        """
        norm = self._norm(path)
        if norm == self.root_marker:
            raise ValueError(f"Cannot remove root directory {norm!r}")

        entry = await self.info(norm)
        if not entry.is_dir:
            await self.remove_file(norm)
            return

        if not recursive:
            await self.rmdir(norm)
            return

        all_files: list[str] = []
        dirs_bottom_up: list[str] = []
        async for dirpath, _dirs, files in self.walk(norm, topdown=False, on_error="raise"):
            all_files.extend(f.path for f in files)
            dirs_bottom_up.append(dirpath)

        if all_files:
            await run_bounded(
                (self.remove_file(f) for f in all_files),
                limit=self.batch_size,
            )
        for dirpath in dirs_bottom_up:
            try:
                await self.rmdir(dirpath)
            except FileNotFoundError:
                # Implied/virtual directories on object stores vanish once their files are deleted.
                continue

    async def copy_file(self, src: str, dst: str, *, overwrite: bool = True) -> None:
        """Copy a single file from ``src`` to exact destination path ``dst``."""
        if self.readonly:
            raise UnsupportedOperation(f"{type(self).__name__} does not support write_bytes")
        src_norm = self._norm(src)
        dst_norm = self._norm(dst)

        src_info = await self.info(src_norm)
        if src_info.is_dir:
            raise IsADirectoryError(errno.EISDIR, "Is a directory", src_norm)

        try:
            dst_info = await self.info(dst_norm)
        except FileNotFoundError:
            dst_info = None

        if dst_info is not None:
            if dst_info.is_dir:
                raise IsADirectoryError(errno.EISDIR, "Is a directory", dst_norm)
            if not overwrite:
                raise FileExistsError(errno.EEXIST, "File exists", dst_norm)

        if src_norm == dst_norm:
            return

        data = await self.read_bytes(src_norm)
        write_mode: WriteMode = "overwrite" if overwrite else "create"
        await self.write_bytes(dst_norm, data, mode=write_mode)

    async def copy(self, src: str, dst: str, *, overwrite: bool = True) -> None:
        """Copy file or directory tree ``src`` onto exact target ``dst``.

        Follows ``pathlib.Path.copy`` semantics: ``dst`` is always the exact target
        path (never "copy into"). For directories, ``dst`` is created or merged into;
        when ``overwrite=False``, any conflicting destination file raises
        :class:`FileExistsError` before anything is written.
        """
        if self.readonly:
            raise UnsupportedOperation(f"{type(self).__name__} does not support write_bytes")

        src_norm = self._norm(src)
        dst_norm = self._norm(dst)

        src_info = await self.info(src_norm)
        if not src_info.is_dir:
            await self.copy_file(src_norm, dst_norm, overwrite=overwrite)
            return

        if (
            dst_norm == src_norm
            or (src_norm == self.root_marker and dst_norm != self.root_marker)
            or dst_norm.startswith(f"{src_norm}/")
        ):
            raise ValueError(f"Cannot copy directory {src_norm!r} into itself ({dst_norm!r})")

        entries = await self.find(src_norm, withdirs=True)
        target_dirs: list[str] = []
        file_pairs: list[tuple[str, str]] = []

        for entry in entries:
            if entry.path == src_norm:
                target_path = dst_norm
            else:
                rel = entry.path[len(src_norm) :].lstrip("/")
                target_path = self._norm(join_path(dst_norm, rel))
            if entry.is_dir:
                target_dirs.append(target_path)
            else:
                file_pairs.append((entry.path, target_path))

        # Pre-flight validation across all targets before writing anything.
        try:
            dst_root_info = await self.info(dst_norm)
        except FileNotFoundError:
            dst_root_info = None

        if dst_root_info is not None and not dst_root_info.is_dir:
            raise FileExistsError(errno.EEXIST, "File exists", dst_norm)

        target_dirs.sort(key=lambda p: (p.count("/"), p))

        if dst_root_info is not None:

            async def _probe(target: str) -> TInfo | None:
                try:
                    return await self.info(target)
                except FileNotFoundError, NotADirectoryError:
                    return None

            nested_dirs = [d for d in target_dirs if d != dst_norm]
            if nested_dirs:
                dir_infos = await run_bounded(
                    (_probe(d) for d in nested_dirs),
                    limit=self.batch_size,
                )
                for target_dir, existing_dir in zip(nested_dirs, dir_infos, strict=True):
                    if existing_dir is not None and not existing_dir.is_dir:
                        raise FileExistsError(errno.EEXIST, "File exists", target_dir)

            if file_pairs:
                file_infos = await run_bounded(
                    (_probe(d) for _, d in file_pairs),
                    limit=self.batch_size,
                )
                for (_src_file, dst_file), existing_file in zip(
                    file_pairs, file_infos, strict=True
                ):
                    if existing_file is not None:
                        if existing_file.is_dir:
                            raise IsADirectoryError(errno.EISDIR, "Is a directory", dst_file)
                        if not overwrite:
                            raise FileExistsError(errno.EEXIST, "File exists", dst_file)

        # Create directories shallow-to-deep, then copy files concurrently.
        for target_dir in target_dirs:
            await self.mkdir(target_dir, parents=True, exist_ok=True)

        if file_pairs:
            await run_bounded(
                (self.copy_file(s, d, overwrite=overwrite) for s, d in file_pairs),
                limit=self.batch_size,
            )

    async def move(self, src: str, dst: str, *, overwrite: bool = True) -> None:
        """Move file or directory tree ``src`` to exact target ``dst``.

        Default implementation copies ``src`` to ``dst`` and removes ``src`` only
        after the copy succeeds, so a failed copy never leaves a partially removed
        source.
        """
        src_norm = self._norm(src)
        dst_norm = self._norm(dst)
        if src_norm == self.root_marker:
            raise ValueError(f"Cannot move root directory {src_norm!r}")

        src_info = await self.info(src_norm)
        if src_norm == dst_norm:
            if not overwrite:
                raise FileExistsError(errno.EEXIST, "File exists", dst_norm)
            if self.readonly:
                raise UnsupportedOperation(f"{type(self).__name__} does not support write_bytes")
            return

        await self.copy(src_norm, dst_norm, overwrite=overwrite)
        await self.remove(src_norm, recursive=src_info.is_dir)

    # --- Local disk transfer ---

    async def get_file(self, rpath: str, lpath: str | os.PathLike[str]) -> None:
        """Download remote file ``rpath`` to local filesystem path ``lpath``."""
        data = await self.read_bytes(rpath)
        local = Path(lpath)
        await asyncio.to_thread(local.write_bytes, data)

    async def put_file(
        self,
        lpath: str | os.PathLike[str],
        rpath: str,
        *,
        mode: WriteMode = "overwrite",
    ) -> None:
        """Upload local filesystem file ``lpath`` to remote path ``rpath``."""
        if self.readonly:
            raise UnsupportedOperation(f"{type(self).__name__} does not support write_bytes")
        local = Path(lpath)
        data = await asyncio.to_thread(local.read_bytes)
        await self.write_bytes(rpath, data, mode=mode)

    # --- Text search & diff ---

    async def grep(
        self,
        path: str,
        pattern: str,
        *,
        max_count: int | None = None,
        case_sensitive: bool | None = None,
        globs: Sequence[str] | None = None,
        context_before: int | None = None,
        context_after: int | None = None,
        multiline: bool = False,
    ) -> list[GrepMatch]:
        """Search file contents under ``path`` using a regular expression.

        Args:
            path: File or directory path to search.
            pattern: Regular expression pattern.
            max_count: Optional maximum number of matches to return.
            case_sensitive: When ``None`` (default), uses smart case: sensitive iff
                ``pattern`` contains an uppercase letter.
            globs: Optional sequence of include / ``!exclude`` glob filters.
            context_before: Optional number of leading context lines per match.
            context_after: Optional number of trailing context lines per match.
            multiline: When ``True``, compile regex with ``re.MULTILINE | re.DOTALL``.

        Returns:
            Ordered list of :class:`GrepMatch` instances. Binary files (containing a
            NUL byte within the first 8 KiB) are skipped.
        """
        if max_count is not None and max_count < 0:
            raise ValueError(f"max_count must be non-negative or None, got {max_count}")
        ctx_before = 0 if context_before is None else context_before
        ctx_after = 0 if context_after is None else context_after
        if ctx_before < 0 or ctx_after < 0:
            raise ValueError("context_before and context_after must be non-negative")

        if case_sensitive is None:
            case_sensitive = any(ch.isupper() for ch in pattern)

        flags = 0 if case_sensitive else re.IGNORECASE
        if multiline:
            flags |= re.MULTILINE | re.DOTALL
        regex = re.compile(pattern, flags)

        norm = self._norm(path)
        root_info = await self.info(norm)
        if max_count == 0:
            return []

        if root_info.is_dir:
            files = await self.find(norm, withdirs=False)
        else:
            files = [root_info]

        if globs:
            files = [entry for entry in files if _matches_globs(entry, norm, globs)]

        matches: list[GrepMatch] = []
        chunk_size = self.batch_size or _DEFAULT_GREP_BATCH_SIZE

        for offset in range(0, len(files), chunk_size):
            if max_count is not None and len(matches) >= max_count:
                break
            batch = files[offset : offset + chunk_size]
            payloads = await run_bounded(
                (self.read_bytes(entry.path) for entry in batch),
                limit=self.batch_size,
                return_exceptions=True,
            )
            for entry, payload in zip(batch, payloads, strict=True):
                if max_count is not None and len(matches) >= max_count:
                    break
                if isinstance(payload, BaseException):
                    if not isinstance(payload, OSError):
                        raise payload
                    continue
                if b"\x00" in payload[:_BINARY_CHECK_BYTES]:
                    continue
                text = payload.decode("utf-8", errors="replace")
                remaining = None if max_count is None else max_count - len(matches)
                matches.extend(
                    _grep_text(
                        entry.path,
                        text,
                        regex,
                        ctx_before=ctx_before,
                        ctx_after=ctx_after,
                        multiline=multiline,
                        remaining=remaining,
                    )
                )

        return matches

    async def diff(self, path1: str, path2: str, *, n: int = 3) -> str:
        """Return a unified diff between two files with their paths as labels."""
        norm1 = self._norm(path1)
        norm2 = self._norm(path2)
        text1, text2 = await run_bounded((
            self.read_text(norm1, errors="replace"),
            self.read_text(norm2, errors="replace"),
        ))
        lines1 = text1.splitlines()
        lines2 = text2.splitlines()
        return "\n".join(
            difflib.unified_diff(
                lines1,
                lines2,
                fromfile=norm1,
                tofile=norm2,
                n=n,
                lineterm="",
            )
        )

    # --- Streaming ---

    @overload
    def open(
        self,
        path: str,
        mode: Literal["rb"] = "rb",
        *,
        block_size: int | None = None,
    ) -> AsyncBufferedReader: ...

    @overload
    def open(
        self,
        path: str,
        mode: Literal["wb", "xb", "ab"],
        *,
        block_size: int | None = None,
    ) -> AsyncBufferedWriter: ...

    def open(
        self,
        path: str,
        mode: str = "rb",
        *,
        block_size: int | None = None,
    ) -> AsyncBufferedReader | AsyncBufferedWriter:
        """Open ``path`` as an async binary buffered stream.

        Args:
            path: Target file path.
            mode: ``"rb"`` (returns :class:`AsyncBufferedReader`), or ``"wb"`` /
                ``"xb"`` / ``"ab"`` (returns :class:`AsyncBufferedWriter`).
            block_size: Optional read block size in bytes for ``"rb"`` mode.

        Raises:
            ValueError: If ``mode`` is not one of ``"rb"``, ``"wb"``, ``"xb"``, ``"ab"``.
        """
        self._check_open()
        norm = self._norm(path)
        if mode == "rb":
            bs = DEFAULT_BLOCK_SIZE if block_size is None else block_size
            return AsyncBufferedReader(self, norm, block_size=bs)
        if mode in ("wb", "xb", "ab"):
            return AsyncBufferedWriter(self, norm, mode=mode)
        raise ValueError(f"Unsupported mode {mode!r}; expected one of 'rb', 'wb', 'xb', 'ab'")
