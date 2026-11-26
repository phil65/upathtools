"""Root-jailed directory prefix wrapper filesystem for ``upathtools.fs``."""

import dataclasses
import os
from typing import TYPE_CHECKING, ClassVar

from upathtools.fs._sync import FSSPEC_SYNC_VERSION, FsspecSyncNote
from upathtools.fs.base import AsyncFS
from upathtools.fs.errors import PathEscapeError
from upathtools.fs.paths import strip_protocol as strip_protocol_path
from upathtools.fs.types import FileInfo, GrepMatch


if TYPE_CHECKING:
    from collections.abc import Sequence

    from upathtools.fs.types import WriteMode


__fsspec_sync__ = FsspecSyncNote(
    version=FSSPEC_SYNC_VERSION,
    upstream=("fsspec/implementations/dirfs.py:DirFileSystem",),
    notes=(
        "Async-first root-jailed prefix wrapper over any AsyncFS[TInfo] with no "
        "asynchronous mode flag mismatch. Rejects any '..' escape above the DirFS "
        "root via normalize_path (raising PathEscapeError before any I/O), rewrites "
        "FileInfo paths with dataclasses.replace to preserve TInfo subclasses, mirrors "
        "the wrapped filesystem's capabilities, and delegates native overrides "
        "(move, find, glob, grep)."
    ),
)


class DirFS[TInfo: FileInfo = FileInfo](AsyncFS[TInfo]):
    """Root-jailed directory prefix wrapper over any :class:`AsyncFS`.

    Every path passed to ``DirFS`` is normalized relative to its virtual ``"/"``
    root before being joined under :attr:`root` on the wrapped filesystem. Any
    ``".."`` segment that would climb above the ``DirFS`` root raises
    :class:`~upathtools.fs.errors.PathEscapeError` before any I/O occurs.

    Behavioural notes:
      - ``root`` is normalized using the wrapped filesystem's own path rules and
        does **not** have to exist at construction time; existence is checked lazily
        on I/O operations.
      - Absolute paths (``"/a/b"``) and relative paths (``"a/b"``) both resolve
        relative to ``root`` (``"<root>/a/b"``).
      - Returned :class:`FileInfo` instances preserve the exact ``TInfo`` subclass
        (e.g. :class:`~upathtools.fs.LocalFSInfo`) via :func:`dataclasses.replace`.
      - :attr:`capabilities` and :attr:`readonly` mirror the wrapped filesystem.
      - The jail is lexical: it rejects paths that leave ``root``, but it does not
        resolve symlinks. On a backend with symlinks (``LocalFS``) a link inside
        ``root`` that points outside is followed by the wrapped filesystem; DirFS is
        a path prefix, not a security sandbox.
      - :meth:`aclose` closes the wrapped filesystem **only** when constructed with
        ``close_wrapped=True`` (defaults to ``False`` so callers sharing an
        underlying filesystem keep control of its lifecycle).
    """

    protocol: ClassVar[str | tuple[str, ...]] = "dir"
    root_marker: ClassVar[str] = "/"
    sep: ClassVar[str] = "/"

    def __init__(
        self,
        fs: AsyncFS[TInfo],
        root: str | os.PathLike[str],
        *,
        close_wrapped: bool = False,
        batch_size: int | None = None,
        use_listings_cache: bool = False,
        listings_expiry_time: float | None = None,
        max_cached_paths: int | None = None,
    ) -> None:
        """Wrap ``fs`` rooted at ``root``.

        Args:
            fs: Underlying :class:`AsyncFS` to delegate to.
            root: Directory prefix on ``fs`` (normalized with ``fs``'s own rules;
                need not exist until I/O is performed).
            close_wrapped: When ``True``, :meth:`aclose` also closes ``fs``.
                Defaults to ``False``.
            batch_size: Optional concurrency limit (defaults to ``fs.batch_size``).
            use_listings_cache: Enable optional thread-safe :class:`DirCache`.
            listings_expiry_time: Optional cache TTL in seconds.
            max_cached_paths: Optional cache LRU size cap.
        """
        super().__init__(
            batch_size=fs.batch_size if batch_size is None else batch_size,
            use_listings_cache=use_listings_cache,
            listings_expiry_time=listings_expiry_time,
            max_cached_paths=max_cached_paths,
        )
        self._fs = fs
        self._root = fs._norm(os.fspath(root))
        self._close_wrapped = close_wrapped

    @property
    def fs(self) -> AsyncFS[TInfo]:
        """Wrapped underlying filesystem."""
        return self._fs

    @property
    def root(self) -> str:
        """Normalized root directory prefix on the wrapped filesystem."""
        return self._root

    @property
    def close_wrapped(self) -> bool:
        """Whether :meth:`aclose` also closes the wrapped filesystem."""
        return self._close_wrapped

    @property
    def capabilities(self) -> frozenset[str]:
        """Mirror the wrapped filesystem's native capabilities."""
        return self._fs.capabilities

    async def aclose(self) -> None:
        """Close this ``DirFS`` (and the wrapped filesystem iff ``close_wrapped=True``)."""
        if not self._closed:
            self._closed = True
            self.invalidate_cache()
            if self._close_wrapped:
                await self._fs.aclose()

    def invalidate_cache(self, path: str | None = None) -> None:
        """Invalidate caches on this ``DirFS`` and the scoped subtree of the wrapped fs."""
        super().invalidate_cache(path)
        if path is None:
            self._fs.invalidate_cache(self._root)
        else:
            self._fs.invalidate_cache(self._join(path))

    # --- Path translation ---

    def _join(self, path: str) -> str:
        """Normalize ``path`` inside the DirFS jail and join it under :attr:`root`."""
        local = self._norm(path)
        if local == "/":
            return self._root
        rel = local.lstrip("/")
        if self._root.endswith("/"):
            return f"{self._root}{rel}"
        return f"{self._root}/{rel}"

    def _strip_root(self, wrapped_path: str) -> str:
        """Rewrite a path from the wrapped filesystem back to a DirFS-rooted path."""
        if wrapped_path == self._root:
            return "/"
        prefix = self._root if self._root.endswith("/") else f"{self._root}/"
        if wrapped_path.startswith(prefix):
            rel = wrapped_path[len(prefix) :]
            return f"/{rel}" if rel else "/"
        if self._root.startswith("/") and not wrapped_path.startswith("/"):
            alt_root = self._root.lstrip("/")
            if wrapped_path == alt_root:
                return "/"
            alt_prefix = f"{alt_root}/"
            if wrapped_path.startswith(alt_prefix):
                return f"/{wrapped_path[len(alt_prefix) :]}"
        raise PathEscapeError(f"Wrapped path {wrapped_path!r} is outside DirFS root {self._root!r}")

    def _rewrite_info(self, entry: TInfo) -> TInfo:
        """Return a copy of ``entry`` with its ``path`` rewritten to DirFS coordinates."""
        return dataclasses.replace(entry, path=self._strip_root(entry.path))

    def _rewrite_match(self, match: GrepMatch) -> GrepMatch:
        """Return a copy of ``match`` with its ``path`` rewritten to DirFS coordinates."""
        return dataclasses.replace(match, path=self._strip_root(match.path))

    # --- Read primitives ---

    async def info(self, path: str) -> TInfo:
        self._check_open()
        entry = await self._fs.info(self._join(path))
        return self._rewrite_info(entry)

    async def ls(self, path: str) -> list[TInfo]:
        self._check_open()
        entries = await self._fs.ls(self._join(path))
        rewritten = [self._rewrite_info(entry) for entry in entries]
        rewritten.sort(key=lambda e: e.path)
        return rewritten

    async def read_bytes(
        self,
        path: str,
        *,
        start: int | None = None,
        end: int | None = None,
    ) -> bytes:
        self._check_open()
        return await self._fs.read_bytes(self._join(path), start=start, end=end)

    # --- Mutation primitives ---

    async def write_bytes(
        self,
        path: str,
        data: bytes | bytearray | memoryview,
        *,
        mode: WriteMode = "overwrite",
    ) -> None:
        self._check_open()
        await self._fs.write_bytes(self._join(path), data, mode=mode)

    async def mkdir(
        self,
        path: str,
        *,
        parents: bool = True,
        exist_ok: bool = False,
    ) -> None:
        self._check_open()
        await self._fs.mkdir(self._join(path), parents=parents, exist_ok=exist_ok)

    async def rmdir(self, path: str) -> None:
        self._check_open()
        local = self._norm(path)
        if local == "/":
            raise ValueError("Cannot remove root directory '/'")
        await self._fs.rmdir(self._join(local))

    async def remove_file(self, path: str) -> None:
        self._check_open()
        await self._fs.remove_file(self._join(path))

    # --- Delegated native operations ---

    async def move(self, src: str, dst: str, *, overwrite: bool = True) -> None:
        self._check_open()
        src_local = self._norm(src)
        if src_local == "/":
            raise ValueError("Cannot move root directory '/'")
        src_wrapped = self._join(src_local)
        dst_wrapped = self._join(dst)
        if "move" in self._fs.capabilities:
            await self._fs.move(src_wrapped, dst_wrapped, overwrite=overwrite)
            return
        await super().move(src_local, dst, overwrite=overwrite)

    async def find(
        self,
        path: str,
        *,
        maxdepth: int | None = None,
        withdirs: bool = False,
    ) -> list[TInfo]:
        self._check_open()
        target = self._join(path)
        if type(self._fs).find is not AsyncFS.find:
            entries = await self._fs.find(target, maxdepth=maxdepth, withdirs=withdirs)
            rewritten = [self._rewrite_info(entry) for entry in entries]
            rewritten.sort(key=lambda e: e.path)
            return rewritten
        return await super().find(path, maxdepth=maxdepth, withdirs=withdirs)

    async def glob(
        self,
        pattern: str,
        *,
        maxdepth: int | None = None,
    ) -> list[TInfo]:
        self._check_open()
        if type(self._fs).glob is not AsyncFS.glob:
            stripped_raw = strip_protocol_path(pattern, self.protocol, root_marker="")
            ends_with_sep = pattern.endswith("/") and stripped_raw != ""
            joined = self._join(pattern)
            if ends_with_sep and not joined.endswith("/"):
                joined = f"{joined}/"
            entries = await self._fs.glob(joined, maxdepth=maxdepth)
            rewritten = [self._rewrite_info(entry) for entry in entries]
            rewritten.sort(key=lambda e: e.path)
            return rewritten
        return await super().glob(pattern, maxdepth=maxdepth)

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
        self._check_open()
        target = self._join(path)
        if "grep" in self._fs.capabilities or type(self._fs).grep is not AsyncFS.grep:
            matches = await self._fs.grep(
                target,
                pattern,
                max_count=max_count,
                case_sensitive=case_sensitive,
                globs=globs,
                context_before=context_before,
                context_after=context_after,
                multiline=multiline,
            )
            return [self._rewrite_match(m) for m in matches]
        return await super().grep(
            path,
            pattern,
            max_count=max_count,
            case_sensitive=case_sensitive,
            globs=globs,
            context_before=context_before,
            context_after=context_after,
            multiline=multiline,
        )

    def __repr__(self) -> str:
        return f"{type(self).__name__}(fs={self._fs!r}, root={self._root!r})"
