"""Explicit, statically typed synchronous facade over :class:`~upathtools.fs.AsyncFS`."""

import io
from typing import TYPE_CHECKING, Any, Literal, Self, overload

from upathtools.fs.runner import LoopRunner, get_default_runner
from upathtools.fs.streams import (
    AsyncBufferedReader,
    AsyncBufferedWriter,
    AsyncReadStream,
    AsyncSeekableStream,
    AsyncWriteStream,
)
from upathtools.fs.types import FileInfo, GrepMatch, WriteMode


if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine, Iterator, Sequence
    import os
    from types import TracebackType

    from upathtools.fs.base import AsyncFS


type _BinaryMode = Literal["rb", "wb", "xb", "ab"]
type _AsyncStream = AsyncReadStream | AsyncWriteStream | AsyncBufferedReader | AsyncBufferedWriter

_TEXT_TO_BINARY_MODE: dict[str, _BinaryMode] = {
    "r": "rb",
    "rt": "rb",
    "w": "wb",
    "wt": "wb",
    "x": "xb",
    "xt": "xb",
    "a": "ab",
    "at": "ab",
}
_BINARY_MODES: dict[str, _BinaryMode] = {
    "rb": "rb",
    "wb": "wb",
    "xb": "xb",
    "ab": "ab",
}


class SyncFileBridge(io.BufferedIOBase):
    """Synchronous binary file bridge over an :class:`AsyncReadStream` or :class:`AsyncWriteStream`.

    All async stream operations execute on the supplied :class:`LoopRunner`.
    Entering and exiting with a context manager preserves the underlying async
    stream's exception semantics (e.g. :class:`~upathtools.fs.AsyncBufferedWriter`
    discards buffered bytes if an exception escapes the ``with`` block).
    """

    __slots__ = (
        "_closed",
        "_name",
        "_readable",
        "_runner",
        "_seekable",
        "_stream",
        "_timeout",
        "_writable",
    )

    def __init__(
        self,
        stream: _AsyncStream,
        runner: LoopRunner,
        *,
        timeout: float | None = None,
        name: str = "",
    ) -> None:
        """Open ``stream`` synchronously via ``runner``.

        Args:
            stream: Underlying async read or write stream.
            runner: :class:`LoopRunner` used to drive stream coroutines.
            timeout: Optional default timeout in seconds for each stream operation.
            name: Optional path/name exposed via the :attr:`name` property.
        """
        super().__init__()
        # Start marked closed so io.IOBase.__del__ does not attempt to close a
        # partially initialized bridge if __aenter__() raises.
        self._closed = True
        self._name = name
        self._stream = stream
        self._runner = runner
        self._timeout = timeout
        self._readable = isinstance(stream, AsyncReadStream)
        self._writable = isinstance(stream, AsyncWriteStream)
        self._seekable = isinstance(stream, AsyncSeekableStream)
        self._run(stream.__aenter__())
        self._closed = False

    def _run[T](self, coro: Coroutine[Any, Any, T]) -> T:
        return self._runner.run(coro, timeout=self._timeout)

    def _check_not_closed(self) -> None:
        if self._closed:
            raise ValueError("I/O operation on closed file")

    @property
    def name(self) -> str:
        """Path or name associated with this file bridge."""
        return self._name

    @property
    def closed(self) -> bool:
        """Return True if this file bridge has been closed."""
        return self._closed

    def readable(self) -> bool:
        """Return True if the underlying stream supports reading."""
        return self._readable

    def writable(self) -> bool:
        """Return True if the underlying stream supports writing."""
        return self._writable

    def seekable(self) -> bool:
        """Return True if the underlying stream supports random-access seeking."""
        return self._seekable

    def read(self, size: int | None = -1, /) -> bytes:
        """Read up to ``size`` bytes, or until EOF when ``size`` is None or negative."""
        self._check_not_closed()
        if not isinstance(self._stream, AsyncReadStream):
            raise io.UnsupportedOperation("not readable")
        n = -1 if size is None else size
        return self._run(self._stream.read(n))

    def read1(self, size: int = -1, /) -> bytes:
        """Read up to ``size`` bytes from the stream (for :class:`io.TextIOWrapper` compat)."""
        return self.read(size)

    def readinto(self, buffer: Any, /) -> int:
        """Read bytes into a pre-allocated writable bytes-like ``buffer``."""
        self._check_not_closed()
        if not isinstance(self._stream, AsyncReadStream):
            raise io.UnsupportedOperation("not readable")
        mv = memoryview(buffer).cast("B")
        data = self.read(len(mv))
        n = len(data)
        mv[:n] = data
        return n

    def readinto1(self, buffer: Any, /) -> int:
        """Read bytes into ``buffer`` using a single read call."""
        return self.readinto(buffer)

    def readline(self, size: int | None = -1, /) -> bytes:
        """Read and return a single line (up to ``size`` bytes if non-negative)."""
        self._check_not_closed()
        if not isinstance(self._stream, AsyncReadStream):
            raise io.UnsupportedOperation("not readable")
        n = -1 if size is None else size
        return self._run(self._stream.readline(n))

    def seek(self, offset: int, whence: int = io.SEEK_SET, /) -> int:
        """Move the stream position and return the new absolute byte offset."""
        self._check_not_closed()
        if not isinstance(self._stream, AsyncSeekableStream):
            raise io.UnsupportedOperation("not seekable")
        stream = self._stream

        async def _seek() -> int:
            return stream.seek(offset, whence)

        return self._run(_seek())

    def tell(self) -> int:
        """Return the current byte offset within the stream."""
        self._check_not_closed()
        if not isinstance(self._stream, AsyncSeekableStream):
            raise io.UnsupportedOperation("not seekable")
        stream = self._stream

        async def _tell() -> int:
            return stream.tell()

        return self._run(_tell())

    def write(self, buffer: Any, /) -> int:
        """Write bytes from ``buffer`` to the stream and return the number of bytes written."""
        self._check_not_closed()
        if not isinstance(self._stream, AsyncWriteStream):
            raise io.UnsupportedOperation("not writable")
        payload = bytes(memoryview(buffer))
        return self._run(self._stream.write(payload))

    def flush(self) -> None:
        """Flush the underlying write stream (no-op for read-only streams)."""
        self._check_not_closed()
        if isinstance(self._stream, AsyncWriteStream):
            self._run(self._stream.flush())

    def close(self) -> None:
        """Close the underlying async stream via :meth:`aclose`. Idempotent."""
        if self._closed:
            return
        try:
            self._run(self._stream.aclose())
        finally:
            self._closed = True

    def __enter__(self) -> Self:
        self._check_not_closed()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._closed:
            return
        if exc_type is not None:
            try:
                self._run(self._stream.__aexit__(exc_type, exc_val, exc_tb))
            finally:
                self._closed = True
            return
        self.close()


class _SyncTextWrapper(io.TextIOWrapper):
    """Text wrapper that aborts writer commits when an exception escapes ``with``."""

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if exc_type is not None:
            buf = self.buffer
            if isinstance(buf, SyncFileBridge):
                buf.__exit__(exc_type, exc_val, exc_tb)
                return
        super().__exit__(exc_type, exc_val, exc_tb)


class SyncFS[TInfo: FileInfo = FileInfo]:
    """Explicit, statically typed synchronous facade over an :class:`~upathtools.fs.AsyncFS`.

    Every async method on :class:`~upathtools.fs.AsyncFS` is mirrored 1:1 with the
    same parameter names, defaults, and static types, executed on ``runner``.
    If ``runner`` is omitted, the process-wide :func:`get_default_runner` is used
    (convenient for scripts; libraries should pass an explicit :class:`LoopRunner`
    or await :class:`~upathtools.fs.AsyncFS` directly).
    """

    __slots__ = ("_fs", "_runner", "_timeout")

    def __init__(
        self,
        fs: AsyncFS[TInfo],
        *,
        runner: LoopRunner | None = None,
        timeout: float | None = None,
    ) -> None:
        """Create a synchronous facade for ``fs``.

        Args:
            fs: Underlying :class:`~upathtools.fs.AsyncFS` instance.
            runner: Optional :class:`LoopRunner` used to execute coroutines.
                Defaults to :func:`get_default_runner`.
            timeout: Optional default timeout in seconds applied to each operation.
        """
        self._fs = fs
        self._runner = get_default_runner() if runner is None else runner
        self._timeout = timeout

    @property
    def fs(self) -> AsyncFS[TInfo]:
        """The underlying :class:`~upathtools.fs.AsyncFS` instance."""
        return self._fs

    @property
    def runner(self) -> LoopRunner:
        """The :class:`LoopRunner` executing coroutines for this facade."""
        return self._runner

    @property
    def timeout(self) -> float | None:
        """Default per-operation timeout in seconds, or ``None`` for no timeout."""
        return self._timeout

    def _run[T](self, coro: Coroutine[Any, Any, T]) -> T:
        return self._runner.run(coro, timeout=self._timeout)

    # --- Lifecycle & cache ---

    def close(self) -> None:
        """Close the underlying filesystem via :meth:`AsyncFS.aclose`. Idempotent."""
        self._run(self._fs.aclose())

    def __enter__(self) -> Self:
        self._fs._check_open()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def invalidate_cache(self, path: str | None = None) -> None:
        """Invalidate cached directory listings on the underlying filesystem."""
        self._fs.invalidate_cache(path)

    # --- Core read & metadata primitives ---

    def info(self, path: str) -> TInfo:
        """Return normalized metadata for ``path``."""
        return self._run(self._fs.info(path))

    def ls(self, path: str) -> list[TInfo]:
        """List direct children of directory ``path``, sorted by ``entry.path``."""
        return self._run(self._fs.ls(path))

    def ls_names(self, path: str) -> list[str]:
        """Return sorted canonical paths of direct children of ``path``."""
        return self._run(self._fs.ls_names(path))

    def read_bytes(
        self,
        path: str,
        *,
        start: int | None = None,
        end: int | None = None,
    ) -> bytes:
        """Read bytes from file ``path``, optionally sliced by ``[start:end]``."""
        return self._run(self._fs.read_bytes(path, start=start, end=end))

    def read_text(
        self,
        path: str,
        *,
        encoding: str = "utf-8",
        errors: str = "strict",
    ) -> str:
        """Read file ``path`` and decode it as text."""
        return self._run(self._fs.read_text(path, encoding=encoding, errors=errors))

    @overload
    def read_many(
        self,
        paths: Sequence[str],
        *,
        limit: int | None = None,
        on_error: Literal["raise", "omit"] = "raise",
    ) -> dict[str, bytes]: ...

    @overload
    def read_many(
        self,
        paths: Sequence[str],
        *,
        limit: int | None = None,
        on_error: Literal["return"],
    ) -> dict[str, bytes | Exception]: ...

    def read_many(
        self,
        paths: Sequence[str],
        *,
        limit: int | None = None,
        on_error: Literal["raise", "omit", "return"] = "raise",
    ) -> dict[str, bytes] | dict[str, bytes | Exception]:
        """Concurrently read multiple files on the runner loop."""
        if on_error == "return":
            return self._run(self._fs.read_many(paths, limit=limit, on_error="return"))
        return self._run(self._fs.read_many(paths, limit=limit, on_error=on_error))

    def exists(self, path: str) -> bool:
        """Return True if ``path`` exists, propagating non-existence errors."""
        return self._run(self._fs.exists(path))

    def is_file(self, path: str) -> bool:
        """Return True if ``path`` exists and is a regular file."""
        return self._run(self._fs.is_file(path))

    def is_dir(self, path: str) -> bool:
        """Return True if ``path`` exists and is a directory."""
        return self._run(self._fs.is_dir(path))

    def size(self, path: str) -> int | None:
        """Return entry size in bytes from :meth:`info`."""
        return self._run(self._fs.size(path))

    # --- Mutation operations ---

    def write_bytes(
        self,
        path: str,
        data: bytes | bytearray | memoryview,
        *,
        mode: WriteMode = "overwrite",
    ) -> None:
        """Write raw bytes to ``path``."""
        return self._run(self._fs.write_bytes(path, data, mode=mode))

    def write_text(
        self,
        path: str,
        text: str,
        *,
        encoding: str = "utf-8",
        errors: str = "strict",
        mode: WriteMode = "overwrite",
    ) -> None:
        """Encode ``text`` and write it to ``path``."""
        return self._run(
            self._fs.write_text(path, text, encoding=encoding, errors=errors, mode=mode)
        )

    def touch(self, path: str, *, truncate: bool = True) -> None:
        """Create an empty file at ``path``, or optionally truncate an existing one."""
        return self._run(self._fs.touch(path, truncate=truncate))

    def mkdir(
        self,
        path: str,
        *,
        parents: bool = True,
        exist_ok: bool = False,
    ) -> None:
        """Create directory ``path``."""
        return self._run(self._fs.mkdir(path, parents=parents, exist_ok=exist_ok))

    def rmdir(self, path: str) -> None:
        """Remove an empty directory ``path``."""
        return self._run(self._fs.rmdir(path))

    def remove_file(self, path: str) -> None:
        """Remove a single file ``path``."""
        return self._run(self._fs.remove_file(path))

    def remove(self, path: str, *, recursive: bool = False) -> None:
        """Remove a file or directory."""
        return self._run(self._fs.remove(path, recursive=recursive))

    def copy_file(self, src: str, dst: str, *, overwrite: bool = True) -> None:
        """Copy a single file from ``src`` to exact destination path ``dst``."""
        return self._run(self._fs.copy_file(src, dst, overwrite=overwrite))

    def copy(self, src: str, dst: str, *, overwrite: bool = True) -> None:
        """Copy file or directory tree ``src`` onto exact target ``dst``."""
        return self._run(self._fs.copy(src, dst, overwrite=overwrite))

    def move(self, src: str, dst: str, *, overwrite: bool = True) -> None:
        """Move file or directory tree ``src`` to exact target ``dst``."""
        return self._run(self._fs.move(src, dst, overwrite=overwrite))

    # --- Traversal & search ---

    def walk(
        self,
        path: str,
        *,
        maxdepth: int | None = None,
        topdown: bool = True,
        on_error: Literal["omit", "raise"] | Callable[[OSError], None] = "omit",
    ) -> Iterator[tuple[str, list[TInfo], list[TInfo]]]:
        """Walk a directory tree and return an iterator of ``(dirpath, dirs, files)``."""

        async def _collect() -> list[tuple[str, list[TInfo], list[TInfo]]]:
            return [
                item
                async for item in self._fs.walk(
                    path,
                    maxdepth=maxdepth,
                    topdown=topdown,
                    on_error=on_error,
                )
            ]

        return iter(self._run(_collect()))

    def find(
        self,
        path: str,
        *,
        maxdepth: int | None = None,
        withdirs: bool = False,
    ) -> list[TInfo]:
        """Return sorted entries beneath ``path``."""
        return self._run(self._fs.find(path, maxdepth=maxdepth, withdirs=withdirs))

    def find_paths(
        self,
        path: str,
        *,
        maxdepth: int | None = None,
        withdirs: bool = False,
    ) -> list[str]:
        """Return sorted canonical paths returned by :meth:`find`."""
        return self._run(self._fs.find_paths(path, maxdepth=maxdepth, withdirs=withdirs))

    def glob(
        self,
        pattern: str,
        *,
        maxdepth: int | None = None,
    ) -> list[TInfo]:
        """Match filesystem entries against a POSIX glob ``pattern``."""
        return self._run(self._fs.glob(pattern, maxdepth=maxdepth))

    def glob_paths(
        self,
        pattern: str,
        *,
        maxdepth: int | None = None,
    ) -> list[str]:
        """Return sorted canonical paths returned by :meth:`glob`."""
        return self._run(self._fs.glob_paths(pattern, maxdepth=maxdepth))

    def du(
        self,
        path: str,
        *,
        maxdepth: int | None = None,
        withdirs: bool = False,
    ) -> dict[str, int]:
        """Compute disk usage in a single pass over :meth:`find`."""
        return self._run(self._fs.du(path, maxdepth=maxdepth, withdirs=withdirs))

    def grep(
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
        """Search file contents under ``path`` using a regular expression."""
        return self._run(
            self._fs.grep(
                path,
                pattern,
                max_count=max_count,
                case_sensitive=case_sensitive,
                globs=globs,
                context_before=context_before,
                context_after=context_after,
                multiline=multiline,
            )
        )

    def diff(self, path1: str, path2: str, *, n: int = 3) -> str:
        """Return a unified diff between two files with their paths as labels."""
        return self._run(self._fs.diff(path1, path2, n=n))

    # --- Local disk transfer ---

    def get_file(self, rpath: str, lpath: str | os.PathLike[str]) -> None:
        """Download remote file ``rpath`` to local filesystem path ``lpath``."""
        return self._run(self._fs.get_file(rpath, lpath))

    def put_file(
        self,
        lpath: str | os.PathLike[str],
        rpath: str,
        *,
        mode: WriteMode = "overwrite",
    ) -> None:
        """Upload local filesystem file ``lpath`` to remote path ``rpath``."""
        return self._run(self._fs.put_file(lpath, rpath, mode=mode))

    # --- Streaming ---

    @overload
    def open(
        self,
        path: str,
        mode: Literal["rb", "wb", "xb", "ab"] = "rb",
        *,
        block_size: int | None = None,
        encoding: None = None,
        errors: None = None,
        newline: None = None,
    ) -> SyncFileBridge: ...

    @overload
    def open(
        self,
        path: str,
        mode: Literal["r", "w", "x", "a", "rt", "wt", "xt", "at"],
        *,
        block_size: int | None = None,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> io.TextIOWrapper: ...

    @overload
    def open(
        self,
        path: str,
        mode: str = "rb",
        *,
        block_size: int | None = None,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> SyncFileBridge | io.TextIOWrapper: ...

    def _open_binary(
        self,
        path: str,
        mode: _BinaryMode,
        block_size: int | None,
    ) -> SyncFileBridge:
        stream: AsyncBufferedReader | AsyncBufferedWriter
        if mode == "rb":
            stream = self._fs.open(path, "rb", block_size=block_size)
        else:
            stream = self._fs.open(path, mode, block_size=block_size)
        return SyncFileBridge(stream, self._runner, timeout=self._timeout, name=path)

    def open(
        self,
        path: str,
        mode: str = "rb",
        *,
        block_size: int | None = None,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> SyncFileBridge | io.TextIOWrapper:
        """Open ``path`` as a synchronous binary or text file object.

        Args:
            path: Target file path.
            mode: Binary mode (``"rb"``, ``"wb"``, ``"xb"``, ``"ab"``) returning a
                :class:`SyncFileBridge`, or text mode (``"r"``, ``"w"``, ``"x"``,
                ``"a"``, or their ``*t`` equivalents) returning an
                :class:`io.TextIOWrapper`.
            block_size: Optional read block size in bytes for read modes.
            encoding: Text encoding (defaults to ``"utf-8"`` in text modes; must be
                ``None`` in binary modes).
            errors: Text error handling scheme (must be ``None`` in binary modes).
            newline: Universal newline control (must be ``None`` in binary modes).

        Raises:
            ValueError: If ``mode`` is unsupported or text-only arguments are passed
                with a binary mode.
        """
        direct_bin = _BINARY_MODES.get(mode)
        if direct_bin is not None:
            if encoding is not None:
                raise ValueError("binary mode doesn't take an encoding argument")
            if errors is not None:
                raise ValueError("binary mode doesn't take an errors argument")
            if newline is not None:
                raise ValueError("binary mode doesn't take a newline argument")
            return self._open_binary(path, direct_bin, block_size)

        bin_mode = _TEXT_TO_BINARY_MODE.get(mode)
        if bin_mode is None:
            raise ValueError(
                f"Unsupported mode {mode!r}; expected one of "
                "'r', 'rb', 'w', 'wb', 'x', 'xb', 'a', 'ab'"
            )

        bridge = self._open_binary(path, bin_mode, block_size)
        try:
            return _SyncTextWrapper(
                bridge,
                encoding="utf-8" if encoding is None else encoding,
                errors=errors,
                newline=newline,
            )
        except BaseException as exc:
            bridge.__exit__(type(exc), exc, exc.__traceback__)
            raise
