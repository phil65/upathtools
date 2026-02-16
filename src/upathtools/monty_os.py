"""Adapter bridging fsspec filesystems to Pydantic Monty's AbstractOS interface.

This module provides `FsspecOS`, an implementation of Monty's `AbstractOS` that
delegates all filesystem operations to an fsspec `BaseAsyncFileSystem`.

This allows running sandboxed Monty Python code against any filesystem that
upathtools supports (E2B, Modal, Beam, MCP, SQLite, memory, etc.).

Example::

    from pydantic_monty import Monty
    from upathtools.monty_os import FsspecOS
    from upathtools.filesystems.isolated_memory_fs import IsolatedMemoryFileSystem

    fs = IsolatedMemoryFileSystem()
    fs.pipe_file("/hello.txt", b"world")

    os_access = FsspecOS(fs)
    result = Monty("Path('/hello.txt').read_text()").run(os=os_access)
    assert result == "world"
"""

from __future__ import annotations

from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any

from upathtools.async_helpers import sync


if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from upathtools.filesystems.base import BaseAsyncFileSystem


class FsspecOS:
    """Monty AbstractOS implementation backed by an fsspec BaseAsyncFileSystem.

    Bridges Monty's synchronous filesystem interface to an async fsspec filesystem
    by running async operations through the filesystem's event loop.

    Subclass and override `getenv` / `get_environ` for dynamic environment
    variable behavior beyond a static dict.

    Args:
        fs: The async filesystem to delegate operations to.
        environ: Optional dict of environment variables accessible to Monty code.
        root_dir: Base directory for resolving relative paths. Default is '/'.
    """

    def __init__(
        self,
        fs: BaseAsyncFileSystem,  # type: ignore[type-arg]
        environ: dict[str, str] | None = None,
        root_dir: str | PurePosixPath = "/",
    ) -> None:
        self._fs = fs
        self._environ = environ or {}
        self._root_dir = PurePosixPath(root_dir)

    def _sync[T](self, func: Callable[..., Coroutine[Any, Any, T]], *args: Any) -> T:
        """Run an async method synchronously using the filesystem's event loop."""
        import fsspec.asyn

        loop = self._fs.loop or fsspec.asyn.get_loop()
        return sync(loop, func, *args)

    def _to_str(self, path: PurePosixPath) -> str:
        """Convert a PurePosixPath to a string path for fsspec."""
        return str(path)

    # --- Dispatch ---

    def __call__(  # noqa: PLR0911
        self,
        function_name: str,
        args: tuple[object, ...],
        kwargs: dict[str, object] | None = None,
    ) -> object:
        """Dispatch a filesystem operation to the appropriate method.

        This is called by Monty when Monty code invokes Path methods or os functions.
        """
        kwargs = kwargs or {}
        match function_name:
            case "Path.exists":
                return self.path_exists(*args)  # type: ignore[arg-type]
            case "Path.is_file":
                return self.path_is_file(*args)  # type: ignore[arg-type]
            case "Path.is_dir":
                return self.path_is_dir(*args)  # type: ignore[arg-type]
            case "Path.is_symlink":
                return self.path_is_symlink(*args)  # type: ignore[arg-type]
            case "Path.read_text":
                return self.path_read_text(*args)  # type: ignore[arg-type]
            case "Path.read_bytes":
                return self.path_read_bytes(*args)  # type: ignore[arg-type]
            case "Path.write_text":
                return self.path_write_text(*args)  # type: ignore[arg-type]
            case "Path.write_bytes":
                return self.path_write_bytes(*args)  # type: ignore[arg-type]
            case "Path.mkdir":
                parents = bool(kwargs.get("parents", False))
                exist_ok = bool(kwargs.get("exist_ok", False))
                path: PurePosixPath = args[0]  # type: ignore[assignment]
                self.path_mkdir(path, parents=parents, exist_ok=exist_ok)
                return None
            case "Path.unlink":
                self.path_unlink(*args)  # type: ignore[arg-type]
                return None
            case "Path.rmdir":
                self.path_rmdir(*args)  # type: ignore[arg-type]
                return None
            case "Path.iterdir":
                return self.path_iterdir(*args)  # type: ignore[arg-type]
            case "Path.stat":
                return self.path_stat(*args)  # type: ignore[arg-type]
            case "Path.rename":
                self.path_rename(*args)  # type: ignore[arg-type]
                return None
            case "Path.resolve":
                return self.path_resolve(*args)  # type: ignore[arg-type]
            case "Path.absolute":
                return self.path_absolute(*args)  # type: ignore[arg-type]
            case "os.getenv":
                return self.getenv(*args)  # type: ignore[arg-type]
            case "os.environ":
                return self.get_environ()
            case _:
                msg = f"Unsupported OS function: {function_name}"
                raise NotImplementedError(msg)

    # --- Filesystem operations ---

    def path_exists(self, path: PurePosixPath) -> bool:
        return self._sync(self._fs._exists, self._to_str(path))

    def path_is_file(self, path: PurePosixPath) -> bool:
        return self._sync(self._fs._isfile, self._to_str(path))

    def path_is_dir(self, path: PurePosixPath) -> bool:
        return self._sync(self._fs._isdir, self._to_str(path))

    def path_is_symlink(self, path: PurePosixPath) -> bool:
        # fsspec filesystems don't support symlinks
        return False

    def path_read_text(self, path: PurePosixPath) -> str:
        data: bytes = self._sync(self._fs._cat_file, self._to_str(path))
        return data.decode("utf-8")

    def path_read_bytes(self, path: PurePosixPath) -> bytes:
        return self._sync(self._fs._cat_file, self._to_str(path))

    def path_write_text(self, path: PurePosixPath, data: str) -> int:
        encoded = data.encode("utf-8")
        self._sync(self._fs._pipe_file, self._to_str(path), encoded)
        return len(data)

    def path_write_bytes(self, path: PurePosixPath, data: bytes) -> int:
        self._sync(self._fs._pipe_file, self._to_str(path), data)
        return len(data)

    def path_mkdir(self, path: PurePosixPath, parents: bool, exist_ok: bool) -> None:
        str_path = self._to_str(path)
        if parents:
            self._sync(self._fs._makedirs, str_path, exist_ok)
        else:
            if exist_ok and self._sync(self._fs._isdir, str_path):
                return
            self._sync(self._fs._mkdir, str_path, False)

    def path_unlink(self, path: PurePosixPath) -> None:
        self._sync(self._fs._rm_file, self._to_str(path))

    def path_rmdir(self, path: PurePosixPath) -> None:
        self._sync(self._fs._rmdir, self._to_str(path))

    def path_iterdir(self, path: PurePosixPath) -> list[PurePosixPath]:
        entries: list[str] = self._sync(self._fs._ls, self._to_str(path), False)
        return [PurePosixPath(entry) for entry in entries]

    def path_stat(self, path: PurePosixPath) -> object:
        from pydantic_monty import StatResult

        info: dict[str, Any] = self._sync(self._fs._info, self._to_str(path))
        file_type = info.get("type", "file")

        if file_type == "directory":
            mode = int(info.get("mode") or 0)
            mtime = float(info.get("mtime") or info.get("modified") or 0)
            return StatResult.dir_stat(mode=mode or 0o755, mtime=mtime or None)

        size = int(info.get("size") or 0)
        mode = int(info.get("mode") or 0)
        mtime = float(info.get("mtime") or info.get("modified") or 0)
        return StatResult.file_stat(size=size, mode=mode or 0o644, mtime=mtime or None)

    def path_rename(self, path: PurePosixPath, target: PurePosixPath) -> None:
        self._sync(self._fs._mv, self._to_str(path), self._to_str(target))

    def path_resolve(self, path: PurePosixPath) -> str:
        return self.path_absolute(path)

    def path_absolute(self, path: PurePosixPath) -> str:
        p = PurePosixPath(path)
        if p.is_absolute():
            return str(p)
        return str(self._root_dir / p)

    # --- Environment variables ---

    def getenv(self, key: str, default: str | None = None) -> str | None:
        return self._environ.get(key, default)

    def get_environ(self) -> dict[str, str]:
        return dict(self._environ)

    def __repr__(self) -> str:
        return f"FsspecOS(fs={self._fs!r}, root_dir={self._root_dir!r})"
