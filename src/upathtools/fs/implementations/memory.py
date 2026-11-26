"""In-memory async filesystem implementation for ``upathtools.fs``."""

from dataclasses import dataclass
import errno
import threading
import time
from typing import TYPE_CHECKING, ClassVar

from upathtools.fs._sync import FSSPEC_SYNC_VERSION, FsspecSyncNote
from upathtools.fs.base import AsyncFS
from upathtools.fs.types import FileInfo


if TYPE_CHECKING:
    from upathtools.fs.types import WriteMode


__fsspec_sync__ = FsspecSyncNote(
    version=FSSPEC_SYNC_VERSION,
    upstream=("fsspec/implementations/memory.py:MemoryFileSystem",),
    notes=(
        "Instance-isolated by default (no class-level mutable globals; explicit "
        "MemoryStore for opt-in sharing). All mutable state is guarded by a single "
        "threading.Lock held only during short synchronous critical sections, never "
        "across an await. Stores explicit directory set plus file/dir ctime and mtime. "
        "write_bytes creates missing parent directories implicitly (object-store style). "
        "Provides atomic native move() for files and directory subtrees."
    ),
)


def _ancestors(path: str) -> list[str]:
    """Return strict ancestor directories of normalized absolute ``path``, shallow to deep."""
    if path == "/":
        return []
    parts = path.strip("/").split("/")
    out = ["/"]
    current = ""
    for segment in parts[:-1]:
        current = f"{current}/{segment}"
        out.append(current)
    return out


@dataclass(slots=True)
class _FileRecord:
    data: bytes
    ctime: float
    mtime: float


class MemoryStore:
    """Thread-safe in-memory storage backing one or more :class:`MemoryFS` instances.

    All operations acquire a single :class:`threading.Lock` for short synchronous
    critical sections and return immutable :class:`FileInfo` / ``bytes`` values.
    """

    __slots__ = ("_dirs", "_files", "_lock")

    def __init__(self) -> None:
        now = time.time()
        self._lock = threading.Lock()
        self._files: dict[str, _FileRecord] = {}
        self._dirs: dict[str, tuple[float, float]] = {"/": (now, now)}

    def _raise_missing_or_not_dir(self, path: str) -> None:
        for anc in _ancestors(path):
            if anc in self._files:
                raise NotADirectoryError(errno.ENOTDIR, "Not a directory", path)
        raise FileNotFoundError(errno.ENOENT, "No such file or directory", path)

    def info(self, path: str) -> FileInfo:
        with self._lock:
            record = self._files.get(path)
            if record is not None:
                return FileInfo(
                    path=path,
                    type="file",
                    size=len(record.data),
                    mtime=record.mtime,
                    ctime=record.ctime,
                )
            dir_ts = self._dirs.get(path)
            if dir_ts is not None:
                ctime, mtime = dir_ts
                return FileInfo(
                    path=path,
                    type="directory",
                    size=0,
                    mtime=mtime,
                    ctime=ctime,
                )
            self._raise_missing_or_not_dir(path)
            raise AssertionError("unreachable")  # pragma: no cover

    def ls(self, path: str) -> list[FileInfo]:
        with self._lock:
            if path in self._files:
                raise NotADirectoryError(errno.ENOTDIR, "Not a directory", path)
            if path not in self._dirs:
                self._raise_missing_or_not_dir(path)

            prefix = "/" if path == "/" else f"{path}/"
            prefix_len = len(prefix)
            entries: list[FileInfo] = []

            for f_path, record in self._files.items():
                if f_path.startswith(prefix) and "/" not in f_path[prefix_len:]:
                    entries.append(
                        FileInfo(
                            path=f_path,
                            type="file",
                            size=len(record.data),
                            mtime=record.mtime,
                            ctime=record.ctime,
                        )
                    )
            for d_path, (ctime, mtime) in self._dirs.items():
                if d_path != path and d_path.startswith(prefix) and "/" not in d_path[prefix_len:]:
                    entries.append(
                        FileInfo(
                            path=d_path,
                            type="directory",
                            size=0,
                            mtime=mtime,
                            ctime=ctime,
                        )
                    )
            entries.sort(key=lambda e: e.path)
            return entries

    def read_bytes(
        self,
        path: str,
        *,
        start: int | None = None,
        end: int | None = None,
    ) -> bytes:
        with self._lock:
            if path in self._dirs:
                raise IsADirectoryError(errno.EISDIR, "Is a directory", path)
            record = self._files.get(path)
            if record is None:
                self._raise_missing_or_not_dir(path)
                raise AssertionError("unreachable")  # pragma: no cover
            return record.data[start:end]

    def write_bytes(
        self,
        path: str,
        payload: bytes,
        *,
        mode: WriteMode = "overwrite",
    ) -> None:
        if mode not in ("overwrite", "create"):
            raise ValueError(f"Unsupported write mode {mode!r}; expected 'overwrite' or 'create'")
        ancestors = _ancestors(path)
        with self._lock:
            if path in self._dirs:
                raise IsADirectoryError(errno.EISDIR, "Is a directory", path)
            for anc in ancestors:
                if anc in self._files:
                    raise NotADirectoryError(errno.ENOTDIR, "Not a directory", path)
            if mode == "create" and path in self._files:
                raise FileExistsError(errno.EEXIST, "File exists", path)

            now = time.time()
            for anc in ancestors:
                if anc not in self._dirs:
                    self._dirs[anc] = (now, now)

            existing = self._files.get(path)
            ctime = existing.ctime if existing is not None else now
            self._files[path] = _FileRecord(data=payload, ctime=ctime, mtime=now)

            if ancestors:
                parent = ancestors[-1]
                p_ctime, _ = self._dirs[parent]
                self._dirs[parent] = (p_ctime, now)

    def mkdir(
        self,
        path: str,
        *,
        parents: bool = True,
        exist_ok: bool = False,
    ) -> None:
        ancestors = _ancestors(path)
        with self._lock:
            if path in self._files:
                raise FileExistsError(errno.EEXIST, "File exists", path)
            if path in self._dirs:
                if not exist_ok:
                    raise FileExistsError(errno.EEXIST, "File exists", path)
                return
            for anc in ancestors:
                if anc in self._files:
                    raise NotADirectoryError(errno.ENOTDIR, "Not a directory", path)
            parent = ancestors[-1]
            if not parents and parent not in self._dirs:
                raise FileNotFoundError(errno.ENOENT, "No such file or directory", parent)

            now = time.time()
            if parents:
                for anc in ancestors:
                    if anc not in self._dirs:
                        self._dirs[anc] = (now, now)
            self._dirs[path] = (now, now)

            p_ctime, _ = self._dirs[parent]
            self._dirs[parent] = (p_ctime, now)

    def rmdir(self, path: str) -> None:
        if path == "/":
            raise ValueError("Cannot remove root directory '/'")
        with self._lock:
            if path in self._files:
                raise NotADirectoryError(errno.ENOTDIR, "Not a directory", path)
            if path not in self._dirs:
                self._raise_missing_or_not_dir(path)

            prefix = f"{path}/"
            if any(p.startswith(prefix) for p in self._files) or any(
                d.startswith(prefix) for d in self._dirs
            ):
                raise OSError(errno.ENOTEMPTY, "Directory not empty", path)
            del self._dirs[path]

    def remove_file(self, path: str) -> None:
        with self._lock:
            if path in self._dirs:
                raise IsADirectoryError(errno.EISDIR, "Is a directory", path)
            if path not in self._files:
                self._raise_missing_or_not_dir(path)
            del self._files[path]

    def move(self, src: str, dst: str, *, overwrite: bool = True) -> None:
        if src == "/":
            raise ValueError("Cannot move root directory '/'")
        dst_ancestors = _ancestors(dst)

        with self._lock:
            src_is_file = src in self._files
            src_is_dir = src in self._dirs
            if not src_is_file and not src_is_dir:
                self._raise_missing_or_not_dir(src)

            if src == dst:
                if not overwrite:
                    raise FileExistsError(errno.EEXIST, "File exists", dst)
                return

            for anc in dst_ancestors:
                if anc in self._files:
                    raise NotADirectoryError(errno.ENOTDIR, "Not a directory", dst)

            if src_is_file:
                if dst in self._dirs:
                    raise IsADirectoryError(errno.EISDIR, "Is a directory", dst)
                if not overwrite and dst in self._files:
                    raise FileExistsError(errno.EEXIST, "File exists", dst)

                now = time.time()
                for anc in dst_ancestors:
                    if anc not in self._dirs:
                        self._dirs[anc] = (now, now)
                record = self._files.pop(src)
                self._files[dst] = record
                return

            # Directory subtree move.
            if dst.startswith(f"{src}/"):
                raise ValueError(f"Cannot move directory {src!r} into itself ({dst!r})")
            if dst in self._files:
                raise FileExistsError(errno.EEXIST, "File exists", dst)

            src_prefix = f"{src}/"
            dst_prefix = "/" if dst == "/" else f"{dst}/"
            sub_files = [
                (p, f"{dst_prefix}{p[len(src_prefix) :]}")
                for p in self._files
                if p.startswith(src_prefix)
            ]
            sub_dirs = [
                (d, f"{dst_prefix}{d[len(src_prefix) :]}")
                for d in self._dirs
                if d.startswith(src_prefix)
            ]

            # Pre-validate all target paths before mutating any state.
            for _, dst_file in sub_files:
                if dst_file in self._dirs:
                    raise IsADirectoryError(errno.EISDIR, "Is a directory", dst_file)
                if not overwrite and dst_file in self._files:
                    raise FileExistsError(errno.EEXIST, "File exists", dst_file)
            for _, dst_dir in sub_dirs:
                if dst_dir in self._files:
                    raise FileExistsError(errno.EEXIST, "File exists", dst_dir)

            now = time.time()
            for anc in dst_ancestors:
                if anc not in self._dirs:
                    self._dirs[anc] = (now, now)

            src_dir_ts = self._dirs.pop(src)
            if dst not in self._dirs:
                self._dirs[dst] = src_dir_ts

            moved_dirs = [(dst_d, self._dirs.pop(src_d)) for src_d, dst_d in sub_dirs]
            for dst_d, ts in moved_dirs:
                if dst_d not in self._dirs:
                    self._dirs[dst_d] = ts

            moved_files = [(dst_f, self._files.pop(src_f)) for src_f, dst_f in sub_files]
            for dst_f, record in moved_files:
                self._files[dst_f] = record


class MemoryFS(AsyncFS[FileInfo]):
    """Instance-isolated, thread-safe in-memory :class:`AsyncFS`.

    Protocol: ``"memory"``.

    Behavioural notes:
      - Each ``MemoryFS()`` creates a fresh, isolated :class:`MemoryStore`. Pass
        ``MemoryFS(store=shared_store)`` to share state explicitly across instances.
      - :meth:`write_bytes` implicitly creates missing parent directories (object-store
        behaviour), raising :class:`NotADirectoryError` if an ancestor is a regular file.
      - :meth:`move` is atomic under the store's lock for both files and subtrees.
      - Closing a ``MemoryFS`` via :meth:`aclose` marks only that instance closed;
        subsequent operations raise :class:`~upathtools.fs.errors.FilesystemClosedError`.
    """

    protocol: ClassVar[str | tuple[str, ...]] = "memory"

    def __init__(
        self,
        *,
        store: MemoryStore | None = None,
        batch_size: int | None = None,
        use_listings_cache: bool = False,
        listings_expiry_time: float | None = None,
        max_cached_paths: int | None = None,
    ) -> None:
        super().__init__(
            batch_size=batch_size,
            use_listings_cache=use_listings_cache,
            listings_expiry_time=listings_expiry_time,
            max_cached_paths=max_cached_paths,
        )
        self._store = MemoryStore() if store is None else store

    @property
    def store(self) -> MemoryStore:
        """Underlying thread-safe :class:`MemoryStore`."""
        return self._store

    async def info(self, path: str) -> FileInfo:
        self._check_open()
        return self._store.info(self._norm(path))

    async def ls(self, path: str) -> list[FileInfo]:
        self._check_open()
        return self._store.ls(self._norm(path))

    async def read_bytes(
        self,
        path: str,
        *,
        start: int | None = None,
        end: int | None = None,
    ) -> bytes:
        self._check_open()
        return self._store.read_bytes(self._norm(path), start=start, end=end)

    async def write_bytes(
        self,
        path: str,
        data: bytes | bytearray | memoryview,
        *,
        mode: WriteMode = "overwrite",
    ) -> None:
        self._check_open()
        # Copy payload outside the store lock to keep the critical section minimal.
        payload = bytes(data)
        self._store.write_bytes(self._norm(path), payload, mode=mode)

    async def mkdir(
        self,
        path: str,
        *,
        parents: bool = True,
        exist_ok: bool = False,
    ) -> None:
        self._check_open()
        self._store.mkdir(self._norm(path), parents=parents, exist_ok=exist_ok)

    async def rmdir(self, path: str) -> None:
        self._check_open()
        self._store.rmdir(self._norm(path))

    async def remove_file(self, path: str) -> None:
        self._check_open()
        self._store.remove_file(self._norm(path))

    async def move(self, src: str, dst: str, *, overwrite: bool = True) -> None:
        self._check_open()
        self._store.move(self._norm(src), self._norm(dst), overwrite=overwrite)
