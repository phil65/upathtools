"""Local disk async filesystem implementation for ``upathtools.fs``."""

import asyncio
import base64
from collections import deque
import contextlib
from dataclasses import dataclass
import errno
import json
import os
from pathlib import Path
import re
import stat
from typing import TYPE_CHECKING, Any, ClassVar
import uuid

from upathtools.fs._sync import FSSPEC_SYNC_VERSION, FsspecSyncNote
from upathtools.fs.base import _BINARY_CHECK_BYTES, AsyncFS, _matches_globs
from upathtools.fs.paths import normalize_path, parent_path
from upathtools.fs.types import EntryType, FileInfo, GrepMatch


if TYPE_CHECKING:
    from collections.abc import Sequence

    from upathtools.fs.types import WriteMode


__fsspec_sync__ = FsspecSyncNote(
    version=FSSPEC_SYNC_VERSION,
    upstream=("fsspec/implementations/local.py:LocalFileSystem",),
    notes=(
        "Async-first local filesystem running all blocking OS calls via asyncio.to_thread. "
        "No global or class-level mutable state; auto_mkdir is strictly per-instance "
        "(default True). write_bytes uses atomic temp-file + os.replace for overwrite "
        "and O_EXCL ('xb') for create. info() uses lstat so symlinks report is_symlink=True "
        "and symlink_target (type follows resolved target, or 'other' when dangling). "
        "Native find/grep use ripgrep-rs where semantics match and fall back cleanly "
        "when ripgrep-rs cannot reproduce base semantics (e.g. symlinks, multiline, "
        "single-file binary/glob filtering, Linux statx btime vs lstat st_ctime)."
    ),
)

_WINDOWS_DRIVE_RE = re.compile(r"^[A-Za-z]:")
_WINDOWS_SLASH_DRIVE_RE = re.compile(r"^/[A-Za-z]:(?:[\\/]|$)")
_LOCAL_PROTO_PREFIXES: tuple[str, ...] = ("file://", "file:", "local://", "local:")


@dataclass(frozen=True, slots=True, kw_only=True)
class LocalFSInfo(FileInfo):
    """Immutable metadata for a local filesystem entry with POSIX stat fields.

    Attributes:
        uid: Numeric owner user ID from ``lstat``, or ``None`` if unavailable.
        gid: Numeric owner group ID from ``lstat``, or ``None`` if unavailable.
        ino: Inode number from ``lstat``, or ``None`` if unavailable.
        nlink: Number of hard links from ``lstat``, or ``None`` if unavailable.
    """

    uid: int | None = None
    gid: int | None = None
    ino: int | None = None
    nlink: int | None = None

    def to_fsspec_dict(self) -> dict[str, Any]:
        """Convert to a standard ``fsspec`` local info dictionary."""
        out = super().to_fsspec_dict()
        if self.uid is not None:
            out["uid"] = self.uid
        if self.gid is not None:
            out["gid"] = self.gid
        if self.ino is not None:
            out["ino"] = self.ino
        if self.nlink is not None:
            out["nlink"] = self.nlink
        return out


def _split_drive(path: str) -> tuple[str, str]:
    """Split a normalized POSIX-style local path into ``(drive, rest)``.

    Handles POSIX paths (``""``, ``"/a/b"``), Windows drive paths (``"C:"``, ``"/a/b"``),
    and UNC paths (``"//server/share"``, ``"/a/b"``).
    """
    if _WINDOWS_DRIVE_RE.match(path):
        return path[:2], path[2:]
    if path.startswith("//") and os.sep != "/":
        idx1 = path.find("/", 2)
        if idx1 == -1:
            return path, ""
        idx2 = path.find("/", idx1 + 1)
        if idx2 == -1:
            return path, ""
        return path[:idx2], path[idx2:]
    return "", path


def _is_root(path: str) -> bool:
    """Return True if normalized local ``path`` is a filesystem root (``/``, ``C:/``, UNC root)."""
    drive, rest = _split_drive(path)
    if drive:
        return rest in ("", "/")
    return path == "/"


def _join_child(parent: str, name: str) -> str:
    """Join a child basename onto a normalized parent directory path."""
    if parent.endswith("/"):
        return f"{parent}{name}"
    return f"{parent}/{name}"


def _mode_to_entry_type(st_mode: int) -> EntryType:
    """Map POSIX ``st_mode`` bits to a normalized :data:`EntryType`."""
    if stat.S_ISDIR(st_mode):
        return "directory"
    if stat.S_ISREG(st_mode):
        return "file"
    return "other"


def _stat_ctime(st: os.stat_result) -> float:
    """Return birth time (``st_birthtime``) when available on this platform, else ``st_ctime``."""
    birthtime = getattr(st, "st_birthtime", None)
    if birthtime is not None:
        return float(birthtime)
    # Windows typeshed deprecates st_ctime, but Linux stat_result lacks st_birthtime.
    return float(st.st_ctime)  # ty: ignore[deprecated]


def _build_info(path: str, lst: os.stat_result) -> LocalFSInfo:
    """Construct a :class:`LocalFSInfo` from an ``lstat`` result for normalized ``path``."""
    is_symlink = stat.S_ISLNK(lst.st_mode)
    symlink_target: str | None = None
    if is_symlink:
        symlink_target = str(Path(path).readlink())
        try:
            target_st = Path(path).stat()
        except OSError:
            entry_type: EntryType = "other"
            size = lst.st_size
        else:
            entry_type = _mode_to_entry_type(target_st.st_mode)
            size = target_st.st_size
    else:
        entry_type = _mode_to_entry_type(lst.st_mode)
        size = lst.st_size

    return LocalFSInfo(
        path=path,
        type=entry_type,
        size=size,
        mtime=float(lst.st_mtime),
        ctime=_stat_ctime(lst),
        mode=lst.st_mode,
        is_symlink=is_symlink,
        symlink_target=symlink_target,
        uid=lst.st_uid,
        gid=lst.st_gid,
        ino=lst.st_ino,
        nlink=lst.st_nlink,
    )


def _info_sync(path: str) -> LocalFSInfo:
    lst = Path(path).lstat()
    return _build_info(path, lst)


def _ls_sync(path: str) -> list[LocalFSInfo]:
    entries: list[LocalFSInfo] = []
    with os.scandir(path) as it:
        for entry in it:
            child_path = _join_child(path, entry.name)
            try:
                lst = entry.stat(follow_symlinks=False)
                entries.append(_build_info(child_path, lst))
            except FileNotFoundError:
                # Entry disappeared concurrently between readdir and stat.
                continue
    entries.sort(key=lambda e: e.path)
    return entries


def _read_bytes_sync(path: str, start: int | None, end: int | None) -> bytes:
    with Path(path).open("rb") as f:
        if start is None and end is None:
            return f.read()
        file_size = os.fstat(f.fileno()).st_size
        s, e, _ = slice(start, end).indices(file_size)
        if s >= e:
            return b""
        if s > 0:
            f.seek(s)
        return f.read(e - s)


def _ensure_parent_dir(parent: str, original_path: str) -> None:
    """Create ``parent`` and its ancestors, raising ``NotADirectoryError`` if blocked by a file."""
    if _is_root(parent):
        return
    try:
        Path(parent).mkdir(parents=True, exist_ok=True)
    except FileExistsError as exc:
        # Path.mkdir(exist_ok=True) raises FileExistsError when `parent` itself
        # is an existing non-directory (e.g. a regular file).
        raise NotADirectoryError(errno.ENOTDIR, "Not a directory", original_path) from exc


def _write_bytes_sync(
    path: str,
    payload: bytes,
    mode: WriteMode,
    auto_mkdir: bool,
) -> None:
    if mode not in ("overwrite", "create"):
        raise ValueError(f"Unsupported write mode {mode!r}; expected 'overwrite' or 'create'")

    target = Path(path)
    try:
        existing_lst = target.lstat()
    except FileNotFoundError:
        existing_lst = None
    except NotADirectoryError:
        raise
    else:
        if stat.S_ISDIR(existing_lst.st_mode):
            raise IsADirectoryError(errno.EISDIR, "Is a directory", path)

    parent = LocalFS.parent(path)
    if auto_mkdir:
        _ensure_parent_dir(parent, path)
    else:
        parent_p = Path(parent)
        try:
            parent_st = parent_p.stat()
        except FileNotFoundError:
            raise FileNotFoundError(errno.ENOENT, "No such file or directory", path) from None
        if not stat.S_ISDIR(parent_st.st_mode):
            raise NotADirectoryError(errno.ENOTDIR, "Not a directory", path)

    if mode == "create":
        with target.open("xb") as f:
            f.write(payload)
        return

    if existing_lst is not None and stat.S_ISLNK(existing_lst.st_mode):
        # Write through the link (like open("wb")) instead of replacing the link itself.
        target = Path(os.path.realpath(target))
        parent = str(target.parent)
    # os.open with 0o666 applies the umask like a plain open() would (mkstemp: 0o600);
    # an overwritten file keeps its permission bits.
    tmp_path = Path(parent) / f".{target.name}.{uuid.uuid4().hex}.upathtools-tmp"
    fd = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0), 0o666)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(payload)
        if existing_lst is not None:
            with contextlib.suppress(OSError):
                tmp_path.chmod(stat.S_IMODE(target.stat().st_mode))
        tmp_path.replace(target)
    except BaseException:
        with contextlib.suppress(OSError):
            tmp_path.unlink()
        raise


def _mkdir_sync(path: str, parents: bool, exist_ok: bool) -> None:
    target = Path(path)
    if parents:
        if not _is_root(path):
            _ensure_parent_dir(LocalFS.parent(path), path)
        target.mkdir(parents=False, exist_ok=exist_ok)
    else:
        target.mkdir(parents=False, exist_ok=exist_ok)


def _rmdir_sync(path: str) -> None:
    if _is_root(path):
        raise ValueError(f"Cannot remove root directory {path!r}")
    Path(path).rmdir()


def _remove_file_sync(path: str) -> None:
    target = Path(path)
    try:
        target.unlink()
    except PermissionError:
        # On some platforms (e.g. macOS) unlink on a directory raises PermissionError.
        if target.is_dir() and not target.is_symlink():
            raise IsADirectoryError(errno.EISDIR, "Is a directory", path) from None
        raise


def _try_native_move_sync(
    src: str,
    dst: str,
    *,
    overwrite: bool,
    auto_mkdir: bool,
) -> bool:
    """Attempt a native OS rename/replace move.

    Returns ``True`` if the move completed natively, or ``False`` when the caller
    should fall back to :meth:`AsyncFS.move` (cross-device ``EXDEV`` or merging a
    source directory onto an already-existing destination directory).
    """
    if _is_root(src):
        raise ValueError(f"Cannot move root directory {src!r}")

    src_p = Path(src)
    src_lst = src_p.lstat()

    if src == dst:
        if not overwrite:
            raise FileExistsError(errno.EEXIST, "File exists", dst)
        return True

    src_is_dir = stat.S_ISDIR(src_lst.st_mode)
    if src_is_dir and dst.startswith(f"{src}/"):
        raise ValueError(f"Cannot move directory {src!r} into itself ({dst!r})")

    dst_p = Path(dst)
    try:
        dst_lst = dst_p.lstat()
    except FileNotFoundError:
        dst_lst = None

    if src_is_dir:
        if dst_lst is not None:
            if not stat.S_ISDIR(dst_lst.st_mode):
                raise FileExistsError(errno.EEXIST, "File exists", dst)
            # Merging into an existing directory requires pre-flight conflict checks
            # and preserving unrelated files in `dst`.
            return False
    elif dst_lst is not None:
        if stat.S_ISDIR(dst_lst.st_mode):
            raise IsADirectoryError(errno.EISDIR, "Is a directory", dst)
        if not overwrite:
            raise FileExistsError(errno.EEXIST, "File exists", dst)

    if auto_mkdir:
        _ensure_parent_dir(LocalFS.parent(dst), dst)

    try:
        src_p.replace(dst_p)
    except OSError as exc:
        if exc.errno == errno.EXDEV:
            return False
        raise
    return True


def _find_scandir_sync(
    root_norm: str,
    root_info: LocalFSInfo,
    *,
    maxdepth: int | None,
    withdirs: bool,
) -> list[LocalFSInfo]:
    """Single-thread BFS walk using ``os.scandir`` for exact parity with ``AsyncFS.find``."""
    entries: list[LocalFSInfo] = [root_info] if withdirs else []
    frontier: deque[tuple[str, int]] = deque([(root_norm, 1)])

    while frontier:
        dirpath, depth = frontier.popleft()
        try:
            with os.scandir(dirpath) as it:
                children: list[LocalFSInfo] = []
                for entry in it:
                    child_path = _join_child(dirpath, entry.name)
                    try:
                        lst = entry.stat(follow_symlinks=False)
                        children.append(_build_info(child_path, lst))
                    except FileNotFoundError:
                        continue
        except OSError:
            continue

        for child in children:
            if child.is_dir:
                if withdirs:
                    entries.append(child)
                if not child.is_symlink and (maxdepth is None or depth < maxdepth):
                    frontier.append((child.path, depth + 1))
            else:
                entries.append(child)

    entries.sort(key=lambda e: e.path)
    return entries


def _find_ripgrep_sync(
    root_norm: str,
    root_info: LocalFSInfo,
    *,
    maxdepth: int | None,
    withdirs: bool,
    hidden: bool,
    no_ignore: bool,
) -> list[LocalFSInfo]:
    """Fast filtered walk using ``ripgrep_rs.files_with_info`` (honoring ignore/hidden rules)."""
    from ripgrep_rs import files_with_info

    rg_result = files_with_info(
        paths=[root_norm],
        hidden=hidden,
        no_ignore=no_ignore,
        include_dirs=withdirs,
        max_depth=maxdepth,
        absolute=False,
    )
    entries: list[LocalFSInfo] = [root_info] if withdirs else []
    for raw_path, rg_info in rg_result.items():
        norm_p = raw_path.replace("\\", "/") if os.sep != "/" else raw_path
        norm_p = norm_p.rstrip("/") or "/"
        if norm_p == root_norm:
            continue
        if rg_info.islink:
            try:
                entries.append(_info_sync(norm_p))
            except FileNotFoundError:
                continue
        else:
            entry_type: EntryType = "directory" if rg_info.type == "directory" else "file"
            entries.append(
                LocalFSInfo(
                    path=norm_p,
                    type=entry_type,
                    size=rg_info.size,
                    mtime=float(rg_info.mtime),
                    ctime=float(rg_info.created),
                    mode=rg_info.mode,
                    is_symlink=False,
                    uid=rg_info.uid,
                    gid=rg_info.gid,
                    ino=rg_info.ino,
                    nlink=rg_info.nlink,
                )
            )
    entries.sort(key=lambda e: e.path)
    return entries


def _is_binary_file(path: str) -> bool:
    """Return True if the first 8 KiB of ``path`` contain a NUL byte."""
    try:
        with Path(path).open("rb") as f:
            return b"\x00" in f.read(_BINARY_CHECK_BYTES)
    except OSError:
        return True


def _collect_file_symlinks(root_norm: str, globs: Sequence[str] | None) -> list[str]:
    """Collect non-binary file symlinks under ``root_norm`` without descending into dir symlinks.

    ``ripgrep_rs.search(follow=False)`` skips symlinks to files during directory walks,
    whereas :meth:`AsyncFS.grep` searches them via :meth:`find`. Using ``os.scandir``
    ``DirEntry.is_symlink()`` reads ``d_type`` without extra ``stat`` syscalls on regular files.
    """
    links: list[str] = []
    stack: list[str] = [root_norm]
    while stack:
        dirpath = stack.pop()
        try:
            with os.scandir(dirpath) as it:
                for entry in it:
                    try:
                        if entry.is_symlink():
                            if entry.is_file(follow_symlinks=True):
                                child_path = _join_child(dirpath, entry.name)
                                if globs is not None:
                                    stub = FileInfo(path=child_path, type="file", is_symlink=True)
                                    if not _matches_globs(stub, root_norm, globs):
                                        continue
                                if not _is_binary_file(child_path):
                                    links.append(child_path)
                        elif entry.is_dir(follow_symlinks=False):
                            stack.append(_join_child(dirpath, entry.name))
                    except OSError:
                        continue
        except OSError:
            continue
    links.sort()
    return links


def _decode_rg_line(lines_obj: dict[str, Any]) -> tuple[str, bytes | None]:
    """Decode a ripgrep JSON ``lines`` object to text without trailing line ending."""
    if "text" in lines_obj:
        text = str(lines_obj["text"]).removesuffix("\n").removesuffix("\r")
        return text, None
    raw_bytes = base64.b64decode(lines_obj["bytes"])
    raw_bytes = raw_bytes.removesuffix(b"\n").removesuffix(b"\r")
    return raw_bytes.decode("utf-8", errors="replace"), raw_bytes


def _byte_span_to_char_span(
    line_text: str,
    raw_bytes: bytes | None,
    start: int,
    end: int,
) -> tuple[int, int]:
    """Convert UTF-8 byte offsets from ripgrep JSON to Python ``str`` character offsets."""
    if raw_bytes is None and line_text.isascii():
        return (start, end)
    encoded = line_text.encode("utf-8") if raw_bytes is None else raw_bytes
    char_start = len(encoded[:start].decode("utf-8", errors="replace"))
    char_end = len(encoded[:end].decode("utf-8", errors="replace"))
    return (char_start, char_end)


def _grep_ripgrep_sync(
    root_norm: str,
    pattern: str,
    *,
    max_count: int | None,
    case_sensitive: bool,
    globs: Sequence[str] | None,
    ctx_before: int,
    ctx_after: int,
    hidden: bool,
    no_ignore: bool,
) -> list[GrepMatch]:
    """Execute directory grep via ``ripgrep_rs.search`` in JSON Lines mode."""
    from ripgrep_rs import PySortMode, PySortModeKind, search as rg_search

    search_paths = [root_norm]
    if hidden and no_ignore:
        search_paths.extend(_collect_file_symlinks(root_norm, globs))

    # ripgrep matches globs containing '/' relative to cwd rather than `root_norm`;
    # filter those in Python after search so behavior matches AsyncFS.grep exactly.
    rg_globs: list[str] | None = None
    post_filter_globs = False
    if globs:
        if any("/" in g for g in globs):
            post_filter_globs = True
        else:
            rg_globs = list(globs)

    rg_kwargs: dict[str, Any] = {
        "patterns": [pattern],
        "paths": search_paths,
        "hidden": hidden,
        "no_ignore": no_ignore,
        "case_sensitive": case_sensitive,
        "json": True,
        "sort": PySortMode(PySortModeKind.Path),
        "follow": False,
    }
    if max_count is not None:
        rg_kwargs["max_count"] = max_count
    if rg_globs is not None:
        rg_kwargs["globs"] = rg_globs
    if ctx_before > 0:
        rg_kwargs["before_context"] = ctx_before
    if ctx_after > 0:
        rg_kwargs["after_context"] = ctx_after

    raw_results = rg_search(**rg_kwargs)

    # Parse JSON Lines per file so adjacent matches can share context lines accurately.
    per_file_lines: dict[str, dict[int, str]] = {}
    raw_matches: list[tuple[str, int, str, tuple[tuple[int, int], ...]]] = []

    for chunk in raw_results:
        for raw_line in chunk.splitlines():
            if not raw_line:
                continue
            try:
                obj = json.loads(raw_line)
            except json.JSONDecodeError:
                continue
            event_type = obj.get("type")
            if event_type not in ("match", "context"):
                continue
            data = obj["data"]
            raw_path = data["path"]["text"]
            norm_path = raw_path.replace("\\", "/") if os.sep != "/" else raw_path
            line_num = int(data["line_number"])
            line_text, raw_bytes = _decode_rg_line(data["lines"])
            per_file_lines.setdefault(norm_path, {})[line_num] = line_text

            if event_type == "match":
                submatches = tuple(
                    _byte_span_to_char_span(line_text, raw_bytes, int(sm["start"]), int(sm["end"]))
                    for sm in data.get("submatches", [])
                )
                raw_matches.append((norm_path, line_num, line_text, submatches))

    raw_matches.sort(key=lambda item: (item[0], item[1]))

    matches: list[GrepMatch] = []
    for norm_path, line_num, line_text, submatches in raw_matches:
        if post_filter_globs and globs is not None:
            stub = FileInfo(path=norm_path, type="file")
            if not _matches_globs(stub, root_norm, globs):
                continue
        file_lines = per_file_lines[norm_path]
        before = (
            tuple(file_lines[n] for n in range(line_num - ctx_before, line_num) if n in file_lines)
            if ctx_before
            else ()
        )
        after = (
            tuple(
                file_lines[n]
                for n in range(line_num + 1, line_num + 1 + ctx_after)
                if n in file_lines
            )
            if ctx_after
            else ()
        )
        matches.append(
            GrepMatch(
                path=norm_path,
                line_number=line_num,
                line=line_text,
                submatches=submatches,
                context_before=before,
                context_after=after,
            )
        )
        if max_count is not None and len(matches) >= max_count:
            break

    return matches


class LocalFS(AsyncFS[LocalFSInfo]):
    """Async-first, free-threading-safe local disk filesystem.

    Paths are normalized to absolute POSIX-style strings with forward slashes
    (e.g. ``"/home/user/file"`` on POSIX, ``"C:/Users/file"`` on Windows). Relative
    paths resolve against the process current working directory at call time.

    Behavioural notes:
      - All blocking OS and ``ripgrep-rs`` calls run via :func:`asyncio.to_thread`.
      - No module-level or class-level mutable state; ``auto_mkdir`` is strictly
        per-instance (defaults to ``True``).
      - :meth:`info` uses ``lstat``: symlinks report ``is_symlink=True`` and
        ``symlink_target`` with lstat's ``mode``/``uid``/``gid``/``ino``/``nlink``/
        ``mtime``/``ctime`` (``st_birthtime`` when available, else ``st_ctime``).
        ``type`` and ``size`` reflect the resolved target when the link resolves,
        or ``type="other"`` and the link's own ``st_size`` when the link dangles.
      - :meth:`write_bytes` writes atomically via a temporary file in the target
        directory plus ``os.replace`` for ``mode="overwrite"``, and opens directly
        with ``"xb"`` (``O_EXCL``) for ``mode="create"``.
      - :meth:`open` inherits :class:`AsyncFS`'s buffered streams backed by ranged
        :meth:`read_bytes` and atomic :meth:`write_bytes`.
    """

    protocol: ClassVar[str | tuple[str, ...]] = ("file", "local")
    root_marker: ClassVar[str] = "/"
    sep: ClassVar[str] = "/"
    local_file: ClassVar[bool] = True

    def __init__(
        self,
        *,
        auto_mkdir: bool = True,
        batch_size: int | None = None,
        use_listings_cache: bool = False,
        listings_expiry_time: float | None = None,
        max_cached_paths: int | None = None,
    ) -> None:
        """Initialize a local filesystem instance.

        Args:
            auto_mkdir: When ``True`` (default), :meth:`write_bytes` and :meth:`move`
                automatically create missing parent directories. When ``False``, a
                missing parent directory raises :class:`FileNotFoundError`.
            batch_size: Default concurrency limit for derived batch operations.
            use_listings_cache: Enable optional thread-safe :class:`DirCache`.
            listings_expiry_time: Optional cache TTL in seconds.
            max_cached_paths: Optional cache LRU size cap.
        """
        super().__init__(
            batch_size=batch_size,
            use_listings_cache=use_listings_cache,
            listings_expiry_time=listings_expiry_time,
            max_cached_paths=max_cached_paths,
        )
        self.auto_mkdir = auto_mkdir

    @classmethod
    def strip_protocol(cls, path: str) -> str:
        r"""Strip ``file://`` / ``local://`` prefixes and return a canonical absolute POSIX path.

        Windows drive paths (``C:\x``, ``C:/x``, ``file:///C:/x``) are preserved as
        ``C:/x`` (and drive roots as ``C:/``). Relative paths are resolved against
        :meth:`pathlib.Path.cwd` at call time.
        """
        p = path
        for prefix in _LOCAL_PROTO_PREFIXES:
            if p.startswith(prefix):
                p = p.removeprefix(prefix)
                break

        if os.sep != "/" or _WINDOWS_DRIVE_RE.match(p) or _WINDOWS_SLASH_DRIVE_RE.match(p):
            p = p.replace("\\", "/")

        if _WINDOWS_SLASH_DRIVE_RE.match(p):
            p = p[1:]

        if p == "~" or p.startswith("~/"):
            p = str(Path(p).expanduser())
            if os.sep != "/":
                p = p.replace("\\", "/")

        if _WINDOWS_DRIVE_RE.match(p):
            drive, rest = p[:2], p[2:]
            if rest and not rest.startswith("/"):
                rest = f"/{rest}"
            norm_rest = normalize_path(rest or "/")
            return f"{drive}{norm_rest}"

        if p.startswith("//") and os.sep != "/":
            drive, rest = _split_drive(p)
            norm_rest = normalize_path(rest or "/")
            return f"{drive}{norm_rest}"

        if p.startswith("/"):
            norm = normalize_path(p)
            if os.sep != "/":
                cwd_drive = Path.cwd().drive
                if cwd_drive:
                    return f"{cwd_drive}{norm}"
            return norm

        # Relative path: resolve against current working directory at call time.
        cwd = str(Path.cwd())
        if os.sep != "/":
            cwd = cwd.replace("\\", "/")
        combined = f"{cwd}/{p}" if p else cwd
        drive, rest = _split_drive(combined)
        norm_rest = normalize_path(rest or "/")
        return f"{drive}{norm_rest}" if drive else norm_rest

    @classmethod
    def parent(cls, path: str) -> str:
        """Return the normalized parent directory of ``path``, preserving drive/UNC roots."""
        norm = cls.strip_protocol(path)
        drive, rest = _split_drive(norm)
        if drive:
            if rest in ("", "/"):
                return f"{drive}/"
            parent_rest = parent_path(rest, root_marker="/")
            return f"{drive}{parent_rest}"
        return parent_path(norm, root_marker="/")

    async def info(self, path: str) -> LocalFSInfo:
        """Return ``lstat``-based :class:`LocalFSInfo` for ``path``.

        Symlinks report ``is_symlink=True`` and ``symlink_target``; ``type`` and
        ``size`` reflect the target when the link resolves, or ``type="other"``
        when the link dangles.
        """
        self._check_open()
        norm = self._norm(path)
        return await asyncio.to_thread(_info_sync, norm)

    async def ls(self, path: str) -> list[LocalFSInfo]:
        """List direct children of directory ``path`` via ``os.scandir``, sorted by path."""
        self._check_open()
        norm = self._norm(path)
        return await asyncio.to_thread(_ls_sync, norm)

    async def read_bytes(
        self,
        path: str,
        *,
        start: int | None = None,
        end: int | None = None,
    ) -> bytes:
        """Read bytes from ``path``, using ``seek`` + bounded ``read`` for byte ranges."""
        self._check_open()
        norm = self._norm(path)
        return await asyncio.to_thread(_read_bytes_sync, norm, start, end)

    async def write_bytes(
        self,
        path: str,
        data: bytes | bytearray | memoryview,
        *,
        mode: WriteMode = "overwrite",
    ) -> None:
        """Write ``data`` to ``path`` atomically (temp file + ``os.replace``) or exclusively."""
        self._check_open()
        norm = self._norm(path)
        payload = bytes(data)
        await asyncio.to_thread(_write_bytes_sync, norm, payload, mode, self.auto_mkdir)

    async def mkdir(
        self,
        path: str,
        *,
        parents: bool = True,
        exist_ok: bool = False,
    ) -> None:
        """Create directory ``path`` on disk."""
        self._check_open()
        norm = self._norm(path)
        await asyncio.to_thread(_mkdir_sync, norm, parents, exist_ok)

    async def rmdir(self, path: str) -> None:
        """Remove an empty directory ``path`` on disk."""
        self._check_open()
        norm = self._norm(path)
        await asyncio.to_thread(_rmdir_sync, norm)

    async def remove_file(self, path: str) -> None:
        """Remove a single file or symlink ``path`` on disk."""
        self._check_open()
        norm = self._norm(path)
        await asyncio.to_thread(_remove_file_sync, norm)

    async def remove(self, path: str, *, recursive: bool = False) -> None:
        """Remove a file, symlink, or directory on disk."""
        self._check_open()
        norm = self._norm(path)
        if _is_root(norm):
            raise ValueError(f"Cannot remove root directory {norm!r}")
        await super().remove(norm, recursive=recursive)

    async def move(self, src: str, dst: str, *, overwrite: bool = True) -> None:
        """Move ``src`` to exact target ``dst``, using native ``os.replace`` when possible.

        Falls back to :meth:`AsyncFS.move` (copy + remove) across devices (``EXDEV``)
        and when merging a directory into an already-existing destination directory.
        """
        self._check_open()
        src_norm = self._norm(src)
        dst_norm = self._norm(dst)
        moved = await asyncio.to_thread(
            _try_native_move_sync,
            src_norm,
            dst_norm,
            overwrite=overwrite,
            auto_mkdir=self.auto_mkdir,
        )
        if not moved:
            await super().move(src_norm, dst_norm, overwrite=overwrite)

    async def find(
        self,
        path: str,
        *,
        maxdepth: int | None = None,
        withdirs: bool = False,
        hidden: bool = True,
        no_ignore: bool = True,
    ) -> list[LocalFSInfo]:
        """Return sorted :class:`LocalFSInfo` entries beneath ``path``.

        With default ``hidden=True, no_ignore=True``, runs a single-thread ``os.scandir``
        BFS walk in a worker thread for exact parity with :meth:`AsyncFS.find` (including
        file/dangling symlinks, ``symlink_target``, and ``lstat`` ``ctime``). When
        ``hidden=False`` or ``no_ignore=False``, delegates to ``ripgrep_rs.files_with_info``
        in a worker thread so ``.gitignore`` and hidden-file filters run in Rust.
        """
        self._check_open()
        if maxdepth is not None and maxdepth < 1:
            raise ValueError("maxdepth must be at least 1")

        norm = self._norm(path)
        try:
            root_info = await self.info(norm)
        except FileNotFoundError, NotADirectoryError:
            return []

        if not root_info.is_dir:
            return [root_info]

        if not hidden or not no_ignore:
            return await asyncio.to_thread(
                _find_ripgrep_sync,
                norm,
                root_info,
                maxdepth=maxdepth,
                withdirs=withdirs,
                hidden=hidden,
                no_ignore=no_ignore,
            )

        return await asyncio.to_thread(
            _find_scandir_sync,
            norm,
            root_info,
            maxdepth=maxdepth,
            withdirs=withdirs,
        )

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
        hidden: bool = True,
        no_ignore: bool = True,
    ) -> list[GrepMatch]:
        """Search file contents under ``path`` using ``ripgrep-rs`` when semantics match.

        Defaults ``hidden=True, no_ignore=True`` and smart-case behavior match
        :meth:`AsyncFS.grep`. Falls back to :meth:`AsyncFS.grep` for single-file
        targets, ``multiline=True``, and Python-only regex syntax (e.g. lookarounds)
        rather than diverging from base results.
        """
        self._check_open()
        if max_count is not None and max_count < 0:
            raise ValueError(f"max_count must be non-negative or None, got {max_count}")
        ctx_before = 0 if context_before is None else context_before
        ctx_after = 0 if context_after is None else context_after
        if ctx_before < 0 or ctx_after < 0:
            raise ValueError("context_before and context_after must be non-negative")

        norm = self._norm(path)
        root_info = await self.info(norm)
        if max_count == 0:
            return []

        if not root_info.is_dir or multiline:
            return await super().grep(
                norm,
                pattern,
                max_count=max_count,
                case_sensitive=case_sensitive,
                globs=globs,
                context_before=context_before,
                context_after=context_after,
                multiline=multiline,
            )

        effective_case = (
            any(ch.isupper() for ch in pattern) if case_sensitive is None else case_sensitive
        )

        try:
            return await asyncio.to_thread(
                _grep_ripgrep_sync,
                norm,
                pattern,
                max_count=max_count,
                case_sensitive=effective_case,
                globs=globs,
                ctx_before=ctx_before,
                ctx_after=ctx_after,
                hidden=hidden,
                no_ignore=no_ignore,
            )
        except ValueError:
            # Rust regex rejects valid Python regex features such as lookarounds or
            # backreferences; fall back if Python accepts the pattern, else re-raise.
            flags = 0 if effective_case else re.IGNORECASE
            re.compile(pattern, flags)
            return await super().grep(
                norm,
                pattern,
                max_count=max_count,
                case_sensitive=case_sensitive,
                globs=globs,
                context_before=context_before,
                context_after=context_after,
                multiline=multiline,
            )
