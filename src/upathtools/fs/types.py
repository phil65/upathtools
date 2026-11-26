"""Core immutable data types and literal aliases for ``upathtools.fs``."""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Literal


if TYPE_CHECKING:
    from collections.abc import Mapping
    from typing import Self


EntryType = Literal["file", "directory", "symlink", "other"]
WriteMode = Literal["overwrite", "create"]
OpenMode = Literal["r", "rb", "w", "wb", "x", "xb", "a", "ab", "r+", "r+b"]

_EMPTY_META: Mapping[str, Any] = MappingProxyType({})
_MTIME_KEYS = ("mtime", "modified", "LastModified", "updated")
_CTIME_KEYS = ("ctime", "created", "CreationTime")
_ETAG_KEYS = ("etag", "ETag")
_SIZE_KEYS = ("size", "Size")


def _to_timestamp(value: Any) -> float | None:
    """Normalize a datetime, ISO-8601 string, or numeric value to UTC POSIX seconds."""
    if value is None:
        return None
    if isinstance(value, bool):
        msg = f"Expected timestamp, got bool: {value!r}"
        raise TypeError(msg)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, datetime):
        dt = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return dt.timestamp()
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return float(text)  # numeric string; anything else raises ValueError
        dt = dt if dt.tzinfo is not None else dt.replace(tzinfo=UTC)
        return dt.timestamp()
    msg = f"Cannot convert {type(value).__name__} to POSIX timestamp"
    raise TypeError(msg)


def _pop_first(remaining: dict[str, Any], keys: tuple[str, ...]) -> Any:
    """Remove all matching keys from ``remaining`` and return the first non-None value."""
    result: Any = None
    for key in keys:
        if key in remaining:
            val = remaining.pop(key)
            if result is None and val is not None:
                result = val
    return result


def _timestamp_or_keep(remaining: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    """Pop a timestamp; a value that is no timestamp stays in ``remaining`` (``extra``).

    Backends put all kinds of values under these keys: an unparsable one must not make
    ``info()`` fail, and must not be dropped silently either.
    """
    present = {key: remaining[key] for key in keys if key in remaining}
    raw = _pop_first(remaining, keys)
    try:
        return _to_timestamp(raw)
    except TypeError, ValueError:
        remaining.update(present)
        return None


def _normalize_entry_type(raw_type: Any) -> EntryType:
    """Map an fsspec entry type string to a canonical ``EntryType``."""
    match raw_type:
        case "file":
            return "file"
        case "directory" | "dir":
            return "directory"
        case "symlink" | "link":
            return "symlink"
        case _:
            return "other"


@dataclass(frozen=True, slots=True, kw_only=True)
class FileInfo:
    """Normalized, immutable metadata for a filesystem entry.

    Attributes:
        path: Canonical filesystem-relative path (no protocol, no trailing ``/`` except root).
        type: Normalized entry kind (``"file"``, ``"directory"``, ``"symlink"``, or ``"other"``).
        size: Entry size in bytes if known.
        mtime: Last modification time as UTC POSIX seconds if available.
        ctime: Creation/birth time as UTC POSIX seconds if available.
        mode: POSIX ``st_mode`` bits if available.
        etag: Content hash, version, or entity tag if available.
        is_symlink: Whether the entry is a symbolic link.
        symlink_target: Link destination path if ``is_symlink`` is True and target is known.
        extra: Read-only mapping of backend-specific metadata fields.
    """

    path: str
    type: EntryType
    size: int | None = None
    mtime: float | None = None
    ctime: float | None = None
    mode: int | None = None
    etag: str | None = None
    is_symlink: bool = False
    symlink_target: str | None = None
    # Excluded from the hash (a mappingproxy is unhashable), still part of equality.
    extra: Mapping[str, Any] = field(default=_EMPTY_META, hash=False)

    def __post_init__(self) -> None:
        if self.extra is _EMPTY_META:
            return
        if not self.extra:
            object.__setattr__(self, "extra", _EMPTY_META)
            return
        object.__setattr__(self, "extra", MappingProxyType(dict(self.extra)))

    @property
    def name(self) -> str:
        """Final path segment (basename)."""
        stripped = self.path.rstrip("/")
        return stripped.rsplit("/", 1)[-1] if stripped else ""

    @property
    def is_file(self) -> bool:
        """Return True when this entry is a regular file."""
        return self.type == "file"

    @property
    def is_dir(self) -> bool:
        """Return True when this entry is a directory."""
        return self.type == "directory"

    def to_fsspec_dict(self) -> dict[str, Any]:
        """Convert to a standard fsspec info dict for interop."""
        out: dict[str, Any] = {
            **self.extra,
            "name": self.path,
            "type": self.type,
            "size": self.size,
        }
        if self.mtime is not None:
            out["mtime"] = self.mtime
        if self.ctime is not None:
            out["created"] = self.ctime
        if self.mode is not None:
            out["mode"] = self.mode
        if self.etag is not None:
            out["etag"] = self.etag
        if self.is_symlink:
            out["islink"] = True
            if self.symlink_target is not None:
                out["destination"] = self.symlink_target
        return out

    @classmethod
    def from_fsspec_dict(cls, data: Mapping[str, Any]) -> Self:
        """Normalize an fsspec info dict (local, S3, GCS, etc.) into a ``FileInfo``.

        Known standard and provider-specific keys are mapped to typed attributes; all
        remaining keys are preserved in the read-only ``extra`` mapping.
        """
        remaining = dict(data)

        if "name" in remaining:
            raw_path = remaining.pop("name")
            path = "" if raw_path is None else str(raw_path)
        elif "Key" in remaining:
            raw_path = remaining.pop("Key")
            path = "" if raw_path is None else str(raw_path)
        else:
            path = ""

        raw_type = remaining.pop("type", None)
        entry_type = _normalize_entry_type(raw_type)

        raw_size = _pop_first(remaining, _SIZE_KEYS)
        size = int(raw_size) if raw_size is not None else None

        mtime = _timestamp_or_keep(remaining, _MTIME_KEYS)
        ctime = _timestamp_or_keep(remaining, _CTIME_KEYS)

        raw_mode = remaining.pop("mode", None)
        mode = int(raw_mode) if raw_mode is not None else None

        raw_etag = _pop_first(remaining, _ETAG_KEYS)
        etag = str(raw_etag) if raw_etag is not None else None

        is_symlink = bool(remaining.pop("islink")) if "islink" in remaining else raw_type == "link"

        raw_dest = remaining.pop("destination", None)
        symlink_target = str(raw_dest) if raw_dest is not None else None

        return cls(
            path=path,
            type=entry_type,
            size=size,
            mtime=mtime,
            ctime=ctime,
            mode=mode,
            etag=etag,
            is_symlink=is_symlink,
            symlink_target=symlink_target,
            extra=remaining,
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class GrepMatch:
    """A single line match returned by a filesystem grep operation.

    Attributes:
        path: Canonical filesystem path of the matched file.
        line_number: 1-based line number of the match within the file.
        line: Full text of the matched line (without trailing newline).
        submatches: ``(start, end)`` character/byte spans of matches within ``line``.
        context_before: Optional lines preceding the match when context was requested.
        context_after: Optional lines following the match when context was requested.
    """

    path: str
    line_number: int
    line: str
    submatches: tuple[tuple[int, int], ...] = ()
    context_before: tuple[str, ...] = ()
    context_after: tuple[str, ...] = ()

    def __str__(self) -> str:
        return f"{self.path}:{self.line_number}:{self.line}"
