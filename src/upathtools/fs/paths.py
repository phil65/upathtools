"""Pure path and URL manipulation utilities for ``upathtools.fs``.

All functions in this module are pure string/path operations with no I/O and no
runtime dependency on ``fsspec``.
"""

from collections.abc import Iterable, Mapping, Set as AbstractSet
from dataclasses import dataclass, field
from hashlib import md5
import os
from pathlib import Path
import re
from typing import Any
from urllib.parse import urlsplit

from upathtools.fs._sync import FSSPEC_SYNC_VERSION, FsspecSyncNote
from upathtools.fs.errors import PathEscapeError


__fsspec_sync__ = FsspecSyncNote(
    version=FSSPEC_SYNC_VERSION,
    upstream=(
        "fsspec/core.py:split_protocol",
        "fsspec/core.py:strip_protocol",
        "fsspec/core.py:_un_chain",
        "fsspec/spec.py:AbstractFileSystem._strip_protocol",
        "fsspec/spec.py:AbstractFileSystem._parent",
        "fsspec/utils.py:check_contained",
        "fsspec/utils.py:infer_storage_options",
        "fsspec/utils.py:tokenize",
    ),
    notes=(
        "Pure functions with no runtime fsspec import or registry lookup. "
        "split_protocol always returns a 2-tuple (even for data: URIs, where fsspec "
        "returns a list). strip_protocol accepts explicit protocols and root_marker "
        "instead of dispatching through fsspec.registry. unchain returns frozen "
        "ChainLink records, uses a configurable chained_protocols set instead of "
        "ChainedFileSystem subclass checks, and never mutates caller kwargs. "
        "check_contained raises PathEscapeError (a ValueError subclass). tokenize "
        "canonicalizes mapping and set ordering for determinism. Adds normalize_path "
        "(raising PathEscapeError on root escape) and join_path."
    ),
)

DEFAULT_CHAINED_PROTOCOLS: frozenset[str] = frozenset({
    "async_wrapper",
    "asyncwrapper",
    "blockcache",
    "cached",
    "dir",
    "filecache",
    "simplecache",
})

_NON_PROTOCOL_RE = re.compile(r".*[^a-z]+.*")
_WINDOWS_DRIVE_RE = re.compile(r"^[a-zA-Z]:[\\/]")
_URI_SCHEME_RE = re.compile(r"^[a-zA-Z0-9+.-]+://")
_WINDOWS_FILE_URI_RE = re.compile(r"^/([a-zA-Z])[:|]([\\/].*)$")


@dataclass(frozen=True, slots=True)
class ChainLink:
    """A single layer in an unchained fsspec-style URL (``a::b::c``).

    Attributes:
        path: Protocol-stripped path for this layer.
        protocol: Protocol identifier for this layer.
        kwargs: Storage options scoped to this layer.
    """

    path: str
    protocol: str
    kwargs: dict[str, Any] = field(default_factory=dict)


def split_protocol(url: str | os.PathLike[str]) -> tuple[str | None, str]:
    """Split a URL into a ``(protocol, path)`` pair following fsspec semantics.

    Single-character prefixes before ``://`` (such as Windows drive letters like
    ``"C:/x"`` or ``"C://x"``) are not treated as protocols. ``data:`` URIs are
    split on the first ``:``.

    Args:
        url: URL or filesystem path.

    Returns:
        A ``(protocol, remainder)`` tuple where ``protocol`` is ``None`` if no
        protocol prefix was detected.
    """
    url_str = os.fspath(url)
    if "://" in url_str:
        protocol, path = url_str.split("://", 1)
        if len(protocol) > 1:
            return protocol, path
    if url_str.startswith("data:"):
        protocol, path = url_str.split(":", 1)
        return protocol, path
    return None, url_str


def strip_protocol(
    url: str | os.PathLike[str],
    protocols: str | Iterable[str] | None = None,
    *,
    root_marker: str = "",
) -> str:
    """Strip protocol prefixes and trailing slashes from a URL or path.

    Args:
        url: URL or filesystem path.
        protocols: Specific protocol name or iterable of protocol names to strip.
            When ``None``, any protocol recognized by :func:`split_protocol` is stripped.
        root_marker: Value returned when stripping leaves an empty string (e.g., ``"/"``).

    Returns:
        The protocol-stripped path with trailing ``/`` characters removed.
    """
    path = os.fspath(url)
    if protocols is None:
        _, path = split_protocol(path)
    else:
        protos = (protocols,) if isinstance(protocols, str) else tuple(protocols)
        for protocol in protos:
            if path.startswith(f"{protocol}://"):
                path = path[len(protocol) + 3 :]
            elif path.startswith(f"{protocol}::"):
                path = path[len(protocol) + 2 :]
            elif protocol == "data" and path.startswith("data:"):
                path = path[5:]
    path = path.rstrip("/")
    return path or root_marker


def parent_path(path: str, root_marker: str = "/") -> str:
    """Return the parent directory of a POSIX-style filesystem path.

    Args:
        path: Filesystem path (without protocol).
        root_marker: Canonical root string returned at the top of the hierarchy.

    Returns:
        The parent path, or ``root_marker`` when ``path`` is at or above root.
    """
    stripped = path.rstrip("/")
    if "/" in stripped:
        parent = stripped.rsplit("/", 1)[0].rstrip("/")
        if root_marker:
            return root_marker + parent.lstrip(root_marker)
        return parent
    return root_marker


def normalize_path(path: str) -> str:
    """Normalize a POSIX-style path, rejecting traversal above root.

    Collapses duplicate ``/`` separators and ``.`` segments, strips trailing ``/``
    (preserving ``"/"`` for absolute root), and resolves ``..`` segments.

    Args:
        path: Absolute or relative POSIX-style path.

    Returns:
        Canonical normalized path.

    Raises:
        PathEscapeError: If ``..`` segments attempt to climb above the root.
    """
    is_abs = path.startswith("/")
    parts: list[str] = []
    for segment in path.split("/"):
        if not segment or segment == ".":
            continue
        if segment == "..":
            if not parts:
                raise PathEscapeError(f"Path {path!r} escapes root")
            parts.pop()
        else:
            parts.append(segment)
    joined = "/".join(parts)
    if is_abs:
        return f"/{joined}"
    return joined


def join_path(*parts: str) -> str:
    """Join POSIX-style path segments with ``/``.

    Preserves a leading ``/`` only if the first non-empty part starts with ``/``.
    Empty segments and redundant boundary slashes are omitted.

    Args:
        *parts: Path segments to join.

    Returns:
        Joined POSIX-style path string.
    """
    non_empty = [p for p in parts if p]
    if not non_empty:
        return ""
    is_abs = non_empty[0].startswith("/")
    cleaned = [stripped for p in non_empty if (stripped := p.strip("/"))]
    joined = "/".join(cleaned)
    if is_abs:
        return f"/{joined}"
    return joined


def unchain(
    url: str | os.PathLike[str],
    kwargs: Mapping[str, Any] | None = None,
    *,
    known_protocols: AbstractSet[str] = frozenset(),
    chained_protocols: AbstractSet[str] = DEFAULT_CHAINED_PROTOCOLS,
) -> list[ChainLink]:
    """Split a chained fsspec URL (``a::b::c``) into ordered :class:`ChainLink` entries.

    Port of ``fsspec.core._un_chain``. Per-protocol keyword arguments nested under
    ``kwargs[protocol]`` are extracted for each layer, and any remaining top-level
    keyword arguments are attached to the outermost (first) layer.

    Args:
        url: Single or ``::``-chained URL.
        kwargs: Optional mapping of storage options (including per-protocol sub-dicts).
        known_protocols: Additional protocol names containing non-lowercase characters
            that should be recognized when written without ``://`` in a chain.
        chained_protocols: Passthrough wrapper protocols (like ``"simplecache"``) that
            inherit the inner layer's path when no path is given for the wrapper itself.

    Returns:
        List of :class:`ChainLink` entries from outermost to innermost layer.
    """
    url_str = os.fspath(url)
    all_known = known_protocols | chained_protocols

    if "::" in url_str:
        bits: list[str] = []
        for part in url_str.split("::"):
            if part in all_known:
                bits.append(f"{part}://")
            elif "://" in part or _NON_PROTOCOL_RE.match(part):
                bits.append(part)
            else:
                bits.append(f"{part}://")
    else:
        bits = [url_str]

    remaining: dict[str, Any] = dict(kwargs) if kwargs else {}
    out: list[ChainLink] = []
    previous_path: str | None = None

    for idx in range(len(bits) - 1, -1, -1):
        bit = bits[idx]
        proto_override = remaining.pop("protocol", None)
        protocol = str(proto_override) if proto_override else (split_protocol(bit)[0] or "file")

        raw_proto_kws = remaining.pop(protocol, None)
        link_kwargs: dict[str, Any] = dict(raw_proto_kws) if raw_proto_kws else {}
        if idx == 0:
            link_kwargs.update(remaining)

        root_marker = "/" if protocol in ("file", "local") and bit.endswith("/") else ""
        stripped = strip_protocol(bit, protocol, root_marker=root_marker)

        if (
            "target_protocol" not in link_kwargs
            and protocol in chained_protocols
            and not stripped
            and previous_path is not None
        ):
            stripped = previous_path

        out.append(ChainLink(path=stripped, protocol=protocol, kwargs=link_kwargs))
        previous_path = stripped

    out.reverse()
    return out


def _logical_abspath(path: str, cwd: Path | None) -> tuple[str, Path | None]:
    """Resolve ``path`` lexically to an absolute path without following symlinks."""
    p = Path(path)
    if not p.is_absolute():
        if cwd is None:
            cwd = Path.cwd()
        p = cwd / p
    return os.path.normpath(str(p)), cwd


def check_contained(root: str, paths: Iterable[str]) -> None:
    """Verify that every path in ``paths`` resolves within ``root``.

    Args:
        root: Destination root directory.
        paths: Candidate destination paths to validate.

    Raises:
        PathEscapeError: If any path resolves outside ``root``.
    """
    root_abs, cwd = _logical_abspath(root, None)
    root_key = os.path.normcase(root_abs)
    prefix = root_key.rstrip(os.sep) + os.sep
    for path in paths:
        path_abs, cwd = _logical_abspath(path, cwd)
        path_key = os.path.normcase(path_abs)
        if path_key != root_key and not path_key.startswith(prefix):
            raise PathEscapeError(
                f"path {path!r} would resolve to {path_abs!r}, which is "
                f"outside the destination {root!r}"
            )


def _update_storage_options(
    options: dict[str, Any],
    inherited: Mapping[str, Any],
) -> None:
    """Merge ``inherited`` into ``options``, raising ``KeyError`` on conflicting values."""
    for key in set(options) & set(inherited):
        if options.get(key) != inherited.get(key):
            raise KeyError(f"Collision between inferred and specified storage option:\n{key}")
    options.update(inherited)


def infer_storage_options(
    urlpath: str,
    inherit_storage_options: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Infer storage options from a URL or path and merge inherited options.

    Port of ``fsspec.utils.infer_storage_options``.

    Args:
        urlpath: Local path or URI (e.g., ``"hdfs://user:pwd@host:8020/path?q=1"``).
        inherit_storage_options: Optional mapping merged into inferred options.

    Returns:
        Dictionary of inferred and inherited storage options.

    Raises:
        KeyError: If an inherited option collides with a different inferred value.
    """
    is_filesystem = _WINDOWS_DRIVE_RE.match(urlpath)
    is_uri = _URI_SCHEME_RE.match(urlpath)

    if is_filesystem or is_uri is None:
        return {"protocol": "file", "path": urlpath}

    parsed_path = urlsplit(urlpath)
    protocol = parsed_path.scheme or "file"
    if parsed_path.fragment:
        path = f"{parsed_path.path}#{parsed_path.fragment}"
    else:
        path = parsed_path.path

    if protocol == "file":
        windows_path = _WINDOWS_FILE_URI_RE.match(path)
        if windows_path:
            drive, win_rest = windows_path.groups()
            path = f"{drive}:{win_rest}"

    if protocol in ("http", "https"):
        return {"protocol": protocol, "path": urlpath}

    options: dict[str, Any] = {"protocol": protocol, "path": path}

    if parsed_path.netloc:
        host = parsed_path.netloc.rsplit("@", 1)[-1].rsplit(":", 1)[0]
        options["host"] = host

        if protocol in ("s3", "s3a", "gcs", "gs"):
            options["path"] = host + options["path"]
        if parsed_path.port:
            options["port"] = parsed_path.port
        if parsed_path.username:
            options["username"] = parsed_path.username
        if parsed_path.password:
            options["password"] = parsed_path.password

    if parsed_path.query:
        options["url_query"] = parsed_path.query
    if parsed_path.fragment:
        options["url_fragment"] = parsed_path.fragment

    if inherit_storage_options:
        _update_storage_options(options, inherit_storage_options)

    return options


def _canonicalize_for_token(obj: Any) -> Any:
    """Recursively normalize dicts and sets so representation is order-independent."""
    if isinstance(obj, Mapping):
        items = [(_canonicalize_for_token(k), _canonicalize_for_token(v)) for k, v in obj.items()]
        items.sort(key=lambda pair: (type(pair[0]).__qualname__, repr(pair[0])))
        return dict(items)
    if isinstance(obj, list):
        return [_canonicalize_for_token(item) for item in obj]
    if isinstance(obj, tuple):
        return tuple(_canonicalize_for_token(item) for item in obj)
    if isinstance(obj, (set, frozenset)):
        normalized = [_canonicalize_for_token(item) for item in obj]
        normalized.sort(key=lambda item: (type(item).__qualname__, repr(item)))
        return (type(obj).__qualname__, tuple(normalized))
    return obj


def tokenize(*args: Any, **kwargs: Any) -> str:
    """Compute a deterministic MD5 hex token for arbitrary arguments.

    Mapping keys and set elements are canonically ordered so identical mappings
    with different insertion orders always produce the same token.

    Args:
        *args: Positional values to hash.
        **kwargs: Keyword values to hash.

    Returns:
        32-character lowercase hexadecimal MD5 digest.
    """
    normalized_args = tuple(_canonicalize_for_token(arg) for arg in args)
    if kwargs:
        normalized_args = (*normalized_args, _canonicalize_for_token(kwargs))
    payload = repr(normalized_args).encode("utf-8")
    return md5(payload, usedforsecurity=False).hexdigest()
