"""Error hierarchy for ``upathtools.fs``."""


class FSError(OSError):
    """Base error for filesystem operations.

    Subclasses ``OSError`` for standard library compatibility.
    """


class FSTimeoutError(TimeoutError):
    """Raised when a filesystem operation or sync bridge times out."""


class UnsupportedOperation(FSError):  # noqa: N818
    """Raised when a backend does not support an optional capability (e.g., write, rmdir)."""


class PathEscapeError(ValueError):
    """Raised when a path attempts to escape a rooted filesystem (e.g., via ``..``)."""


class FilesystemClosedError(RuntimeError):
    """Raised when operating on a closed filesystem instance."""
