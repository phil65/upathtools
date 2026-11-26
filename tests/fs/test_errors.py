import pytest

from upathtools.fs import (
    FilesystemClosedError,
    FSError,
    FSTimeoutError,
    PathEscapeError,
    UnsupportedOperation,
)


def test_error_hierarchy() -> None:
    assert issubclass(FSError, OSError)
    assert issubclass(FSTimeoutError, TimeoutError)
    assert issubclass(UnsupportedOperation, FSError)
    assert issubclass(UnsupportedOperation, OSError)
    assert issubclass(PathEscapeError, ValueError)
    assert issubclass(FilesystemClosedError, RuntimeError)


def test_unsupported_operation_caught_as_oserror() -> None:
    with pytest.raises(OSError, match="read-only"):
        raise UnsupportedOperation("backend is read-only")


def test_error_messages_preserved() -> None:
    assert str(FSError("boom")) == "boom"
    assert str(FSTimeoutError("timed out")) == "timed out"
    assert str(PathEscapeError("../secret")) == "../secret"
    assert str(FilesystemClosedError("already closed")) == "already closed"
