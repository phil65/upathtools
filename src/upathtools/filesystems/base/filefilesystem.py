"""Base class for filesystems that operate on file content."""

from __future__ import annotations

from abc import abstractmethod
from enum import Enum
from typing import TYPE_CHECKING, Any, ClassVar

from upath import UPath

from upathtools.filesystems.base.basefilesystem import BaseAsyncFileSystem, BaseFileSystem


if TYPE_CHECKING:
    from fsspec.asyn import AsyncFileSystem
    from fsspec.spec import AbstractFileSystem


class ProbeResult(Enum):
    """Result of probing content to determine filesystem compatibility."""

    SUPPORTED = "supported"  # Definitely can handle this content
    UNSUPPORTED = "unsupported"  # Definitely cannot handle this content
    MAYBE = "maybe"  # Extension matches but can't verify without full parse


class FileFileSystemMixin:
    """Mixin providing extension and content probing for file-based filesystems.

    File filesystems are filesystems that parse and expose the internal structure
    of specific file types (e.g., markdown headers, SQLite tables, Python AST).

    The detection system works in two phases:
    1. Extension check via `supports_extension()` - quick filter
    2. Content probing via `probe_content()` - verify content is valid

    For filesystems that share extensions (e.g., JSON Schema vs OpenAPI both use .json),
    the `priority` class variable determines which is tried first.
    """

    supported_extensions: ClassVar[frozenset[str]] = frozenset()
    priority: ClassVar[int] = 100  # Lower = higher priority, default is 100

    @classmethod
    def supports_extension(cls, extension: str) -> bool:
        """Check if this filesystem supports the given file extension.

        Args:
            extension: File extension to check (with or without leading dot).

        Returns:
            True if the filesystem might handle files with this extension.
        """
        ext = extension.lower().lstrip(".")
        return ext in cls.supported_extensions

    @classmethod
    def get_supported_extensions(cls) -> frozenset[str]:
        """Get all supported file extensions.

        Returns:
            Frozenset of supported extensions (without leading dots).
        """
        return cls.supported_extensions

    @classmethod
    def probe_content(cls, content: bytes, extension: str = "") -> ProbeResult:
        """Probe content to determine if this filesystem can handle it.

        This method allows filesystems to inspect file content to determine
        compatibility. Useful for formats that share extensions (e.g., JSON).

        Args:
            content: File content to probe (may be partial for efficiency).
            extension: File extension hint (without leading dot).

        Returns:
            ProbeResult indicating compatibility level.

        Note:
            Default implementation returns MAYBE if extension matches,
            UNSUPPORTED otherwise. Subclasses should override for smarter probing.
        """
        if cls.supports_extension(extension):
            return ProbeResult.MAYBE
        return ProbeResult.UNSUPPORTED

    @classmethod
    def get_probe_size(cls) -> int | None:
        """Get the number of bytes needed for probing.

        Returns:
            Number of bytes to read for probing, or None to read entire file.
            Default is None (read entire file for probe).

        Note:
            Subclasses can override this to enable efficient partial reads.
            For example, a filesystem checking for magic bytes might return 16.
        """
        return None


class BaseFileFileSystem[TPath: UPath, TInfoDict = dict[str, Any]](
    FileFileSystemMixin, BaseFileSystem[TPath, TInfoDict]
):
    """Base class for sync file-based filesystems.

    File filesystems parse specific file types and expose their internal structure
    as a virtual filesystem. Examples include:
    - MarkdownFileSystem: Exposes markdown headers as directories
    - SqliteFileSystem: Exposes database tables as directories

    Subclasses should:
    1. Set `supported_extensions` class variable
    2. Set `priority` if competing with other filesystems for same extensions
    3. Override `probe_content()` for content-based detection
    4. Implement `from_content()` for content-based creation
    5. Implement standard filesystem methods (_ls, _cat_file, etc.)
    """

    @classmethod
    @abstractmethod
    def from_file(
        cls,
        path: str,
        target_protocol: str | None = None,
        target_options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> BaseFileFileSystem[TPath, TInfoDict]:
        """Create filesystem instance from a file path.

        Args:
            path: Path to the source file.
            target_protocol: Protocol for accessing the source file.
            target_options: Options for the target protocol.
            **kwargs: Additional filesystem options.

        Returns:
            Configured filesystem instance.
        """
        ...

    @classmethod
    def from_filesystem(
        cls,
        path: str,
        fs: AbstractFileSystem,
        **kwargs: Any,
    ) -> BaseFileFileSystem[TPath, TInfoDict]:
        """Create filesystem instance with access to a parent filesystem.

        This method allows the file filesystem to access the source file
        through the parent filesystem, enabling read-write operations for
        filesystems that need direct file access (e.g., SQLite).

        Args:
            path: Path to the file within the parent filesystem.
            fs: Parent filesystem to read/write through.
            **kwargs: Additional filesystem options.

        Returns:
            Configured filesystem instance.

        Note:
            Default implementation reads content via from_content.
            Subclasses that need direct file access should override this.
        """
        content = fs.cat_file(path)
        if isinstance(content, str):
            content = content.encode()
        return cls.from_content(content, **kwargs)

    @classmethod
    def from_content(
        cls,
        content: bytes,
        **kwargs: Any,
    ) -> BaseFileFileSystem[TPath, TInfoDict]:
        """Create filesystem instance from raw content bytes.

        Args:
            content: Raw file content as bytes.
            **kwargs: Additional filesystem options.

        Returns:
            Configured filesystem instance.

        Raises:
            NotImplementedError: If the filesystem doesn't support content-based creation.
        """
        msg = f"{cls.__name__} does not support creation from content"
        raise NotImplementedError(msg)


class BaseAsyncFileFileSystem[TPath: UPath, TInfoDict = dict[str, Any]](
    FileFileSystemMixin, BaseAsyncFileSystem[TPath, TInfoDict]
):
    """Base class for async file-based filesystems.

    Async variant of BaseFileFileSystem for filesystems that require
    async I/O operations.

    Subclasses should:
    1. Set `supported_extensions` class variable
    2. Set `priority` if competing with other filesystems for same extensions
    3. Override `probe_content()` for content-based detection
    4. Implement `from_content()` for content-based creation
    5. Implement standard async filesystem methods (_ls, _cat_file, etc.)
    """

    @classmethod
    @abstractmethod
    def from_file(
        cls,
        path: str,
        target_protocol: str | None = None,
        target_options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> BaseAsyncFileFileSystem[TPath, TInfoDict]:
        """Create filesystem instance from a file path.

        Args:
            path: Path to the source file.
            target_protocol: Protocol for accessing the source file.
            target_options: Options for the target protocol.
            **kwargs: Additional filesystem options.

        Returns:
            Configured filesystem instance.
        """
        ...

    @classmethod
    async def from_filesystem_async(
        cls,
        path: str,
        fs: AsyncFileSystem,
        **kwargs: Any,
    ) -> BaseAsyncFileFileSystem[TPath, TInfoDict]:
        """Create filesystem instance with access to a parent filesystem (async).

        Args:
            path: Path to the file within the parent filesystem.
            fs: Parent async filesystem to read/write through.
            **kwargs: Additional filesystem options.

        Returns:
            Configured filesystem instance.

        Note:
            Default implementation reads content via from_content.
            Subclasses that need direct file access should override this.
        """
        content = await fs._cat_file(path)
        if isinstance(content, str):
            content = content.encode()
        return cls.from_content(content, **kwargs)

    @classmethod
    def from_filesystem(
        cls,
        path: str,
        fs: AbstractFileSystem,
        **kwargs: Any,
    ) -> BaseAsyncFileFileSystem[TPath, TInfoDict]:
        """Create filesystem instance with access to a parent filesystem.

        Args:
            path: Path to the file within the parent filesystem.
            fs: Parent filesystem to read/write through.
            **kwargs: Additional filesystem options.

        Returns:
            Configured filesystem instance.

        Note:
            Default implementation reads content via from_content.
            Subclasses that need direct file access should override this.
        """
        content = fs.cat_file(path)
        if isinstance(content, str):
            content = content.encode()
        return cls.from_content(content, **kwargs)

    @classmethod
    def from_content(
        cls,
        content: bytes,
        **kwargs: Any,
    ) -> BaseAsyncFileFileSystem[TPath, TInfoDict]:
        """Create filesystem instance from raw content bytes.

        Args:
            content: Raw file content as bytes.
            **kwargs: Additional filesystem options.

        Returns:
            Configured filesystem instance.

        Raises:
            NotImplementedError: If the filesystem doesn't support content-based creation.
        """
        msg = f"{cls.__name__} does not support creation from content"
        raise NotImplementedError(msg)
