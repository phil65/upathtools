"""Filesystem implementation that flattens multiple filesystems into a single view."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Literal, overload

from fsspec.asyn import AsyncFileSystem
from fsspec.implementations.asyn_wrapper import AsyncFileSystemWrapper
from fsspec.spec import AbstractFileSystem

from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath, FileInfo


if TYPE_CHECKING:
    from collections.abc import Sequence

    from upath.types import JoinablePathLike


class FlatUnionInfo(FileInfo, total=False):
    """Info dict for flat union filesystem paths."""

    size: int
    filesystems: int


logger = logging.getLogger(__name__)


def to_async(filesystem: AbstractFileSystem) -> AsyncFileSystem:
    if not isinstance(filesystem, AsyncFileSystem):
        return AsyncFileSystemWrapper(filesystem)
    return filesystem


class FlatUnionPath(BaseUPath[FlatUnionInfo]):
    """UPath implementation for browsing FlatUnionFS."""

    __slots__ = ()

    def iterdir(self):
        if not self.is_dir():
            raise NotADirectoryError(str(self))
        yield from super().iterdir()

    @property
    def path(self) -> str:
        """Return the path part without protocol."""
        path = super().path
        if path in ("", ".", "/"):
            return "/"  # Root paths should return "/"
        return path.lstrip("/")  # Other paths strip leading slash


class FlatUnionFileSystem(BaseAsyncFileSystem[FlatUnionPath, FlatUnionInfo]):
    """Filesystem that merges multiple filesystems into a single flat view.

    Unlike UnionFileSystem which organizes by protocol, FlatUnionFileSystem
    combines all files directly at the root level, creating a merged view
    of all filesystems.

    Warning:
        Name conflicts between filesystems will be resolved by priority -
        the first filesystem in the list that contains a file with a given path
        will be the one accessed.

    Examples:
        Create from filesystem instances:
        ```python
        from fsspec import filesystem
        fs1 = filesystem("memory")
        fs2 = filesystem("file")
        union_fs = FlatUnionFileSystem(filesystems=[fs1, fs2])
        ```

        Create from paths:
        ```python
        union_fs = FlatUnionFileSystem(paths=["/tmp", "memory://", "s3://bucket/"])
        ```

        Create from URL:
        ```python
        kwargs = FlatUnionFileSystem._get_kwargs_from_urls("flatunion://memory://,/tmp")
        union_fs = FlatUnionFileSystem(**kwargs)
        ```

        URL formats supported:
        - `flatunion://path1,path2,path3` (comma-separated filesystems)
        - `flatunion://?filesystems=path1,path2,path3` (query parameter)
    """

    protocol = "flatunion"
    upath_cls = FlatUnionPath
    root_marker = "/"
    cachable = False  # Underlying filesystems handle their own caching

    def __init__(
        self,
        filesystems: Sequence[AbstractFileSystem | JoinablePathLike],
        **kwargs: Any,
    ) -> None:
        """Initialize the filesystem.

        Args:
            filesystems: Sequence of filesystems or paths to merge
            kwargs: Additional keyword arguments for AsyncFileSystem
        """
        from upathtools.helpers import upath_to_fs

        super().__init__(**kwargs)
        # Convert paths to filesystems
        resolved_filesystems: list[AsyncFileSystem] = []
        for fs_or_path in filesystems:
            if isinstance(fs_or_path, AbstractFileSystem):
                # It's already a filesystem
                resolved_filesystems.append(to_async(fs_or_path))
            else:
                # It's a path - convert to filesystem
                resolved_filesystems.append(upath_to_fs(fs_or_path, asynchronous=True))

        self.filesystems = resolved_filesystems
        logger.debug("Created FlatUnionFileSystem with %d filesystems", len(resolved_filesystems))

    @staticmethod
    def _get_kwargs_from_urls(path: str) -> dict[str, Any]:
        """Parse flatunion URL and return constructor kwargs.

        Supports URL formats:
        - flatunion://path1,path2,path3  (comma-separated filesystems)
        - flatunion://?filesystems=path1,path2,path3  (query parameter)

        Args:
            path: flatunion URL path

        Returns:
            Dictionary with 'filesystems' key containing list of path strings
        """
        # Remove protocol prefix first
        path_without_protocol = path.removeprefix("flatunion://")

        # Check if using query parameter format
        if "?" in path_without_protocol:
            import urllib.parse

            _path_part, query_part = path_without_protocol.split("?", 1)
            query_params = urllib.parse.parse_qs(query_part)
            if "filesystems" in query_params:
                filesystems_str = query_params["filesystems"][0]
                paths = [p.strip() for p in filesystems_str.split(",") if p.strip()]
                return {"filesystems": paths}

        # Default: comma-separated paths in the path itself
        if path_without_protocol:
            paths = [p.strip() for p in path_without_protocol.split(",") if p.strip()]
            return {"filesystems": paths}

        return {}

    async def _get_matching_fs(
        self,
        path: str,
        **kwargs: Any,
    ) -> tuple[AsyncFileSystem, int] | tuple[None, None]:
        """Find the first filesystem containing the given path.

        Returns:
            Tuple of (filesystem, fs_index) or (None, None) if not found
        """
        if not path or path == self.root_marker:
            return self, -1

        path = path.lstrip("/")

        # Try each filesystem in order
        for i, fs in enumerate(self.filesystems):
            try:
                if await fs._exists(path, **kwargs):
                    logger.debug("Path %s found in filesystem %d", path, i)
                    return fs, i
            except Exception as e:  # noqa: BLE001
                logger.debug("Error checking path %s in filesystem %d: %s", path, i, e)

        return None, None

    async def _cat_file(self, path: str, start=None, end=None, **kwargs: Any) -> bytes:
        """Get file contents."""
        logger.debug("Reading from path: %s", path)
        fs, _ = await self._get_matching_fs(path, **kwargs)

        if fs is None:
            raise FileNotFoundError(f"File not found: {path}")

        if fs is self:
            raise IsADirectoryError(f"Cannot read from directory: {path}")

        # Use the same path in the target filesystem
        return await fs._cat_file(path.lstrip("/"), start=start, end=end, **kwargs)

    async def _pipe_file(self, path: str, value, **kwargs: Any) -> None:
        """Write file contents.

        Writes to the first filesystem in the list.
        """
        logger.debug("Writing to path: %s", path)
        if not self.filesystems:
            raise RuntimeError("No filesystems available to write to")

        fs = self.filesystems[0]
        norm_path = path.lstrip("/")
        await fs._pipe_file(norm_path, value, **kwargs)

    async def _info(self, path: str, **kwargs: Any) -> FlatUnionInfo:
        """Get info about a path."""
        logger.debug("Getting info for path: %s", path)

        if not path or path == self.root_marker:
            return FlatUnionInfo(
                name="",
                size=0,
                type="directory",
                filesystems=len(self.filesystems),
            )

        # Try to find the path in any filesystem
        fs, _ = await self._get_matching_fs(path, **kwargs)

        if fs is None:
            # Check if it might be a virtual directory
            if await self._isdir(path, **kwargs):
                return FlatUnionInfo(
                    name=path.strip("/").split("/")[-1] if path.strip("/") else "",
                    size=0,
                    type="directory",
                )

            raise FileNotFoundError(f"Path not found: {path}")

        if fs is self:
            return FlatUnionInfo(
                name="",
                size=0,
                type="directory",
            )

        # Use the same path in the target filesystem
        norm_path = path.lstrip("/")
        info = await fs._info(norm_path, **kwargs)
        # Normalize the name to be relative to our root and create FlatUnionInfo
        name = info.get("name", "")
        if "/" in name:
            name = name.split("/")[-1]

        return FlatUnionInfo(
            name=name,
            type=info.get("type", "file"),
            size=info.get("size", 0),
        )

    @overload
    async def _ls(
        self, path: str, detail: Literal[True] = ..., **kwargs: Any
    ) -> list[FlatUnionInfo]: ...

    @overload
    async def _ls(self, path: str, detail: Literal[False], **kwargs: Any) -> list[str]: ...

    async def _ls(
        self,
        path: str,
        detail: bool = True,
        **kwargs: Any,
    ) -> list[FlatUnionInfo] | list[str]:
        """List contents of a path."""
        logger.debug("Listing path: %s", path)
        norm_path = path.rstrip("/").lstrip("/")
        # Find all filesystems that have contents at this path
        results = []
        seen_names = set()
        # Try listing this path in each filesystem
        for i, fs in enumerate(self.filesystems):
            try:
                # Check if the path exists in this filesystem
                exists = await fs._exists(norm_path, **kwargs) if norm_path else True
                if not exists:
                    continue

                listing = await fs._ls(norm_path, detail=True, **kwargs)  # Get detailed listings
                # Process each entry
                for entry in listing:
                    name = entry["name"]
                    if "/" in name:
                        name = name.rsplit("/", 1)[-1]

                    # Skip duplicates (first filesystem wins)
                    if name not in seen_names:
                        seen_names.add(name)
                        entry_copy = entry.copy()
                        entry_copy["name"] = name
                        results.append(entry_copy)

            except Exception as e:  # noqa: BLE001
                msg = "Error listing path %s in filesystem %d: %s"
                logger.debug(msg, norm_path, i, e)

        # Special case for subdirectories of non-existent paths
        if not results and norm_path:
            # Check if we might have directories at a deeper level
            prefix = f"{norm_path}/"

            for i, fs in enumerate(self.filesystems):
                try:
                    # List the root to see if any entries start with our path
                    root_listing = await fs._ls("/", detail=True, **kwargs)
                    for entry in root_listing:
                        entry_path = entry.get("name", "").lstrip("/")

                        # Check if it's a subdirectory of our path
                        if entry_path.startswith(prefix):
                            next_part = entry_path[len(prefix) :].split("/", 1)[0]
                            if next_part and next_part not in seen_names:
                                seen_names.add(next_part)
                                results.append(
                                    FlatUnionInfo(
                                        name=next_part,
                                        type="directory",
                                        size=0,
                                    )
                                )
                except Exception as e:  # noqa: BLE001
                    logger.debug("Error checking subpaths in filesystem %d: %s", i, e)

        if not results and norm_path and not await self._isdir(path, **kwargs):
            # No results and not a directory
            msg = f"Directory not found: {path}"
            raise FileNotFoundError(msg)

        if detail:
            return results
        return [entry["name"] for entry in results]

    async def _exists(self, path: str, **kwargs: Any) -> bool:
        """Check if a path exists in any filesystem."""
        fs, _ = await self._get_matching_fs(path, **kwargs)
        return fs is not None

    async def _isdir(self, path: str, **kwargs: Any) -> bool:
        """Check if a path is a directory in any filesystem."""
        if not path or path == self.root_marker:
            return True

        norm_path = path.lstrip("/")

        # Check if the exact path exists as a directory in any filesystem
        for fs in self.filesystems:
            try:
                if await fs._exists(norm_path, **kwargs) and await fs._isdir(norm_path, **kwargs):
                    return True
            except Exception:  # noqa: BLE001
                pass

        # Check if any path starts with this prefix (virtual directory)
        prefix = f"{norm_path}/"
        for fs in self.filesystems:
            try:
                root_listing = await fs._ls("/", detail=False, **kwargs)
                for entry in root_listing:
                    entry_path = entry.lstrip("/")
                    if entry_path.startswith(prefix):
                        return True
            except Exception:  # noqa: BLE001
                pass

        return False

    async def _isfile(self, path: str, **kwargs: Any) -> bool:
        """Check if a path is a file in any filesystem."""
        try:
            fs, _ = await self._get_matching_fs(path, **kwargs)
            if fs is None or fs is self:
                return False

            norm_path = path.lstrip("/")
            return await fs._isfile(norm_path, **kwargs)
        except FileNotFoundError:
            return False

    async def _makedirs(self, path: str, exist_ok: bool = False, **kwargs: Any) -> None:
        """Create a directory and parents in the first filesystem."""
        logger.debug("Making directories: %s", path)
        if not self.filesystems:
            msg = "No filesystems available to create directories in"
            raise RuntimeError(msg)

        fs = self.filesystems[0]
        norm_path = path.lstrip("/")
        await fs._makedirs(norm_path, exist_ok=exist_ok, **kwargs)

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        """Remove a file from any filesystem that contains it."""
        logger.debug("Removing file: %s", path)
        fs, _ = await self._get_matching_fs(path, **kwargs)

        if fs is None:
            msg = f"File not found: {path}"
            raise FileNotFoundError(msg)

        if fs is self:
            msg = f"Cannot remove directory: {path}"
            raise IsADirectoryError(msg)

        norm_path = path.lstrip("/")
        await fs._rm_file(norm_path, **kwargs)

    async def _rm(self, path: str, recursive: bool = False, **kwargs: Any) -> None:
        """Remove files or directories from any filesystem that contains them."""
        logger.debug("Removing path: %s (recursive=%s)", path, recursive)

        # Try each filesystem
        fs, _ = await self._get_matching_fs(path, **kwargs)

        if fs is None:
            msg = f"Path not found: {path}"
            raise FileNotFoundError(msg)

        if fs is self and not recursive:
            msg = "Cannot remove root directory without recursive=True"
            raise ValueError(msg)

        if fs is self and recursive:
            # Special case: removing everything
            for fs_idx, fs in enumerate(self.filesystems):
                try:
                    await fs._rm("/", recursive=True, **kwargs)
                except Exception:
                    msg = "Error removing content from filesystem %d"
                    logger.exception(msg, fs_idx)
            return

        # Normal case: remove from specific filesystem
        norm_path = path.lstrip("/")
        await fs._rm(norm_path, recursive=recursive, **kwargs)

    async def _cp_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        """Copy a file, possibly between filesystems."""
        logger.debug("Copying %s to %s", path1, path2)
        # Find source filesystem
        src_fs, _src_idx = await self._get_matching_fs(path1, **kwargs)
        if src_fs is None:
            msg = f"Source file not found: {path1}"
            raise FileNotFoundError(msg)
        if src_fs is self:
            msg = f"Source is a directory: {path1}"
            raise IsADirectoryError(msg)
        # Normalize paths
        src_path = path1.lstrip("/")
        dst_path = path2.lstrip("/")
        # Check if destination exists
        dst_fs, _dst_idx = await self._get_matching_fs(path2, **kwargs)
        # If destination doesn't exist, use first filesystem
        if dst_fs is None:
            if not self.filesystems:
                msg = "No filesystems available to copy to"
                raise RuntimeError(msg)
            dst_fs = self.filesystems[0]
        # Handle copying between filesystems
        if src_fs is dst_fs:
            # Same filesystem
            await src_fs._cp_file(src_path, dst_path, **kwargs)
        else:
            # Different filesystems - copy via stream
            content = await src_fs._cat_file(src_path)
            await dst_fs._pipe_file(dst_path, content)


def create_flat_union_path(paths: Sequence[JoinablePathLike]) -> UPath:
    """Create a FlatUnionFileSystem from a list of paths.

    This function takes multiple paths, potentially from different filesystem types,
    and creates a flat union filesystem that presents their contents in a single
    merged view without protocol-specific directories.
    - In case of filename conflicts, the first filesystem (in the order provided) wins
    - Write operations default to the first filesystem in the list

    Args:
        paths: List of UPath objects or path-like objects

    Returns:
        UPath: A path object based on the FlatUnionFileSystem
    """
    flat_fs = FlatUnionFileSystem(filesystems=paths)
    p = UPath("flatunion://")
    p._fs_cached = flat_fs  # pyright: ignore[reportAttributeAccessIssue]
    return p


if __name__ == "__main__":
    import asyncio

    from upath import UPath

    async def main() -> None:
        # Create test directories in memory
        from fsspec.implementations.memory import MemoryFileSystem

        # Create two memory filesystems with different content
        mem1 = MemoryFileSystem()
        mem1.mkdir("dir1")
        mem1.pipe("file1.txt", b"content from fs1")
        mem1.pipe("dir1/nested.txt", b"nested content from fs1")

        mem2 = MemoryFileSystem()
        mem2.mkdir("dir2")
        mem2.pipe("file2.txt", b"content from fs2")
        mem2.pipe("dir2/nested.txt", b"nested content from fs2")

        # Create a flat union filesystem
        # flat_fs = FlatUnionFileSystem([mem1, mem2])

        # # Test listing the flat union
        # print("\nListing flat union contents:")
        # listing = await flat_fs._ls("/")
        # for item in listing:
        #     print(f"- {item['name']} ({item['type']})")

        # # Test file access
        # content = await flat_fs._cat_file("file1.txt")
        # print(f"\nContent of file1.txt: {content.decode()}")

        # content = await flat_fs._cat_file("file2.txt")
        # print(f"Content of file2.txt: {content.decode()}")

        # Test the helper function
        print("\nTesting the helper function:")
        path1 = UPath("memory://", fs=mem1)
        path2 = UPath("memory://", fs=mem2)
        print(list(path1.iterdir()))
        flat_path = create_flat_union_path([path1, path2])
        print(f"Created {flat_path}")

        print("\nContents via UPath:")
        for p in flat_path.iterdir():
            print(f"- {p.name}")

    asyncio.run(main())
