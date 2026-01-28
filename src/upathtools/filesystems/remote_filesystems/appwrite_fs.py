"""Filesystem implementation for Appwrite Storage service."""

from __future__ import annotations

import contextlib
import io
import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, overload

from fsspec.utils import tokenize

from upathtools.async_helpers import sync_wrapper
from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath, FileInfo


if TYPE_CHECKING:
    from appwrite.client import Client
    from appwrite.services.storage import Storage

logger = logging.getLogger(__name__)


class AppwriteInfo(FileInfo, total=False):
    """Info dict for Appwrite storage paths."""

    size: int
    bucket_name: str | None
    enabled: bool | None
    file_id: str | None
    created: str | None
    updated: str | None
    mime_type: str | None


class AppwritePath(BaseUPath[AppwriteInfo]):
    """UPath implementation for Appwrite storage."""

    __slots__ = ()

    def iterdir(self):
        if not self.is_dir():
            raise NotADirectoryError(str(self))
        yield from super().iterdir()


class AppwriteFileSystem(BaseAsyncFileSystem[AppwritePath, AppwriteInfo]):
    """Filesystem for Appwrite storage service.

    This filesystem allows you to interact with Appwrite storage buckets
    using the standard filesystem interface. The path format is:

    appwrite://<bucket_id>/<file_path>

    If a default bucket_id is provided during initialization,
    you can use paths without specifying the bucket:

    appwrite://file.txt (uses the default bucket)
    """

    protocol = "appwrite"
    upath_cls = AppwritePath

    def __init__(
        self,
        endpoint: str | None = None,
        project: str | None = None,
        key: str | None = None,
        bucket_id: str | None = None,
        self_signed: bool = False,
        client: Client | None = None,
        asynchronous: bool = False,
        **kwargs: Any,
    ):
        """Initialize the filesystem.

        Args:
            endpoint: Appwrite API endpoint
            project: Appwrite project ID
            key: Appwrite API key
            bucket_id: Default bucket ID (required for operations)
            self_signed: Whether to allow self-signed certificates
            client: Existing Appwrite client (optional)
            asynchronous: Whether to use async operations
            **kwargs: Additional filesystem options
        """
        super().__init__(asynchronous=asynchronous, **kwargs)

        if client:
            self.client = client
            self._endpoint = self.client._endpoint
            self._project = self.client._global_headers["x-appwrite-project"]
        else:
            from appwrite.client import Client

            # Check environment variables if parameters not provided
            self._endpoint = endpoint or os.environ.get("APPWRITE_ENDPOINT")
            self._project = project or os.environ.get("APPWRITE_PROJECT")
            key = key or os.environ.get("APPWRITE_API_KEY")
            self_signed = (
                self_signed or os.environ.get("APPWRITE_SELF_SIGNED", "false").lower() == "true"
            )

            if self._endpoint and self._project and key:
                self.client = Client()
                self.client.set_endpoint(self._endpoint)
                self.client.set_project(self._project)
                self.client.set_key(key)

                if self_signed:
                    self.client.set_self_signed()
            else:
                msg = "Either provide a client or endpoint, project, and key (or set APPWRITE_* env variables)"  # noqa: E501
                raise ValueError(msg)

        # Read bucket_id from environment if not provided
        self.bucket_id = bucket_id or os.environ.get("APPWRITE_BUCKET_ID")
        self._storage: Storage | None = None

    @staticmethod
    def _get_kwargs_from_urls(path: str) -> dict[str, Any]:
        path = path.removeprefix("appwrite://")
        return {"project": path}

    @property
    def storage(self) -> Storage:
        """Get the Appwrite storage service."""
        if self._storage is None:
            from appwrite.services.storage import Storage

            self._storage = Storage(self.client)
        return self._storage

    def _split_path(self, path: str) -> tuple[str, str]:
        """Split path into bucket_id and file_id.

        This handles paths in the format:
        - /bucket_id/file_id
        - /bucket_id/      (returns bucket_id and empty string)
        - bucket_id/file_id
        - bucket_id/       (returns bucket_id and empty string)
        - file_id          (uses default bucket if set)
        """
        original_path = path
        path = self._strip_protocol(path)  # pyright: ignore
        path = path.strip("/")

        # Empty path or root
        if not path:
            return "", ""

        # Check if original path ended with slash
        has_trailing_slash = original_path.rstrip("/") != original_path

        # Split the path
        parts = path.split("/", 1)

        if len(parts) == 1:
            if has_trailing_slash:
                # Format: "bucket_id/" - treat as bucket with no file path
                return parts[0], ""

            # Format: "file_id" - use default bucket
            if not self.bucket_id:
                msg = "Default bucket_id not set. Path must include bucket_id."
                raise ValueError(msg)
            return self.bucket_id, parts[0]

        # Format: "bucket_id/file_path"
        return parts[0], parts[1]

    @overload
    async def _ls(
        self, path: str, detail: Literal[True] = ..., **kwargs: Any
    ) -> list[AppwriteInfo]: ...

    @overload
    async def _ls(self, path: str, detail: Literal[False], **kwargs: Any) -> list[str]: ...

    async def _ls(  # noqa: PLR0911
        self,
        path: str,
        detail: bool = True,
        **kwargs: Any,
    ) -> list[AppwriteInfo] | list[str]:
        """List files in a bucket or path."""
        # Strip protocol but preserve trailing slash
        original_path = path
        path = self._strip_protocol(path)  # pyright: ignore
        has_trailing_slash = original_path.rstrip("/") != original_path

        # Root path lists buckets
        if not path or path == "/":
            try:
                response = self.storage.list_buckets()
                buckets = response.get("buckets", [])

                if detail:
                    return [
                        AppwriteInfo(
                            name=bucket["$id"],
                            type="directory",
                            size=0,
                            bucket_name=bucket["name"],
                            enabled=bucket.get("enabled", True),
                        )
                        for bucket in buckets
                    ]
                return [bucket["$id"] for bucket in buckets]

            except Exception as e:  # noqa: BLE001
                logger.warning("Failed to list buckets: %s", str(e))
                return []

        # Clean the path for processing
        clean_path = path.strip("/")
        parts = clean_path.split("/", 1)

        # Special case: direct bucket path with trailing slash
        if len(parts) == 1 and has_trailing_slash:
            bucket_id = parts[0]
            file_path = ""
        else:
            # Normal path processing
            try:
                bucket_id, file_path = self._split_path(path)
            except ValueError as e:
                # This might be a single component without trailing slash
                # interpret it as a bucket if it ends with trailing slash in original
                if has_trailing_slash and len(parts) == 1:
                    bucket_id = parts[0]
                    file_path = ""
                else:
                    raise ValueError(str(e)) from e

        try:
            # Simulate directory structure
            prefix = file_path if file_path.endswith("/") else file_path + "/" if file_path else ""
            # List all files in bucket
            response = self.storage.list_files(bucket_id=bucket_id)
            files = response.get("files", [])
            # Apply prefix filtering to simulate directories
            if prefix:
                files = [f for f in files if f["name"].startswith(prefix) or f["name"] == file_path]

            # Extract virtual directories from paths
            result: list[Any] = []
            virtual_dirs = set()
            for file in files:
                name = file["name"]
                if prefix and name.startswith(prefix):
                    # Get relative path
                    rel_name = name[len(prefix) :]

                    # Extract first directory component
                    if "/" in rel_name:
                        dir_name = rel_name.split("/", 1)[0]
                        full_dir = f"{prefix}{dir_name}/"
                        virtual_dirs.add(full_dir)
                    # It's a file within the prefix
                    elif detail:
                        result.append(
                            AppwriteInfo(
                                name=name,
                                type="file",
                                size=file.get("sizeOriginal", 0),
                                file_id=file["$id"],
                                created=file.get("$createdAt"),
                                updated=file.get("$updatedAt"),
                                mime_type=file.get("mimeType"),
                            )
                        )
                    else:
                        result.append(name)
                elif not prefix:
                    # No prefix, add files at root level
                    if "/" in name:
                        # Extract first directory component
                        dir_name = name.split("/", 1)[0] + "/"
                        virtual_dirs.add(dir_name)
                    # It's a file in root
                    elif detail:
                        result.append(
                            AppwriteInfo(
                                name=name,
                                type="file",
                                size=file.get("sizeOriginal", 0),
                                file_id=file["$id"],
                                created=file.get("$createdAt"),
                                updated=file.get("$updatedAt"),
                                mime_type=file.get("mimeType"),
                            )
                        )
                    else:
                        result.append(name)
                elif name == file_path:
                    # Exact file match
                    if detail:
                        result.append(
                            AppwriteInfo(
                                name=name,
                                type="file",
                                size=file.get("sizeOriginal", 0),
                                file_id=file["$id"],
                                created=file.get("$createdAt"),
                                updated=file.get("$updatedAt"),
                                mime_type=file.get("mimeType"),
                            )
                        )
                    else:
                        result.append(name)

            # Add virtual directories
            for vdir in virtual_dirs:
                if detail:
                    result.append(
                        AppwriteInfo(
                            name=vdir,
                            type="directory",
                            size=0,
                        )
                    )
                else:
                    result.append(vdir)
        except Exception as e:  # noqa: BLE001
            logger.warning("Failed to list files: %s", str(e))
            if file_path:
                # Try to get a single file
                try:
                    file_id = self._get_file_id_by_name(bucket_id, file_path)
                    if file_id:
                        file_info = self.storage.get_file(bucket_id, file_id)
                        if detail:
                            return [
                                AppwriteInfo(
                                    name=file_info["name"],
                                    type="file",
                                    size=file_info.get("sizeOriginal", 0),
                                    file_id=file_info["$id"],
                                    created=file_info.get("$createdAt"),
                                    updated=file_info.get("$updatedAt"),
                                    mime_type=file_info.get("mimeType"),
                                )
                            ]
                        return [file_info["name"]]
                except Exception:  # noqa: BLE001
                    pass

            return []
        else:
            return result

    ls = sync_wrapper(_ls)

    def _get_file_id_by_name(self, bucket_id: str, file_name: str) -> str | None:
        """Get file ID by name from Appwrite.

        Args:
            bucket_id: Bucket ID
            file_name: File name to find

        Returns:
            File ID if found, None otherwise
        """
        try:
            response = self.storage.list_files(bucket_id=bucket_id)
            files = response.get("files", [])

            for file in files:
                if file["name"] == file_name:
                    return file["$id"]
        except Exception as e:  # noqa: BLE001
            logger.warning("Failed to get file ID: %s", str(e))
            return None
        else:
            return None

    async def _info(self, path: str, **kwargs: Any) -> AppwriteInfo:
        """Get info about a file or directory."""
        path = self._strip_protocol(path)  # pyright: ignore

        if not path or path == "/":
            return AppwriteInfo(
                name="",
                type="directory",
                size=0,
            )

        bucket_id, file_path = self._split_path(path)

        # Check if it's a bucket
        if not file_path:
            try:
                bucket = self.storage.get_bucket(bucket_id)
                return AppwriteInfo(
                    name=bucket_id,
                    type="directory",
                    size=0,
                    bucket_name=bucket["name"],
                    enabled=bucket.get("enabled", True),
                )
            except Exception as e:
                msg = f"Bucket not found: {bucket_id}"
                raise FileNotFoundError(msg) from e

        # Check if it's a directory (virtual)
        if file_path.endswith("/"):
            # Verify this directory exists by listing files
            try:
                files = await self._ls(path, detail=False)
                if files:
                    return AppwriteInfo(
                        name=file_path.rstrip("/").split("/")[-1] + "/",
                        type="directory",
                        size=0,
                    )
                msg = f"Directory not found: {path}"
                raise FileNotFoundError(msg)  # noqa: TRY301
            except Exception as e:
                msg = f"Directory not found: {path}"
                raise FileNotFoundError(msg) from e

        # It must be a file
        file_id = self._get_file_id_by_name(bucket_id, file_path)

        if not file_id:
            # Check if it's a virtual directory
            try:
                test_path = f"{path}/"
                files = await self._ls(test_path, detail=False)
                if files:
                    return AppwriteInfo(
                        name=file_path.split("/")[-1],
                        type="directory",
                        size=0,
                    )
                msg = f"File not found: {path}"
                raise FileNotFoundError(msg)  # noqa: TRY301
            except Exception as e:
                msg = f"File not found: {path}"
                raise FileNotFoundError(msg) from e

        try:
            file_info = self.storage.get_file(bucket_id, file_id)
            return AppwriteInfo(
                name=file_info["name"],
                type="file",
                size=file_info.get("sizeOriginal", 0),
                file_id=file_info["$id"],
                created=file_info.get("$createdAt"),
                updated=file_info.get("$updatedAt"),
                mime_type=file_info.get("mimeType"),
            )
        except Exception as e:
            msg = f"File not found: {path}"
            raise FileNotFoundError(msg) from e

    info = sync_wrapper(_info)

    async def _cat_file(
        self,
        path: str,
        start: int | None = None,
        end: int | None = None,
        **kwargs: Any,
    ) -> bytes:
        """Read file contents.

        Args:
            path: Path to the file
            start: Start byte position
            end: End byte position
            **kwargs: Additional arguments

        Returns:
            File contents as bytes

        Raises:
            FileNotFoundError: If the file doesn't exist
        """
        bucket_id, file_path = self._split_path(path)

        if not file_path:
            msg = f"Cannot read bucket: {bucket_id}"
            raise IsADirectoryError(msg)

        # Get file ID from name
        file_id = self._get_file_id_by_name(bucket_id, file_path)

        if not file_id:
            msg = f"File not found: {path}"
            raise FileNotFoundError(msg)

        try:
            # Download the entire file
            content = self.storage.get_file_download(bucket_id, file_id)

            # Apply start/end ranges if needed
            if start is not None or end is not None:
                start = start or 0
                end = min(end or len(content), len(content))
                return content[start:end]
        except Exception as e:
            msg = f"Failed to read file: {path}"
            raise OSError(msg) from e
        else:
            return content

    cat_file = sync_wrapper(_cat_file)  # pyright: ignore[reportAssignmentType]

    async def _pipe_file(self, path: str, value: bytes, **kwargs: Any) -> None:
        """Write bytes to a file.

        Args:
            path: Path to the file
            value: Content to write
            **kwargs: Additional arguments including:
                - permissions: List of Appwrite permissions for the file

        Raises:
            IOError: If writing fails
        """
        from appwrite.id import ID
        from appwrite.input_file import InputFile

        bucket_id, file_path = self._split_path(path)

        if not file_path:
            msg = f"Cannot write to bucket root: {bucket_id}"
            raise IsADirectoryError(msg)

        # Check if the file already exists
        file_id = self._get_file_id_by_name(bucket_id, file_path)

        # Prepare permissions
        permissions = kwargs.get("permissions")

        temp_path = Path(f"/tmp/appwrite_upload_{tokenize(path, value)}")
        temp_path.parent.mkdir(exist_ok=True)

        with temp_path.open("wb") as f:
            f.write(value)

        try:
            if file_id:
                # Delete existing file and create new one
                try:
                    self.storage.delete_file(bucket_id, file_id)
                except Exception as e:  # noqa: BLE001
                    logger.warning("Error deleting existing file: %s", str(e))

            # Create new file
            input_file = InputFile.from_path(temp_path)
            self.storage.create_file(
                bucket_id=bucket_id,
                file_id=ID.unique(),
                file=input_file,
                permissions=permissions or [],
            )

        except Exception as e:
            msg = f"Failed to write to {path}: {e!s}"
            raise OSError(msg) from e
        finally:
            # Clean up temp file
            with contextlib.suppress(Exception):
                temp_path.unlink()

    pipe_file = sync_wrapper(_pipe_file)

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        """Delete a file.

        Args:
            path: Path to the file
            **kwargs: Additional arguments

        Raises:
            FileNotFoundError: If the file doesn't exist
            IsADirectoryError: If the path is a directory
        """
        bucket_id, file_path = self._split_path(path)

        if not file_path:
            msg = f"Cannot delete bucket with rm_file: {bucket_id}"
            raise IsADirectoryError(msg)

        # Get file ID from name
        file_id = self._get_file_id_by_name(bucket_id, file_path)

        if not file_id:
            msg = f"File not found: {path}"
            raise FileNotFoundError(msg)

        try:
            self.storage.delete_file(bucket_id, file_id)
        except Exception as e:
            msg = f"Failed to delete file: {path}"
            raise OSError(msg) from e

    rm_file = sync_wrapper(_rm_file)

    async def _rm(self, path: str, recursive: bool = False, **kwargs: Any) -> None:
        """Remove a file or directory.

        Args:
            path: Path to remove
            recursive: Whether to recursively remove contents
            **kwargs: Additional arguments

        Raises:
            FileNotFoundError: If the path doesn't exist
            ValueError: If trying to delete a non-empty directory without recursive=True
        """
        path = self._strip_protocol(path)  # pyright: ignore

        if not path or path == "/":
            if not recursive:
                msg = "Cannot delete root directory without recursive=True"
                raise ValueError(msg)

            # List all buckets and delete files in them
            # Note: Appwrite doesn't support bucket deletion via API
            return

        bucket_id, file_path = self._split_path(path)

        if not file_path:
            # It's a bucket
            if not recursive:
                # Check if bucket is empty
                files = await self._ls(path, detail=False)
                if files:
                    msg = f"Cannot delete non-empty bucket without recursive=True: {bucket_id}"
                    raise ValueError(msg)

            # Note: Appwrite doesn't support bucket deletion via API
            # This is just for interface compatibility
            try:
                if recursive:
                    file_dicts = await self._ls(path, detail=True)
                    for file in file_dicts:
                        if file["type"] == "file":
                            file_path = f"{bucket_id}/{file['name']}"
                            await self._rm_file(file_path)
            except Exception as e:
                msg = f"Failed to delete bucket: {bucket_id}"
                raise OSError(msg) from e

            return

        # Check if it's a directory
        if file_path.endswith("/") or await self._isdir(path):
            if not recursive:
                # Check if directory is empty
                contents = await self._ls(path, detail=False)
                if contents:
                    msg = f"Cannot delete non-empty dir without recursive=True: {path}"
                    raise ValueError(msg)

            if recursive:
                # List and delete all files in directory
                files_dicts = await self._ls(path, detail=True)
                for file_dict in files_dicts:
                    if file_dict["type"] == "file":
                        file_path = f"{bucket_id}/{file_dict['name']}"
                        await self._rm_file(file_path)
                    elif file_dict["type"] == "directory" and recursive:
                        subdir = f"{bucket_id}/{file_dict['name']}"
                        await self._rm(subdir, recursive=True)

            return

        # It's a file
        await self._rm_file(path)

    rm = sync_wrapper(_rm)

    async def _mkdir(self, path: str, **kwargs: Any) -> None:
        """Create a directory.

        This is a no-op for Appwrite since directories are virtual.
        """
        # Directories are virtual in Appwrite, so this is a no-op
        return

    mkdir = sync_wrapper(_mkdir)

    async def _makedirs(self, path: str, exist_ok: bool = False, **kwargs: Any) -> None:
        """Create a directory and parent directories.

        This is a no-op for Appwrite since directories are virtual.
        """
        return

    makedirs = sync_wrapper(_makedirs)

    async def _isdir(self, path: str, **kwargs: Any) -> bool:  # noqa: PLR0911
        """Check if path is a directory.

        Args:
            path: Path to check
            **kwargs: Additional arguments

        Returns:
            True if path is a directory, False otherwise
        """
        path = self._strip_protocol(path)  # pyright: ignore

        if not path or path == "/":
            return True

        bucket_id, file_path = self._split_path(path)

        if not file_path:
            # Bucket is a directory
            try:
                self.storage.get_bucket(bucket_id)
            except Exception:  # noqa: BLE001
                return False
            else:
                return True

        # Check if it's a virtual directory by listing files
        prefix = file_path if file_path.endswith("/") else file_path + "/"

        try:
            response = self.storage.list_files(bucket_id=bucket_id)
            files = response.get("files", [])

            # Check if any file starts with the prefix
            for file in files:
                if file["name"].startswith(prefix):
                    return True

            # Special case: empty directories
            if file_path.endswith("/"):
                # Check if an exact match exists
                for file in files:
                    if file["name"] == file_path.rstrip("/"):
                        return True
        except Exception:  # noqa: BLE001
            return False
        else:
            return False

    isdir = sync_wrapper(_isdir)

    async def _isfile(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a file.

        Args:
            path: Path to check
            **kwargs: Additional arguments

        Returns:
            True if path is a file, False otherwise
        """
        path = self._strip_protocol(path)  # pyright: ignore

        if not path or path == "/":
            return False

        bucket_id, file_path = self._split_path(path)

        if not file_path:
            # Bucket is not a file
            return False

        # Check if file exists
        file_id = self._get_file_id_by_name(bucket_id, file_path)
        return bool(file_id)

    isfile = sync_wrapper(_isfile)

    async def _exists(self, path: str, **kwargs: Any) -> bool:
        """Check if path exists.

        Args:
            path: Path to check
            **kwargs: Additional arguments

        Returns:
            True if path exists, False otherwise
        """
        path = self._strip_protocol(path)  # pyright: ignore

        if not path or path == "/":
            return True

        return await self._isfile(path) or await self._isdir(path)

    exists = sync_wrapper(_exists)  # pyright: ignore

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: int | None = None,
        **kwargs: Any,
    ) -> AppwriteBufferedFile:
        """Open a file.

        Args:
            path: Path to the file
            mode: File mode (rb, wb)
            block_size: Ignored
            **kwargs: Additional arguments

        Returns:
            File-like object

        Raises:
            ValueError: If mode is not supported
        """
        if mode not in ("rb", "wb", "ab"):
            msg = f"Unsupported mode: {mode}"
            raise ValueError(msg)

        return AppwriteBufferedFile(fs=self, path=path, mode=mode, **kwargs)

    def ukey(self, path: str) -> str:
        """Create a unique identifier for this path.

        Args:
            path: The path

        Returns:
            Unique identifier string
        """
        return tokenize(path, self.protocol, self.storage)


class AppwriteBufferedFile(io.BytesIO):
    """File-like object for working with Appwrite files."""

    def __init__(
        self,
        fs: AppwriteFileSystem,
        path: str,
        mode: str = "rb",
        **kwargs: Any,
    ):
        """Initialize the file-like object.

        Args:
            fs: AppwriteFileSystem instance
            path: Path to the file
            mode: File mode (rb, wb, ab)
            **kwargs: Additional arguments for write operations
        """
        super().__init__()
        self.fs = fs
        self.path = path
        self._mode = mode
        self.kwargs = kwargs
        self._closed = False

        # Load existing content for read mode
        if "r" in mode:
            try:
                content = fs.cat_file(path)
                assert isinstance(content, bytes)
                super().__init__(content)
            except FileNotFoundError:
                if "w" not in mode and "a" not in mode:
                    raise

    def close(self) -> None:
        """Close the file and write to Appwrite if in write mode."""
        if self._closed:
            return

        if "w" in self._mode or "a" in self._mode:
            # Get buffer contents
            content = self.getvalue()

            if content:
                # Write to Appwrite
                self.fs.pipe_file(self.path, content, **self.kwargs)

        super().close()
        self._closed = True

    def readable(self) -> bool:
        """Check if file is readable."""
        return "r" in self._mode

    def writable(self) -> bool:
        """Check if file is writable."""
        return "w" in self._mode or "a" in self._mode

    def seekable(self) -> bool:
        """Check if file is seekable."""
        return True


if __name__ == "__main__":
    import os

    print(f"APPWRITE_ENDPOINT: {os.environ.get('APPWRITE_ENDPOINT')}")
    print(f"APPWRITE_PROJECT: {os.environ.get('APPWRITE_PROJECT')}")
    print(f"APPWRITE_API_KEY: {'Set' if os.environ.get('APPWRITE_API_KEY') else 'Not set'}")
    print(f"APPWRITE_BUCKET_ID: {os.environ.get('APPWRITE_BUCKET_ID')}")

    fs = AppwriteFileSystem(
        endpoint=os.environ.get("APPWRITE_ENDPOINT"),
        project=os.environ.get("APPWRITE_PROJECT"),
        key=os.environ.get("APPWRITE_API_KEY"),
    )

    bucket_id = "680063fa003d7354e929"
    print("\nTrying to list files in bucket..")
    bucket_path = f"{bucket_id}/"
    print(f"Using path: '{bucket_path}'")

    bucket, file_path = fs._split_path(bucket_path)
    print(f"_split_path result: bucket='{bucket}', file_path='{file_path}'")

    # Now try listing
    result = fs.ls(bucket_path)
    print(f"Success! Found {len(result)} files in bucket")
    if result:
        print(f"Files: {result}")
