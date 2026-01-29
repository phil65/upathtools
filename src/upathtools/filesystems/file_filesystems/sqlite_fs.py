"""Filesystem implementation for browsing SQLite databases."""

from __future__ import annotations

import asyncio
import csv
import io
import tempfile
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Required, overload

import fsspec

from upathtools.async_helpers import sync_wrapper
from upathtools.filesystems.base import (
    BaseAsyncFileFileSystem,
    BaseUPath,
    FileInfo,
    GrepMatch,
    ProbeResult,
)


if TYPE_CHECKING:
    from collections.abc import Sequence

    from fsspec.asyn import AsyncFileSystem
    from sqlalchemy.ext.asyncio import AsyncEngine


class SqliteInfo(FileInfo, total=False):
    """Info dict for SQLite filesystem paths."""

    size: Required[int]
    table_type: str


class SqlitePath(BaseUPath[SqliteInfo]):
    """UPath implementation for browsing SQLite databases."""

    __slots__ = ()

    def iterdir(self):
        if not self.is_dir():
            raise NotADirectoryError(str(self))
        yield from super().iterdir()


class SqliteFileSystem(BaseAsyncFileFileSystem[SqlitePath, SqliteInfo]):
    """Filesystem for browsing SQLite databases."""

    protocol = "sqlite"
    upath_cls = SqlitePath
    supported_extensions: ClassVar[frozenset[str]] = frozenset({"db", "sqlite", "sqlite3"})
    priority: ClassVar[int] = 50

    # SQLite magic bytes: "SQLite format 3\x00"
    SQLITE_MAGIC = b"SQLite format 3\x00"

    @classmethod
    def probe_content(cls, content: bytes, extension: str = "") -> ProbeResult:
        """Probe content to check if it's a SQLite database.

        Checks for SQLite magic bytes at the start of the file.
        """
        if len(content) >= 16 and content[:16] == cls.SQLITE_MAGIC:  # noqa: PLR2004
            return ProbeResult.SUPPORTED
        # Extension matches but no magic bytes - might be empty or corrupted
        if cls.supports_extension(extension):
            return ProbeResult.MAYBE
        return ProbeResult.UNSUPPORTED

    @classmethod
    def get_probe_size(cls) -> int | None:
        """Only need first 16 bytes to check SQLite magic."""
        return 16

    def __init__(
        self,
        db_path: str = "",
        target_protocol: str | None = None,
        target_options: dict[str, Any] | None = None,
        parent_fs: AsyncFileSystem | None = None,
        parent_path: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the filesystem.

        Args:
            db_path: Path to SQLite database file (local path or temp file)
            target_protocol: Protocol for source database file
            target_options: Options for target protocol
            parent_fs: Parent filesystem for read-write access (optional)
            parent_path: Path within parent filesystem (optional)
            **kwargs: Additional filesystem options
        """
        super().__init__(**kwargs)
        self.db_path = db_path
        self.target_protocol = target_protocol
        self.target_options = target_options or {}
        self._parent_fs = parent_fs
        self._parent_path = parent_path
        self._engine: AsyncEngine | None = None
        self._temp_file: str | None = None
        self._is_temp_copy = False

    @classmethod
    def from_file(
        cls,
        path: str,
        target_protocol: str | None = None,
        target_options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> SqliteFileSystem:
        """Create filesystem instance from a SQLite database file path."""
        return cls(
            db_path=path, target_protocol=target_protocol, target_options=target_options, **kwargs
        )

    @classmethod
    async def from_filesystem_async(
        cls,
        path: str,
        fs: AsyncFileSystem,
        **kwargs: Any,
    ) -> SqliteFileSystem:
        """Create filesystem instance with parent filesystem access.

        For local files, uses the file directly. For remote files,
        creates a temp copy but retains parent fs reference for potential
        write-back operations.
        """
        from fsspec.implementations.local import LocalFileSystem

        # Check if parent fs is local - if so, use file directly
        underlying = getattr(fs, "fs", None)
        if isinstance(fs, LocalFileSystem) or isinstance(underlying, LocalFileSystem):
            return cls(db_path=path, parent_fs=fs, parent_path=path, **kwargs)

        # Remote file - download to temp, keep parent reference for write-back
        content = await fs._cat_file(path)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as tmp:
            if isinstance(content, str):
                content = content.encode()
            tmp.write(content)
            tmp_name = tmp.name

        instance = cls(db_path=tmp_name, parent_fs=fs, parent_path=path, **kwargs)
        instance._temp_file = tmp_name
        instance._is_temp_copy = True
        return instance

    @classmethod
    def from_content(
        cls,
        content: bytes,
        **kwargs: Any,
    ) -> SqliteFileSystem:
        """Create filesystem instance from raw SQLite database content.

        Note: This creates a read-only temp copy. Use from_filesystem_async
        for read-write access to remote databases.
        """
        with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as tmp:
            tmp.write(content)
            tmp_name = tmp.name
        instance = cls(db_path=tmp_name, **kwargs)
        instance._temp_file = tmp_name
        instance._is_temp_copy = True
        return instance

    async def sync_to_parent(self) -> None:
        """Write temp database back to parent filesystem.

        Only applicable when created via from_filesystem_async with a remote fs.
        """
        import asyncio
        from pathlib import Path

        if not self._is_temp_copy or self._parent_fs is None or self._parent_path is None:
            return

        # Use asyncio executor to avoid blocking
        content = await asyncio.get_event_loop().run_in_executor(
            None, Path(self.db_path).read_bytes
        )

        try:
            await self._parent_fs._pipe_file(self._parent_path, content)
        except AttributeError:
            self._parent_fs.pipe_file(self._parent_path, content)

    @staticmethod
    def _get_kwargs_from_urls(path: str) -> dict[str, Any]:
        path = path.removeprefix("sqlite://")
        return {"db_path": path}

    async def _get_engine(self) -> AsyncEngine:
        """Get or create async SQLAlchemy engine."""
        from sqlalchemy.ext.asyncio import create_async_engine

        if self._engine is not None:
            return self._engine

        if self.target_protocol:
            # Download remote DB to temp file
            with (
                fsspec.open(
                    self.db_path,
                    protocol=self.target_protocol,
                    **self.target_options,
                ) as f,
                tempfile.NamedTemporaryFile(delete=False) as tmp,
            ):
                tmp.write(f.read())  # type: ignore[reportArgumentType]
                self._temp_file = tmp.name
                db_url = f"sqlite+aiosqlite:///{tmp.name}"
        else:
            db_url = f"sqlite+aiosqlite:///{self.db_path}"

        self._engine = create_async_engine(db_url)
        return self._engine

    @overload
    async def _ls(
        self,
        path: str,
        detail: Literal[True] = ...,
        **kwargs: Any,
    ) -> list[SqliteInfo]: ...

    @overload
    async def _ls(self, path: str, detail: Literal[False], **kwargs: Any) -> list[str]: ...

    async def _ls(
        self,
        path: str,
        detail: bool = True,
        **kwargs: Any,
    ) -> Sequence[str | SqliteInfo]:
        """List database tables and views."""
        from sqlalchemy import text

        engine = await self._get_engine()
        async with engine.begin() as conn:
            # Get table names
            result = await conn.execute(
                text("""
                SELECT name, type FROM sqlite_master
                WHERE type IN ('table', 'view')
                ORDER BY name
                """)
            )

            items = []
            for row in result:  # type: ignore[reportAttributeAccessIssue]
                if detail:
                    # Could add COUNT(*) query if needed
                    item = SqliteInfo(name=row.name, type="file", size=0, table_type=row.type)  # type: ignore[reportAttributeAccessIssue]
                    items.append(item)
                else:
                    items.append(row.name)  # type: ignore[reportAttributeAccessIssue]

        return items

    ls = sync_wrapper(_ls)  # pyright: ignore[reportAssignmentType]

    async def _cat_file(
        self, path: str, start: int | None = None, end: int | None = None, **kwargs: Any
    ) -> bytes:
        """Get table data as CSV."""
        from sqlalchemy import text

        engine = await self._get_engine()
        path = self._strip_protocol(path).strip("/")  # type: ignore[reportAttributeAccessIssue]

        if not path:
            msg = "Cannot cat root directory"
            raise IsADirectoryError(msg)

        # Handle special files
        if path == ".schema":
            return await self._get_schema()
        if path.endswith(".schema"):
            table_name = path.removesuffix(".schema")
            return await self._get_table_schema(table_name)

        # Regular table data
        async with engine.begin() as conn:
            result = await conn.execute(text(f"SELECT * FROM `{path}`"))
            rows = result.fetchall()
            columns = result.keys()

            # Convert to CSV
            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow(columns)
            writer.writerows(rows)

            content = output.getvalue().encode()

            # Handle byte range if specified
            if start is not None or end is not None:
                start = start or 0
                end = end or len(content)
                content = content[start:end]

            return content

    cat_file = sync_wrapper(_cat_file)  # pyright: ignore[reportAssignmentType]

    async def _get_schema(self) -> bytes:
        """Get full database schema."""
        from sqlalchemy import text

        engine = await self._get_engine()

        async with engine.begin() as conn:
            result = await conn.execute(
                text("""
                    SELECT sql FROM sqlite_master
                    WHERE sql IS NOT NULL
                    ORDER BY type, name
                """)
            )

            schema_lines = [
                row.sql + ";"  # type: ignore[reportAttributeAccessIssue]
                for row in result  # type: ignore[reportAttributeAccessIssue]
                if row.sql  # type: ignore[reportAttributeAccessIssue]
            ]

            return "\n".join(schema_lines).encode()

    async def _get_table_schema(self, table_name: str) -> bytes:
        """Get schema for specific table."""
        from sqlalchemy import text

        engine = await self._get_engine()

        async with engine.begin() as conn:
            result = await conn.execute(
                text("SELECT sql FROM sqlite_master WHERE name = :name AND sql IS NOT NULL"),
                {"name": table_name},
            )
            row = result.fetchone()

            if row and row.sql:  # type: ignore[reportAttributeAccessIssue]
                return (row.sql + ";").encode()  # type: ignore[reportAttributeAccessIssue]

            msg = f"Table {table_name} not found"
            raise FileNotFoundError(msg)

    async def _info(self, path: str, **kwargs: Any) -> SqliteInfo:
        """Get info about database objects."""
        from sqlalchemy import text

        engine = await self._get_engine()
        path = self._strip_protocol(path).strip("/")  # type: ignore[reportAttributeAccessIssue]

        if not path or path == "/":
            # Root directory info
            return SqliteInfo(
                name="root",
                type="directory",
                size=0,
            )

        # Handle special files
        if path == ".schema":
            schema_content = await self._get_schema()
            return SqliteInfo(
                name=".schema",
                type="file",
                size=len(schema_content),
            )
        if path.endswith(".schema"):
            table_name = path.removesuffix(".schema")
            schema_content = await self._get_table_schema(table_name)
            return SqliteInfo(
                name=path,
                type="file",
                size=len(schema_content),
            )

        # Regular table info
        async with engine.begin() as conn:
            # Check if table exists
            result = await conn.execute(
                text("SELECT type FROM sqlite_master WHERE name = :name"),
                {"name": path},
            )
            row = result.fetchone()

            if not row:
                msg = f"Table {path} not found"
                raise FileNotFoundError(msg)

            # Get row count
            count_result = await conn.execute(text(f"SELECT COUNT(*) FROM `{path}`"))
            count = count_result.scalar()

            return SqliteInfo(
                name=path,
                type="file",
                size=count or 0,
                table_type=row.type,  # type: ignore[reportAttributeAccessIssue]
            )

    info = sync_wrapper(_info)

    async def _exists(self, path: str, **kwargs: Any) -> bool:
        """Check if table or view exists."""
        try:
            await self._info(path)
        except FileNotFoundError:
            return False
        else:
            return True

    exists = sync_wrapper(_exists)  # pyright: ignore[reportAssignmentType]

    async def _isdir(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a directory."""
        path = self._strip_protocol(path).strip("/")  # type: ignore[reportAttributeAccessIssue]
        return not path or path == "/"

    isdir = sync_wrapper(_isdir)

    async def _isfile(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a file."""
        return await self._exists(path) and not await self._isdir(path)

    isfile = sync_wrapper(_isfile)

    @overload
    async def _glob(
        self,
        path: str,
        maxdepth: int | None = None,
        *,
        detail: Literal[False] = False,
        **kwargs: Any,
    ) -> list[str]: ...

    @overload
    async def _glob(
        self,
        path: str,
        maxdepth: int | None = None,
        *,
        detail: Literal[True],
        **kwargs: Any,
    ) -> dict[str, SqliteInfo]: ...

    async def _glob(
        self,
        path: str,
        maxdepth: int | None = None,
        *,
        detail: bool = False,
        **kwargs: Any,
    ) -> list[str] | dict[str, SqliteInfo]:
        """Glob for tables using SQL LIKE pattern matching.

        Converts glob patterns to SQL LIKE patterns for efficient matching.
        """
        import fnmatch

        from sqlalchemy import text

        path = self._strip_protocol(path).strip("/")  # type: ignore[reportAttributeAccessIssue]

        # Check for glob magic characters
        glob_chars = {"*", "?", "["}
        if not any(c in path for c in glob_chars):
            if await self._exists(path):
                if detail:
                    info = await self._info(path)
                    return {path: info}
                return [path]
            return {} if detail else []

        engine = await self._get_engine()

        async with engine.begin() as conn:
            result = await conn.execute(
                text("""
                SELECT name FROM sqlite_master
                WHERE type IN ('table', 'view')
                ORDER BY name
                """)
            )
            all_tables = [row.name for row in result]  # type: ignore[reportAttributeAccessIssue]

        # Use fnmatch for glob pattern matching
        matches = [t for t in all_tables if fnmatch.fnmatch(t, path)]

        if not detail:
            return matches

        # Return barebone info dicts
        return {t: SqliteInfo(name=t, type="file", size=0) for t in matches}

    glob = sync_wrapper(_glob)  # pyright: ignore[reportAssignmentType]

    async def _grep(
        self,
        path: str,
        pattern: str,
        *,
        max_count: int | None = None,
        case_sensitive: bool | None = None,
        hidden: bool = False,
        no_ignore: bool = False,
        globs: list[str] | None = None,
        context_before: int | None = None,
        context_after: int | None = None,
        multiline: bool = False,
    ) -> list[GrepMatch]:
        """Search table contents using SQL LIKE.

        Searches all text columns in the specified table for the pattern.
        Much faster than reading CSV and searching in Python.
        """
        from sqlalchemy import text

        path = self._strip_protocol(path).strip("/")  # type: ignore[reportAttributeAccessIssue]

        if not path or path == "/":
            msg = "Cannot grep root directory"
            raise IsADirectoryError(msg)

        engine = await self._get_engine()

        async with engine.begin() as conn:
            # Get column info for the table
            col_result = await conn.execute(text(f"PRAGMA table_info(`{path}`)"))
            columns = [row[1] for row in col_result]  # column name is at index 1

            if not columns:
                msg = f"Table {path} not found"
                raise FileNotFoundError(msg)

            # Build WHERE clause to search all text columns
            like_pattern = f"%{pattern}%"
            if case_sensitive:
                conditions = " OR ".join(f"CAST(`{col}` AS TEXT) LIKE :pattern" for col in columns)
            else:
                conditions = " OR ".join(
                    f"LOWER(CAST(`{col}` AS TEXT)) LIKE LOWER(:pattern)" for col in columns
                )

            query = f"SELECT rowid, * FROM `{path}` WHERE {conditions}"
            result = await conn.execute(text(query), {"pattern": like_pattern})

            matches: list[GrepMatch] = []
            check_pattern = pattern if case_sensitive else pattern.lower()
            for row in result:
                row_dict = dict(row._mapping)
                rowid = row_dict.pop("rowid", None)
                # Find which columns matched
                for val in row_dict.values():
                    val_str = str(val) if val is not None else None
                    if val_str is not None and check_pattern in (
                        val_str if case_sensitive else val_str.lower()
                    ):
                        matches.append(
                            GrepMatch(
                                path=path,
                                line_number=rowid or 0,
                                text=val_str,
                            )
                        )
                        if max_count is not None and len(matches) >= max_count:
                            return matches

            return matches

    grep = sync_wrapper(_grep)

    def _open(self, path: str, mode: str = "rb", **kwargs: Any) -> Any:
        """Provide file-like access to table data."""
        if "w" in mode or "a" in mode:
            msg = "Write mode not supported"
            raise NotImplementedError(msg)
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        content = loop.run_until_complete(self._cat_file(path))
        return io.BytesIO(content)

    async def _close(self) -> None:
        """Close the database engine and clean up resources."""
        if self._engine is not None:
            await self._engine.dispose()
            self._engine = None

    close = sync_wrapper(_close)

    def __del__(self) -> None:
        """Clean up resources."""
        if self._temp_file:
            import contextlib
            from pathlib import Path

            with contextlib.suppress(OSError):
                Path(self._temp_file).unlink()


if __name__ == "__main__":
    import sqlite3
    import tempfile

    async def demo():
        # Create a demo database
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            demo_db = f.name

        # Create some test data
        conn = sqlite3.connect(demo_db)
        conn.execute("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT UNIQUE
            )
        """)
        conn.execute("""
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                amount REAL,
                FOREIGN KEY (user_id) REFERENCES users (id)
            )
        """)
        conn.executemany(
            "INSERT INTO users (name, email) VALUES (?, ?)",
            [("Alice", "alice@example.com"), ("Bob", "bob@example.com")],
        )
        conn.executemany(
            "INSERT INTO orders (user_id, amount) VALUES (?, ?)",
            [(1, 100.0), (1, 200.0), (2, 50.0)],
        )
        conn.commit()
        conn.close()

        # Create filesystem
        fs = SqliteFileSystem(demo_db)

        # List tables
        print("\nTables:")
        tables = await fs._ls("/", detail=True)
        for table in tables:
            print(f"- {table['name']} ({table.get('table_type')})")

        # Read table data
        print("\nUsers table:")
        users_data = await fs._cat_file("users")
        print(users_data.decode())

        # Get schema
        print("\nDatabase schema:")
        schema = await fs._cat_file(".schema")
        print(schema.decode())

        # Clean up
        from pathlib import Path

        Path(demo_db).unlink()

    asyncio.run(demo())
