"""Tests for SQLite filesystem implementation."""

from __future__ import annotations

from pathlib import Path
import sqlite3
import tempfile

import pytest
import sqlalchemy.exc

from upathtools.filesystems import SqliteFileSystem


# Skip all tests in this file on Windows due to SQLite file locking issues
pytest.importorskip("sys")
import sys


if sys.platform == "win32":
    pytest.skip("SQLite file locking issues on Windows", allow_module_level=True)


@pytest.fixture
def sample_db():
    """Create a sample SQLite database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    conn = sqlite3.connect(db_path)

    # Create tables
    conn.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            amount REAL,
            status TEXT DEFAULT 'pending',
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
    """)

    conn.execute("""
        CREATE VIEW user_orders AS
        SELECT u.name, u.email, o.amount, o.status
        FROM users u
        JOIN orders o ON u.id = o.user_id
    """)

    # Insert sample data
    conn.executemany(
        "INSERT INTO users (name, email) VALUES (?, ?)",
        [
            ("Alice Johnson", "alice@example.com"),
            ("Bob Smith", "bob@example.com"),
            ("Carol Davis", "carol@example.com"),
        ],
    )

    conn.executemany(
        "INSERT INTO orders (user_id, amount, status) VALUES (?, ?, ?)",
        [
            (1, 100.50, "completed"),
            (1, 75.25, "pending"),
            (2, 200.00, "completed"),
            (3, 50.75, "pending"),
        ],
    )

    conn.commit()
    conn.close()

    yield db_path

    # Cleanup
    Path(db_path).unlink()


@pytest.fixture
def sqlite_fs(sample_db):
    """Create SqliteFileSystem instance with sample database."""
    return SqliteFileSystem(db_path=sample_db)


class TestSqliteFileSystem:
    """Test cases for SqliteFileSystem."""

    @pytest.mark.asyncio
    async def test_ls_tables_and_views(self, sqlite_fs):
        """Test listing tables and views."""
        items = await sqlite_fs._ls("/", detail=True)

        assert len(items) == 3  # noqa: PLR2004

        # Check tables
        table_names = {item["name"] for item in items}
        assert "users" in table_names
        assert "orders" in table_names
        assert "user_orders" in table_names

        # Check metadata
        users_item = next(item for item in items if item["name"] == "users")
        assert users_item["type"] == "file"
        assert users_item["table_type"] == "table"

        view_item = next(item for item in items if item["name"] == "user_orders")
        assert view_item["type"] == "file"
        assert view_item["table_type"] == "view"

    @pytest.mark.asyncio
    async def test_ls_simple(self, sqlite_fs):
        """Test simple listing without details."""
        items = await sqlite_fs._ls("/", detail=False)

        assert "users" in items
        assert "orders" in items
        assert "user_orders" in items

    @pytest.mark.asyncio
    async def test_cat_table_data(self, sqlite_fs):
        """Test reading table data as CSV."""
        data = await sqlite_fs._cat_file("users")
        content = data.decode()

        lines = content.strip().split("\n")
        assert len(lines) == 4  # Header + 3 data rows  # noqa: PLR2004

        # Check header
        header = lines[0]
        assert "id,name,email,created_at" in header

        # Check data
        assert "Alice Johnson,alice@example.com" in content
        assert "Bob Smith,bob@example.com" in content
        assert "Carol Davis,carol@example.com" in content

    @pytest.mark.asyncio
    async def test_cat_view_data(self, sqlite_fs):
        """Test reading view data as CSV."""
        data = await sqlite_fs._cat_file("user_orders")
        content = data.decode()

        lines = content.strip().split("\n")
        assert len(lines) == 5  # Header + 4 data rows  # noqa: PLR2004

        # Check header
        header = lines[0]
        assert "name,email,amount,status" in header

        # Check data contains joined information
        assert "Alice Johnson" in content
        assert "100.5" in content
        assert "completed" in content

    @pytest.mark.asyncio
    async def test_cat_full_schema(self, sqlite_fs):
        """Test reading full database schema."""
        data = await sqlite_fs._cat_file(".schema")
        content = data.decode()

        # Should contain CREATE statements
        assert "CREATE TABLE orders" in content
        assert "CREATE TABLE users" in content
        assert "CREATE VIEW user_orders" in content

        # Should be properly terminated
        assert content.count(";") >= 3  # noqa: PLR2004

    @pytest.mark.asyncio
    async def test_cat_table_schema(self, sqlite_fs):
        """Test reading specific table schema."""
        data = await sqlite_fs._cat_file("users.schema")
        content = data.decode()

        assert "CREATE TABLE users" in content
        assert "id INTEGER PRIMARY KEY" in content
        assert "name TEXT NOT NULL" in content
        assert "email TEXT UNIQUE" in content

    @pytest.mark.asyncio
    async def test_cat_nonexistent_table(self, sqlite_fs):
        """Test reading from nonexistent table."""
        with pytest.raises(sqlalchemy.exc.OperationalError):
            await sqlite_fs._cat_file("nonexistent")

    @pytest.mark.asyncio
    async def test_cat_root_directory(self, sqlite_fs):
        """Test trying to cat root directory."""
        with pytest.raises(IsADirectoryError):
            await sqlite_fs._cat_file("")

    @pytest.mark.asyncio
    async def test_info_table(self, sqlite_fs):
        """Test getting info about a table."""
        info = await sqlite_fs._info("users")

        assert info["name"] == "users"
        assert info["type"] == "file"
        assert info["size"] == 3  # Row count  # noqa: PLR2004
        assert info["table_type"] == "table"

    @pytest.mark.asyncio
    async def test_info_view(self, sqlite_fs):
        """Test getting info about a view."""
        info = await sqlite_fs._info("user_orders")

        assert info["name"] == "user_orders"
        assert info["type"] == "file"
        assert info["size"] == 4  # Row count from joined tables  # noqa: PLR2004
        assert info["table_type"] == "view"

    @pytest.mark.asyncio
    async def test_info_schema_file(self, sqlite_fs):
        """Test getting info about schema files."""
        info = await sqlite_fs._info(".schema")

        assert info["name"] == ".schema"
        assert info["type"] == "file"
        assert info["size"] > 0

        # Test table-specific schema
        info = await sqlite_fs._info("users.schema")
        assert info["name"] == "users.schema"
        assert info["type"] == "file"
        assert info["size"] > 0

    @pytest.mark.asyncio
    async def test_info_root(self, sqlite_fs):
        """Test getting info about root directory."""
        info = await sqlite_fs._info("")

        assert info["name"] == "root"
        assert info["type"] == "directory"
        assert info["size"] == 0

    @pytest.mark.asyncio
    async def test_info_nonexistent_table(self, sqlite_fs):
        """Test getting info about nonexistent table."""
        with pytest.raises(FileNotFoundError):
            await sqlite_fs._info("nonexistent")

    @pytest.mark.asyncio
    async def test_exists(self, sqlite_fs):
        """Test checking if tables exist."""
        assert await sqlite_fs._exists("users")
        assert await sqlite_fs._exists("orders")
        assert await sqlite_fs._exists("user_orders")
        assert not await sqlite_fs._exists("nonexistent")

    @pytest.mark.asyncio
    async def test_isdir(self, sqlite_fs):
        """Test directory detection."""
        assert await sqlite_fs._isdir("")
        assert await sqlite_fs._isdir("/")
        assert not await sqlite_fs._isdir("users")
        assert not await sqlite_fs._isdir("orders")

    @pytest.mark.asyncio
    async def test_isfile(self, sqlite_fs):
        """Test file detection."""
        assert await sqlite_fs._isfile("users")
        assert await sqlite_fs._isfile("orders")
        assert await sqlite_fs._isfile("user_orders")
        assert not await sqlite_fs._isfile("")
        assert not await sqlite_fs._isfile("/")
        assert not await sqlite_fs._isfile("nonexistent")

    def test_open_read_mode(self, sqlite_fs):
        """Test opening files in read mode."""
        with sqlite_fs._open("users", "rb") as f:
            content = f.read().decode()
            assert "Alice Johnson" in content
            assert "id,name,email" in content

    def test_open_write_mode_not_supported(self, sqlite_fs):
        """Test that write mode is not supported."""
        with pytest.raises(NotImplementedError):
            sqlite_fs._open("users", "wb")

    def test_get_kwargs_from_urls(self):
        """Test URL parsing."""
        kwargs = SqliteFileSystem._get_kwargs_from_urls("sqlite://path/to/database.db")
        assert kwargs == {"db_path": "path/to/database.db"}

        kwargs = SqliteFileSystem._get_kwargs_from_urls("sqlite:///absolute/path.db")
        assert kwargs == {"db_path": "/absolute/path.db"}


class TestSqliteFileSystemChaining:
    """Test filesystem chaining functionality."""

    @pytest.fixture
    def http_server_db(self, sample_db):
        """Create a simple HTTP server serving the database file."""
        import http.server
        from pathlib import Path
        import socketserver
        import threading

        # Copy database to a temporary directory for serving
        serve_dir = Path(tempfile.mkdtemp())
        db_copy = serve_dir / "test.db"

        # Copy the database file
        import shutil

        shutil.copy2(sample_db, db_copy)

        # Start HTTP server
        port = 0  # Let the OS choose a free port
        with socketserver.TCPServer(
            ("", port),
            lambda *args: http.server.SimpleHTTPRequestHandler(*args, directory=serve_dir),
        ) as httpd:
            port = httpd.server_address[1]
            server_thread = threading.Thread(target=httpd.serve_forever, daemon=True)
            server_thread.start()

            yield f"http://127.0.0.1:{port}/test.db"

            httpd.shutdown()
            shutil.rmtree(serve_dir)

    @pytest.mark.asyncio
    async def test_chained_filesystem_http(self, http_server_db):
        """Test accessing database via HTTP."""
        fs = SqliteFileSystem(db_path=http_server_db, target_protocol="http")

        # Test basic functionality
        items = await fs._ls("/", detail=False)
        assert "users" in items
        assert "orders" in items

        # Test reading data
        data = await fs._cat_file("users")
        content = data.decode()
        assert "Alice Johnson" in content


class TestSqliteFileSystemEmpty:
    """Test behavior with empty database."""

    @pytest.fixture
    def empty_db(self):
        """Create an empty SQLite database."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        # Just create the file, no tables
        conn = sqlite3.connect(db_path)
        conn.close()

        yield db_path
        Path(db_path).unlink()

    @pytest.mark.asyncio
    async def test_empty_database(self, empty_db):
        """Test behavior with empty database."""
        fs = SqliteFileSystem(db_path=empty_db)

        items = await fs._ls("/", detail=True)
        assert len(items) == 0

        items = await fs._ls("/", detail=False)
        assert len(items) == 0


class TestSqliteFileSystemNonexistent:
    """Test behavior with nonexistent database."""

    @pytest.mark.asyncio
    async def test_nonexistent_database(self):
        """Test behavior with nonexistent database file."""
        fs = SqliteFileSystem(db_path="/nonexistent/path/database.db")

        with pytest.raises(sqlalchemy.exc.OperationalError):
            await fs._ls("/")


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
