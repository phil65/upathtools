# test_notionfs.py
from __future__ import annotations

import contextlib
import os

import pytest

from upathtools.filesystems.notion_fs import NotionFS


pytestmark = pytest.mark.integration


# Constants
TEST_PAGE_TITLE = "FSSpec Test Page"
TEST_CONTENT = "Hello from NotionFS Test!"
NESTED_PATH = "/Test Folder/Nested Page"
INVALID_PAGE_ID = "invalid_id_123"
NON_EXISTENT_PATH = "/This/Path/Does/Not/Exist"

# Environment variables
NOTION_TOKEN = os.environ.get("NOTION_API_KEY")
PARENT_PAGE_ID = os.environ.get("NOTION_PARENT_PAGE_ID")

if not NOTION_TOKEN or not PARENT_PAGE_ID:
    pytest.skip(
        "NOTION_API_KEY and NOTION_PARENT_PAGE_ID must be set", allow_module_level=True
    )


@pytest.fixture
def fs():
    """Create a NotionFS instance."""
    assert NOTION_TOKEN
    assert PARENT_PAGE_ID
    fs = NotionFS(token=NOTION_TOKEN, parent_page_id=PARENT_PAGE_ID)
    yield fs
    # Cleanup all test pages after each test
    for page in fs.ls("/"):
        page_name = page["name"] if isinstance(page, dict) else page
        if any(
            test_prefix in page_name
            for test_prefix in ["Test", "FSSpec", "Special", "Large"]
        ):
            with contextlib.suppress(Exception):
                fs.rm(f"/{page_name}")


@pytest.fixture
def test_page(fs: NotionFS):
    """Create a test page and clean it up after tests."""
    page_path = f"/{TEST_PAGE_TITLE}"
    with fs.open(page_path, "w") as f:
        f.write(TEST_CONTENT)
    yield TEST_PAGE_TITLE
    with contextlib.suppress(Exception):
        fs.rm(page_path)


def test_initialization():
    """Test NotionFS initialization."""
    fs = NotionFS(token=NOTION_TOKEN, parent_page_id=PARENT_PAGE_ID)  # type: ignore
    assert fs.protocol == "notion"
    assert fs.parent_page_id == PARENT_PAGE_ID


def test_read_write(fs: NotionFS, test_page: str):
    """Test basic read and write operations."""
    # Test writing
    with fs.open(f"/{test_page}", "w") as f:
        f.write(TEST_CONTENT)

    # Test reading
    with fs.open(f"/{test_page}", "r") as f:
        content = f.read()
        assert content == TEST_CONTENT


def test_binary_mode(fs: NotionFS, test_page: str):
    """Test binary mode operations."""
    binary_content = TEST_CONTENT.encode("utf-8")

    # Test binary write
    with fs.open(f"/{test_page}", "wb") as f:
        f.write(binary_content)  # pyright: ignore[reportArgumentType]

    # Test binary read
    with fs.open(f"/{test_page}", "rb") as f:
        content = f.read()
        assert content.decode("utf-8").strip() == TEST_CONTENT.strip()  # pyright: ignore[reportAttributeAccessIssue]


def test_invalid_mode(fs: NotionFS):
    """Test opening file with invalid mode."""
    with pytest.raises(ValueError, match="Only read/write modes supported"):
        fs.open("/some_page", "x")


def test_nested_directories(fs: NotionFS):
    """Test creating and accessing nested pages."""
    fs.makedirs(NESTED_PATH, exist_ok=True)
    assert fs.exists(NESTED_PATH)


def test_ls_operations(fs: NotionFS, test_page: str):
    """Test directory listing operations."""
    # Test basic listing
    listings = fs.ls("/")
    assert isinstance(listings, list)
    assert test_page in listings

    # Test detailed listing
    detailed = fs.ls("/", detail=True)
    assert isinstance(detailed, list)
    assert isinstance(detailed[0], dict)
    assert "name" in detailed[0]
    assert "size" in detailed[0]
    assert "type" in detailed[0]


def test_exists(fs: NotionFS, test_page: str):
    """Test path existence checking."""
    assert fs.exists(f"/{test_page}")
    assert not fs.exists(NON_EXISTENT_PATH)


def test_makedirs(fs: NotionFS):
    """Test directory creation with makedirs."""
    test_path = "/Test Dir 1/Test Dir 2"

    # Clean up if exists
    if fs.exists(test_path):
        fs.rm("/Test Dir 1")

    # Test normal creation
    fs.makedirs(test_path, exist_ok=True)
    assert fs.exists(test_path)

    # Test exist_ok=True
    fs.makedirs(test_path, exist_ok=True)

    # Test exist_ok=False
    with pytest.raises(OSError, match="Path already exists"):
        fs.makedirs(test_path, exist_ok=False)


def test_file_operations(fs: NotionFS, test_page: str):
    """Test file-like operations."""
    with fs.open(f"/{test_page}", "r") as f:
        # Test basic file operations
        assert f.readable()
        assert not f.writable()
        assert f.seekable()
        assert not f.closed

        # Test seek and tell
        f.seek(0)
        assert f.tell() == 0

        # Read partial content
        f.seek(6)
        partial = f.read(4)
        assert len(partial) == 4  # noqa: PLR2004


def test_context_manager(fs: NotionFS, test_page: str):
    """Test context manager functionality."""
    with fs.open(f"/{test_page}", "r") as f:
        content = f.read()
    assert f.closed
    assert content == TEST_CONTENT


def test_error_handling(fs: NotionFS):
    """Test error handling scenarios."""
    # Test reading non-existent page
    with pytest.raises(FileNotFoundError):
        fs.open(NON_EXISTENT_PATH, "r")

    # Test invalid token
    with pytest.raises(ValueError, match="Invalid Notion token") as exc_info:
        NotionFS(token="invalid_token", parent_page_id=INVALID_PAGE_ID)
    assert "Invalid Notion token" in str(exc_info.value)


def test_file_closure(fs: NotionFS, test_page: str):
    """Test file closure behavior."""
    f = fs.open(f"/{test_page}", "r")
    f.close()
    assert f.closed
    with pytest.raises(ValueError, match="operation on closed file"):
        f.read()


def test_large_content(fs: NotionFS):
    """Test handling of large content."""
    large_content = "Large content\n" * 10
    test_large_page = "/Large Test Page"

    with fs.open(test_large_page, "w") as f:
        f.write(large_content)

    with fs.open(test_large_page, "r") as f:
        content = f.read()
        assert content == large_content


def test_special_characters(fs: NotionFS):
    """Test handling of special characters in page names."""
    special_chars_title = "/Special !@#$%^&*() Page"

    with fs.open(special_chars_title, "w") as f:
        f.write(TEST_CONTENT)

    assert fs.exists(special_chars_title)
    with fs.open(special_chars_title, "r") as f:
        content = f.read()
        assert content == TEST_CONTENT


def test_delete_operations(fs: NotionFS):
    """Test page deletion operations."""
    # Create a test page
    test_path = "/Delete Test Page"
    with fs.open(test_path, "w") as f:
        f.write("Test content")

    assert fs.exists(test_path)

    # Test deletion
    fs.rm(test_path)
    assert not fs.exists(test_path)

    # Test deleting non-existent page
    with pytest.raises(FileNotFoundError):
        fs.rm("/NonExistentPage")


def test_nested_delete(fs: NotionFS):
    """Test deleting nested pages."""
    # Create nested structure
    nested_path = "/Delete Parent/Delete Child"
    fs.makedirs(nested_path, exist_ok=True)
    with fs.open(f"{nested_path}/test.txt", "w") as f:
        f.write("Test content")

    # Delete parent should remove all children
    fs.rm("/Delete Parent")
    assert not fs.exists("/Delete Parent")
    assert not fs.exists(nested_path)
