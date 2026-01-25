from __future__ import annotations

from fsspec.implementations.memory import MemoryFileSystem
import pytest
from upath import UPath

import upathtools
from upathtools.filesystems import AsyncLocalFileSystem, UnionFileSystem, UnionPath


upathtools.register_all_filesystems()


@pytest.fixture
def union_fs() -> UnionFileSystem:
    """Create a UnionFileSystem with memory and local backends."""
    mem_fs = MemoryFileSystem()
    local_fs = AsyncLocalFileSystem()

    # Create some test files in memory
    mem_fs.mkdirs("memdir", exist_ok=True)
    mem_fs.pipe("test.txt", b"memory content")
    mem_fs.pipe("memdir/nested.txt", b"nested content")

    return UnionFileSystem({
        "memory": mem_fs,
        "file": local_fs,
    })


@pytest.fixture
def union_fs_from_list() -> UnionFileSystem:
    """Create a UnionFileSystem from a list of filesystems."""
    mem_fs = MemoryFileSystem()
    local_fs = AsyncLocalFileSystem()

    # Create some test files in memory
    mem_fs.mkdirs("memdir", exist_ok=True)
    mem_fs.pipe("test.txt", b"memory content")
    mem_fs.pipe("memdir/nested.txt", b"nested content")

    return UnionFileSystem([mem_fs, local_fs])


async def test_root_listing_dict(union_fs: UnionFileSystem):
    """Test listing the root shows available mount points (dict keys)."""
    listing = await union_fs._ls("/")
    assert len(listing) == 2  # noqa: PLR2004
    mount_points = {item["name"] for item in listing}
    assert mount_points == {"memory", "file"}


async def test_root_listing_list(union_fs_from_list: UnionFileSystem):
    """Test listing the root shows available mount points (protocols)."""
    listing = await union_fs_from_list._ls("/")
    assert len(listing) == 2  # noqa: PLR2004
    mount_points = {item["name"] for item in listing}
    assert mount_points == {"memory", "file"}


async def test_mount_point_routing(union_fs: UnionFileSystem):
    """Test operations are routed to correct filesystem."""
    # Read from memory fs
    content = await union_fs._cat_file("memory/test.txt")
    assert content == b"memory content"

    # Write to memory fs
    await union_fs._pipe_file("memory/new.txt", b"new content")
    assert await union_fs._cat_file("memory/new.txt") == b"new content"


async def test_nested_paths(union_fs: UnionFileSystem):
    """Test operations on nested paths."""
    listing = await union_fs._ls("memory/memdir")
    assert len(listing) == 1
    assert listing[0]["name"] == "memory/memdir/nested.txt"

    content = await union_fs._cat_file("memory/memdir/nested.txt")
    assert content == b"nested content"


async def test_cross_filesystem_copy(union_fs: UnionFileSystem, tmp_path):
    """Test copying between different filesystems."""
    dest = f"file/{tmp_path}/copied.txt"

    # Copy from memory to local
    await union_fs._cp_file("memory/test.txt", dest)

    # Verify content
    with open(tmp_path / "copied.txt", "rb") as f:  # noqa: PTH123
        assert f.read() == b"memory content"


async def test_invalid_mount_point(union_fs: UnionFileSystem):
    """Test error handling for invalid mount points."""
    with pytest.raises(ValueError, match="Unknown mount point"):
        await union_fs._cat_file("invalid/test.txt")


async def test_directory_operations(union_fs: UnionFileSystem):
    """Test directory operations."""
    # Create directory
    await union_fs._makedirs("memory/newdir/subdir", exist_ok=True)

    # Write file in new directory
    await union_fs._pipe_file("memory/newdir/subdir/file.txt", b"test")

    # List directory
    listing = await union_fs._ls("memory/newdir", detail=False)
    assert "memory/newdir/subdir" in listing

    # Remove directory recursively
    await union_fs._rm("memory/newdir", recursive=True)

    # Verify it's gone
    with pytest.raises(FileNotFoundError):
        await union_fs._ls("memory/newdir")


async def test_file_operations(union_fs: UnionFileSystem):
    """Test basic file operations."""
    # Write
    await union_fs._pipe_file("memory/test2.txt", b"test content")

    # Read
    assert await union_fs._cat_file("memory/test2.txt") == b"test content"

    # Get info
    info = await union_fs._info("memory/test2.txt")
    assert info["type"] == "file"
    assert info["name"] == "memory/test2.txt"

    # Delete
    await union_fs._rm_file("memory/test2.txt")

    # Verify deletion
    with pytest.raises(FileNotFoundError):
        await union_fs._cat_file("memory/test2.txt")


async def test_exists_operations(union_fs: UnionFileSystem):
    """Test exists, isdir, and isfile operations."""
    # Test existing file
    assert await union_fs._exists("memory/test.txt")
    assert await union_fs._isfile("memory/test.txt")
    assert not await union_fs._isdir("memory/test.txt")

    # Test existing directory
    assert await union_fs._exists("memory/memdir")
    assert await union_fs._isdir("memory/memdir")
    assert not await union_fs._isfile("memory/memdir")

    # Test non-existing path
    assert not await union_fs._exists("memory/nonexistent.txt")
    assert not await union_fs._isfile("memory/nonexistent.txt")
    assert not await union_fs._isdir("memory/nonexistent.txt")

    # Test invalid mount point
    assert not await union_fs._exists("invalid/test.txt")
    assert not await union_fs._isfile("invalid/test.txt")
    assert not await union_fs._isdir("invalid/test.txt")

    # Test root paths
    assert await union_fs._exists("/")
    assert await union_fs._isdir("/")
    assert not await union_fs._isfile("/")


def test_root_path_representation():
    """Test root path string representation."""
    # Test regular UPath root
    path = UPath("union://")
    assert str(path) == "union://"
    assert path.path == "/"

    # Test our UnionPath root
    path = UnionPath("union://")
    assert str(path) == "union://"
    assert path.path == "/"

    # Test with extra slashes
    path = UPath("union:///")
    assert str(path) == "union://"
    assert path.path == "/"

    path = UnionPath("union:///")
    assert str(path) == "union://"
    assert path.path == "/"


async def test_filesystem_root_operations(union_fs: UnionFileSystem):
    """Test filesystem operations with root paths."""
    # Test listing with different root path formats
    root_listings = [
        await union_fs._ls("union://"),
        await union_fs._ls("union:///"),
        await union_fs._ls("/"),
        await union_fs._ls(""),
    ]

    # All should give same results
    assert all(
        len(listing) == 2  # noqa: PLR2004
        for listing in root_listings
    )  # memory and file
    assert all(
        {item["name"] for item in listing} == {"memory", "file"} for listing in root_listings
    )

    # Test info with different root path formats
    root_infos = [
        await union_fs._info("union://"),
        await union_fs._info("union:///"),
        await union_fs._info("/"),
        await union_fs._info(""),
    ]

    # All should give same results
    assert all(info["type"] == "directory" for info in root_infos)
    assert all(info["name"] == "/" for info in root_infos)


async def test_path_normalization(union_fs: UnionFileSystem):
    """Test various path formats are normalized correctly."""
    # Test different ways to specify the same path
    paths = [
        "memory/test.txt",
        "/memory/test.txt",
        "memory//test.txt",
        "/memory//test.txt",
    ]

    for path in paths:
        content = await union_fs._cat_file(path)
        assert content == b"memory content"


def test_url_parsing():
    """Test URL parsing for creating filesystems."""
    # Test basic format
    kwargs = UnionFileSystem._get_kwargs_from_urls("union://cache=memory://,data=/tmp")
    assert kwargs == {"filesystems": {"cache": "memory://", "data": "/tmp"}}

    # Test query parameter format
    kwargs = UnionFileSystem._get_kwargs_from_urls("union://?cache=memory://&data=/tmp")
    assert kwargs == {"filesystems": {"cache": "memory://", "data": "/tmp"}}

    # Test empty URL
    kwargs = UnionFileSystem._get_kwargs_from_urls("union://")
    assert kwargs == {}


async def test_empty_initialization():
    """Test creating empty UnionFileSystem and adding filesystems dynamically."""
    union_fs = UnionFileSystem()

    # Should start empty
    assert union_fs.list_mount_points() == []
    listing = await union_fs._ls("/")
    assert listing == []

    # Add a filesystem
    mem_fs = MemoryFileSystem()
    mem_fs.pipe("test.txt", b"test content")

    union_fs.register("memory", mem_fs)
    assert "memory" in union_fs.list_mount_points()

    # Should now show in listing
    listing = await union_fs._ls("/")
    assert len(listing) == 1
    assert listing[0]["name"] == "memory"

    # Should be able to access files
    content = await union_fs._cat_file("memory/test.txt")
    assert content == b"test content"


async def test_register_operations():
    """Test register, unregister, and list operations."""
    union_fs = UnionFileSystem()

    # Register filesystem
    mem_fs = MemoryFileSystem()
    union_fs.register("cache", mem_fs)
    assert union_fs.list_mount_points() == ["cache"]

    # Test replace=False (should raise)
    with pytest.raises(ValueError, match="Mount point already exists"):
        union_fs.register("cache", mem_fs, replace=False)

    # Test replace=True (should work)
    local_fs = AsyncLocalFileSystem()
    union_fs.register("cache", local_fs, replace=True)
    assert union_fs.list_mount_points() == ["cache"]

    # Register another
    union_fs.register("data", mem_fs)
    assert set(union_fs.list_mount_points()) == {"cache", "data"}

    # Unregister
    union_fs.unregister("cache")
    assert union_fs.list_mount_points() == ["data"]

    # Unregister non-existing should raise
    with pytest.raises(ValueError, match="Mount point not found"):
        union_fs.unregister("nonexistent")


if __name__ == "__main__":
    pytest.main(["-v", __file__])
