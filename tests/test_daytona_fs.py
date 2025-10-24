"""Tests for Daytona filesystem implementation."""

import contextlib

import pytest

from upathtools.filesystems.daytona_fs import DaytonaFS


pytestmark = pytest.mark.asyncio


@pytest.fixture(scope="session")
def daytona_api_available():
    """Check if Daytona API is available via environment."""
    try:
        import daytona  # noqa: F401
    except ImportError:
        pytest.skip("daytona package not available")
    else:
        return True


@pytest.fixture(scope="session")
async def shared_daytona_fs(daytona_api_available):
    """Create shared Daytona filesystem instance for all tests."""
    fs = DaytonaFS(timeout=600)
    await fs.set_session()
    yield fs
    await fs.close_session()


@pytest.mark.integration
async def test_daytona_session_management(daytona_api_available):
    """Test session creation and cleanup."""
    fs = DaytonaFS()
    assert not fs._session_started

    await fs.set_session()
    assert fs._session_started
    assert fs._sandbox is not None

    await fs.close_session()
    assert not fs._session_started


@pytest.mark.integration
async def test_daytona_file_crud_operations(shared_daytona_fs):
    """Test file create, read, update, delete operations."""
    test_file = "/tmp/test_file.txt"
    content = b"Hello, Daytona!"

    # Create and verify
    await shared_daytona_fs._pipe_file(test_file, content)
    assert await shared_daytona_fs._exists(test_file)
    assert await shared_daytona_fs._isfile(test_file)
    assert not await shared_daytona_fs._isdir(test_file)

    # Read and verify
    read_content = await shared_daytona_fs._cat_file(test_file)
    assert read_content == content

    # Update content
    new_content = b"Updated content"
    await shared_daytona_fs._pipe_file(test_file, new_content)
    updated_content = await shared_daytona_fs._cat_file(test_file)
    assert updated_content == new_content

    # File metadata
    size = await shared_daytona_fs._size(test_file)
    assert size == len(new_content)

    mtime = await shared_daytona_fs._modified(test_file)
    assert isinstance(mtime, float)

    # Delete
    await shared_daytona_fs._rm(test_file)
    assert not await shared_daytona_fs._exists(test_file)


@pytest.mark.integration
async def test_daytona_directory_operations(shared_daytona_fs):
    """Test directory create, list, delete operations."""
    test_dir = "/tmp/test_directory"

    await shared_daytona_fs._mkdir(test_dir)
    assert await shared_daytona_fs._exists(test_dir)
    assert await shared_daytona_fs._isdir(test_dir)

    # Create nested file and list
    test_file = f"{test_dir}/nested.txt"
    await shared_daytona_fs._pipe_file(test_file, b"nested content")

    items = await shared_daytona_fs._ls(test_dir, detail=True)
    file_items = [item for item in items if item["name"].endswith("nested.txt")]
    assert len(file_items) >= 1
    assert file_items[0]["type"] == "file"

    # Cleanup
    await shared_daytona_fs._rm(test_file)
    await shared_daytona_fs._rm(test_dir)


@pytest.mark.integration
async def test_daytona_partial_reads_and_nested_dirs(shared_daytona_fs):
    """Test partial file reads and nested directory creation."""
    # Test partial reads
    test_file = "/tmp/partial_test.txt"
    content = b"0123456789ABCDEF"
    await shared_daytona_fs._pipe_file(test_file, content)

    partial = await shared_daytona_fs._cat_file(test_file, start=0, end=5)
    assert partial == b"01234"

    partial = await shared_daytona_fs._cat_file(test_file, start=10)
    assert partial == b"ABCDEF"

    await shared_daytona_fs._rm(test_file)

    # Test nested directories
    nested_path = "/tmp/level1/level2/level3"
    await shared_daytona_fs._mkdir(nested_path, create_parents=True)

    deep_file = f"{nested_path}/deep.txt"
    await shared_daytona_fs._pipe_file(deep_file, b"deep content")
    assert await shared_daytona_fs._exists(deep_file)

    await shared_daytona_fs._rm(deep_file)
    await shared_daytona_fs._rm("/tmp/level1")


@pytest.mark.integration
async def test_daytona_error_conditions(shared_daytona_fs):
    """Test error handling for nonexistent files/dirs."""
    with pytest.raises(FileNotFoundError):
        await shared_daytona_fs._cat_file("/tmp/nonexistent.txt")

    with pytest.raises(FileNotFoundError):
        await shared_daytona_fs._size("/tmp/nonexistent.txt")

    with pytest.raises(FileNotFoundError):
        await shared_daytona_fs._rm("/tmp/nonexistent.txt")


@pytest.mark.integration
async def test_daytona_content_types(shared_daytona_fs):
    """Test binary and unicode content handling."""
    # Binary content
    binary_file = "/tmp/binary.bin"
    binary_content = bytes(range(256))
    await shared_daytona_fs._pipe_file(binary_file, binary_content)
    read_binary = await shared_daytona_fs._cat_file(binary_file)
    assert read_binary == binary_content
    await shared_daytona_fs._rm(binary_file)

    # Unicode content
    unicode_file = "/tmp/unicode.txt"
    unicode_text = "Hello 🌍! Тест! こんにちは!"
    unicode_content = unicode_text.encode("utf-8")
    await shared_daytona_fs._pipe_file(unicode_file, unicode_content)
    read_unicode = await shared_daytona_fs._cat_file(unicode_file)
    assert read_unicode.decode("utf-8") == unicode_text
    await shared_daytona_fs._rm(unicode_file)


@pytest.mark.integration
async def test_daytona_sync_interface(daytona_api_available):
    """Test synchronous wrapper methods."""
    fs = DaytonaFS(timeout=300)
    try:
        test_file = "/tmp/sync_test.txt"
        content = b"Sync content"

        fs.pipe_file(test_file, content)
        assert fs.exists(test_file)
        assert fs.cat_file(test_file) == content
        fs.rm(test_file)
        assert not fs.exists(test_file)
    finally:
        with contextlib.suppress(AttributeError):
            fs.close_session()


@pytest.mark.integration
async def test_daytona_existing_sandbox_connection(daytona_api_available):
    """Test connecting to existing sandbox."""
    fs1 = DaytonaFS(timeout=300)
    await fs1.set_session()
    sandbox_id = fs1._sandbox_id

    test_file = "/tmp/shared_file.txt"
    content = b"Shared content"
    await fs1._pipe_file(test_file, content)

    fs2 = DaytonaFS(sandbox_id=sandbox_id)
    await fs2.set_session()

    assert await fs2._exists(test_file)
    assert await fs2._cat_file(test_file) == content

    await fs2._rm(test_file)
    await fs2.close_session()
    await fs1.close_session()


@pytest.mark.integration
async def test_daytona_script_execution_workflow(shared_daytona_fs):
    """Test complete script execution workflow."""
    script_path = "/workspace/test_script.py"
    output_path = "/workspace/output.txt"

    script_content = f"""
with open("{output_path}", "w") as f:
    f.write("Script executed!\\nWorking dir: /workspace\\n")
print("Done")
""".encode()

    await shared_daytona_fs._pipe_file(script_path, script_content)

    # Execute via sandbox
    sandbox = await shared_daytona_fs._get_sandbox()
    sandbox.run_command(f"python {script_path}")

    assert await shared_daytona_fs._exists(output_path)
    output = await shared_daytona_fs._cat_file(output_path)
    assert b"Script executed!" in output

    # Cleanup
    await shared_daytona_fs._rm(script_path)
    await shared_daytona_fs._rm(output_path)


if __name__ == "__main__":
    pytest.main(["-v", __file__])
