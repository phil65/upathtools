"""Tests for E2B filesystem implementation."""

import os

import pytest

from upathtools.filesystems.e2b_fs import E2BFS


pytestmark = pytest.mark.asyncio


@pytest.fixture
def api_key():
    """Get E2B API key from environment."""
    key = os.getenv("E2B_API_KEY")
    if not key:
        pytest.skip("E2B_API_KEY environment variable not set")
    return key


@pytest.fixture
async def e2b_fs(api_key):
    """Create E2B filesystem instance with fresh sandbox."""
    # Don't pass sandbox_id - always create fresh sandbox for tests
    fs = E2BFS(api_key=api_key, template="code-interpreter-v1")
    await fs.set_session()
    yield fs
    await fs.close_session()


class TestE2BFS:
    """Test E2B filesystem operations."""

    async def test_session_management(self, api_key):
        """Test session creation and cleanup."""
        fs = E2BFS(api_key=api_key)

        # Session should not be started initially
        assert not fs._session_started

        # Start session
        await fs.set_session()
        assert fs._session_started
        assert fs._sandbox is not None

        # Close session
        await fs.close_session()
        assert not fs._session_started

    async def test_file_operations(self, e2b_fs):
        """Test basic file operations."""
        test_file = "/tmp/test_file.txt"
        content = b"Hello, E2B filesystem!"

        # Write file
        await e2b_fs._pipe_file(test_file, content)

        # Check file exists
        assert await e2b_fs._exists(test_file)
        assert await e2b_fs._isfile(test_file)
        assert not await e2b_fs._isdir(test_file)

        # Read file
        read_content = await e2b_fs._cat_file(test_file)
        assert read_content == content

        # Get file info
        size = await e2b_fs._size(test_file)
        assert size == len(content)

        mtime = await e2b_fs._modified(test_file)
        assert isinstance(mtime, float)
        assert mtime > 0

        # Clean up
        await e2b_fs._rm_file(test_file)
        assert not await e2b_fs._exists(test_file)

    async def test_directory_operations(self, e2b_fs):
        """Test directory operations."""
        test_dir = "/tmp/test_directory"

        # Create directory
        await e2b_fs._mkdir(test_dir)

        # Check directory exists
        assert await e2b_fs._exists(test_dir)
        assert await e2b_fs._isdir(test_dir)
        assert not await e2b_fs._isfile(test_dir)

        # List empty directory
        items = await e2b_fs._ls(test_dir, detail=True)
        assert isinstance(items, list)
        assert len(items) == 0

        # Create file in directory
        test_file = f"{test_dir}/nested_file.txt"
        content = b"Nested file content"
        await e2b_fs._pipe_file(test_file, content)

        # List directory with file
        items = await e2b_fs._ls(test_dir, detail=True)
        assert len(items) == 1
        assert items[0]["name"] == test_file
        assert items[0]["type"] == "file"
        assert items[0]["size"] == len(content)

        # Test non-detailed listing
        names = await e2b_fs._ls(test_dir, detail=False)
        assert names == [test_file]

        # Clean up
        await e2b_fs._rm_file(test_file)
        await e2b_fs._rmdir(test_dir)
        assert not await e2b_fs._exists(test_dir)

    async def test_partial_file_read(self, e2b_fs):
        """Test reading part of a file."""
        test_file = "/tmp/partial_read_test.txt"
        content = b"0123456789ABCDEFGHIJ"

        await e2b_fs._pipe_file(test_file, content)

        # Read first 5 bytes
        partial = await e2b_fs._cat_file(test_file, start=0, end=5)
        assert partial == b"01234"

        # Read middle section
        partial = await e2b_fs._cat_file(test_file, start=5, end=10)
        assert partial == b"56789"

        # Read from position to end
        partial = await e2b_fs._cat_file(test_file, start=15)
        assert partial == b"FGHIJ"

        # Clean up
        await e2b_fs._rm_file(test_file)

    async def test_nested_directory_creation(self, e2b_fs):
        """Test creating nested directories."""
        nested_path = "/tmp/level1/level2/level3"

        # Create nested directories
        await e2b_fs._mkdir(nested_path, create_parents=True)

        # Check all levels exist
        assert await e2b_fs._exists("/tmp/level1")
        assert await e2b_fs._exists("/tmp/level1/level2")
        assert await e2b_fs._exists(nested_path)

        # Create file in nested directory
        test_file = f"{nested_path}/deep_file.txt"
        await e2b_fs._pipe_file(test_file, b"Deep content")

        assert await e2b_fs._exists(test_file)
        content = await e2b_fs._cat_file(test_file)
        assert content == b"Deep content"

        # Clean up (reverse order)
        await e2b_fs._rm_file(test_file)
        await e2b_fs._rmdir(nested_path)
        await e2b_fs._rmdir("/tmp/level1/level2")
        await e2b_fs._rmdir("/tmp/level1")

    async def test_error_handling(self, e2b_fs):
        """Test error handling for invalid operations."""
        # Use standard FileNotFoundError

        nonexistent_file = "/tmp/nonexistent_file.txt"
        nonexistent_dir = "/tmp/nonexistent_directory"

        # Reading nonexistent file should raise error
        with pytest.raises(FileNotFoundError):
            await e2b_fs._cat_file(nonexistent_file)

        # Getting size of nonexistent file should raise error
        with pytest.raises(FileNotFoundError):
            await e2b_fs._size(nonexistent_file)

        # Removing nonexistent file should raise error
        with pytest.raises(FileNotFoundError):
            await e2b_fs._rm_file(nonexistent_file)

        # Removing nonexistent directory should raise error
        with pytest.raises(FileNotFoundError):
            await e2b_fs._rmdir(nonexistent_dir)

        # Listing nonexistent directory should raise error
        with pytest.raises(FileNotFoundError):
            await e2b_fs._ls(nonexistent_dir)

    async def test_sync_wrappers(self, api_key):
        """Test synchronous wrapper methods."""
        fs = E2BFS(api_key=api_key)

        try:
            test_file = "/tmp/sync_test.txt"
            content = b"Sync test content"

            # Use synchronous methods
            fs.pipe_file(test_file, content)
            assert fs.exists(test_file)
            assert fs.isfile(test_file)

            read_content = fs.cat_file(test_file)
            assert read_content == content

            size = fs.size(test_file)
            assert size == len(content)

            # Clean up
            fs.rm_file(test_file)
            assert not fs.exists(test_file)

        finally:
            # Sync interface auto-manages sessions, no explicit cleanup needed
            pass

    async def test_binary_content(self, e2b_fs):
        """Test handling of binary content."""
        test_file = "/tmp/binary_test.bin"

        # Create binary content with various byte values
        binary_content = bytes(range(256))

        await e2b_fs._pipe_file(test_file, binary_content)
        read_content = await e2b_fs._cat_file(test_file)

        assert read_content == binary_content
        assert len(read_content) == 256  # noqa: PLR2004

        # Clean up
        await e2b_fs._rm_file(test_file)

    async def test_large_file(self, e2b_fs):
        """Test handling of larger files."""
        test_file = "/tmp/large_file.txt"

        # Create a larger file (1MB)
        large_content = b"A" * (1024 * 1024)

        await e2b_fs._pipe_file(test_file, large_content)

        # Check size
        size = await e2b_fs._size(test_file)
        assert size == len(large_content)

        # Read back (this might be slow)
        read_content = await e2b_fs._cat_file(test_file)
        assert len(read_content) == len(large_content)
        assert read_content[:100] == b"A" * 100  # Check beginning
        assert read_content[-100:] == b"A" * 100  # Check end

        # Clean up
        await e2b_fs._rm_file(test_file)

    async def test_unicode_content(self, e2b_fs):
        """Test handling of Unicode content."""
        test_file = "/tmp/unicode_test.txt"

        # Create Unicode content
        unicode_text = "Hello 🌍! Здравствуй мир! こんにちは世界! مرحبا بالعالم!"
        unicode_content = unicode_text.encode("utf-8")

        await e2b_fs._pipe_file(test_file, unicode_content)
        read_content = await e2b_fs._cat_file(test_file)

        assert read_content == unicode_content
        assert read_content.decode("utf-8") == unicode_text

        # Clean up
        await e2b_fs._rm_file(test_file)

    async def test_path_object_creation(self, api_key):
        """Test E2BPath object creation."""
        fs = E2BFS(api_key=api_key)

        path = fs._make_path("/test/path")
        assert isinstance(path, fs._make_path("/test/path").__class__)
        assert str(path) == "/test/path"

    async def test_connect_to_existing_sandbox(self, api_key):
        """Test connecting to an existing sandbox."""
        # First create a sandbox and get its ID
        fs1 = E2BFS(api_key=api_key)
        await fs1.set_session()
        sandbox_id = fs1._sandbox_id

        # Write a test file
        test_file = "/tmp/existing_sandbox_test.txt"
        content = b"Content from first connection"
        await fs1._pipe_file(test_file, content)

        # Don't close the first session yet - sandbox should still be alive

        # Now connect to the same sandbox with a new filesystem instance
        fs2 = E2BFS(api_key=api_key, sandbox_id=sandbox_id)
        await fs2.set_session()

        # The file should still exist from the first connection
        assert await fs2._exists(test_file)
        read_content = await fs2._cat_file(test_file)
        assert read_content == content

        # Clean up
        await fs2._rm_file(test_file)
        await fs2.close_session()
        await fs1.close_session()


@pytest.mark.integration
async def test_python_script_execution_integration(e2b_fs):
    """Test creating and executing Python scripts through the filesystem."""
    script_path = "/tmp/test_script.py"
    output_path = "/tmp/script_output.txt"

    # Create a Python script
    script_content = f"""
import os
with open("{output_path}", "w") as f:
    f.write("Script executed successfully!\\n")
    f.write(f"Working directory: {{os.getcwd()}}\\n")
print("Script completed")
""".encode()

    await e2b_fs._pipe_file(script_path, script_content)

    # Execute the script using the sandbox
    sandbox = await e2b_fs._get_sandbox()
    execution = await sandbox.run_code(f"exec(open('{script_path}').read())")

    # Check execution was successful
    assert execution.error is None

    # Check output file was created
    assert await e2b_fs._exists(output_path)

    output_content = await e2b_fs._cat_file(output_path)
    output_text = output_content.decode()

    assert "Script executed successfully!" in output_text
    assert "Working directory:" in output_text

    # Clean up
    await e2b_fs._rm_file(script_path)
    await e2b_fs._rm_file(output_path)


@pytest.mark.integration
async def test_data_processing_workflow_integration(e2b_fs):
    """Test a complete data processing workflow."""
    # Create input data
    input_file = "/tmp/input_data.csv"
    csv_content = b"""name,age,city
Alice,25,New York
Bob,30,London
Charlie,35,Tokyo"""

    await e2b_fs._pipe_file(input_file, csv_content)

    # Create processing script
    script_path = "/tmp/process_data.py"
    script_content = b"""
import csv

# Read input data
with open("/tmp/input_data.csv", "r") as f:
    reader = csv.DictReader(f)
    data = list(reader)

# Process data (add age category)
for row in data:
    age = int(row["age"])
    if age < 30:
        row["category"] = "young"
    else:
        row["category"] = "mature"

# Write output data
with open("/tmp/output_data.csv", "w") as f:
    fieldnames = ["name", "age", "city", "category"]
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(data)

print(f"Processed {len(data)} rows")
"""

    await e2b_fs._pipe_file(script_path, script_content)

    # Execute processing script
    sandbox = await e2b_fs._get_sandbox()
    execution = await sandbox.run_code(f"exec(open('{script_path}').read())")

    assert execution.error is None

    # Check output file
    output_file = "/tmp/output_data.csv"
    assert await e2b_fs._exists(output_file)

    output_content = await e2b_fs._cat_file(output_file)
    output_text = output_content.decode()

    # Verify processed data
    assert "name,age,city,category" in output_text
    assert "Alice,25,New York,young" in output_text
    assert "Bob,30,London,mature" in output_text
    assert "Charlie,35,Tokyo,mature" in output_text

    # Clean up
    await e2b_fs._rm_file(input_file)
    await e2b_fs._rm_file(script_path)
    await e2b_fs._rm_file(output_file)


if __name__ == "__main__":
    pytest.main(["-v", __file__, "-m", "integration"])
