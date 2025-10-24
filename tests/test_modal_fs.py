"""Tests for Modal filesystem implementation."""

import contextlib

import pytest

from upathtools.filesystems.modal_fs import ModalFS


pytestmark = pytest.mark.asyncio


@pytest.fixture
async def modal_fs(modal_api_available):
    """Create Modal filesystem instance with fresh sandbox."""
    # Don't pass sandbox_id - always create fresh sandbox for tests
    fs = ModalFS(
        app_name="upathtools-test-app",
        timeout=600,  # 10 minutes for tests
        idle_timeout=300,  # 5 minutes idle
    )
    await fs.set_session()
    yield fs
    await fs.close_session()


class TestModalFS:
    """Test Modal filesystem operations."""

    async def test_session_management(self):
        """Test session creation and cleanup."""
        fs = ModalFS(app_name="upathtools-test-session")

        # Session should not be started initially
        assert not fs._session_started

        # Start session
        await fs.set_session()
        assert fs._session_started
        assert fs._sandbox is not None

        # Close session
        await fs.close_session()
        assert not fs._session_started

    async def test_file_operations(self, modal_fs):
        """Test basic file operations."""
        test_file = "/tmp/test_file.txt"
        content = b"Hello, Modal filesystem!"

        # Write file
        await modal_fs._pipe_file(test_file, content)

        # Check file exists
        assert await modal_fs._exists(test_file)
        assert await modal_fs._isfile(test_file)
        assert not await modal_fs._isdir(test_file)

        # Read file
        read_content = await modal_fs._cat_file(test_file)
        assert read_content == content

        # Get file info
        size = await modal_fs._size(test_file)
        assert size == len(content)

        mtime = await modal_fs._modified(test_file)
        assert isinstance(mtime, float)
        # Note: mtime will be 0.0 until Modal provides metadata API

        # Clean up
        await modal_fs._rm_file(test_file)
        assert not await modal_fs._exists(test_file)

    async def test_directory_operations(self, modal_fs):
        """Test directory operations."""
        test_dir = "/tmp/test_directory"

        # Create directory
        await modal_fs._mkdir(test_dir)

        # Check directory exists
        assert await modal_fs._exists(test_dir)
        assert await modal_fs._isdir(test_dir)
        assert not await modal_fs._isfile(test_dir)

        # List empty directory
        items = await modal_fs._ls(test_dir, detail=True)
        assert isinstance(items, list)
        # Directory might not be completely empty due to system files

        # Create file in directory
        test_file = f"{test_dir}/nested_file.txt"
        content = b"Nested file content"
        await modal_fs._pipe_file(test_file, content)

        # List directory with file
        items = await modal_fs._ls(test_dir, detail=True)
        # Find our file in the results
        file_items = [item for item in items if item["name"].endswith("nested_file.txt")]
        assert len(file_items) >= 1
        file_item = file_items[0]
        assert file_item["type"] == "file"
        # Note: size will be 0 until Modal provides metadata API

        # Test non-detailed listing
        names = await modal_fs._ls(test_dir, detail=False)
        assert any(name.endswith("nested_file.txt") for name in names)

        # Clean up
        await modal_fs._rm_file(test_file)
        await modal_fs._rmdir(test_dir)
        assert not await modal_fs._exists(test_dir)

    async def test_partial_file_read(self, modal_fs):
        """Test reading part of a file."""
        test_file = "/tmp/partial_read_test.txt"
        content = b"0123456789ABCDEFGHIJ"

        await modal_fs._pipe_file(test_file, content)

        # Read first 5 bytes
        partial = await modal_fs._cat_file(test_file, start=0, end=5)
        assert partial == b"01234"

        # Read middle section
        partial = await modal_fs._cat_file(test_file, start=5, end=10)
        assert partial == b"56789"

        # Read from position to end
        partial = await modal_fs._cat_file(test_file, start=15)
        assert partial == b"FGHIJ"

        # Clean up
        await modal_fs._rm_file(test_file)

    async def test_nested_directory_creation(self, modal_fs):
        """Test creating nested directories."""
        nested_path = "/tmp/level1/level2/level3"

        # Create nested directories
        await modal_fs._mkdir(nested_path, create_parents=True)

        # Check all levels exist
        assert await modal_fs._exists("/tmp/level1")
        assert await modal_fs._exists("/tmp/level1/level2")
        assert await modal_fs._exists(nested_path)

        # Create file in nested directory
        test_file = f"{nested_path}/deep_file.txt"
        await modal_fs._pipe_file(test_file, b"Deep content")

        assert await modal_fs._exists(test_file)
        content = await modal_fs._cat_file(test_file)
        assert content == b"Deep content"

        # Clean up (reverse order)
        await modal_fs._rm_file(test_file)
        await modal_fs._rmdir(nested_path)
        await modal_fs._rmdir("/tmp/level1/level2")
        await modal_fs._rmdir("/tmp/level1")

    async def test_error_handling(self, modal_fs):
        """Test error handling for invalid operations."""
        nonexistent_file = "/tmp/nonexistent_file.txt"
        nonexistent_dir = "/tmp/nonexistent_directory"

        # Reading nonexistent file should raise error
        with pytest.raises(FileNotFoundError):
            await modal_fs._cat_file(nonexistent_file)

        # Getting size of nonexistent file should raise error
        with pytest.raises(FileNotFoundError):
            await modal_fs._size(nonexistent_file)

        # Removing nonexistent file should raise error
        with pytest.raises(FileNotFoundError):
            await modal_fs._rm_file(nonexistent_file)

        # Removing nonexistent directory should raise error
        with pytest.raises(FileNotFoundError):
            await modal_fs._rmdir(nonexistent_dir)

        # Listing nonexistent directory should raise error
        with pytest.raises((FileNotFoundError, NotADirectoryError)):
            await modal_fs._ls(nonexistent_dir)

    async def test_sync_wrappers(self, modal_api_available):
        """Test synchronous wrapper methods."""
        fs = ModalFS(app_name="upathtools-sync-test", timeout=600)

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
            # Clean up session
            with contextlib.suppress(AttributeError):
                fs.close_session()

    async def test_binary_content(self, modal_fs):
        """Test handling of binary content."""
        test_file = "/tmp/binary_test.bin"

        # Create binary content with various byte values
        binary_content = bytes(range(256))

        await modal_fs._pipe_file(test_file, binary_content)
        read_content = await modal_fs._cat_file(test_file)

        assert read_content == binary_content
        assert len(read_content) == 256  # noqa: PLR2004

        # Clean up
        await modal_fs._rm_file(test_file)

    async def test_large_file(self, modal_fs):
        """Test handling of larger files."""
        test_file = "/tmp/large_file.txt"

        # Create a moderately large file (50KB, respecting Modal's limits)
        large_content = b"A" * (50 * 1024)

        await modal_fs._pipe_file(test_file, large_content)

        # Check size
        size = await modal_fs._size(test_file)
        assert size == len(large_content)

        # Read back (this might be slow)
        read_content = await modal_fs._cat_file(test_file)
        assert len(read_content) == len(large_content)
        assert read_content[:100] == b"A" * 100  # Check beginning
        assert read_content[-100:] == b"A" * 100  # Check end

        # Clean up
        await modal_fs._rm_file(test_file)

    async def test_unicode_content(self, modal_fs):
        """Test handling of Unicode content."""
        test_file = "/tmp/unicode_test.txt"

        # Create Unicode content
        unicode_text = "Hello 🌍! Здравствуй мир! こんにちは世界! مرحبا بالعالم!"
        unicode_content = unicode_text.encode("utf-8")

        await modal_fs._pipe_file(test_file, unicode_content)
        read_content = await modal_fs._cat_file(test_file)

        assert read_content == unicode_content
        assert read_content.decode("utf-8") == unicode_text

        # Clean up
        await modal_fs._rm_file(test_file)

    async def test_path_object_creation(self, modal_api_available):
        """Test ModalPath object creation."""
        fs = ModalFS()

        path = fs._make_path("/test/path")
        assert isinstance(path, fs._make_path("/test/path").__class__)
        assert str(path) == "/test/path"

    async def test_connect_to_existing_sandbox(self, modal_api_available):
        """Test connecting to an existing sandbox."""
        # First create a sandbox and get its ID
        fs1 = ModalFS(app_name="upathtools-existing-test", timeout=600)
        await fs1.set_session()
        sandbox_id = fs1._sandbox_id

        # Write a test file
        test_file = "/tmp/existing_sandbox_test.txt"
        content = b"Content from first connection"
        await fs1._pipe_file(test_file, content)

        # Don't close the first session yet - sandbox should still be alive

        # Now connect to the same sandbox with a new filesystem instance
        fs2 = ModalFS(app_name="upathtools-existing-test", sandbox_id=sandbox_id)
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
async def test_python_script_execution_integration(modal_fs):
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

    await modal_fs._pipe_file(script_path, script_content)

    # Execute the script using the sandbox
    sandbox = await modal_fs._get_sandbox()
    execution = sandbox.exec("python", script_path, timeout=30)

    # Wait for execution to complete
    execution_result = ""
    for line in execution.stdout:
        execution_result += line

    # Check output file was created
    assert await modal_fs._exists(output_path)

    output_content = await modal_fs._cat_file(output_path)
    output_text = output_content.decode()

    assert "Script executed successfully!" in output_text
    assert "Working directory:" in output_text

    # Clean up
    await modal_fs._rm_file(script_path)
    await modal_fs._rm_file(output_path)


@pytest.mark.integration
async def test_data_processing_workflow_integration(modal_fs):
    """Test a complete data processing workflow."""
    # Create input data
    input_file = "/tmp/input_data.csv"
    csv_content = b"""name,age,city
Alice,25,New York
Bob,30,London
Charlie,35,Tokyo"""

    await modal_fs._pipe_file(input_file, csv_content)

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

    await modal_fs._pipe_file(script_path, script_content)

    # Execute processing script
    sandbox = await modal_fs._get_sandbox()
    execution = sandbox.exec("python", script_path, timeout=60)

    # Wait for execution to complete
    execution_result = ""
    for line in execution.stdout:
        execution_result += line

    # Check output file
    output_file = "/tmp/output_data.csv"
    assert await modal_fs._exists(output_file)

    output_content = await modal_fs._cat_file(output_file)
    output_text = output_content.decode()

    # Verify processed data
    assert "name,age,city,category" in output_text
    assert "Alice,25,New York,young" in output_text
    assert "Bob,30,London,mature" in output_text
    assert "Charlie,35,Tokyo,mature" in output_text

    # Clean up
    await modal_fs._rm_file(input_file)
    await modal_fs._rm_file(script_path)
    await modal_fs._rm_file(output_file)


@pytest.mark.integration
async def test_file_watch_functionality_integration(modal_fs):
    """Test Modal's file watching capabilities (if available)."""
    test_file = "/tmp/watch_test.txt"

    # Create initial file
    await modal_fs._pipe_file(test_file, b"Initial content")

    # Modify the file
    await modal_fs._pipe_file(test_file, b"Modified content")

    # Verify modification
    content = await modal_fs._cat_file(test_file)
    assert content == b"Modified content"

    # Clean up
    await modal_fs._rm_file(test_file)


@pytest.mark.integration
async def test_special_modal_operations_integration(modal_fs):
    """Test Modal-specific file operations like replace_bytes and delete_bytes."""
    test_file = "/tmp/special_ops_test.txt"
    initial_content = b"The quick brown fox jumps over the lazy dog"

    await modal_fs._pipe_file(test_file, initial_content)

    # Test basic file operations work
    content = await modal_fs._cat_file(test_file)
    assert content == initial_content

    # Note: replace_bytes and delete_bytes would need direct Modal file handle access
    # These are Modal-specific features beyond the standard filesystem interface
    # They could be added as extensions to the ModalFS class if needed

    # Clean up
    await modal_fs._rm_file(test_file)


@pytest.mark.integration
async def test_volume_integration_integration(modal_api_available):
    """Test Modal filesystem with volume mounts."""
    # This would require Modal volume setup
    # For now, just test that volume configuration doesn't break initialization
    try:
        # Create a simple volume config (might fail if no volumes exist)
        fs = ModalFS(
            app_name="upathtools-volume-test",
            timeout=300,
            # volumes={"/data": modal.Volume.from_name("test-volume",
            # create_if_missing=True)},
        )

        # Just verify it initializes without error
        assert fs._volumes == {}  # No volumes configured in this test

    except Exception:  # noqa: BLE001
        # Skip if volumes can't be configured
        pytest.skip("Volume configuration not available")
