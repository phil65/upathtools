"""Tests for Modal filesystem implementation."""

import contextlib

import pytest

from upathtools.filesystems.modal_fs import ModalFS


@pytest.fixture(scope="session")
def modal_api_available():
    """Check if Modal API is available."""
    try:
        import modal  # noqa: F401
    except ImportError:
        pytest.skip("modal package not available")
    else:
        return True


@pytest.fixture(scope="session")
async def shared_modal_fs(modal_api_available):
    """Create shared Modal filesystem instance for all tests."""
    fs = ModalFS(app_name="upathtools-test", timeout=600, idle_timeout=300)
    await fs.set_session()
    yield fs
    await fs.close_session()


@pytest.mark.integration
async def test_modal_session_management(modal_api_available):
    """Test session creation and cleanup."""
    fs = ModalFS(app_name="upathtools-session-test")
    assert not fs._session_started

    await fs.set_session()
    assert fs._session_started
    assert fs._sandbox is not None

    await fs.close_session()
    assert not fs._session_started


@pytest.mark.integration
async def test_modal_file_crud_operations(shared_modal_fs):
    """Test file create, read, update, delete operations."""
    test_file = "/tmp/test_file.txt"
    content = b"Hello, Modal!"

    # Create and verify
    await shared_modal_fs._pipe_file(test_file, content)
    assert await shared_modal_fs._exists(test_file)
    assert await shared_modal_fs._isfile(test_file)
    assert not await shared_modal_fs._isdir(test_file)

    # Read and verify
    read_content = await shared_modal_fs._cat_file(test_file)
    assert read_content == content

    # Update content
    new_content = b"Updated content"
    await shared_modal_fs._pipe_file(test_file, new_content)
    updated_content = await shared_modal_fs._cat_file(test_file)
    assert updated_content == new_content

    # File metadata
    size = await shared_modal_fs._size(test_file)
    assert size == len(new_content)

    mtime = await shared_modal_fs._modified(test_file)
    assert isinstance(mtime, float)

    # Delete
    await shared_modal_fs._rm_file(test_file)
    assert not await shared_modal_fs._exists(test_file)


@pytest.mark.integration
async def test_modal_directory_operations(shared_modal_fs):
    """Test directory create, list, delete operations."""
    test_dir = "/tmp/test_directory"

    await shared_modal_fs._mkdir(test_dir)
    assert await shared_modal_fs._exists(test_dir)
    assert await shared_modal_fs._isdir(test_dir)

    # Create nested file and list
    test_file = f"{test_dir}/nested.txt"
    await shared_modal_fs._pipe_file(test_file, b"nested content")

    items = await shared_modal_fs._ls(test_dir, detail=True)
    file_items = [item for item in items if item["name"].endswith("nested.txt")]
    assert len(file_items) >= 1
    assert file_items[0]["type"] == "file"

    names = await shared_modal_fs._ls(test_dir, detail=False)
    assert any(name.endswith("nested.txt") for name in names)

    # Cleanup
    await shared_modal_fs._rm_file(test_file)
    await shared_modal_fs._rmdir(test_dir)


@pytest.mark.integration
async def test_modal_partial_reads_and_nested_dirs(shared_modal_fs):
    """Test partial file reads and nested directory creation."""
    # Test partial reads
    test_file = "/tmp/partial_test.txt"
    content = b"0123456789ABCDEF"
    await shared_modal_fs._pipe_file(test_file, content)

    partial = await shared_modal_fs._cat_file(test_file, start=0, end=5)
    assert partial == b"01234"

    partial = await shared_modal_fs._cat_file(test_file, start=10)
    assert partial == b"ABCDEF"

    await shared_modal_fs._rm_file(test_file)

    # Test nested directories
    nested_path = "/tmp/level1/level2/level3"
    await shared_modal_fs._mkdir(nested_path, create_parents=True)

    deep_file = f"{nested_path}/deep.txt"
    await shared_modal_fs._pipe_file(deep_file, b"deep content")
    assert await shared_modal_fs._exists(deep_file)

    await shared_modal_fs._rm_file(deep_file)
    await shared_modal_fs._rmdir(nested_path)
    await shared_modal_fs._rmdir("/tmp/level1/level2")
    await shared_modal_fs._rmdir("/tmp/level1")


@pytest.mark.integration
async def test_modal_error_conditions(shared_modal_fs):
    """Test error handling for nonexistent files/dirs."""
    with pytest.raises(FileNotFoundError):
        await shared_modal_fs._cat_file("/tmp/nonexistent.txt")

    with pytest.raises(FileNotFoundError):
        await shared_modal_fs._size("/tmp/nonexistent.txt")

    with pytest.raises(FileNotFoundError):
        await shared_modal_fs._rm_file("/tmp/nonexistent.txt")

    with pytest.raises(FileNotFoundError):
        await shared_modal_fs._rmdir("/tmp/nonexistent_dir")


@pytest.mark.integration
async def test_modal_content_types(shared_modal_fs):
    """Test binary and unicode content handling."""
    # Binary content
    binary_file = "/tmp/binary.bin"
    binary_content = bytes(range(256))
    await shared_modal_fs._pipe_file(binary_file, binary_content)
    read_binary = await shared_modal_fs._cat_file(binary_file)
    assert read_binary == binary_content
    await shared_modal_fs._rm_file(binary_file)

    # Unicode content
    unicode_file = "/tmp/unicode.txt"
    unicode_text = "Hello 🌍! Тест! こんにちは!"
    unicode_content = unicode_text.encode("utf-8")
    await shared_modal_fs._pipe_file(unicode_file, unicode_content)
    read_unicode = await shared_modal_fs._cat_file(unicode_file)
    assert read_unicode.decode("utf-8") == unicode_text
    await shared_modal_fs._rm_file(unicode_file)


@pytest.mark.integration
async def test_modal_large_file_handling(shared_modal_fs):
    """Test handling of larger files."""
    test_file = "/tmp/large_file.txt"
    # Keep it reasonable for testing
    large_content = b"A" * (50 * 1024)

    await shared_modal_fs._pipe_file(test_file, large_content)

    size = await shared_modal_fs._size(test_file)
    assert size == len(large_content)

    read_content = await shared_modal_fs._cat_file(test_file)
    assert len(read_content) == len(large_content)
    assert read_content[:100] == b"A" * 100
    assert read_content[-100:] == b"A" * 100

    await shared_modal_fs._rm_file(test_file)


@pytest.mark.integration
async def test_modal_sync_interface(modal_api_available):
    """Test synchronous wrapper methods."""
    fs = ModalFS(app_name="upathtools-sync-test", timeout=300)
    try:
        test_file = "/tmp/sync_test.txt"
        content = b"Sync content"

        fs.pipe_file(test_file, content)
        assert fs.exists(test_file)
        assert fs.cat_file(test_file) == content
        fs.rm_file(test_file)
        assert not fs.exists(test_file)
    finally:
        with contextlib.suppress(AttributeError):
            fs.close_session()


@pytest.mark.integration
async def test_modal_path_creation(modal_api_available):
    """Test ModalPath object creation."""
    fs = ModalFS()
    path = fs._make_path("/test/path")
    assert str(path) == "/test/path"


@pytest.mark.integration
async def test_modal_existing_sandbox_connection(modal_api_available):
    """Test connecting to existing sandbox."""
    fs1 = ModalFS(app_name="upathtools-existing-test", timeout=300)
    await fs1.set_session()
    sandbox_id = fs1._sandbox_id

    test_file = "/tmp/shared_file.txt"
    content = b"Shared content"
    await fs1._pipe_file(test_file, content)

    fs2 = ModalFS(app_name="upathtools-existing-test", sandbox_id=sandbox_id)
    await fs2.set_session()

    assert await fs2._exists(test_file)
    assert await fs2._cat_file(test_file) == content

    await fs2._rm_file(test_file)
    await fs2.close_session()
    await fs1.close_session()


@pytest.mark.integration
async def test_modal_script_execution_workflow(shared_modal_fs):
    """Test complete script execution workflow."""
    script_path = "/tmp/test_script.py"
    output_path = "/tmp/output.txt"

    script_content = f"""
with open("{output_path}", "w") as f:
    f.write("Script executed!\\nWorking dir: /tmp\\n")
print("Done")
""".encode()

    await shared_modal_fs._pipe_file(script_path, script_content)

    # Execute via sandbox
    sandbox = await shared_modal_fs._get_sandbox()
    execution = sandbox.exec("python", script_path, timeout=30)

    # Wait for completion
    for _line in execution.stdout:
        pass  # Consume output

    assert await shared_modal_fs._exists(output_path)
    output = await shared_modal_fs._cat_file(output_path)
    assert b"Script executed!" in output

    # Cleanup
    await shared_modal_fs._rm_file(script_path)
    await shared_modal_fs._rm_file(output_path)


@pytest.mark.integration
async def test_modal_data_processing_workflow(shared_modal_fs):
    """Test complete data processing workflow."""
    input_file = "/tmp/input.csv"
    script_path = "/tmp/process.py"
    output_file = "/tmp/output.csv"

    csv_data = b"""name,age,city
Alice,25,New York
Bob,30,London"""

    script_content = b"""
import csv

with open("/tmp/input.csv", "r") as f:
    reader = csv.DictReader(f)
    data = list(reader)

for row in data:
    age = int(row["age"])
    row["category"] = "young" if age < 30 else "mature"

with open("/tmp/output.csv", "w") as f:
    fieldnames = ["name", "age", "city", "category"]
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(data)

print(f"Processed {len(data)} rows")
"""

    await shared_modal_fs._pipe_file(input_file, csv_data)
    await shared_modal_fs._pipe_file(script_path, script_content)

    sandbox = await shared_modal_fs._get_sandbox()
    execution = sandbox.exec("python", script_path, timeout=60)

    for _line in execution.stdout:
        pass  # Consume output

    assert await shared_modal_fs._exists(output_file)
    output = await shared_modal_fs._cat_file(output_file)
    output_text = output.decode()

    assert "name,age,city,category" in output_text
    assert "Alice,25,New York,young" in output_text
    assert "Bob,30,London,mature" in output_text

    # Cleanup
    await shared_modal_fs._rm_file(input_file)
    await shared_modal_fs._rm_file(script_path)
    await shared_modal_fs._rm_file(output_file)


if __name__ == "__main__":
    pytest.main(["-v", __file__])
