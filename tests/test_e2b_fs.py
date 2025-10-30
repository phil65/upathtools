"""Tests for E2B filesystem implementation."""

import os

import pytest

from upathtools.filesystems.e2b_fs import E2BFS


@pytest.fixture(scope="session")
def api_key():
    """Get E2B API key from environment."""
    key = os.getenv("E2B_API_KEY")
    if not key:
        pytest.skip("E2B_API_KEY environment variable not set")
    return key


@pytest.fixture(scope="session")
async def shared_e2b_fs(api_key):
    """Create shared E2B filesystem instance for all tests."""
    fs = E2BFS(api_key=api_key, template="code-interpreter-v1")
    await fs.set_session()
    yield fs
    await fs.close_session()


@pytest.mark.integration
async def test_e2b_session_management(api_key):
    """Test session creation and cleanup."""
    fs = E2BFS(api_key=api_key)
    assert not fs._session_started

    await fs.set_session()
    assert fs._session_started
    assert fs._sandbox is not None

    await fs.close_session()
    assert not fs._session_started


@pytest.mark.integration
async def test_e2b_file_crud_operations(shared_e2b_fs):
    """Test file create, read, update, delete operations."""
    test_file = "/tmp/test_file.txt"
    content = b"Hello, E2B!"

    # Create and verify
    await shared_e2b_fs._pipe_file(test_file, content)
    assert await shared_e2b_fs._exists(test_file)
    assert await shared_e2b_fs._isfile(test_file)
    assert not await shared_e2b_fs._isdir(test_file)

    # Read and verify
    read_content = await shared_e2b_fs._cat_file(test_file)
    assert read_content == content

    # Update content
    new_content = b"Updated content"
    await shared_e2b_fs._pipe_file(test_file, new_content)
    updated_content = await shared_e2b_fs._cat_file(test_file)
    assert updated_content == new_content

    # File metadata
    size = await shared_e2b_fs._size(test_file)
    assert size == len(new_content)

    mtime = await shared_e2b_fs._modified(test_file)
    assert isinstance(mtime, float)
    assert mtime > 0

    # Delete
    await shared_e2b_fs._rm_file(test_file)
    assert not await shared_e2b_fs._exists(test_file)


@pytest.mark.integration
async def test_e2b_directory_operations(shared_e2b_fs):
    """Test directory create, list, delete operations."""
    test_dir = "/tmp/test_directory"

    await shared_e2b_fs._mkdir(test_dir)
    assert await shared_e2b_fs._exists(test_dir)
    assert await shared_e2b_fs._isdir(test_dir)

    # Empty directory
    items = await shared_e2b_fs._ls(test_dir, detail=True)
    assert isinstance(items, list)
    assert len(items) == 0

    # Create nested file and list
    test_file = f"{test_dir}/nested.txt"
    await shared_e2b_fs._pipe_file(test_file, b"nested content")

    items = await shared_e2b_fs._ls(test_dir, detail=True)
    assert len(items) == 1
    assert items[0]["name"] == test_file
    assert items[0]["type"] == "file"

    names = await shared_e2b_fs._ls(test_dir, detail=False)
    assert names == [test_file]

    # Cleanup
    await shared_e2b_fs._rm_file(test_file)
    await shared_e2b_fs._rmdir(test_dir)


@pytest.mark.integration
async def test_e2b_partial_reads_and_nested_dirs(shared_e2b_fs):
    """Test partial file reads and nested directory creation."""
    # Test partial reads
    test_file = "/tmp/partial_test.txt"
    content = b"0123456789ABCDEF"
    await shared_e2b_fs._pipe_file(test_file, content)

    partial = await shared_e2b_fs._cat_file(test_file, start=0, end=5)
    assert partial == b"01234"

    partial = await shared_e2b_fs._cat_file(test_file, start=10)
    assert partial == b"ABCDEF"

    await shared_e2b_fs._rm_file(test_file)

    # Test nested directories
    nested_path = "/tmp/level1/level2/level3"
    await shared_e2b_fs._mkdir(nested_path, create_parents=True)

    deep_file = f"{nested_path}/deep.txt"
    await shared_e2b_fs._pipe_file(deep_file, b"deep content")
    assert await shared_e2b_fs._exists(deep_file)

    await shared_e2b_fs._rm_file(deep_file)
    await shared_e2b_fs._rmdir(nested_path)
    await shared_e2b_fs._rmdir("/tmp/level1/level2")
    await shared_e2b_fs._rmdir("/tmp/level1")


@pytest.mark.integration
async def test_e2b_error_conditions(shared_e2b_fs):
    """Test error handling for nonexistent files/dirs."""
    with pytest.raises(FileNotFoundError):
        await shared_e2b_fs._cat_file("/tmp/nonexistent.txt")

    with pytest.raises(FileNotFoundError):
        await shared_e2b_fs._size("/tmp/nonexistent.txt")

    with pytest.raises(FileNotFoundError):
        await shared_e2b_fs._rm_file("/tmp/nonexistent.txt")

    with pytest.raises(FileNotFoundError):
        await shared_e2b_fs._rmdir("/tmp/nonexistent_dir")

    with pytest.raises(FileNotFoundError):
        await shared_e2b_fs._ls("/tmp/nonexistent_dir")


@pytest.mark.integration
async def test_e2b_content_types(shared_e2b_fs):
    """Test binary and unicode content handling."""
    # Binary content
    binary_file = "/tmp/binary.bin"
    binary_content = bytes(range(256))
    await shared_e2b_fs._pipe_file(binary_file, binary_content)
    read_binary = await shared_e2b_fs._cat_file(binary_file)
    assert read_binary == binary_content
    await shared_e2b_fs._rm_file(binary_file)

    # Unicode content
    unicode_file = "/tmp/unicode.txt"
    unicode_text = "Hello 🌍! Тест! こんにちは!"
    unicode_content = unicode_text.encode("utf-8")
    await shared_e2b_fs._pipe_file(unicode_file, unicode_content)
    read_unicode = await shared_e2b_fs._cat_file(unicode_file)
    assert read_unicode.decode("utf-8") == unicode_text
    await shared_e2b_fs._rm_file(unicode_file)


@pytest.mark.integration
async def test_e2b_large_file_handling(shared_e2b_fs):
    """Test handling of larger files."""
    test_file = "/tmp/large_file.txt"
    large_content = b"A" * (1024 * 1024)  # 1MB

    await shared_e2b_fs._pipe_file(test_file, large_content)

    size = await shared_e2b_fs._size(test_file)
    assert size == len(large_content)

    read_content = await shared_e2b_fs._cat_file(test_file)
    assert len(read_content) == len(large_content)
    assert read_content[:100] == b"A" * 100
    assert read_content[-100:] == b"A" * 100

    await shared_e2b_fs._rm_file(test_file)


@pytest.mark.integration
async def test_e2b_sync_interface(api_key):
    """Test synchronous wrapper methods."""
    fs = E2BFS(api_key=api_key)
    try:
        test_file = "/tmp/sync_test.txt"
        content = b"Sync content"

        fs.pipe_file(test_file, content)
        assert fs.exists(test_file)
        assert fs.cat_file(test_file) == content
        fs.rm_file(test_file)
        assert not fs.exists(test_file)
    finally:
        # Sync interface auto-manages sessions
        pass


@pytest.mark.integration
async def test_e2b_path_creation(api_key):
    """Test E2BPath object creation."""
    fs = E2BFS(api_key=api_key)
    path = fs._make_path("/test/path")
    assert str(path) == "/test/path"


@pytest.mark.integration
async def test_e2b_existing_sandbox_connection(api_key):
    """Test connecting to existing sandbox."""
    fs1 = E2BFS(api_key=api_key)
    await fs1.set_session()
    sandbox_id = fs1._sandbox_id

    test_file = "/tmp/shared_file.txt"
    content = b"Shared content"
    await fs1._pipe_file(test_file, content)

    fs2 = E2BFS(api_key=api_key, sandbox_id=sandbox_id)
    await fs2.set_session()

    assert await fs2._exists(test_file)
    assert await fs2._cat_file(test_file) == content

    await fs2._rm_file(test_file)
    await fs2.close_session()
    await fs1.close_session()


@pytest.mark.integration
async def test_e2b_script_execution_workflow(shared_e2b_fs):
    """Test complete script execution workflow."""
    script_path = "/tmp/test_script.py"
    output_path = "/tmp/output.txt"

    script_content = f"""
with open("{output_path}", "w") as f:
    f.write("Script executed!\\nWorking dir: /tmp\\n")
print("Done")
""".encode()

    await shared_e2b_fs._pipe_file(script_path, script_content)

    # Execute via sandbox
    sandbox = await shared_e2b_fs._get_sandbox()
    execution = await sandbox.run_code(f"exec(open('{script_path}').read())")

    assert execution.error is None
    assert await shared_e2b_fs._exists(output_path)

    output = await shared_e2b_fs._cat_file(output_path)
    assert b"Script executed!" in output

    # Cleanup
    await shared_e2b_fs._rm_file(script_path)
    await shared_e2b_fs._rm_file(output_path)


@pytest.mark.integration
async def test_e2b_data_processing_workflow(shared_e2b_fs):
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

    await shared_e2b_fs._pipe_file(input_file, csv_data)
    await shared_e2b_fs._pipe_file(script_path, script_content)

    sandbox = await shared_e2b_fs._get_sandbox()
    execution = await sandbox.run_code(f"exec(open('{script_path}').read())")

    assert execution.error is None
    assert await shared_e2b_fs._exists(output_file)

    output = await shared_e2b_fs._cat_file(output_file)
    output_text = output.decode()

    assert "name,age,city,category" in output_text
    assert "Alice,25,New York,young" in output_text
    assert "Bob,30,London,mature" in output_text

    # Cleanup
    await shared_e2b_fs._rm_file(input_file)
    await shared_e2b_fs._rm_file(script_path)
    await shared_e2b_fs._rm_file(output_file)


if __name__ == "__main__":
    pytest.main(["-v", __file__])
