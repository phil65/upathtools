"""Tests for Beam filesystem implementation."""

import contextlib

import pytest

from upathtools.filesystems.beam_fs import BeamFS


@pytest.fixture(scope="session")
async def shared_beam_fs():
    """Create shared Beam filesystem instance for all tests."""
    fs = BeamFS(cpu=1.0, memory=512, keep_warm_seconds=300)
    await fs.set_session()
    yield fs
    await fs.close_session()


@pytest.mark.integration
async def test_beam_session_management():
    """Test session creation and cleanup."""
    fs = BeamFS()
    assert not fs._session_started

    await fs.set_session()
    assert fs._session_started
    assert fs._sandbox_instance is not None

    await fs.close_session()
    assert not fs._session_started


@pytest.mark.integration
async def test_beam_file_crud_operations(shared_beam_fs: BeamFS):
    """Test file create, read, update, delete operations."""
    test_file = "/tmp/test_file.txt"
    content = b"Hello, Beam!"

    # Create and verify
    await shared_beam_fs._pipe_file(test_file, content)
    assert await shared_beam_fs._exists(test_file)
    assert await shared_beam_fs._isfile(test_file)
    assert not await shared_beam_fs._isdir(test_file)

    # Read and verify
    read_content = await shared_beam_fs._cat_file(test_file)
    assert read_content == content

    # Update content
    new_content = b"Updated content"
    await shared_beam_fs._pipe_file(test_file, new_content)
    updated_content = await shared_beam_fs._cat_file(test_file)
    assert updated_content == new_content

    # File metadata
    size = await shared_beam_fs._size(test_file)
    assert size == len(new_content)

    mtime = await shared_beam_fs._modified(test_file)
    assert isinstance(mtime, float)

    # Delete
    await shared_beam_fs._rm_file(test_file)
    assert not await shared_beam_fs._exists(test_file)


@pytest.mark.integration
async def test_beam_directory_operations(shared_beam_fs: BeamFS):
    """Test directory create, list, delete operations."""
    test_dir = "/tmp/test_directory"

    await shared_beam_fs._mkdir(test_dir)
    assert await shared_beam_fs._exists(test_dir)
    assert await shared_beam_fs._isdir(test_dir)

    # Create nested file and list
    test_file = f"{test_dir}/nested.txt"
    await shared_beam_fs._pipe_file(test_file, b"nested content")

    items = await shared_beam_fs._ls(test_dir, detail=True)
    file_items = [item for item in items if item["name"].endswith("nested.txt")]
    assert len(file_items) >= 1
    assert file_items[0]["type"] == "file"

    names = await shared_beam_fs._ls(test_dir, detail=False)
    assert any(name.endswith("nested.txt") for name in names)  # pyright: ignore[reportAttributeAccessIssue]

    # Cleanup
    await shared_beam_fs._rm_file(test_file)
    await shared_beam_fs._rmdir(test_dir)


@pytest.mark.integration
async def test_beam_partial_reads_and_nested_dirs(shared_beam_fs: BeamFS):
    """Test partial file reads and nested directory creation."""
    # Test partial reads
    test_file = "/tmp/partial_test.txt"
    content = b"0123456789ABCDEF"
    await shared_beam_fs._pipe_file(test_file, content)

    partial = await shared_beam_fs._cat_file(test_file, start=0, end=5)
    assert partial == b"01234"

    partial = await shared_beam_fs._cat_file(test_file, start=10)
    assert partial == b"ABCDEF"

    await shared_beam_fs._rm_file(test_file)

    # Test nested directories
    nested_path = "/tmp/level1/level2/level3"
    await shared_beam_fs._mkdir(nested_path, create_parents=True)

    deep_file = f"{nested_path}/deep.txt"
    await shared_beam_fs._pipe_file(deep_file, b"deep content")
    assert await shared_beam_fs._exists(deep_file)

    await shared_beam_fs._rm_file(deep_file)
    await shared_beam_fs._rmdir(nested_path)
    await shared_beam_fs._rmdir("/tmp/level1/level2")
    await shared_beam_fs._rmdir("/tmp/level1")


@pytest.mark.integration
async def test_beam_error_conditions(shared_beam_fs: BeamFS):
    """Test error handling for nonexistent files/dirs."""
    with pytest.raises(FileNotFoundError):
        await shared_beam_fs._cat_file("/tmp/nonexistent.txt")

    with pytest.raises(FileNotFoundError):
        await shared_beam_fs._size("/tmp/nonexistent.txt")

    with pytest.raises(FileNotFoundError):
        await shared_beam_fs._rm_file("/tmp/nonexistent.txt")


@pytest.mark.integration
async def test_beam_content_types(shared_beam_fs: BeamFS):
    """Test binary and unicode content handling."""
    # Binary content
    binary_file = "/tmp/binary.bin"
    binary_content = bytes(range(256))
    await shared_beam_fs._pipe_file(binary_file, binary_content)
    read_binary = await shared_beam_fs._cat_file(binary_file)
    assert read_binary == binary_content
    await shared_beam_fs._rm_file(binary_file)

    # Unicode content
    unicode_file = "/tmp/unicode.txt"
    unicode_text = "Hello 🌍! Тест! こんにちは!"
    unicode_content = unicode_text.encode("utf-8")
    await shared_beam_fs._pipe_file(unicode_file, unicode_content)
    read_unicode = await shared_beam_fs._cat_file(unicode_file)
    assert read_unicode.decode("utf-8") == unicode_text
    await shared_beam_fs._rm_file(unicode_file)


@pytest.mark.integration
async def test_beam_large_file_handling(shared_beam_fs: BeamFS):
    """Test handling of larger files."""
    test_file = "/tmp/large_file.txt"
    large_content = b"A" * (50 * 1024)

    await shared_beam_fs._pipe_file(test_file, large_content)

    size = await shared_beam_fs._size(test_file)
    assert size == len(large_content)

    read_content = await shared_beam_fs._cat_file(test_file)
    assert len(read_content) == len(large_content)
    assert read_content[:100] == b"A" * 100
    assert read_content[-100:] == b"A" * 100

    await shared_beam_fs._rm_file(test_file)


@pytest.mark.integration
async def test_beam_sync_interface():
    """Test synchronous wrapper methods."""
    fs = BeamFS()
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
            await fs.close_session()


@pytest.mark.integration
async def test_beam_existing_sandbox_connection():
    """Test connecting to existing sandbox."""
    fs1 = BeamFS()
    await fs1.set_session()
    assert fs1._sandbox_instance
    sandbox_id = fs1._sandbox_instance.container_id

    test_file = "/tmp/shared_file.txt"
    content = b"Shared content"
    await fs1._pipe_file(test_file, content)

    fs2 = BeamFS(sandbox_id=sandbox_id)
    await fs2.set_session()

    assert await fs2._exists(test_file)
    assert await fs2._cat_file(test_file) == content

    await fs2._rm_file(test_file)
    await fs2.close_session()
    await fs1.close_session()


@pytest.mark.integration
async def test_beam_script_execution_workflow(shared_beam_fs: BeamFS):
    """Test complete script execution workflow."""
    script_path = "/tmp/test_script.py"
    output_path = "/tmp/output.txt"

    script_content = f"""
with open("{output_path}", "w") as f:
    f.write("Script executed!\\nWorking dir: /tmp\\n")
print("Done")
""".encode()

    await shared_beam_fs._pipe_file(script_path, script_content)

    # Note: Beam script execution would require specific Beam API calls
    # This is a placeholder for the integration test structure

    # Cleanup
    await shared_beam_fs._rm_file(script_path)


@pytest.mark.integration
async def test_beam_data_processing_workflow(shared_beam_fs: BeamFS):
    """Test complete data processing workflow."""
    input_file = "/tmp/input.csv"
    script_path = "/tmp/process.py"

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

    await shared_beam_fs._pipe_file(input_file, csv_data)
    await shared_beam_fs._pipe_file(script_path, script_content)

    # Note: Beam script execution would require specific Beam API calls
    # This is a placeholder for the integration test structure

    # Cleanup
    await shared_beam_fs._rm_file(input_file)
    await shared_beam_fs._rm_file(script_path)


if __name__ == "__main__":
    pytest.main(["-v", __file__, "-m", "integration"])
