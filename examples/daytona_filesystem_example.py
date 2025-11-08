"""Example demonstrating Daytona filesystem usage with upathtools."""

import asyncio
import os
from pathlib import Path

from upathtools.filesystems import DaytonaFS


async def main():
    """Demonstrate Daytona filesystem operations."""
    # Initialize Daytona filesystem
    # You can pass an API key directly or use DAYTONA_API_KEY environment variable
    api_key = os.getenv("DAYTONA_API_KEY")

    if not api_key:
        print("Please set DAYTONA_API_KEY environment variable")
        return

    # Create filesystem instance
    daytona_fs = DaytonaFS(api_key=api_key)

    print("=== Daytona Filesystem Demo ===")

    # Set up session (creates a new sandbox)
    await daytona_fs.set_session()
    print(f"Connected to sandbox: {daytona_fs._sandbox_id}")

    # Create a test directory
    test_dir = "/workspace/daytona_test"
    await daytona_fs._mkdir(test_dir)
    print(f"Created directory: {test_dir}")

    # Write a test file
    test_file = f"{test_dir}/hello.txt"
    content = b"Hello from Daytona filesystem!\nThis is a test file."
    await daytona_fs._pipe_file(test_file, content)
    print(f"Written {len(content)} bytes to {test_file}")

    # Read the file back
    read_content = await daytona_fs._cat_file(test_file)
    print(f"Read back: {read_content.decode()}")

    # Check file info
    exists = await daytona_fs._exists(test_file)
    is_file = await daytona_fs._isfile(test_file)
    size = await daytona_fs._size(test_file)
    mtime = await daytona_fs._modified(test_file)

    print(f"File exists: {exists}")
    print(f"Is file: {is_file}")
    print(f"Size: {size} bytes")
    print(f"Modified time: {mtime}")

    # List directory contents
    print(f"\nContents of {test_dir}:")
    items = await daytona_fs._ls(test_dir, detail=True)
    for item in items:
        print(f"  {item['name']} ({item['type']}, {item['size']} bytes)")

    # Create multiple files for testing
    files_data = {
        f"{test_dir}/data1.json": b'{"key": "value1", "number": 42}',
        f"{test_dir}/data2.txt": b"Line 1\nLine 2\nLine 3",
        f"{test_dir}/script.py": b"#!/usr/bin/env python3\nprint('Hello from Daytona!')",
    }

    for file_path, file_content in files_data.items():
        await daytona_fs._pipe_file(file_path, file_content)
        print(f"Created {Path(file_path).name}")

    # Test search functionality
    print(f"\nSearching for Python files in {test_dir}:")
    python_files = await daytona_fs._find(test_dir, pattern="*.py")
    for py_file in python_files:
        print(f"  Found Python file: {py_file}")

    # Test grep functionality
    print("\nSearching for 'Hello' in files:")
    matches = await daytona_fs._grep(test_dir, "Hello")
    for match in matches:
        print(f"  {match['file']}:{match['line']}: {match['content'].strip()}")

    # Test file permissions
    script_file = f"{test_dir}/script.py"
    await daytona_fs._chmod(script_file, 0o755)
    print("Permissions updated successfully")
    # Test file move/rename
    old_name = f"{test_dir}/data1.json"
    new_name = f"{test_dir}/renamed_data.json"
    await daytona_fs._mv_file(old_name, new_name)
    print(f"Renamed {Path(old_name).name} to {Path(new_name).name}")

    # Upload a local file (if it exists)
    local_file = Path(__file__).parent / "sample.txt"
    if local_file.exists():
        remote_file = f"{test_dir}/uploaded_sample.txt"
        await daytona_fs._put_file(str(local_file), remote_file)
        print(f"Uploaded {local_file} to {remote_file}")
    else:
        # Create a sample file to upload
        sample_content = b"This is a sample file for upload testing.\nLine 2\nLine 3"
        local_sample = "/tmp/local_sample.txt"
        with open(local_sample, "wb") as f:  # noqa: PTH123
            f.write(sample_content)

        remote_sample = f"{test_dir}/uploaded_local_sample.txt"
        await daytona_fs._put_file(local_sample, remote_sample)
        print(f"Uploaded {local_sample} to {remote_sample}")

        # Clean up local file
        os.unlink(local_sample)  # noqa: PTH108

    # Test partial file reading
    large_file = f"{test_dir}/large_content.txt"
    large_content = b"0123456789" * 10  # 100 bytes
    await daytona_fs._pipe_file(large_file, large_content)

    # Read first 20 bytes
    partial = await daytona_fs._cat_file(large_file, start=0, end=20)
    print(f"\nFirst 20 bytes: {partial}")

    # Read middle section
    partial = await daytona_fs._cat_file(large_file, start=20, end=40)
    print(f"Bytes 20-40: {partial}")

    # Final directory listing
    print(f"\nFinal contents of {test_dir}:")
    items = await daytona_fs._ls(test_dir, detail=True)
    for item in items:
        item_type = "📁" if item["type"] == "directory" else "📄"
        print(f"  {item_type} {Path(item['name']).name} ({item['size']} bytes)")

    # Test simple listing (names only)
    print("\nSimple listing (names only):")
    names = await daytona_fs._ls(test_dir, detail=False)
    for name in names:
        print(f"  {name}")

    # Clean up (optional)
    print("\nCleaning up...")
    for item in items:
        if item["type"] == "file":
            await daytona_fs._rm_file(item["name"])
            print(f"Removed file: {Path(item['name']).name}")

    await daytona_fs._rmdir(test_dir)
    print(f"Removed directory: {test_dir}")

    await daytona_fs.close_session()
    print("Session closed.")


def sync_example():
    """Example using synchronous interface."""
    print("\n=== Synchronous Interface Demo ===")

    api_key = os.getenv("DAYTONA_API_KEY")
    if not api_key:
        print("Please set DAYTONA_API_KEY environment variable")
        return

    # Create filesystem instance
    daytona_fs = DaytonaFS(api_key=api_key)

    test_file = "/workspace/sync_test.txt"
    content = b"Hello from synchronous interface!"

    # Write file
    daytona_fs.pipe_file(test_file, content)
    print(f"Written file: {test_file}")

    # Read file
    read_content = daytona_fs.cat_file(test_file)
    print(f"Read content: {read_content.decode()}")

    # Check if file exists
    exists = daytona_fs.exists(test_file)
    print(f"File exists: {exists}")

    # Get file size
    size = daytona_fs.size(test_file)
    print(f"File size: {size} bytes")

    # List directory
    items = daytona_fs.ls("/workspace", detail=False)
    print(f"Files in /workspace: {items}")

    # Clean up
    daytona_fs.rm_file(test_file)
    print("File removed")


async def advanced_example():
    """Advanced usage example with file processing."""
    print("\n=== Advanced File Processing Demo ===")

    api_key = os.getenv("DAYTONA_API_KEY")
    if not api_key:
        print("Please set DAYTONA_API_KEY environment variable")
        return

    daytona_fs = DaytonaFS(api_key=api_key)

    await daytona_fs.set_session()

    # Create a data processing workspace
    workspace = "/workspace/data_processing"
    await daytona_fs._mkdir(workspace)

    # Create input data
    csv_data = b"""name,age,city
Alice,25,New York
Bob,30,London
Charlie,35,Tokyo
Diana,28,Paris"""

    await daytona_fs._pipe_file(f"{workspace}/input.csv", csv_data)

    # Create a processing script
    python_script = b"""#!/usr/bin/env python3
import csv
import json

# Read input data
with open('/workspace/data_processing/input.csv', 'r') as f:
reader = csv.DictReader(f)
data = list(reader)

# Process data (add age category)
for row in data:
age = int(row['age'])
row['category'] = 'young' if age < 30 else 'mature'

# Write output
with open('/workspace/data_processing/output.json', 'w') as f:
json.dump(data, f, indent=2)

print(f"Processed {len(data)} records")
"""

    script_path = f"{workspace}/process.py"
    await daytona_fs._pipe_file(script_path, python_script)
    await daytona_fs._chmod(script_path, 0o755)

    print("Created data processing workflow")
    print(f"Input file: {workspace}/input.csv")
    print(f"Processing script: {script_path}")

    # Note: To actually execute the script, you would need to use
    # the sandbox's process execution capabilities, not the filesystem

    # List the workspace
    files = await daytona_fs._ls(workspace, detail=True)
    print("\nWorkspace contents:")
    for file in files:
        print(f"  {file['name']} ({file['size']} bytes)")

    # Clean up
    for file in files:
        await daytona_fs._rm_file(file["name"])
    await daytona_fs._rmdir(workspace)

    await daytona_fs.close_session()


if __name__ == "__main__":
    # Run async example
    asyncio.run(main())

    # Run sync example
    sync_example()

    # Run advanced example
    asyncio.run(advanced_example())

    print("\n=== Daytona Filesystem Demo Complete ===")
