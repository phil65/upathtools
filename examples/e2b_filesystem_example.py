"""Example demonstrating E2B filesystem usage with upathtools."""

import asyncio
import os
from pathlib import Path

from upathtools.filesystems import E2BFS


async def main():
    """Demonstrate E2B filesystem operations."""
    e2b_fs = E2BFS(template="code-interpreter-v1")
    print("=== E2B Filesystem Demo ===")
    await e2b_fs.set_session()
    print(f"Connected to sandbox: {e2b_fs._sandbox_id}")
    test_dir = "/tmp/e2b_test"
    await e2b_fs._mkdir(test_dir)
    print(f"Created directory: {test_dir}")
    test_file = f"{test_dir}/hello.txt"
    content = b"Hello from E2B filesystem!\nThis is a test file."
    await e2b_fs._pipe_file(test_file, content)
    print(f"Written {len(content)} bytes to {test_file}")
    read_content = await e2b_fs._cat_file(test_file)
    print(f"Read back: {read_content.decode()}")
    exists = await e2b_fs._exists(test_file)
    is_file = await e2b_fs._isfile(test_file)
    size = await e2b_fs._size(test_file)
    mtime = await e2b_fs._modified(test_file)
    print(f"File exists: {exists}")
    print(f"Is file: {is_file}")
    print(f"Size: {size} bytes")
    print(f"Modified time: {mtime}")
    # List directory contents
    print(f"\nContents of {test_dir}:")
    items = await e2b_fs._ls(test_dir, detail=True)
    for item in items:
        print(f"  {item['name']} ({item['type']}, {item['size']} bytes)")

    # Create a Python script and execute it
    script_content = b"""
import os
import sys

print("Python version:", sys.version)
print("Current directory:", os.getcwd())
print("Environment variables:")
for key, value in sorted(os.environ.items())[:5]:  # Show first 5
    print(f"  {key}={value}")

# Create a data file
with open("data.txt", "w") as f:
    f.write("Generated from Python script\\n")
    f.write("Line 2\\n")
    f.write("Line 3\\n")

print("\\nCreated data.txt")
"""

    script_file = f"{test_dir}/test_script.py"
    await e2b_fs._pipe_file(script_file, script_content)
    print(f"\nCreated Python script: {script_file}")

    # Execute the script using the sandbox
    sandbox = await e2b_fs._get_sandbox()
    execution = await sandbox.run_code(f"exec(open('{script_file}').read())")

    if execution.error:
        print(f"Script execution error: {execution.error}")
    else:
        print("Script output:")
        for result in execution.results:
            if result.text:
                print(result.text)

    # Check if the script created the data file
    data_file = f"{test_dir}/data.txt"
    if await e2b_fs._exists(data_file):
        data_content = await e2b_fs._cat_file(data_file)
        print(f"\nScript created {data_file}:")
        print(data_content.decode())

    # Upload a local file (if it exists)
    local_file = Path(__file__).parent / "sample.txt"
    if local_file.exists():
        remote_file = f"{test_dir}/uploaded_sample.txt"
        await e2b_fs._put_file(str(local_file), remote_file)
        print(f"Uploaded {local_file} to {remote_file}")
    else:
        # Create a sample file to upload
        sample_content = b"This is a sample file for upload testing."
        local_sample = "/tmp/local_sample.txt"
        with open(local_sample, "wb") as f:  # noqa: PTH123
            f.write(sample_content)

        remote_sample = f"{test_dir}/uploaded_local_sample.txt"
        await e2b_fs._put_file(local_sample, remote_sample)
        print(f"Uploaded {local_sample} to {remote_sample}")

        # Clean up local file
        os.unlink(local_sample)  # noqa: PTH108

    # Final directory listing
    print(f"\nFinal contents of {test_dir}:")
    items = await e2b_fs._ls(test_dir, detail=True)
    for item in items:
        item_type = "📁" if item["type"] == "directory" else "📄"
        print(f"  {item_type} {Path(item['name']).name} ({item['size']} bytes)")

    # Clean up (optional)
    print("\nCleaning up...")
    for item in items:
        if item["type"] == "file":
            await e2b_fs._rm_file(item["name"])
            print(f"Removed file: {Path(item['name']).name}")

    await e2b_fs._rmdir(test_dir)
    print(f"Removed directory: {test_dir}")

    await e2b_fs.close_session()
    print("Session closed.")


if __name__ == "__main__":
    # Run async example
    asyncio.run(main())

    print("\n=== E2B Filesystem Demo Complete ===")
