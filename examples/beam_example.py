"""Example usage of BeamFS - Beam sandbox filesystem integration.

This example demonstrates how to use BeamFS to interact with files
in a Beam sandbox environment through the upathtools interface.
"""

import asyncio
import contextlib
from pathlib import Path

from upathtools.filesystems import BeamFS


async def basic_file_operations():
    """Demonstrate basic file operations with BeamFS."""
    print("🚀 Creating Beam sandbox...")

    # Create filesystem instance
    fs = BeamFS(
        cpu=1.0,
        memory=512,
        keep_warm_seconds=300,  # Keep alive for 5 minutes
    )

    try:
        await fs.set_session()
        print(f"✅ Connected to Beam sandbox: {fs._sandbox_id}")

        # Create a test file
        test_file = "/workspace/hello.txt"
        content = b"Hello from BeamFS!"

        print(f"📝 Writing file: {test_file}")
        await fs._pipe_file(test_file, content)

        # Check if file exists
        exists = await fs._exists(test_file)
        print(f"📁 File exists: {exists}")

        # Read file back
        read_content = await fs._cat_file(test_file)
        print(f"📖 File content: {read_content.decode()}")

        # Get file info
        size = await fs._size(test_file)
        print(f"📏 File size: {size} bytes")

        # List files in directory
        print("📋 Files in /workspace:")
        files = await fs._ls("/workspace", detail=True)
        for file_info in files:
            file_type = "📁" if file_info["type"] == "directory" else "📄"
            print(f"  {file_type} {file_info['name']} ({file_info['size']} bytes)")

        # Create a directory
        test_dir = "/workspace/test_dir"
        await fs._mkdir(test_dir)
        print(f"📁 Created directory: {test_dir}")

        # Create nested file
        nested_file = f"{test_dir}/nested.txt"
        await fs._pipe_file(nested_file, b"Nested content")
        print(f"📝 Created nested file: {nested_file}")

        # Clean up
        await fs._rm_file(nested_file)
        await fs._rmdir(test_dir)
        await fs._rm_file(test_file)
        print("🧹 Cleaned up test files")

    finally:
        await fs.close_session()
        print("🔒 Closed Beam session")


async def sync_operations_example():
    """Demonstrate synchronous operations using sync wrappers."""
    print("\n🔄 Testing synchronous operations...")

    fs = BeamFS(keep_warm_seconds=300)

    try:
        # Use sync methods (these internally handle async setup)
        test_file = "/workspace/sync_test.txt"
        content = b"Synchronous file operation!"

        print(f"📝 Writing file synchronously: {test_file}")
        fs.pipe_file(test_file, content)

        print("📖 Reading file synchronously...")
        read_content = fs.cat_file(test_file)
        print(f"Content: {read_content.decode()}")

        # Clean up
        fs.rm_file(test_file)
        print("🧹 Cleaned up sync test file")

    finally:
        # Close session if needed
        with contextlib.suppress(Exception):
            await fs.close_session()


async def python_execution_example():
    """Demonstrate Python code execution in Beam sandbox."""
    print("\n🐍 Testing Python code execution...")

    fs = BeamFS(keep_warm_seconds=300)

    try:
        await fs.set_session()
        sandbox = await fs._get_sandbox()

        # Create a Python script
        script_path = "/workspace/demo_script.py"
        script_content = """
import os
import sys
import json

# Create some demo data
data = {
    "message": "Hello from Beam sandbox!",
    "python_version": sys.version,
    "working_directory": os.getcwd(),
    "environment_vars": dict(os.environ)
}

# Save to JSON file
with open("/workspace/output.json", "w") as f:
    json.dump(data, f, indent=2)

print("✅ Script executed successfully!")
print(f"📁 Working in: {os.getcwd()}")
print(f"🐍 Python version: {sys.version.split()[0]}")
""".encode()

        print("📝 Creating Python script...")
        await fs._pipe_file(script_path, script_content)

        # Execute the script
        print("🚀 Executing Python script in sandbox...")
        result = sandbox.process.run_code(f"exec(open('{script_path}').read())")

        print(f"Exit code: {result.exit_code}")
        print(f"Output: {result.result}")

        # Read the output file
        if await fs._exists("/workspace/output.json"):
            output_content = await fs._cat_file("/workspace/output.json")
            print("📄 Generated output.json:")
            print(output_content.decode())

            # Clean up
            await fs._rm_file("/workspace/output.json")

        await fs._rm_file(script_path)
        print("🧹 Cleaned up script files")

    finally:
        await fs.close_session()


async def file_upload_example():
    """Demonstrate file upload from local filesystem."""
    print("\n📤 Testing file upload...")

    # Create a local test file
    local_file = Path("test_upload.txt")
    local_file.write_text("This file was uploaded from local filesystem! 📤")

    fs = BeamFS(keep_warm_seconds=300)

    try:
        await fs.set_session()

        # Upload the file
        remote_path = "/workspace/uploaded_file.txt"
        print(f"📤 Uploading {local_file} to {remote_path}")
        await fs._put_file(str(local_file), remote_path)

        # Verify upload
        if await fs._exists(remote_path):
            content = await fs._cat_file(remote_path)
            print(f"✅ Upload successful! Content: {content.decode()}")

            # Clean up
            await fs._rm_file(remote_path)
        else:
            print("❌ Upload failed!")

    finally:
        await fs.close_session()
        # Clean up local file
        if local_file.exists():
            local_file.unlink()


async def main():
    """Run all examples."""
    print("🌟 BeamFS Example - Beam Sandbox Filesystem Integration\n")
    await basic_file_operations()
    await sync_operations_example()
    await python_execution_example()
    await file_upload_example()

    print("\n🎉 All examples completed successfully!")


if __name__ == "__main__":
    asyncio.run(main())
