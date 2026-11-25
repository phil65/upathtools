"""Example usage of ModalFS - Modal sandbox filesystem integration.

This example demonstrates how to use ModalFS to interact with files
in a Modal sandbox environment through the upathtools interface.
"""

import asyncio
import contextlib
from pathlib import Path

from upathtools.filesystems import ModalFS


async def basic_file_operations():
    """Demonstrate basic file operations with ModalFS."""
    print("🚀 Creating Modal sandbox...")

    # Create filesystem instance
    fs = ModalFS(
        app_name="upathtools-demo",
        timeout=600,  # 10 minutes
        idle_timeout=300,  # 5 minutes idle timeout
    )

    try:
        await fs.set_session()
        print(f"✅ Connected to Modal sandbox: {fs._sandbox_id}")

        # Create a test file
        test_file = "/tmp/hello.txt"
        content = b"Hello from ModalFS!"

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
        print("📋 Files in /tmp:")
        files = await fs._ls("/tmp", detail=True)
        for file_info in files[:5]:  # Show first 5 files
            file_type = "📁" if file_info["type"] == "directory" else "📄"
            print(f"  {file_type} {file_info['name']}")

        # Create a directory
        test_dir = "/tmp/test_dir"
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
        print("🔒 Closed Modal session")


async def sync_operations_example():
    """Demonstrate synchronous operations using sync wrappers."""
    print("\n🔄 Testing synchronous operations...")

    fs = ModalFS(app_name="upathtools-sync-demo", timeout=600)

    try:
        # Use sync methods (these internally handle async setup)
        test_file = "/tmp/sync_test.txt"
        content = b"Synchronous file operation!"

        print(f"📝 Writing file synchronously: {test_file}")
        fs.pipe_file(test_file, content)

        print("📖 Reading file synchronously...")
        read_content = fs.cat_file(test_file)
        print(f"Content: {read_content.decode()}")  # pyright: ignore[reportAttributeAccessIssue]

        # Clean up
        fs.rm_file(test_file)
        print("🧹 Cleaned up sync test file")

    finally:
        # Close session if needed
        with contextlib.suppress(Exception):
            await fs.close_session()


async def python_execution_example():
    """Demonstrate Python code execution in Modal sandbox."""
    print("\n🐍 Testing Python code execution...")

    fs = ModalFS(app_name="upathtools-python-demo", timeout=600)

    try:
        await fs.set_session()
        sandbox = await fs._get_sandbox()

        # Create a Python script
        script_path = "/tmp/demo_script.py"
        script_content = """
import os
import sys
import json

# Create some demo data
data = {
    "message": "Hello from Modal sandbox!",
    "python_version": sys.version,
    "working_directory": os.getcwd(),
    "pid": os.getpid()
}

# Save to JSON file
with open("/tmp/output.json", "w") as f:
    json.dump(data, f, indent=2)

print("✅ Script executed successfully!")
print(f"📁 Working in: {os.getcwd()}")
print(f"🐍 Python version: {sys.version.split()[0]}")
""".encode()

        print("📝 Creating Python script...")
        await fs._pipe_file(script_path, script_content)

        # Execute the script
        print("🚀 Executing Python script in sandbox...")
        process = sandbox.exec("python", script_path, timeout=30)

        # Collect output
        output_lines = []
        for line in process.stdout:
            output_lines.append(line.strip())
            print(f"Output: {line.strip()}")

        # Read the output file
        if await fs._exists("/tmp/output.json"):
            output_content = await fs._cat_file("/tmp/output.json")
            print("📄 Generated output.json:")
            print(output_content.decode())

            # Clean up
            await fs._rm_file("/tmp/output.json")

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

    fs = ModalFS(app_name="upathtools-upload-demo", timeout=600)

    try:
        await fs.set_session()

        # Note: Modal doesn't have direct upload like other platforms
        # We simulate this by writing the content
        remote_path = "/tmp/uploaded_file.txt"
        content = local_file.read_bytes()

        print(f"📤 Uploading content to {remote_path}")
        await fs._pipe_file(remote_path, content)

        # Verify upload
        if await fs._exists(remote_path):
            read_content = await fs._cat_file(remote_path)
            print(f"✅ Upload successful! Content: {read_content.decode()}")

            # Clean up
            await fs._rm_file(remote_path)
        else:
            print("❌ Upload failed!")

    finally:
        await fs.close_session()
        # Clean up local file
        if local_file.exists():
            local_file.unlink()


async def advanced_modal_features():
    """Demonstrate Modal-specific features."""
    print("\n⚡ Testing Modal-specific features...")

    fs = ModalFS(
        app_name="upathtools-advanced-demo",
        timeout=900,  # 15 minutes
        idle_timeout=300,
        workdir="/workspace",  # Set working directory
    )

    try:
        await fs.set_session()
        sandbox = await fs._get_sandbox()

        # Test working directory
        process = sandbox.exec("pwd", timeout=10)
        pwd_output = ""
        for line in process.stdout:
            pwd_output += line.strip()
        print(f"📁 Working directory: {pwd_output}")

        # Create file with Modal's native API for comparison
        print("🔧 Using Modal's native file API...")
        modal_file = sandbox.open("/workspace/native_test.txt", "w")
        await modal_file.write.aio("Content written with native Modal API")
        await modal_file.close.aio()

        # Read it back with our filesystem
        content = await fs._cat_file("/workspace/native_test.txt")
        print(f"📖 Read back via ModalFS: {content.decode()}")

        # Test environment variables (if any)
        process = sandbox.exec("env", timeout=10)
        for env_count, line in enumerate(process.stdout):
            if env_count <= 3:  # Show first 3 env vars  # noqa: PLR2004
                print(f"🌐 Env: {line.strip()}")

        # Clean up
        await fs._rm_file("/workspace/native_test.txt")
        print("🧹 Cleaned up advanced test files")

    finally:
        await fs.close_session()


async def error_handling_example():
    """Demonstrate error handling patterns."""
    print("\n❌ Testing error handling...")

    fs = ModalFS(app_name="upathtools-error-demo", timeout=600)

    try:
        await fs.set_session()

        # Test file not found
        try:
            await fs._cat_file("/nonexistent/file.txt")
        except FileNotFoundError:
            print("✅ Correctly caught FileNotFoundError")

        # Test directory operations on files
        test_file = "/tmp/test_file_for_errors.txt"
        await fs._pipe_file(test_file, b"test content")

        try:
            await fs._ls(test_file)  # Try to list a file as directory
        except NotADirectoryError, OSError:
            print("✅ Correctly caught directory operation error on file")

        # Test size of nonexistent file
        try:
            await fs._size("/nonexistent/file.txt")
        except FileNotFoundError:
            print("✅ Correctly caught FileNotFoundError for size check")

        # Clean up
        await fs._rm_file(test_file)
        print("🧹 Cleaned up error test files")

    except Exception as e:  # noqa: BLE001
        print(f"⚠️ Unexpected error: {e}")
    finally:
        await fs.close_session()


async def main():
    """Run all examples."""
    print("🌟 ModalFS Example - Modal Sandbox Filesystem Integration\n")

    try:
        await basic_file_operations()
        await sync_operations_example()
        await python_execution_example()
        await file_upload_example()
        await advanced_modal_features()
        await error_handling_example()

        print("\n🎉 All examples completed successfully!")
        print("\n💡 Note: Some features like file metadata (size, mtime) are")
        print("   currently limited due to Modal's Alpha filesystem API.")
        print("   These will be enhanced as Modal's API evolves.")

    except ImportError as e:
        print(f"❌ Missing dependency: {e}")
        print("💡 Install with: pip install modal")

    except Exception as e:  # noqa: BLE001
        print(f"❌ Error running examples: {e}")
        print("💡 Make sure your Modal credentials are configured")
        print("💡 Run: modal setup")


if __name__ == "__main__":
    asyncio.run(main())
