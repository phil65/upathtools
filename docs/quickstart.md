# Quick Start

Get up and running with UPathTools in minutes.

## Installation

```bash
pip install upathtools
```

With optional dependencies:

```bash
# HTTP support
pip install upathtools[httpx]

# All extras
pip install upathtools[all]
```

## Basic Usage

### AsyncUPath - Async File Operations

```python
from upathtools import AsyncUPath
import asyncio

async def main():
    # Read a file asynchronously
    path = AsyncUPath("https://example.com/data.json")
    content = await path.aread_text()
    
    # Write asynchronously
    output = AsyncUPath("/tmp/output.txt")
    await output.awrite_text("Hello, async!")
    
    # Iterate directory
    async for item in AsyncUPath("/tmp").aiterdir():
        if await item.ais_file():
            print(item)

asyncio.run(main())
```

### Batch File Operations

```python
from upathtools import read_folder, list_files
import asyncio

async def main():
    # Read multiple files in parallel
    files = await read_folder(
        "src/",
        pattern="**/*.py",
        load_parallel=True
    )
    
    for filepath, content in files.items():
        print(f"{filepath}: {len(content)} bytes")
    
    # Just list files
    py_files = await list_files(
        "src/",
        pattern="**/*.py",
        recursive=True
    )

asyncio.run(main())
```

### Type-Safe Filesystems

```python
from upathtools.filesystems import ModuleFileSystem

# Browse Python modules as filesystems
fs = ModuleFileSystem(module_name="upathtools")

# List module contents
files = fs.ls("/", detail=True)

# Read file with type safety
path = fs.get_upath("async_upath.py")
code = path.read_text()
```

## Common Patterns

### Convert Paths

```python
from upathtools.helpers import to_upath
from pathlib import Path

# Convert any path type to UPath
path = to_upath("/tmp/file.txt")
path = to_upath(Path("/tmp/file.txt"))

# Get async wrapper
async_path = to_upath("/tmp/file.txt", as_async=True)
```

### Read Single File Async

```python
from upathtools import read_path

async def main():
    # Text mode
    text = await read_path("file.txt", mode="rt")
    
    # Binary mode
    data = await read_path("image.png", mode="rb")
```

### Write Files

```python
from upathtools.helpers import write_file

# Handles parent directory creation automatically
write_file("content", "/deep/nested/path/file.txt")
write_file(b"binary", "/path/to/file.bin")
```

### Copy Files

```python
from upathtools.helpers import copy

# Copy between filesystems
copy("s3://bucket/source.txt", "/tmp/local.txt")

# Copy to directory (preserves name)
copy("/tmp/file.txt", "/backup/")
```

### Find Files

```python
from upathtools.helpers import multi_glob

# Find with multiple patterns
files = multi_glob(
    directory="src/",
    keep_globs=["**/*.py", "**/*.md"],
    drop_globs=["**/test_*.py", "**/__pycache__/**"]
)
```

### Combine Multiple Files

```python
from upathtools import read_folder_as_text

async def main():
    # Create single document from multiple files
    combined = await read_folder_as_text(
        "docs/",
        pattern="**/*.md",
        recursive=True
    )
    print(combined)
```

## Specialized Filesystems

### Browse Python Modules

```python
from upathtools.filesystems import ModuleFileSystem

fs = ModuleFileSystem(module_name="requests")
files = fs.ls("/")
```

### Access SQLite as Filesystem

```python
from upathtools.filesystems import SqliteFileSystem

fs = SqliteFileSystem(db_path="data.db")
tables = fs.ls("/")  # List tables
```

### Parse Python AST

```python
from upathtools.filesystems import PythonAstFileSystem

fs = PythonAstFileSystem(file_path="module.py")
classes = fs.ls("/classes/")
functions = fs.ls("/functions/")
```

### HTTP with Better Typing

```python
from upathtools import register_http_filesystems

# Replace fsspec's HTTP with improved version
register_http_filesystems()

# Now use with UPath
from upath import UPath
path = UPath("https://example.com/data.json")
```

### Aggregate Multiple Sources

```python
from upathtools.filesystems import UnionFileSystem

fs = UnionFileSystem(
    filesystems=[
        ("file:///overlay", {}),  # Higher priority
        ("file:///base", {}),     # Lower priority
    ]
)
```

## Custom Filesystem

```python
from typing import TypedDict
from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath

# Define info structure
class MyInfo(TypedDict):
    name: str
    size: int
    type: str

# Create path class
class MyPath(BaseUPath[MyInfo]):
    pass

# Create filesystem
class MyFS(BaseAsyncFileSystem[MyPath, MyInfo]):
    protocol = "my"
    upath_cls = MyPath
    
    async def _ls(self, path: str, detail: bool = True):
        if detail:
            return [MyInfo(name="file.txt", size=100, type="file")]
        return ["file.txt"]
    
    async def _info(self, path: str):
        return MyInfo(name="file.txt", size=100, type="file")
    
    async def _cat_file(self, path: str):
        return b"content"
```

## Cheat Sheet

### Async Operations

| Operation | Code |
|-----------|------|
| Read text | `await path.aread_text()` |
| Read bytes | `await path.aread_bytes()` |
| Write text | `await path.awrite_text("data")` |
| Write bytes | `await path.awrite_bytes(b"data")` |
| Check exists | `await path.aexists()` |
| Is file | `await path.ais_file()` |
| Is dir | `await path.ais_dir()` |
| Create dir | `await path.amkdir(parents=True)` |
| Remove file | `await path.aunlink()` |
| Iterate dir | `async for item in path.aiterdir()` |
| Glob | `async for match in path.aglob("*.py")` |

### Batch Operations

| Operation | Function |
|-----------|----------|
| Read multiple files | `read_folder(path, pattern="**/*")` |
| List files | `list_files(path, pattern="**/*")` |
| Combine files | `read_folder_as_text(path, pattern="**/*")` |
| Read single | `read_path(path, mode="rt")` |

### Helpers

| Operation | Function |
|-----------|----------|
| Convert to UPath | `to_upath(path)` |
| Get filesystem | `upath_to_fs(path)` |
| Copy file | `copy(src, dst)` |
| Write file | `write_file(content, path)` |
| Multi-glob | `multi_glob(keep_globs=[...])` |
| Clean dir | `clean_directory(path)` |

### Registration

```python
from upathtools import (
    register_all_filesystems,
    register_http_filesystems
)

# Register all
register_all_filesystems()

# Register HTTP only
register_http_filesystems()
```

## Next Steps

- [Async Operations](async.md) - Deep dive into async capabilities
- [Type Safety](typing.md) - Understanding the type system
- [Base Classes](base-classes.md) - Build custom filesystems
- [Helpers](helpers.md) - All utility functions
- [Filesystems](filesystems.md) - Complete filesystem catalog

## Tips

1. **Always use async for I/O operations** - Much faster for remote filesystems
2. **Use `load_parallel=True`** - When reading multiple files
3. **Define TypedDict** - For type-safe custom filesystems
4. **Register filesystems** - For convenient UPath usage
5. **Use helpers** - They handle edge cases and parent directories

## Common Gotchas

### Parent Directory Creation

```python
# Bad - fails if parent doesn't exist
with open("/deep/path/file.txt", "w") as f:
    f.write("content")

# Good - creates parents automatically
from upathtools.helpers import write_file
write_file("content", "/deep/path/file.txt")
```

### Async Context

```python
# Bad - mixing sync and async
path = AsyncUPath("/tmp/file.txt")
content = path.read_text()  # Wrong!

# Good - use async consistently
content = await path.aread_text()
```

### Type Safety

```python
# Bad - untyped info dict
fs = fsspec.filesystem("file")
info = fs.info("/path")  # dict[str, Any]
size = info.get("size", 0)

# Good - typed info dict
class MyInfo(TypedDict):
    name: str
    size: int

fs = MyFileSystem()
info = await fs._info("/path")  # MyInfo
size = info["size"]  # Type-safe!
```
