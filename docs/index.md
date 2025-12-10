# UPathTools

**UPathTools** is a Python library that extends [UPath](https://github.com/fsspec/universal_pathlib) and [fsspec](https://filesystem-spec.readthedocs.io/) with powerful async capabilities, improved type safety, and a collection of specialized filesystem implementations.

## Key Features

### 🚀 Async-First Design
- **AsyncUPath**: Fully async path operations with `aread_bytes()`, `awrite_text()`, `aiterdir()`, etc.
- **Async filesystem operations**: Batch file reading with configurable parallelism
- **Automatic fallback**: Thread-pool execution for filesystems without native async support

### 🎯 Type Safety
- **Generic filesystem base classes** with typed info dictionaries / UPath subclasses
- **Type-safe path classes** specific to each filesystem
- **Proper overloads** for detail/non-detail variants of operations

### 📦 Specialized Filesystems
- **Virtual filesystems**: Browse Python AST, module structures, SQLite databases
- **Developer tools**: CLI command output, Git repositories, package metadata
- **Aggregation**: Filesystem wrappers to compose multiple filesystems into one
- **Remote**: GitHub Gists / Issues / Wiki, Notion, Linear and multiple other remote filesystems

### 🛠️ Utility Functions
- **Async batch operations**: `read_folder()`, `list_files()`, `read_folder_as_text()`
- **Path helpers**: `to_upath()`, `upath_to_fs()`, type conversions
- **Tree visualization**: Rich directory tree rendering

## Quick Start

### Basic Usage

```python
from upathtools import AsyncUPath
import asyncio

async def main():
    # Read a file asynchronously
    path = AsyncUPath("https://example.com/data.json")
    content = await path.aread_text()
    
    # Iterate directory contents
    async for item in path.aiterdir():
        print(item)

asyncio.run(main())
```

### Batch File Operations

```python
from upathtools import read_folder
import asyncio

async def main():
    # Read all Python files in parallel
    files = await read_folder(
        "src/",
        pattern="**/*.py",
        load_parallel=True,
        chunk_size=50
    )
    
    for filepath, content in files.items():
        print(f"{filepath}: {len(content)} bytes")

asyncio.run(main())
```

### Type-Safe Filesystem Access

```python
from upathtools.filesystems import ModuleFileSystem, ModulePath

# Browse a Python module as a filesystem
fs = ModuleFileSystem(module_name="upathtools")
path: ModulePath = fs.get_upath("async_upath.py")

# Typed info dictionaries
info = path.info()
# info has type-safe fields specific to ModuleFileSystem
```

### Generic base classes with full typing

UPathTools provides:

```python
from upathtools.filesystems.base import BaseAsyncFileSystem

class MyFileSystem[MyPath, MyInfoDict](BaseAsyncFileSystem):
    """Fully typed filesystem with custom info structure."""
    
    upath_cls = MyPath
    
    async def _ls(self, path: str, detail: bool = True) -> list[MyInfoDict]:
        # Return typed info dictionaries
        ...
```

## Installation

```bash
uv add upathtools[httpx]

# All optional dependencies
uv add upathtools[all]
```

## Extras

/// mknodes
{{ "extras"| MkDependencyGroups }}
///


## Package Overview

/// mknodes
{{ "src/upathtools/"| MkTreeView(exclude_patterns=["*.pyc", "__pycache__"]) }}
///


## Dependencies

/// mknodes
{{ "upathtools"| MkDependencyTable }}
///

## License

MIT License - see [LICENSE](https://github.com/phil65/upathtools/blob/main/LICENSE) for details.


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
