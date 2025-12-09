# UPathTools

**UPathTools** is a Python library that extends [UPath](https://github.com/fsspec/universal_pathlib) and [fsspec](https://filesystem-spec.readthedocs.io/) with powerful async capabilities, improved type safety, and a collection of specialized filesystem implementations.

## Key Features

### 🚀 Async-First Design
- **AsyncUPath**: Fully async path operations with `aread_bytes()`, `awrite_text()`, `aiterdir()`, etc.
- **Async filesystem operations**: Batch file reading with configurable parallelism
- **Automatic fallback**: Thread-pool execution for filesystems without native async support

### 🎯 Better Type Safety
- **Generic filesystem base classes** with typed info dictionaries
- **Type-safe path classes** specific to each filesystem
- **Proper overloads** for detail/non-detail variants of operations

### 📦 Specialized Filesystems
- **Virtual filesystems**: Browse Python AST, module structures, SQLite databases
- **Developer tools**: CLI command output, Git repositories, package metadata
- **Aggregation**: Union and flat-union filesystems for merging multiple sources
- **Remote**: GitHub Gists, wikis, improved HTTP/HTTPS support

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

## Why UPathTools?

### Problem: fsspec lacks async and type safety

Standard fsspec filesystems:
- Return generic `dict[str, Any]` for file info
- No async operation support
- Limited type hints for path operations

### Solution: Generic base classes with full typing

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
pip install upathtools
```

For optional dependencies:

```bash
# HTTP support with httpx
pip install upathtools[httpx]

# All optional dependencies
pip install upathtools[all]
```

## Architecture Overview

```
upathtools/
├── filesystems/
│   ├── base/              # Generic base classes
│   │   ├── basefilesystem.py    # BaseFileSystem[TPath, TInfoDict]
│   │   ├── baseupath.py         # BaseUPath with async methods
│   │   └── wrapper.py           # WrapperFileSystem base
│   ├── file_filesystems/  # Virtual file-like filesystems
│   ├── remote_filesystems/  # HTTP, Git, GitHub, etc.
│   └── ...                # Other specialized implementations
├── async_upath.py         # AsyncUPath wrapper
├── async_ops.py           # Batch async operations
└── helpers.py             # Utility functions
```

## Next Steps

- [**Async Operations**](async.md) - Learn about async capabilities
- [**Type Safety**](typing.md) - Understand the type system improvements
- [**Base Classes**](base-classes.md) - Build your own filesystems
- [**Helpers**](helpers.md) - Utility functions and tools
- [**Filesystems**](filesystems.md) - Overview of available implementations

## License

MIT License - see [LICENSE](https://github.com/phil65/upathtools/blob/main/LICENSE) for details.