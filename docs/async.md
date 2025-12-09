# Async Operations

UPathTools provides comprehensive async/await support for filesystem operations, enabling efficient concurrent I/O and better performance for network-based filesystems.

## AsyncUPath

`AsyncUPath` is a wrapper around regular `UPath` objects that provides async versions of all I/O methods, prefixed with `a`.

### Creating AsyncUPath Objects

```python
from upathtools import AsyncUPath
import asyncio

async def main():
    # From a URL
    path = AsyncUPath("https://example.com/data.json")
    
    # From a local path
    local = AsyncUPath("/tmp/file.txt")
    
    # From an existing UPath
    from upath import UPath
    regular_path = UPath("s3://bucket/key")
    async_path = AsyncUPath._from_upath(regular_path)

asyncio.run(main())
```

### File Reading

```python
async def read_examples():
    path = AsyncUPath("https://example.com/data.json")
    
    # Read as bytes
    data: bytes = await path.aread_bytes()
    
    # Read as text
    text: str = await path.aread_text(encoding="utf-8")
    
    # Read with custom encoding
    content = await path.aread_text(
        encoding="latin-1",
        errors="ignore"
    )
```

### File Writing

```python
async def write_examples():
    path = AsyncUPath("s3://bucket/output.txt")
    
    # Write bytes
    await path.awrite_bytes(b"binary data")
    
    # Write text
    await path.awrite_text("Hello, world!", encoding="utf-8")
```

### Path Operations

```python
async def path_operations():
    path = AsyncUPath("/tmp/example")
    
    # Check existence
    exists: bool = await path.aexists()
    
    # Check if file or directory
    is_file: bool = await path.ais_file()
    is_dir: bool = await path.ais_dir()
    
    # Create directory
    await path.amkdir(parents=True, exist_ok=True)
    
    # Touch file
    await path.atouch(exist_ok=True)
    
    # Remove file
    await path.aunlink(missing_ok=True)
    
    # Remove directory
    await path.armdir()
    
    # Get file stats
    stat = await path.astat()
    print(f"Size: {stat.st_size} bytes")
```

### Directory Iteration

```python
async def iterate_directory():
    path = AsyncUPath("/tmp")
    
    # Iterate over directory contents
    async for item in path.aiterdir():
        print(f"Found: {item}")
        if await item.ais_file():
            content = await item.aread_text()
```

### Globbing

```python
async def glob_examples():
    path = AsyncUPath("/project")
    
    # Find all Python files
    async for py_file in path.aglob("**/*.py"):
        print(py_file)
    
    # Recursive glob with pattern
    async for match in path.arglob("*.json"):
        data = await match.aread_bytes()
```

### File Operations

```python
async def file_operations():
    source = AsyncUPath("/tmp/source.txt")
    target = AsyncUPath("/tmp/target.txt")
    
    # Copy file
    copied = await source.acopy(target)
    
    # Move file
    moved = await source.amove(target)
    
    # Open file
    async with await source.aopen("r") as f:
        content = await f.read()
```

## Batch Operations

UPathTools provides high-level async functions for efficient batch file operations.

### read_folder()

Read multiple files concurrently:

```python
from upathtools import read_folder
import asyncio

async def main():
    # Read all Python files in parallel
    files = await read_folder(
        "src/",
        pattern="**/*.py",
        recursive=True,
        load_parallel=True,
        chunk_size=50,  # Process 50 files at a time
        mode="rt",
        encoding="utf-8"
    )
    
    # files is a dict mapping relative paths to contents
    for filepath, content in files.items():
        print(f"{filepath}: {len(content)} characters")

asyncio.run(main())
```

**Parameters:**

- `path`: Base directory to read from
- `pattern`: Glob pattern (default: `"**/*"`)
- `recursive`: Search subdirectories (default: `True`)
- `include_dirs`: Include directories in results (default: `False`)
- `exclude`: List of patterns to exclude
- `max_depth`: Maximum directory depth
- `mode`: Read mode (`"rt"` for text, `"rb"` for binary)
- `encoding`: Text encoding (default: `"utf-8"`)
- `load_parallel`: Load files concurrently (default: `True`)
- `chunk_size`: Files per batch (default: `50`)

### list_files()

List files matching a pattern without reading content:

```python
from upathtools import list_files

async def main():
    # Get list of Python files
    files = await list_files(
        "src/",
        pattern="**/*.py",
        recursive=True,
        exclude=["**/test_*.py", "**/__pycache__/**"]
    )
    
    # files is a list of UPath objects
    for file in files:
        print(file)
```

With detailed info:

```python
async def main():
    # Get detailed file information
    file_infos = await list_files(
        "src/",
        pattern="**/*.py",
        detail=True
    )
    
    # file_infos is a list of info dictionaries
    for info in file_infos:
        print(f"{info['name']}: {info['size']} bytes")
```

### read_folder_as_text()

Combine multiple files into a single text document:

```python
from upathtools import read_folder_as_text

async def main():
    # Create a concatenated document
    text = await read_folder_as_text(
        "docs/",
        pattern="**/*.md",
        recursive=True,
        load_parallel=True
    )
    
    # Output format:
    # # Content of docs/intro.md
    # 
    # Introduction text...
    # 
    # 
    # # Content of docs/guide.md
    # 
    # Guide text...
    print(text)
```

### read_path()

Read a single file asynchronously:

```python
from upathtools import read_path

async def main():
    # Read as text
    text = await read_path("file.txt", mode="rt", encoding="utf-8")
    
    # Read as bytes
    data = await read_path("image.png", mode="rb")
```

## Async Filesystem Access

Custom filesystems inheriting from `BaseAsyncFileSystem` have native async support:

```python
from upathtools.filesystems import HTTPFileSystem

async def main():
    fs = HTTPFileSystem()
    
    # Async operations
    content = await fs._cat_file("https://example.com/data.json")
    files = await fs._ls("https://example.com/", detail=True)
    exists = await fs._exists("https://example.com/file.txt")
    
    # Get async UPath
    path = fs.get_upath("https://example.com/", as_async=True)
    async for item in path.aiterdir():
        print(item)
```

## BaseUPath with Async Methods

Custom UPath classes can inherit from `BaseUPath` to get async methods:

```python
from upathtools.filesystems.base import BaseUPath

class MyPath(BaseUPath):
    """Custom path with async support."""
    pass

async def main():
    path = MyPath("/some/path")
    
    # All async methods available
    content = await path.aread_text()
    async for item in path.aiterdir():
        print(item)
```

## Performance Considerations

### Parallel vs Sequential

For remote filesystems or many files, parallel loading provides significant speedups:

```python
# Sequential (slow for many files)
files = await read_folder("s3://bucket/", load_parallel=False)

# Parallel (much faster)
files = await read_folder("s3://bucket/", load_parallel=True, chunk_size=100)
```

### Chunk Size Tuning

The `chunk_size` parameter controls memory usage vs speed:

```python
# Small chunks: Lower memory, more overhead
files = await read_folder("data/", chunk_size=10)

# Large chunks: Higher memory, less overhead
files = await read_folder("data/", chunk_size=200)

# Balance for most cases
files = await read_folder("data/", chunk_size=50)
```

### Async Context Managers

Use async context managers for file operations:

```python
async def safe_file_operations():
    path = AsyncUPath("/tmp/data.txt")
    
    # Proper resource cleanup
    async with await path.aopen("r") as f:
        content = await f.read()
    # File is automatically closed
```

## Fallback Behavior

For filesystems without native async support, AsyncUPath automatically falls back to running operations in thread pools:

```python
async def main():
    # Local filesystem has no native async
    path = AsyncUPath("/tmp/file.txt")
    
    # Automatically runs in thread pool
    content = await path.aread_text()
```

This ensures all operations work consistently regardless of the underlying filesystem implementation.

## Error Handling

```python
import asyncio
from upathtools import AsyncUPath

async def robust_reading():
    path = AsyncUPath("https://example.com/might-not-exist.txt")
    
    try:
        content = await path.aread_text()
    except FileNotFoundError:
        print("File not found")
    except PermissionError:
        print("Permission denied")
    except asyncio.TimeoutError:
        print("Operation timed out")
    except Exception as e:
        print(f"Unexpected error: {e}")
```

## Advanced Usage

### Custom Async Filesystem

Create filesystems with async operations:

```python
from upathtools.filesystems.base import BaseAsyncFileSystem
from typing import TypedDict

class MyInfoDict(TypedDict):
    name: str
    size: int
    type: str

class MyFileSystem(BaseAsyncFileSystem[MyPath, MyInfoDict]):
    protocol = "my"
    upath_cls = MyPath
    
    async def _ls(self, path: str, detail: bool = True) -> list[MyInfoDict]:
        # Native async implementation
        async with aiohttp.ClientSession() as session:
            response = await session.get(f"{path}/list")
            data = await response.json()
            return data
```

### Concurrent Operations

Combine multiple async operations:

```python
async def concurrent_operations():
    paths = [
        AsyncUPath(f"https://example.com/file{i}.txt")
        for i in range(10)
    ]
    
    # Read all files concurrently
    contents = await asyncio.gather(*[p.aread_text() for p in paths])
    
    # Process results
    for path, content in zip(paths, contents):
        print(f"{path}: {len(content)} characters")
```

## Best Practices

1. **Use async consistently**: Don't mix sync and async operations in the same context
2. **Tune chunk_size**: Balance memory usage and performance based on file sizes
3. **Handle errors gracefully**: Network operations can fail, always use try/except
4. **Use context managers**: For file operations, use `async with` for proper cleanup
5. **Monitor concurrency**: Too much parallelism can overwhelm systems or APIs