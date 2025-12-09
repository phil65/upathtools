# Base Classes

UPathTools provides generic base classes for building type-safe, async-capable filesystems with proper typing and UPath integration.

## Overview

The base classes form a foundation for creating custom filesystems:

- **`BaseAsyncFileSystem[TPath, TInfoDict]`**: Async filesystem with typed paths and info dicts
- **`BaseFileSystem[TPath, TInfoDict]`**: Sync filesystem with typed paths and info dicts
- **`BaseUPath[TInfoDict]`**: UPath subclass with async methods and typed info
- **`WrapperFileSystem`**: Base for filesystems that wrap another filesystem

## BaseAsyncFileSystem

The primary base class for async filesystems.

### Generic Parameters

```python
from upathtools.filesystems.base import BaseAsyncFileSystem
from typing import TypedDict

class MyInfoDict(TypedDict):
    name: str
    size: int
    type: str

class MyPath(BaseUPath[MyInfoDict]):
    pass

class MyFileSystem(BaseAsyncFileSystem[MyPath, MyInfoDict]):
    """
    TPath: Your custom UPath subclass
    TInfoDict: Structure of info dictionaries returned by this filesystem
    """
    protocol = "my"
    upath_cls = MyPath
```

### Required Methods

At minimum, implement these async methods:

```python
class MyFileSystem(BaseAsyncFileSystem[MyPath, MyInfoDict]):
    protocol = "my"
    upath_cls = MyPath
    
    async def _ls(
        self, 
        path: str, 
        detail: bool = True
    ) -> list[MyInfoDict] | list[str]:
        """List directory contents."""
        if detail:
            return [
                MyInfoDict(name="file.txt", size=100, type="file")
            ]
        return ["file.txt"]
    
    async def _info(self, path: str, **kwargs) -> MyInfoDict:
        """Get file information."""
        return MyInfoDict(name="file.txt", size=100, type="file")
    
    async def _cat_file(self, path: str, **kwargs) -> bytes:
        """Read file contents."""
        return b"file contents"
```

### Optional Methods

Override these for custom behavior:

```python
class MyFileSystem(BaseAsyncFileSystem[MyPath, MyInfoDict]):
    
    async def _exists(self, path: str, **kwargs) -> bool:
        """Check if path exists."""
        try:
            await self._info(path)
            return True
        except FileNotFoundError:
            return False
    
    async def _isfile(self, path: str) -> bool:
        """Check if path is a file."""
        info = await self._info(path)
        return info["type"] == "file"
    
    async def _isdir(self, path: str) -> bool:
        """Check if path is a directory."""
        info = await self._info(path)
        return info["type"] == "directory"
    
    async def _pipe_file(self, path: str, value: bytes, **kwargs) -> None:
        """Write file contents."""
        # Implementation for writing
        pass
    
    async def _rm_file(self, path: str) -> None:
        """Remove a file."""
        # Implementation for deletion
        pass
    
    async def _makedirs(self, path: str, exist_ok: bool = False) -> None:
        """Create directories."""
        # Implementation for directory creation
        pass
    
    async def _glob(
        self,
        path: str,
        maxdepth: int | None = None,
        detail: bool = False,
        **kwargs,
    ) -> list[str] | dict[str, MyInfoDict]:
        """Glob for files matching pattern."""
        # Implementation for globbing
        pass
```

### Built-in Methods

These methods are provided automatically:

#### get_upath()

Get a UPath object for a path:

```python
fs = MyFileSystem()

# Get regular path
path: MyPath = fs.get_upath("/path/to/file")

# Get async path
async_path: AsyncUPath = fs.get_upath("/path/to/file", as_async=True)
```

#### list_root_async()

List root directory contents:

```python
# Get names only
names = await fs.list_root_async(detail=False)

# Get detailed info
infos = await fs.list_root_async(detail=True)
```

#### get_tree()

Get visual directory tree:

```python
tree = fs.get_tree(
    path="/",
    show_size=True,
    show_icons=True,
    max_depth=3
)
print(tree)
```

#### get_info_fields()

Get field names from TypedDict:

```python
fields = MyFileSystem.get_info_fields()
# Returns: ["name", "size", "type"]
```

## BaseFileSystem

Sync variant of BaseAsyncFileSystem:

```python
from upathtools.filesystems.base import BaseFileSystem

class MySyncFS(BaseFileSystem[MyPath, MyInfoDict]):
    protocol = "mysync"
    upath_cls = MyPath
    
    def _ls(
        self, 
        path: str, 
        detail: bool = True
    ) -> list[MyInfoDict] | list[str]:
        """Sync listing."""
        if detail:
            return [MyInfoDict(name="file.txt", size=100, type="file")]
        return ["file.txt"]
    
    def _info(self, path: str, **kwargs) -> MyInfoDict:
        """Sync info."""
        return MyInfoDict(name="file.txt", size=100, type="file")
```

The API is identical to BaseAsyncFileSystem, but methods are synchronous.

## BaseUPath

Custom UPath class with async methods and typed info.

### Basic Usage

```python
from upathtools.filesystems.base import BaseUPath
from typing import TypedDict

class MyInfoDict(TypedDict):
    name: str
    size: int
    custom_field: str

class MyPath(BaseUPath[MyInfoDict]):
    """Custom path with typed info."""
    
    def get_custom_field(self) -> str:
        """Access custom info field."""
        info = self.info()
        return info["custom_field"]
```

### Async Methods

BaseUPath provides all async I/O methods:

```python
path = MyPath("/path/to/file")

# Read operations
content = await path.aread_bytes()
text = await path.aread_text(encoding="utf-8")

# Write operations
await path.awrite_bytes(b"data")
await path.awrite_text("text", encoding="utf-8")

# Path checks
exists = await path.aexists()
is_file = await path.ais_file()
is_dir = await path.ais_dir()

# Directory operations
await path.amkdir(parents=True)
await path.atouch()

# Iteration
async for item in path.aiterdir():
    print(item)

# Globbing
async for match in path.aglob("*.py"):
    print(match)
```

### Name Property Fix

BaseUPath fixes a upath bug with the `name` property:

```python
path = MyPath("relative/path/file.txt")
# Standard UPath incorrectly treats first char as root
# BaseUPath returns correct name
assert path.name == "file.txt"
```

## WrapperFileSystem

Base class for filesystems that wrap another filesystem.

### Basic Usage

```python
from upathtools.filesystems.base import WrapperFileSystem

class MyWrapper(WrapperFileSystem):
    protocol = "mywrap"
    
    def __init__(self, **kwargs):
        # Wrap an existing filesystem
        super().__init__(
            target_protocol="file",
            target_options={},
            **kwargs
        )
```

### Info Callbacks

Enrich file info with callbacks:

```python
def enrich_info(info: dict, fs: WrapperFileSystem) -> dict:
    """Add custom field to info dict."""
    info["custom"] = "value"
    return info

fs = WrapperFileSystem(
    target_protocol="file",
    info_callback=enrich_info
)

# Info dicts now include custom field
info = await fs._info("/path")
assert info["custom"] == "value"
```

### Batch Info Callbacks

Process multiple info dicts efficiently:

```python
async def enrich_batch(
    infos: list[dict], 
    fs: WrapperFileSystem
) -> list[dict]:
    """Enrich multiple info dicts."""
    # Fetch additional data in batch
    extra_data = await fetch_batch_data([i["name"] for i in infos])
    
    # Enrich each info
    for info, extra in zip(infos, extra_data):
        info["extra"] = extra
    
    return infos

fs = WrapperFileSystem(
    target_protocol="s3",
    ls_info_callback=enrich_batch
)
```

### Async/Sync Callbacks

Callbacks can be sync or async:

```python
# Sync callback
def sync_enrich(info: dict, fs: WrapperFileSystem) -> dict:
    info["sync"] = True
    return info

# Async callback
async def async_enrich(info: dict, fs: WrapperFileSystem) -> dict:
    info["async"] = await fetch_async_data(info["name"])
    return info

# Both work
fs1 = WrapperFileSystem(info_callback=sync_enrich)
fs2 = WrapperFileSystem(info_callback=async_enrich)
```

### Delegation

Most methods are delegated to wrapped filesystem:

```python
fs = WrapperFileSystem(target_protocol="s3")

# These are delegated to S3FileSystem
await fs._cat_file("s3://bucket/key")
await fs._makedirs("s3://bucket/dir")
```

## Complete Example

Here's a complete filesystem implementation:

```python
from typing import TypedDict, Literal
from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath
import aiohttp

# 1. Define info structure
class GitHubInfo(TypedDict):
    name: str
    path: str
    size: int
    type: Literal["file", "dir"]
    sha: str
    url: str

# 2. Create path class
class GitHubPath(BaseUPath[GitHubInfo]):
    """Path in a GitHub repository."""
    
    def get_sha(self) -> str:
        """Get git SHA for this file."""
        return self.info()["sha"]
    
    def get_url(self) -> str:
        """Get GitHub URL."""
        return self.info()["url"]

# 3. Create filesystem
class GitHubFileSystem(BaseAsyncFileSystem[GitHubPath, GitHubInfo]):
    protocol = "gh"
    upath_cls = GitHubPath
    
    def __init__(self, repo: str, token: str | None = None, **kwargs):
        super().__init__(**kwargs)
        self.repo = repo
        self.token = token
        self.base_url = f"https://api.github.com/repos/{repo}/contents"
    
    async def _get_json(self, url: str) -> dict | list:
        """Fetch JSON from GitHub API."""
        headers = {}
        if self.token:
            headers["Authorization"] = f"token {self.token}"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                return await resp.json()
    
    async def _ls(
        self, 
        path: str, 
        detail: bool = True
    ) -> list[GitHubInfo] | list[str]:
        """List repository contents."""
        url = f"{self.base_url}/{path.lstrip('/')}"
        data = await self._get_json(url)
        
        if not isinstance(data, list):
            raise ValueError("Path is not a directory")
        
        if detail:
            return [
                GitHubInfo(
                    name=item["name"],
                    path=item["path"],
                    size=item["size"],
                    type=item["type"],
                    sha=item["sha"],
                    url=item["url"]
                )
                for item in data
            ]
        return [item["path"] for item in data]
    
    async def _info(self, path: str, **kwargs) -> GitHubInfo:
        """Get file information."""
        url = f"{self.base_url}/{path.lstrip('/')}"
        data = await self._get_json(url)
        
        return GitHubInfo(
            name=data["name"],
            path=data["path"],
            size=data["size"],
            type=data["type"],
            sha=data["sha"],
            url=data["url"]
        )
    
    async def _cat_file(self, path: str, **kwargs) -> bytes:
        """Read file contents."""
        info = await self._info(path)
        url = info["url"]
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                data = await resp.json()
                import base64
                return base64.b64decode(data["content"])

# 4. Usage
async def main():
    fs = GitHubFileSystem(repo="owner/repo", token="ghp_xxx")
    
    # List files
    files = await fs._ls("/src", detail=True)
    for info in files:
        print(f"{info['name']}: {info['size']} bytes, sha: {info['sha']}")
    
    # Read file
    content = await fs._cat_file("/README.md")
    
    # Use path
    path = fs.get_upath("/README.md", as_async=True)
    text = await path.aread_text()
    sha = path.get_sha()
```

## Best Practices

1. **Always define TypedDict for info structure**
2. **Implement at minimum: _ls, _info, _cat_file**
3. **Use async for I/O-bound operations**
4. **Provide proper type annotations**
5. **Document your TypedDict fields**
6. **Override _exists, _isfile, _isdir for efficiency**
7. **Implement _glob for better performance**
8. **Use WrapperFileSystem when you just need to enrich info**

## Testing Your Filesystem

```python
import pytest
from upathtools.filesystems.base import BaseAsyncFileSystem

@pytest.mark.asyncio
async def test_my_filesystem():
    fs = MyFileSystem()
    
    # Test listing
    files = await fs._ls("/", detail=True)
    assert len(files) > 0
    
    # Test info
    info = await fs._info("/file.txt")
    assert info["type"] == "file"
    
    # Test reading
    content = await fs._cat_file("/file.txt")
    assert isinstance(content, bytes)
    
    # Test path creation
    path = fs.get_upath("/file.txt")
    assert path.name == "file.txt"
```
