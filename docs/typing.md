# Type Safety

UPathTools significantly improves type safety over standard fsspec by using Python's generic types and TypedDict for structured file information.

## The Problem with fsspec

Standard fsspec filesystems return untyped dictionaries:

```python
import fsspec

fs = fsspec.filesystem("file")
info = fs.info("/tmp/file.txt")
# info: dict[str, Any] - no type information!

# No IDE autocomplete, no type checking
size = info["size"]  # Could be anything
name = info["name"]  # Might not even exist
```

## The UPathTools Solution

UPathTools uses generic type parameters to provide full type safety:

```python
from typing import TypedDict
from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath

class MyInfoDict(TypedDict):
    name: str
    size: int
    type: str
    modified: float

class MyPath(BaseUPath[MyInfoDict]):
    """Type-safe path with known info structure."""
    pass

class MyFileSystem(BaseAsyncFileSystem[MyPath, MyInfoDict]):
    """Type-safe filesystem with structured info."""
    protocol = "my"
    upath_cls = MyPath
```

## Generic Base Classes

### BaseAsyncFileSystem[TPath, TInfoDict]

The async filesystem base class takes two generic parameters:

```python
from upathtools.filesystems.base import BaseAsyncFileSystem

class MyFS[TPath: UPath, TInfoDict = dict[str, Any]](BaseAsyncFileSystem):
    """
    TPath: The UPath subclass for this filesystem
    TInfoDict: The structure of info dictionaries
    """
    
    upath_cls: type[TPath]
    
    async def _ls(self, path: str, detail: bool = True) -> list[TInfoDict]:
        # Return typed info dictionaries
        ...
```

### BaseFileSystem[TPath, TInfoDict]

The sync variant works identically:

```python
from upathtools.filesystems.base import BaseFileSystem

class MySyncFS[TPath: UPath, TInfoDict = dict[str, Any]](BaseFileSystem):
    upath_cls: type[TPath]
```

### BaseUPath[TInfoDict]

Custom UPath classes can specify their info structure:

```python
from upathtools.filesystems.base import BaseUPath

class MyPath(BaseUPath[MyInfoDict]):
    """Path with type-safe info access."""
    
    def info(self) -> MyInfoDict:
        # Returns typed dictionary
        return super().info()
```

## TypedDict for Info Structures

Define file information schemas using TypedDict:

```python
from typing import TypedDict, Literal

class FileInfo(TypedDict):
    """Standard file information."""
    name: str
    size: int
    type: Literal["file", "directory"]
    mtime: float

class ExtendedInfo(FileInfo):
    """Extended information with additional fields."""
    permissions: str
    owner: str
    mime_type: str
```

## Type-Safe Operations

### Listing with Detail

The `detail` parameter is properly overloaded:

```python
from upathtools.filesystems.base import BaseAsyncFileSystem
from typing import overload, Literal

class MyFS(BaseAsyncFileSystem[MyPath, MyInfoDict]):
    
    @overload
    async def _ls(
        self, 
        path: str, 
        detail: Literal[True] = ...
    ) -> list[MyInfoDict]: ...
    
    @overload
    async def _ls(
        self, 
        path: str, 
        detail: Literal[False]
    ) -> list[str]: ...
    
    async def _ls(
        self, 
        path: str, 
        detail: bool = True
    ) -> list[MyInfoDict] | list[str]:
        """Type-safe listing with proper return types."""
        ...
```

Usage:

```python
# Type checker knows this returns list[MyInfoDict]
detailed = await fs._ls("/path", detail=True)
for info in detailed:
    print(info["name"])  # Typed access

# Type checker knows this returns list[str]
names = await fs._ls("/path", detail=False)
for name in names:
    print(name.upper())  # String methods
```

### Glob with Detail

Similarly for glob operations:

```python
@overload
async def _glob(
    self,
    path: str,
    maxdepth: int | None = None,
    *,
    detail: Literal[False] = False,
    **kwargs: Any,
) -> list[str]: ...

@overload
async def _glob(
    self,
    path: str,
    maxdepth: int | None = None,
    *,
    detail: Literal[True],
    **kwargs: Any,
) -> dict[str, TInfoDict]: ...

async def _glob(
    self,
    path: str,
    maxdepth: int | None = None,
    *,
    detail: bool = False,
    **kwargs: Any,
) -> list[str] | dict[str, TInfoDict]:
    """Type-safe glob with proper return types."""
    ...
```

## AsyncUPath Type Safety

The `get_upath()` method is also type-safe:

```python
from typing import overload, Literal
from upathtools.async_upath import AsyncUPath

class MyFS(BaseAsyncFileSystem[MyPath, MyInfoDict]):
    
    @overload
    def get_upath(
        self, 
        path: str | None = None, 
        *, 
        as_async: Literal[True]
    ) -> AsyncUPath: ...
    
    @overload
    def get_upath(
        self, 
        path: str | None = None, 
        *, 
        as_async: Literal[False] = False
    ) -> MyPath: ...
    
    def get_upath(
        self, 
        path: str | None = None, 
        *, 
        as_async: bool = False
    ) -> MyPath | AsyncUPath:
        """Get typed path object."""
        ...
```

Usage:

```python
# Type: MyPath
sync_path = fs.get_upath("/path", as_async=False)

# Type: AsyncUPath
async_path = fs.get_upath("/path", as_async=True)
```

## Real-World Example

Here's a complete example with full type safety:

```python
from typing import TypedDict, Literal
from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath

# 1. Define info structure
class PackageInfo(TypedDict):
    name: str
    version: str
    size: int
    type: Literal["package", "module", "file"]
    path: str

# 2. Create typed path class
class PackagePath(BaseUPath[PackageInfo]):
    """Path in a Python package."""
    
    def get_version(self) -> str:
        """Type-safe access to version info."""
        info = self.info()
        return info["version"]  # Type checker knows this exists

# 3. Create typed filesystem
class PackageFS(BaseAsyncFileSystem[PackagePath, PackageInfo]):
    protocol = "pkg"
    upath_cls = PackagePath
    
    async def _ls(
        self, 
        path: str, 
        detail: bool = True
    ) -> list[PackageInfo] | list[str]:
        """List package contents with typed info."""
        if detail:
            return [
                PackageInfo(
                    name="module.py",
                    version="1.0.0",
                    size=1024,
                    type="module",
                    path="/pkg/module.py"
                )
            ]
        return ["/pkg/module.py"]

# 4. Usage with full type safety
async def main():
    fs = PackageFS()
    
    # Type: list[PackageInfo]
    items = await fs._ls("/", detail=True)
    
    # IDE autocomplete and type checking work
    for item in items:
        print(f"{item['name']}: {item['version']}")
    
    # Type: PackagePath
    path = fs.get_upath("/module.py")
    version = path.get_version()  # Type-safe method
```

## Introspection

Get info field names at runtime:

```python
class MyFS(BaseAsyncFileSystem[MyPath, MyInfoDict]):
    pass

# Get field names from TypedDict
fields = MyFS.get_info_fields()
# Returns: ["name", "size", "type", "modified"]
```

## Helper Type Conversions

### to_upath()

Convert various path types with type safety:

```python
from upathtools.helpers import to_upath
from typing import overload, Literal

@overload
def to_upath(path: str, as_async: Literal[True]) -> AsyncUPath: ...

@overload
def to_upath(path: str, as_async: Literal[False]) -> UPath: ...

# Usage
regular = to_upath("/path")  # Type: UPath
async_path = to_upath("/path", as_async=True)  # Type: AsyncUPath
```

## Benefits

### 1. IDE Autocomplete

```python
info = path.info()
# IDE shows: name, size, type, modified
info["n"]  # Autocomplete suggests "name"
```

### 2. Type Checking

```python
info: MyInfoDict = path.info()
# mypy/pyright verify structure

size: int = info["size"]  # ✓ Correct
size: str = info["size"]  # ✗ Type error
```

### 3. Refactoring Safety

```python
# Rename field in TypedDict
class MyInfoDict(TypedDict):
    file_name: str  # Was: name
    size: int

# All usages are caught by type checker
info["name"]  # ✗ Error: "name" doesn't exist
info["file_name"]  # ✓ Correct
```

### 4. Documentation

TypedDict serves as documentation:

```python
class S3Info(TypedDict):
    """S3 object information.
    
    Fields:
        name: Object key
        size: Size in bytes
        etag: Entity tag
        last_modified: Last modification time
        storage_class: S3 storage class
    """
    name: str
    size: int
    etag: str
    last_modified: float
    storage_class: str
```

## Best Practices

1. **Always define TypedDict for custom filesystems**
   ```python
   class MyInfo(TypedDict):
       # Define all fields
       ...
   ```

2. **Use Literal for constrained values**
   ```python
   type: Literal["file", "dir", "link"]
   ```

3. **Inherit from existing TypedDicts when extending**
   ```python
   class ExtendedInfo(BaseInfo):
       extra_field: str
   ```

4. **Use overloads for detail parameters**
   ```python
   @overload
   async def _ls(self, path: str, detail: Literal[True]) -> list[TInfoDict]: ...
   ```

5. **Document TypedDict fields**
   ```python
   class MyInfo(TypedDict):
       name: str  # File name without path
       size: int  # Size in bytes
   ```

## Migration from fsspec

Converting existing fsspec code:

```python
# Before (fsspec)
fs = fsspec.filesystem("file")
info = fs.info("/path")  # dict[str, Any]
size = info.get("size", 0)  # Defensive coding required

# After (upathtools)
class FileInfo(TypedDict):
    name: str
    size: int
    type: str

fs = MyFileSystem()
info = await fs._info("/path")  # FileInfo
size = info["size"]  # Type-safe, no defensive coding needed
```
