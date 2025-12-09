# Helpers and Utilities

UPathTools provides various helper functions and utilities to simplify common filesystem operations and path conversions.

## Path Conversion

### to_upath()

Convert various path types to UPath objects:

```python
from upathtools.helpers import to_upath
from pathlib import Path

# From string
path = to_upath("/tmp/file.txt")

# From pathlib.Path
path = to_upath(Path("/tmp/file.txt"))

# From os.PathLike
import os
path = to_upath(os.fspath(Path("/tmp")))

# Already a UPath - returns as-is
from upath import UPath
existing = UPath("/tmp/file.txt")
path = to_upath(existing)
```

**Get AsyncUPath:**

```python
# Get async wrapper
async_path = to_upath("/tmp/file.txt", as_async=True)

# Now use async methods
content = await async_path.aread_text()
```

**Type Safety:**

```python
from typing import reveal_type

# Type checker knows the return type
regular = to_upath("/path")  # UPath
async_p = to_upath("/path", as_async=True)  # AsyncUPath
```

### upath_to_fs()

Extract filesystem from a UPath and optionally root it at the path's directory:

```python
from upathtools.helpers import upath_to_fs
from upath import UPath

# Get filesystem rooted at the path's directory
path = UPath("s3://bucket/folder/subfolder/file.txt")
fs = upath_to_fs(path)
# fs is rooted at "bucket/folder/subfolder/"

# List relative to that directory
files = fs.ls("/")  # Lists contents of bucket/folder/subfolder/

# Get async filesystem
async_fs = upath_to_fs(path, asynchronous=True)
files = await async_fs._ls("/")
```

**Use Cases:**

```python
# Extract filesystem for operations on parent directory
file_path = UPath("s3://bucket/data/2024/file.csv")
fs = upath_to_fs(file_path)

# Work relative to the file's directory
sibling_files = fs.ls("/")  # Files in data/2024/

# Upload more files to same directory
fs.pipe("new_file.csv", b"data")
```

## File Operations

### copy()

Copy files or directories with proper parent directory creation:

```python
from upathtools.helpers import copy
from upath import UPath

# Copy local file
copy("/tmp/source.txt", "/tmp/backup/target.txt")

# Copy between filesystems
source = UPath("s3://bucket/file.txt")
target = UPath("file:///tmp/local.txt")
copy(source, target)

# Copy to directory (preserves filename)
copy("/tmp/file.txt", "/tmp/backup/")

# Overwrite control
copy(source, target, exist_ok=True)   # Allow overwrite
copy(source, target, exist_ok=False)  # Raise error if exists
```

### write_file()

Write content to a file with automatic parent directory creation:

```python
from upathtools.helpers import write_file

# Write text
write_file("Hello, world!", "/tmp/output.txt")

# Write bytes
write_file(b"binary data", "/tmp/output.bin")

# Custom encoding
write_file("Ünïcödé", "/tmp/unicode.txt", encoding="utf-8")

# Error handling
write_file(
    "text with errors",
    "/tmp/output.txt",
    errors="replace"  # or "ignore", "strict", etc.
)
```

**With UPath:**

```python
from upath import UPath

# Write to remote filesystem
write_file("data", UPath("s3://bucket/file.txt"))

# Encoding is automatic based on content type
write_file(b"bytes", path)  # Binary mode
write_file("text", path)    # Text mode with UTF-8
```

### clean_directory()

Remove directory contents but keep the directory:

```python
from upathtools.helpers import clean_directory

# Clean directory (keeps hidden files)
clean_directory("/tmp/cache")

# Remove hidden files too
clean_directory("/tmp/cache", remove_hidden=True)
```

**Use Cases:**

```python
# Clean build directory
clean_directory("build/")

# Clean cache but keep .gitkeep
clean_directory("cache/", remove_hidden=False)
```

## Multi-Glob

### multi_glob()

Find files matching multiple glob patterns with include/exclude:

```python
from upathtools.helpers import multi_glob

# Find Python files
files = multi_glob(
    directory="src/",
    keep_globs=["**/*.py"],
)

# Complex filtering
files = multi_glob(
    directory="project/",
    keep_globs=["**/*.py", "**/*.md"],
    drop_globs=["**/test_*.py", "**/__pycache__/**"],
)

# Returns relative paths
for file in files:
    print(file)  # e.g., "src/module.py"
```

**Default Behavior:**

```python
# Default: include all, exclude .git
files = multi_glob(".")
# Equivalent to:
files = multi_glob(
    ".",
    keep_globs=["**/*"],
    drop_globs=[".git/**/*"]
)
```

**Use Cases:**

```python
# Find all source files except tests
source_files = multi_glob(
    "src/",
    keep_globs=["**/*.py"],
    drop_globs=["**/test_*.py", "**/*_test.py"]
)

# Find documentation
docs = multi_glob(
    keep_globs=["**/*.md", "**/*.rst"],
    drop_globs=["**/node_modules/**", "**/venv/**"]
)
```

## Async Batch Operations

See [Async Operations](async.md) for detailed documentation on:

- `read_folder()` - Read multiple files in parallel
- `read_folder_as_text()` - Combine files into single document
- `list_files()` - List files matching patterns
- `read_path()` - Read single file asynchronously

## Type Utilities

### Common Types

```python
from upathtools.common_types import VFSPathLike, AnyPathLike, AnyPath

# Protocol for virtual filesystem paths
class MyVFSPath:
    def __vfspath__(self) -> str:
        return "/virtual/path"

# Type annotations
def process_path(path: AnyPath) -> None:
    """Accept str, PathLike, or VFSPathLike."""
    ...
```

## Pydantic Integration

### UPathType

Use UPath in Pydantic models:

```python
from upathtools.pydantic_type import UPathType
from pydantic import BaseModel

class Config(BaseModel):
    input_file: UPathType
    output_dir: UPathType

# Validate and convert to UPath
config = Config(
    input_file="s3://bucket/input.txt",
    output_dir="/tmp/output/"
)

# Access as UPath
assert isinstance(config.input_file, UPath)
```

## Directory Trees

### get_directory_tree()

Generate visual directory tree representations (used internally by filesystems):

```python
from upathtools.filetree import get_directory_tree
from upath import UPath

path = UPath("/project")
tree = get_directory_tree(
    path,
    show_size=True,
    show_icons=True,
    max_depth=3,
    show_date=True
)
print(tree)
```

**Output:**

```
📁 project/
├── 📁 src/
│   ├── 📄 main.py (2.4 KB) 2024-01-15
│   └── 📄 utils.py (1.8 KB) 2024-01-14
├── 📁 tests/
│   └── 📄 test_main.py (3.1 KB) 2024-01-15
└── 📄 README.md (5.2 KB) 2024-01-10
```

**Options:**

```python
tree = get_directory_tree(
    path,
    show_hidden=False,           # Hide hidden files
    show_size=True,              # Show file sizes
    show_date=True,              # Show modification dates
    show_permissions=False,      # Show permissions
    show_icons=True,             # Show icons
    max_depth=None,              # Unlimited depth
    include_pattern=r"\.py$",    # Include only .py files
    exclude_pattern=r"__pycache__",  # Exclude __pycache__
    allowed_extensions={".py", ".md"},  # Only these extensions
    hide_empty=True,             # Hide empty directories
    sort_criteria="name",        # Sort by name, size, or date
    reverse_sort=False,          # Reverse sort order
    date_format="%Y-%m-%d",      # Date format string
)
```

## Filesystem Access

### Via Base Classes

Filesystems provide `get_tree()` method:

```python
from upathtools.filesystems import ModuleFileSystem

fs = ModuleFileSystem(module_name="upathtools")
tree = fs.get_tree(
    path="/",
    show_size=True,
    max_depth=2
)
print(tree)
```

## Helper Patterns

### Safe File Writing

```python
from upathtools.helpers import write_file, to_upath

def safe_write(content: str, path: str) -> None:
    """Write with error handling."""
    try:
        write_file(content, path)
    except PermissionError:
        print(f"Permission denied: {path}")
    except OSError as e:
        print(f"OS error: {e}")
```

### Batch Copying

```python
from upathtools.helpers import copy, multi_glob

def copy_source_files(src_dir: str, dst_dir: str) -> None:
    """Copy all source files to destination."""
    files = multi_glob(
        src_dir,
        keep_globs=["**/*.py"],
        drop_globs=["**/test_*.py"]
    )
    
    for file in files:
        src = f"{src_dir}/{file}"
        dst = f"{dst_dir}/{file}"
        copy(src, dst)
```

### Filesystem Conversion

```python
from upathtools.helpers import to_upath, upath_to_fs

def get_parent_fs(file_path: str):
    """Get filesystem rooted at file's parent directory."""
    path = to_upath(file_path)
    parent = path.parent
    return upath_to_fs(parent, asynchronous=True)
```

### Type-Safe Path Handling

```python
from upathtools.helpers import to_upath
from typing import Union
import os

def process_any_path(path: Union[str, os.PathLike, UPath]) -> str:
    """Handle various path types safely."""
    upath = to_upath(path)
    return upath.read_text()
```

## Best Practices

1. **Use to_upath() for path normalization**
   ```python
   def my_function(path: str | UPath):
       path = to_upath(path)  # Now guaranteed to be UPath
   ```

2. **Use write_file() instead of manual open()**
   ```python
   # Good - handles parent directories
   write_file(content, path)
   
   # Avoid - might fail if parent doesn't exist
   with open(path, "w") as f:
       f.write(content)
   ```

3. **Use multi_glob() for complex file finding**
   ```python
   # Better than manual filtering
   files = multi_glob(
       keep_globs=["**/*.py"],
       drop_globs=["**/test_*.py"]
   )
   ```

4. **Use upath_to_fs() for directory operations**
   ```python
   # Get filesystem for directory-level operations
   fs = upath_to_fs(path.parent)
   all_files = fs.ls("/")
   ```

5. **Prefer async variants for I/O**
   ```python
   # Use async operations for better performance
   async def main():
       files = await read_folder("data/", load_parallel=True)
   ```

## See Also

- [Async Operations](async.md) - Async batch operations
- [Base Classes](base-classes.md) - Building custom filesystems
- [Type Safety](typing.md) - Type system details