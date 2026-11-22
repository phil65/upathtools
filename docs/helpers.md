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

Get the filesystem of a path, rooted at that path (via `DirFileSystem`).
By default an async filesystem is returned:

```python
from upathtools.helpers import upath_to_fs
from upath import UPath

path = UPath("s3://bucket/folder/subfolder")

# Async filesystem (default), rooted at "bucket/folder/subfolder"
async_fs = upath_to_fs(path)
files = await async_fs._ls("/")

# Sync filesystem
fs = upath_to_fs(path, asynchronous=False)
files = fs.ls("/")  # Lists contents of bucket/folder/subfolder/
```

**Use Cases:**

```python
# Work relative to a directory
fs = upath_to_fs(UPath("s3://bucket/data/2024"), asynchronous=False)
files = fs.ls("/")  # Files in data/2024/

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

# Text is always written as UTF-8
write_file("Ünïcödé", "/tmp/unicode.txt")

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

### UPath field types

Use UPath in Pydantic models (`upathtools.pydantic_type`):

- `UPath`: converts strings to `upath.UPath`, serializes to `str` in JSON mode
- `UPathExists`, `UPathFile`, `UPathDir`: additionally validate existence / type

```python
import upath
from pydantic import BaseModel

from upathtools.pydantic_type import UPath, UPathDir


class Config(BaseModel):
    input_file: UPath
    output_dir: UPathDir  # must be an existing directory


config = Config(input_file="s3://bucket/input.txt", output_dir="/tmp")

assert isinstance(config.input_file, upath.UPath)
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
import re

tree = get_directory_tree(
    path,
    show_hidden=False,           # Hide hidden files
    show_size=True,              # Show file sizes
    show_date=True,              # Show modification dates
    show_permissions=False,      # Show permissions
    show_icons=True,             # Show icons
    max_depth=None,              # Unlimited depth
    include_pattern=None,        # Compiled regex: only include matching entries
    exclude_pattern=re.compile(r"__pycache__"),  # Compiled regex: exclude matches
    allowed_extensions={".py", ".md"},  # Only these extensions
    hide_empty=False,            # Hide empty directories
    sort_criteria="name",        # Sort by name, size, or date
    reverse_sort=False,          # Reverse sort order
    date_format="%Y-%m-%d",      # Date format string
)
```

## Filesystem Access

### Via Base Classes

Filesystems provide `get_tree()` method:

```python

fs = SomeFileSystem(...)
tree = fs.get_tree(
    path="/",
    show_size=True,
    max_depth=2
)
print(tree)
```

## Helpers

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
