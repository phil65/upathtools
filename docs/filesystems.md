# Filesystem Implementations

UPathTools provides a collection of specialized filesystem implementations for various use cases, from virtual filesystems to remote access and aggregation.

## Categories

Filesystems in UPathTools are organized into several categories:

- **File-based**: Virtual filesystems for structured file formats (SQLite, Markdown, Python AST)
- **Remote**: Network-based access (HTTP/HTTPS, GitHub Gists, Wikis)
- **Development**: Tools for developers (CLI commands, module/package browsing, distributions)
- **Aggregation**: Combine multiple filesystems (Union, FlatUnion)
- **Wrappers**: Enhance existing filesystems with additional functionality
- **Delegating**: Automatically delegate to file-based filesystems based on file extensions

## Virtual Filesystems

### ModuleFileSystem

Browse Python module contents as a filesystem:

```python
from upathtools.filesystems import ModuleFileSystem, ModulePath

# Browse a Python module
fs = ModuleFileSystem(module_name="upathtools")

# List module contents
files = fs.ls("/", detail=True)

# Get path to specific file
path: ModulePath = fs.get_upath("async_upath.py")
content = path.read_text()
```

### PackageFileSystem

Access installed Python package contents:

```python
from upathtools.filesystems import PackageFileSystem, PackagePath

# Access package files
fs = PackageFileSystem(package_name="requests")

# Read package metadata
path = fs.get_upath("__init__.py")
code = path.read_text()
```

### DistributionFileSystem

Browse Python distribution metadata:

```python
from upathtools.filesystems import DistributionFileSystem, DistributionPath

# Access distribution info
fs = DistributionFileSystem(distribution_name="upathtools")

# Read metadata
metadata = fs.get_upath("METADATA").read_text()
```


### SqliteFileSystem

Browse SQLite databases as filesystems:

```python
from upathtools.filesystems import SqliteFileSystem, SqlitePath

# Access SQLite database
fs = SqliteFileSystem(db_path="data.db")

# Tables appear as directories
tables = fs.ls("/")

# Query data through filesystem interface
rows = fs.get_upath("/users/").read_text()
```

### MarkdownFileSystem

Navigate Markdown documents by headers:

```python
from upathtools.filesystems import MarkdownFileSystem, MarkdownPath

# Parse Markdown file
fs = MarkdownFileSystem(file_path="README.md")

# Headers become paths
sections = fs.ls("/")
intro = fs.get_upath("/Introduction").read_text()
```

## Remote Filesystems

### HTTPFileSystem (Enhanced)

Improved HTTP/HTTPS support with httpx:

```python
from upathtools.filesystems import HTTPFileSystem, HttpPath

# Better than fsspec's built-in HTTP
fs = HTTPFileSystem()

# Async support
path = fs.get_upath("https://example.com/data.json", as_async=True)
content = await path.aread_text()

# Better error handling and typing
info = await fs._info("https://example.com/file.txt")
```

**Features:**
- Native async support with httpx
- Better type safety
- Improved error messages
- Streaming support

### GistFileSystem

Access GitHub Gists as filesystems:

```python
from upathtools.filesystems import GistFileSystem, GistPath

# Access gist by ID
fs = GistFileSystem(gist_id="abc123...", token="ghp_...")

# List gist files
files = fs.ls("/")

# Read gist content
content = fs.get_upath("/file.py").read_text()
```

### WikiFileSystem

Browse wiki contents:

```python
from upathtools.filesystems import WikiFileSystem, WikiPath

# Access wiki
fs = WikiFileSystem(wiki_url="https://github.com/owner/repo/wiki")

# Browse pages
pages = fs.ls("/")
content = fs.get_upath("/Home").read_text()
```

## Development Tools

### CliFileSystem

Execute CLI commands and access output as files:

```python
from upathtools.filesystems import CliFileSystem, CliPath

# Run commands through filesystem
fs = CliFileSystem()

# Command output as files
output = fs.get_upath("ls -la").read_text()
json_data = fs.get_upath("curl https://api.example.com/data").read_text()
```

**Use Cases:**
- Testing CLI tools
- Scripting automation
- Command output processing

## Delegating Filesystem

### DelegatingFileSystem

Automatically delegates to file-based sub-filesystems based on file extensions and content:

```python
from upathtools.filesystems import DelegatingFileSystem

# Wrap any filesystem with auto-delegation
fs = DelegatingFileSystem(target_protocol="file", target_options={"path": "/data"})

# Access files normally - delegation is automatic
regular_file = fs.cat_file("document.txt")  # Normal file access

# Use :: separator to access internal structure of supported files
markdown_section = fs.cat_file("README.md::Introduction")  # Markdown header
sqlite_table = fs.cat_file("data.db::users")  # SQLite table
python_class = fs.cat_file("module.py::MyClass")  # Python AST node
```

**How it works:**

1. **Extension Detection**: Checks file extension against registered file filesystems
2. **Content Probing**: For ambiguous extensions (e.g., `.json`), probes content to find best match
3. **Automatic Caching**: File filesystem instances are cached for performance
4. **Read-Write Support**: Supports write-back to parent filesystem when possible

**Path Format:**
```
{file_path}::{internal_path}
```

**Examples:**

```python
# Markdown files - access headers as paths
content = fs.cat_file("docs/README.md::Quick Start")

# SQLite databases - access tables and queries
users = fs.cat_file("app.db::users")
user_count = fs.cat_file("app.db::SELECT COUNT(*) FROM users")

# Python modules - browse AST structure
class_def = fs.cat_file("src/main.py::classes/MyClass")
functions = fs.ls("src/main.py::functions/")

# JSON Schema - browse schema structure
properties = fs.ls("schema.json::properties/")
```

**Supported File Types:**
- **Markdown** (`.md`, `.markdown`) → MarkdownFileSystem
- **SQLite** (`.db`, `.sqlite`, `.sqlite3`) → SqliteFileSystem  
- **Python** (`.py`) → PythonAstFileSystem
- **JSON Schema** (`.json`) → JsonSchemaFileSystem
- **OpenAPI** (`.json`, `.yaml`, `.yml`) → OpenAPIFileSystem
- **Tree-sitter** (various) → TreeSitterFileSystem

**Registration:**

```python
from upathtools.filesystems.delegating_fs import register_file_filesystem

@register_file_filesystem
class MyCustomFileSystem(BaseAsyncFileFileSystem):
    supported_extensions = frozenset(["myext"])
    priority = 50  # Lower = higher priority
    
    @classmethod
    def probe_content(cls, content: bytes, extension: str = "") -> ProbeResult:
        # Custom content detection logic
        if content.startswith(b"MYFORMAT"):
            return ProbeResult.SUPPORTED
        return ProbeResult.UNSUPPORTED
```

**Advanced Usage:**

```python
# Custom parent filesystem
from fsspec import filesystem
parent_fs = filesystem("s3", bucket="my-bucket")
fs = DelegatingFileSystem(fs=parent_fs)

# Access remote files with delegation
data = fs.cat_file("s3://my-bucket/data.db::table_name")

# Clear cache when files change
fs.clear_cache()

# Get registered filesystems
from upathtools.filesystems.delegating_fs import get_registered_filesystems
registered = get_registered_filesystems()
```

**Use Cases:**
- **Data Exploration**: Browse structured files without knowing their format
- **ETL Pipelines**: Process mixed file types uniformly
- **Documentation Systems**: Access markdown headers programmatically
- **Database Analysis**: Query SQLite files through filesystem interface
- **Code Analysis**: Browse Python modules as directory structures

## Aggregation Filesystems

### UnionFileSystem

Overlay multiple filesystems with priority:

```python
from upathtools.filesystems import UnionFileSystem, UnionPath

# Create union of filesystems
fs = UnionFileSystem(
    filesystems=[
        ("file:///overlay", {}),      # Higher priority
        ("file:///base", {}),         # Lower priority
    ]
)

# Files from overlay shadow files in base
content = fs.get_upath("/file.txt").read_text()
```

**Use Cases:**
- Configuration overlays
- Development vs production
- Layered data sources

### FlatUnionFileSystem

Merge multiple directories into single flat view:

```python
from upathtools.filesystems import FlatUnionFileSystem, FlatUnionPath

# Combine multiple sources
fs = FlatUnionFileSystem(
    paths=[
        "/data/2023/",
        "/data/2024/",
        "s3://bucket/archive/",
    ]
)

# All files appear in single directory
all_files = fs.ls("/")
```

**Use Cases:**
- Aggregating log files
- Combining data partitions
- Multi-source data access

## Specialized Filesystems

### BaseModelFileSystem

Browse Pydantic model schemas:

```python
from upathtools.filesystems import BaseModelFileSystem
from pydantic import BaseModel

class User(BaseModel):
    name: str
    email: str

# Browse model structure
fs = BaseModelFileSystem(model=User)
fields = fs.ls("/fields/")
```

### TypeAdapterFileSystem

Work with Pydantic TypeAdapter schemas:

```python
from upathtools.filesystems import TypeAdapterFileSystem
from pydantic import TypeAdapter

adapter = TypeAdapter(list[dict[str, str]])
fs = TypeAdapterFileSystem(adapter=adapter)
```

## Registration

### Register Individual Filesystem

```python
from fsspec import register_implementation
from upath import registry

# Register with fsspec
register_implementation("myfs", MyFileSystem)

# Register with UPath
registry.register_implementation("myfs", MyPath)
```

### Register All UPathTools Filesystems

```python
from upathtools import register_all_filesystems

# Register everything
register_all_filesystems()

# Now use any upathtools filesystem
from upath import UPath
path = UPath("mod://upathtools/async_upath.py")
```

### Register HTTP Only

```python
from upathtools import register_http_filesystems

# Replace fsspec's HTTP with upathtools version
register_http_filesystems()
```

## Common Patterns

### Read-Only Access

Most virtual filesystems are read-only:

```python
fs = ModuleFileSystem(module_name="upathtools")

# Reading works
content = fs.cat_file("/file.py")

# Writing raises NotImplementedError
try:
    fs.pipe_file("/new.py", b"code")
except NotImplementedError:
    print("Filesystem is read-only")
```

### Async Operations

All filesystems support async via AsyncUPath:

```python
fs = SQLiteFileSystem(db_path="data.db")
path = fs.get_upath("/table", as_async=True)

# Use async methods
rows = await path.aread_text()
async for item in path.aiterdir():
    print(item)
```

### Type-Safe Info

Each filesystem has its own info structure:

```python
from upathtools.filesystems import HTTPFileSystem

fs = HTTPFileSystem()
info = await fs._info("https://example.com/file.txt")

# info is typed based on filesystem
assert "content-type" in info
assert "size" in info
```

## Creating Custom Filesystems

See [Base Classes](base-classes.md) for building your own filesystems.

Quick example:

```python
from upathtools.filesystems.base import BaseAsyncFileSystem
from typing import TypedDict

class MyInfo(TypedDict):
    name: str
    size: int

class MyFS(BaseAsyncFileSystem[MyPath, MyInfo]):
    protocol = "my"
    upath_cls = MyPath
    
    async def _ls(self, path: str, detail: bool = True):
        # Implementation
        ...
```

## Filesystem Comparison

| Filesystem | Async | Writable | Use Case |
|------------|-------|----------|----------|
| HTTPFileSystem | ✓ | ✗ | Remote file access |
| ModuleFileSystem | ✓ | ✗ | Browse Python modules |
| SQLiteFileSystem | ✓ | ✗ | Query databases |
| UnionFileSystem | ✓ | ✓ | Layer filesystems |
| CliFileSystem | ✓ | ✗ | CLI output access |
| GistFileSystem | ✓ | ✓ | GitHub Gists |
| DelegatingFileSystem | ✓ | Partial | Auto-delegate to file formats |

## Best Practices

1. **Use appropriate filesystem for your use case**
   - Virtual filesystems for read-only structured data
   - Remote filesystems for network access
   - Aggregation for combining sources

2. **Prefer async for I/O operations**
   ```python
   path = fs.get_upath("/path", as_async=True)
   content = await path.aread_text()
   ```

3. **Check filesystem capabilities**
   ```python
   # Not all filesystems support writing
   if hasattr(fs, "_pipe_file"):
       await fs._pipe_file("/path", b"data")
   ```

4. **Use registration for convenience**
   ```python
   register_all_filesystems()
   path = UPath("mod://package/file.py")
   ```

5. **Handle read-only filesystems gracefully**
   ```python
   try:
       fs.write_text("/path", "data")
   except (NotImplementedError, PermissionError):
       print("Filesystem is read-only")
   ```

## Examples

### Browse Package Structure

```python
from upathtools.filesystems import PackageFileSystem

async def explore_package():
    fs = PackageFileSystem(package_name="requests")
    
    # Get all Python files
    py_files = await fs._glob("**/*.py", detail=False)
    
    for file in py_files:
        print(f"Found: {file}")

import asyncio
asyncio.run(explore_package())
```

### Aggregate Logs

```python
from upathtools.filesystems import FlatUnionFileSystem

# Combine multiple log directories
fs = FlatUnionFileSystem(paths=[
    "/var/log/app/2024-01/",
    "/var/log/app/2024-02/",
    "/var/log/app/2024-03/",
])

# Access all logs through single interface
all_logs = fs.ls("/")
```

### Query Database via Filesystem

```python
from upathtools.filesystems import SqliteFileSystem

fs = SqliteFileSystem(db_path="analytics.db")

# Tables as directories
tables = fs.ls("/")

# Read table data
users_data = fs.get_upath("/users").read_text()
```

## See Also

- [Base Classes](base-classes.md) - Build custom filesystems
- [Async Operations](async.md) - Async filesystem usage
- [Type Safety](typing.md) - Type-safe filesystem access
