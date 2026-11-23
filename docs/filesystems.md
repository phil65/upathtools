# Filesystem Implementations

UPathTools provides a collection of specialized filesystem implementations for various use cases, from virtual filesystems to remote access and aggregation.

All of them are importable from `upathtools.filesystems` and registered as fsspec / UPath
protocols via entry points, so `fsspec.filesystem("sqlite", db_path="data.db")` and
`UPath("sqlite://users", db_path="data.db")` work once upathtools is installed.
Backends with third-party dependencies need the matching extra (`uv add "upathtools[e2b]"`).

## Categories

Filesystems in UPathTools are organized into several categories:

- **File-based**: Virtual filesystems for the contents of a single file (SQLite, Markdown, tree-sitter code, OpenAPI, JSON Schema)
- **Python**: Installed packages and distributions, Pydantic models and instances
- **Remote**: Network-based access (HTTP/HTTPS, GitHub Gists / Issues / Wiki, GitLab, Linear, Notion, Appwrite, MCP)
- **Sandboxes**: Files inside remote code-execution sandboxes (E2B, Modal, Daytona, Beam, ...)
- **Aggregation**: Combine multiple filesystems (Union, FlatUnion, Overlay)
- **Delegating**: Automatically delegate to file-based filesystems based on file extensions

## Overview

| Filesystem | Protocol | Extra | Description |
|------------|----------|-------|-------------|
| `MarkdownFileSystem` | `md` | | Markdown document by header hierarchy |
| `SqliteFileSystem` | `sqlite` | `sqlite` | Tables of a SQLite database |
| `TreeSitterFileSystem` | `ts` | `tree-sitter` | Classes / functions of source files |
| `OpenAPIFileSystem` | `openapi` | `openapi3` | OpenAPI specifications |
| `JsonSchemaFileSystem` | `jsonschema` | `jsonschema` | JSON Schema specifications |
| `PackageFileSystem` | `pkg` | | Modules of an installed package |
| `DistributionFileSystem` | `distribution` | | Installed distributions and their modules |
| `BaseModelFileSystem` | `basemodel` | | Pydantic model schemas / fields |
| `BaseModelInstanceFileSystem` | `basemodel-instance` | | Pydantic model instance data |
| `HTTPFileSystem` | `httpx` | `httpx` | HTTP(S) via httpx2 |
| `GistFileSystem` | `gist` | `httpx` | GitHub Gists |
| `IssueFileSystem` | `githubissues` | `httpx` | GitHub Issues |
| `WikiFileSystem` | `wiki` | | GitHub Wiki pages (needs `git`) |
| `GitLabFileSystem` | `gitlab` | `gitlab` | GitLab repository contents |
| `LinearIssueFileSystem` | `linear` | `httpx` | Linear issues |
| `NotionFileSystem` | `notion` | `notion-client` | Notion pages |
| `AppwriteFileSystem` | `appwrite` | `appwrite` | Appwrite storage |
| `MCPFileSystem` | `mcp` | `mcp` | MCP server resources |
| `MCPToolsFileSystem` | `mcptools` | `mcp` | MCP tools as generated Python code |
| `E2BFS`, `ModalFS`, `DaytonaFS`, `BeamFS`, `VercelFS`, `CloudflareFS`, `MicrosandboxFS`, `HopXFS`, `SpritesFS`, `CodexFS` | `e2b`, `modal`, ... | same name | Sandbox environments |
| `UnionFileSystem` | `union` | | Multiple filesystems under named mount points |
| `FlatUnionFileSystem` | `flatunion` | | Multiple filesystems merged into one view |
| `OverlayFileSystem` | `overlay` | | Layered filesystems with copy-on-write |
| `AsyncLocalFileSystem` | `asynclocal` | | Async local filesystem (ripgrep-based grep) |
| `IsolatedMemoryFileSystem` | `memory` | | Memory filesystem with per-key isolated storage |

## Virtual Filesystems


### PackageFileSystem

Access installed Python package contents (modules appear as files, subpackages as directories):

```python
from upath import UPath

from upathtools.filesystems import PackageFileSystem

fs = PackageFileSystem("requests")

# List modules and subpackages
modules = fs.ls("/", detail=False)  # ['__version__', 'adapters', 'api', ...]

# Read module source
code = fs.get_upath("api").read_text()

# Or via UPath
path = UPath("pkg://", package="requests")
```

### DistributionFileSystem

Browse the distributions installed in the current environment:

```python
from upathtools.filesystems import DistributionFileSystem

fs = DistributionFileSystem()

distributions = fs.ls("/", detail=False)  # ['fsspec', 'rich', ...]
modules = fs.ls("fsspec", detail=False)  # ['_version', 'archive', 'asyn', ...]
source = fs.cat_file("fsspec/core")
```


### SqliteFileSystem

Browse SQLite databases as filesystems:

```python
from upathtools.filesystems import SqliteFileSystem

fs = SqliteFileSystem(db_path="data.db")

# Tables appear as files
tables = fs.ls("/", detail=False)

# Reading a table returns its rows as CSV
rows = fs.cat_file("users")
```

### MarkdownFileSystem

Navigate Markdown documents by headers:

```python
from upathtools.filesystems import MarkdownFileSystem

fs = MarkdownFileSystem("README.md")

# Headers become paths
sections = fs.ls("/", detail=False)  # ['Introduction', 'Usage', ...]
intro = fs.cat_file("Introduction")
```

### TreeSitterFileSystem, OpenAPIFileSystem, JsonSchemaFileSystem

Browse the structure of source files (classes, functions), OpenAPI specs (paths, operations,
schemas) and JSON Schemas the same way. They are mostly used through the
[DelegatingFileSystem](#delegating-filesystem).

## Remote Filesystems

### HTTPFileSystem (Enhanced)

Improved HTTP/HTTPS support with [httpx2](https://github.com/pydantic/httpx2) (extra `httpx`, alias `httpx2`; protocol `httpx`):

```python
from upathtools import AsyncUPath, register_http_filesystems
from upathtools.filesystems import HTTPFileSystem

fs = HTTPFileSystem()

# Native async API
info = await fs._info("https://example.com/file.txt")
content = await fs._cat_file("https://example.com/data.json")

# Use it for all http(s):// URLs
register_http_filesystems()
text = await AsyncUPath("https://example.com/data.json").aread_text()
```

**Features:**
- Native async support with httpx2
- Typed info dicts (`HttpInfo`)
- Streaming support

### GistFileSystem

Access GitHub Gists as filesystems:

```python
from upathtools.filesystems import GistFileSystem

# Access a gist by ID (token optional for public gists, GITHUB_TOKEN is used if set)
fs = GistFileSystem(gist_id="abc123...", token="ghp_...")

files = fs.ls("/", detail=False)
content = fs.cat_file("file.py")
```

### WikiFileSystem

Browse GitHub wiki pages:

```python
from upathtools.filesystems import WikiFileSystem

fs = WikiFileSystem(owner="owner", repo="repo")

pages = fs.ls("/", detail=False)
content = fs.cat_file("Home.md")
```

### MCP, Notion, Linear, GitLab, Appwrite

`MCPFileSystem` exposes the resources of an MCP server, `MCPToolsFileSystem` exposes its tools
as generated Python code. `NotionFileSystem`, `LinearIssueFileSystem`, `GitLabFileSystem`,
`IssueFileSystem` and `AppwriteFileSystem` map the respective APIs onto files; credentials are
passed as arguments or read from environment variables (`NOTION_API_KEY`, `LINEAR_API_KEY`,
`GITLAB_TOKEN`, `GITHUB_TOKEN`, `APPWRITE_*`).

## Sandbox Filesystems

`E2BFS`, `ModalFS`, `DaytonaFS`, `BeamFS`, `VercelFS`, `CloudflareFS`, `MicrosandboxFS`,
`HopXFS`, `SpritesFS`, `CodexFS` and `SRTFS` (in `upathtools.filesystems.sandbox_filesystems`) give file access to remote or local code-execution
sandboxes, including fast `grep`/`find` running inside the sandbox. See the
[examples](https://github.com/phil65/upathtools/tree/main/examples) directory.

```python
from upathtools.filesystems import E2BFS

fs = E2BFS()  # creates a sandbox (or pass sandbox_id=...)
await fs._pipe_file("/home/user/hello.py", b"print('hi')")
print(await fs._ls("/home/user", detail=False))
```

## Delegating Filesystem

Automatically delegates to file-based sub-filesystems based on file extensions and content:

```python
from upathtools.filesystems.delegating_fs import DelegatingFileSystem

# Wrap a filesystem (default: local) with auto-delegation
fs = DelegatingFileSystem(target_protocol="file")

# Access files normally - delegation is automatic
regular_file = fs.cat_file("document.txt")  # Normal file access

# Use :: separator to access internal structure of supported files
markdown_section = fs.cat_file("README.md::Introduction")  # Markdown header
sqlite_table = fs.cat_file("data.db::users")  # SQLite table
python_class = fs.cat_file("module.py::MyClass")  # tree-sitter node
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

# SQLite databases - access tables
users = fs.cat_file("app.db::users")

# Source files - browse classes and functions (tree-sitter)
members = fs.ls("src/main.py::", detail=False)  # ['src/main.py::MyClass', ...]
class_def = fs.cat_file("src/main.py::MyClass")

# JSON Schema - browse schema structure
properties = fs.ls("schema.json::properties/")
```

**Supported File Types:**
- **Markdown** (`.md`, `.markdown`) → MarkdownFileSystem
- **SQLite** (`.db`, `.sqlite`, `.sqlite3`) → SqliteFileSystem
- **JSON Schema** (`.json`) → JsonSchemaFileSystem
- **OpenAPI** (`.json`, `.yaml`, `.yml`) → OpenAPIFileSystem
- **Tree-sitter** (`.py`, `.js`, `.ts`, `.rs`, `.go`, ...) → TreeSitterFileSystem

**Registration:**

```python
from upathtools.filesystems.base import BaseAsyncFileFileSystem, ProbeResult
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
parent_fs = filesystem("s3")
fs = DelegatingFileSystem(fs=parent_fs)

# Access remote files with delegation
data = fs.cat_file("my-bucket/data.db::table_name")

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
- **Database Analysis**: Read SQLite tables through a filesystem interface
- **Code Analysis**: Browse source files as directory structures

## Aggregation Filesystems

### UnionFileSystem

Combine multiple filesystems under named mount points:

```python
from upathtools.filesystems import IsolatedMemoryFileSystem, PackageFileSystem, UnionFileSystem

fs = UnionFileSystem({
    "pkg": PackageFileSystem("upathtools"),
    "data": "/path/to/data",  # paths / URLs are resolved to filesystems
    "scratch": IsolatedMemoryFileSystem(),
})

fs.ls("/", detail=False)  # ['pkg', 'data', 'scratch']
content = fs.cat_file("data/file.txt")
```

**Use Cases:**
- Exposing several sources to an agent / tool as one tree
- Cross-filesystem copies (`fs.copy("data/file.txt", "scratch/file.txt")`)

### FlatUnionFileSystem

Merge multiple directories into single flat view:

```python
from upathtools.filesystems import FlatUnionFileSystem

# Combine multiple sources
fs = FlatUnionFileSystem([
    "/data/2023/",
    "/data/2024/",
    "s3://bucket/archive/",
])

# All files appear in a single directory tree
all_files = fs.ls("/")
```

**Use Cases:**
- Aggregating log files
- Combining data partitions
- Multi-source data access

### OverlayFileSystem

Layer filesystems on top of each other: reads search from the top layer down, writes always
go to the top layer (copy-on-write, like Docker's overlay filesystem):

```python
from upathtools.filesystems import IsolatedMemoryFileSystem, OverlayFileSystem

upper = IsolatedMemoryFileSystem()  # writable layer
lower = IsolatedMemoryFileSystem()  # e.g. a read-only base
lower.pipe_file("/config.toml", b"base")

fs = OverlayFileSystem([upper, lower])
fs.cat_file("/config.toml")  # b"base" (from lower)
fs.pipe_file("/config.toml", b"changed")  # written to upper, lower is untouched
```

**Use Cases:**
- Configuration overlays
- Development vs production
- Scratch space on top of read-only data

## Specialized Filesystems

### BaseModelFileSystem

Browse Pydantic model schemas:

```python
from pydantic import BaseModel

from upathtools.filesystems import BaseModelFileSystem, BaseModelInstanceFileSystem

class User(BaseModel):
    name: str
    email: str

# Browse model structure (fields plus __schema__, __fields_info__, ...)
fs = BaseModelFileSystem(User)
fields = fs.ls("/", detail=False)
field_info = fs.cat_file("name")

# Browse the data of an instance
fs = BaseModelInstanceFileSystem(User(name="Ada", email="ada@example.com"))
fs.cat_file("name")
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

# Or, for subclasses of the upathtools base classes:
MyFileSystem.register_fs()
```

### Register All UPathTools Filesystems

The entry points make all protocols available automatically. `register_all_filesystems()`
additionally replaces fsspec's `http(s)` and `asynclocal` implementations with the upathtools
versions and (re-)registers the most common virtual filesystems:

```python
from upath import UPath

from upathtools import register_all_filesystems

register_all_filesystems()

path = UPath("pkg://", package="upathtools")
print([p.name for p in path.iterdir()])
```

### Register HTTP Only

```python
from upathtools import register_http_filesystems

# Replace fsspec's HTTP with upathtools version
register_http_filesystems()
```

## Common Patterns

### Async Operations

All filesystems support async via AsyncUPath:

```python
fs = SqliteFileSystem(db_path="data.db")
path = fs.get_upath("users", as_async=True)

# Use async methods
rows = await path.aread_text()
async for item in path.parent.aiterdir():
    print(item)
```

### Type-Safe Info

Each filesystem has its own info structure:

```python
from upathtools.filesystems import HTTPFileSystem

fs = HTTPFileSystem()
info = await fs._info("https://example.com/file.txt")  # HttpInfo

# info is typed based on filesystem
size = info["size"]
mimetype = info.get("mimetype")
```

## Creating Custom Filesystems

See [Base Classes](base-classes.md) for building your own filesystems.

Quick example:

```python
from typing import TypedDict

from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath

class MyInfo(TypedDict):
    name: str
    size: int

class MyPath(BaseUPath[MyInfo]):
    pass

class MyFS(BaseAsyncFileSystem[MyPath, MyInfo]):
    protocol = "my"
    upath_cls = MyPath

    async def _ls(self, path: str, detail: bool = True, **kwargs):
        # Implementation
        ...
```

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

4. **Handle read-only filesystems gracefully**
   ```python
   try:
       fs.write_text("/path", "data")
   except (NotImplementedError, PermissionError):
       print("Filesystem is read-only")
   ```

## Examples

### Browse Package Structure

```python
import asyncio

from upathtools.filesystems import PackageFileSystem

async def explore_package():
    fs = PackageFileSystem("requests")

    for info in await fs._ls("/", detail=True):
        print(f"{info['name']} ({info['type']})")

asyncio.run(explore_package())
```

### Aggregate Logs

```python
from upathtools.filesystems import FlatUnionFileSystem

# Combine multiple log directories
fs = FlatUnionFileSystem([
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

# Tables as files
tables = fs.ls("/", detail=False)

# Read table data (CSV)
users_data = fs.get_upath("users").read_text()
```
