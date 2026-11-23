# UPathTools

[![PyPI License](https://img.shields.io/pypi/l/upathtools.svg)](https://pypi.org/project/upathtools/)
[![Package status](https://img.shields.io/pypi/status/upathtools.svg)](https://pypi.org/project/upathtools/)
[![Monthly downloads](https://img.shields.io/pypi/dm/upathtools.svg)](https://pypi.org/project/upathtools/)
[![Distribution format](https://img.shields.io/pypi/format/upathtools.svg)](https://pypi.org/project/upathtools/)
[![Wheel availability](https://img.shields.io/pypi/wheel/upathtools.svg)](https://pypi.org/project/upathtools/)
[![Python version](https://img.shields.io/pypi/pyversions/upathtools.svg)](https://pypi.org/project/upathtools/)
[![Implementation](https://img.shields.io/pypi/implementation/upathtools.svg)](https://pypi.org/project/upathtools/)
[![Releases](https://img.shields.io/github/downloads/phil65/upathtools/total.svg)](https://github.com/phil65/upathtools/releases)
[![Github Contributors](https://img.shields.io/github/contributors/phil65/upathtools)](https://github.com/phil65/upathtools/graphs/contributors)
[![Github Forks](https://img.shields.io/github/forks/phil65/upathtools)](https://github.com/phil65/upathtools/forks)
[![Github Issues](https://img.shields.io/github/issues/phil65/upathtools)](https://github.com/phil65/upathtools/issues)
[![Github Issues](https://img.shields.io/github/issues-pr/phil65/upathtools)](https://github.com/phil65/upathtools/pulls)
[![Github Watchers](https://img.shields.io/github/watchers/phil65/upathtools)](https://github.com/phil65/upathtools/watchers)
[![Github Stars](https://img.shields.io/github/stars/phil65/upathtools)](https://github.com/phil65/upathtools/stars)
[![Github Repository size](https://img.shields.io/github/repo-size/phil65/upathtools)](https://github.com/phil65/upathtools)
[![Github last commit](https://img.shields.io/github/last-commit/phil65/upathtools)](https://github.com/phil65/upathtools/commits)
[![Github release date](https://img.shields.io/github/release-date/phil65/upathtools)](https://github.com/phil65/upathtools/releases)
[![Github language count](https://img.shields.io/github/languages/count/phil65/upathtools)](https://github.com/phil65/upathtools)
[![Github commits this month](https://img.shields.io/github/commit-activity/m/phil65/upathtools)](https://github.com/phil65/upathtools)
[![Package status](https://codecov.io/gh/phil65/upathtools/branch/main/graph/badge.svg)](https://codecov.io/gh/phil65/upathtools/)
[![PyUp](https://pyup.io/repos/github/phil65/upathtools/shield.svg)](https://pyup.io/repos/github/phil65/upathtools/)

**Async-first helpers, typed base classes and a large collection of filesystems for
[fsspec](https://filesystem-spec.readthedocs.io/) and
[universal-pathlib](https://github.com/fsspec/universal_pathlib).**

📖 Documentation: <https://phil65.github.io/upathtools/>

## Installation

```bash
uv add upathtools
# optional backends are extras, e.g.
uv add "upathtools[httpx,sqlite,mcp]"
```

Requires Python 3.13+.

## Quickstart

```python
import asyncio

from upathtools import AsyncUPath, list_files, read_folder


async def main() -> None:
    path = AsyncUPath("notes/hello.txt")
    await path.parent.amkdir(parents=True, exist_ok=True)
    await path.awrite_text("Hello from upathtools!")
    print(await path.aread_text())

    # Read many files concurrently (works for any fsspec protocol: s3://, gcs://, ...)
    files = await read_folder("notes", pattern="**/*.txt")
    for name, content in files.items():
        print(name, len(content))

    print(await list_files("notes", pattern="**/*.txt"))


asyncio.run(main())
```

Filesystems can be used directly or combined:

```python
from upathtools.filesystems import IsolatedMemoryFileSystem, PackageFileSystem, UnionFileSystem

pkg = PackageFileSystem("upathtools")  # browse an installed package's modules
print(pkg.get_upath("helpers").read_text()[:34])

docs = IsolatedMemoryFileSystem()
docs.pipe_file("/readme.md", b"# Hi")

union = UnionFileSystem({"pkg": pkg, "docs": docs})  # mount filesystems side by side
print(union.ls("/", detail=False))  # ['pkg', 'docs']
print(union.cat_file("docs/readme.md"))
```

## Features

- **`AsyncUPath`**: async versions of the `UPath` API (`aread_text`, `awrite_bytes`,
  `aiterdir`, `aglob`, `acopy`, ...), falling back to threads for sync filesystems.
- **Batch operations**: `read_folder`, `list_files`, `read_folder_as_text`, `read_path`,
  `fsspec_grep` with parallel loading.
- **Typed base classes**: `BaseAsyncFileSystem[TPath, TInfoDict]`, `BaseUPath[TInfoDict]`,
  `WrapperFileSystem`, `BaseAsyncFileFileSystem` for building your own filesystems with typed
  info dicts, `get_upath()`, `get_tree()` and a shell-like `cli("grep TODO . -r")`.
- **Filesystems** (registered as fsspec / UPath protocols via entry points):
  - file contents: Markdown sections, SQLite tables, tree-sitter code, OpenAPI and JSON Schema specs
  - Python: installed packages (`pkg://`), distributions, Pydantic models and instances
  - remote: HTTP (httpx2), GitHub Gists / Issues / Wiki, GitLab, Linear, Notion, Appwrite, MCP
    resources and tools
  - sandboxes: E2B, Modal, Daytona, Beam, Vercel, Cloudflare, Microsandbox, HopX, Sprites, Codex, SRT
  - combining: union (mount points), flat union (merged view), overlay (layered writes),
    delegating (`file.md::Section` access into files)
- **`upathtools_config`**: lightweight pydantic models describing filesystems
  (`FilesystemConfigType` discriminated union) with `create_fs()`.
- Helpers: `to_upath`, `upath_to_fs`, `copy`, `write_file`, `multi_glob`, directory trees,
  pydantic `UPath` field types.

## License

MIT
