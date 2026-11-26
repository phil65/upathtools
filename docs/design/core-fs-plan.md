> Design for the `core-fs` branch (`upathtools.fs`, the typed async-first filesystem core); built step by step on that branch.

# upathtools core: typed, async-first filesystem layer (plan)

Status: proposal for the owner, 2026-11-26, not started. Body below written by Gemini 4 Neon
(agy, from the upathtools code and fsspec 2026.9.0), reviewed by Claude against an independent
Claude draft (`reviews/2026-11-26/upathtools-core/PLAN_CORE.claude.md`). Both drafts converged on
the same shape: own async core, adapters both ways, port only what we use, keep UPath for now.
Gemini's is the base because it is better grounded (call-site counts, fsspec line refs).

## Review notes (Claude): verified, and changes to the body

Verified on the code: `fsspec/tests/abstract/` ships in the fsspec wheel (copy/get/put/mv/open/
pipe + glob edge cases), so the benchmark needs no vendoring; `fsspec.asyn.reset_after_fork`
declares `global lock` while the variable is `_lock` (an upstream bug in 2026.9.0, fixed on
fsspec master by `reset_lock()`, not released yet as of 2026-11-26);
`pyproject.toml` ignores ty's `incompatible-{attribute,method,mutable}-override`; 26 files carry
`= sync_wrapper(...)` boilerplate.

Changes I'd make (apply before Step 1):

1. **Tests from the first backend on.** Move `MemoryFS` into Step 3 and run
   `fsspec.tests.abstract` through `to_fsspec(MemoryFS())` as soon as Step 4 lands, instead of
   collecting conformance in Step 6. Step 6 then only adds the divergence matrix, the free-threading
   stress suite and the sync-note gate.
2. **Split Step 3**: (a) `AsyncFS` base + streams + `MemoryFS`; (b) `LoopRunner` + `SyncFS`. Each
   is a separate Gemini build; (a) is the one that sets the semantics, review it closely.
3. **No `fs.sync` property.** It ties every filesystem to a hidden default runner (fsspec's global
   loop thread in a new coat). Keep the facade explicit: `SyncFS(fs, runner=...)`, with a
   documented default runner for scripts. agentpool is async and doesn't need it.
4. **"Synced with" = the locked fsspec version.** The abstract tests run against whatever
   `uv.lock` pins; `FSSPEC_SYNC_VERSION` must equal it (the sync-note test checks that), and a
   Dependabot bump of fsspec is the moment to audit the listed upstream symbols.
5. **Names**: `upathtools.fs` (not `upathtools.core`, which exists), `FileInfo`, `AsyncFS`; agreed.
   `ls()` returns a list (Claude's draft had an async iterator: drop it, add `iter_ls` only if a
   backend needs paging).
6. **Releases**: Steps 1-7 additive in 1.x (agentpool untouched); the 2.0 cut at Steps 8-11.
   No transition aliases beyond 2.0 (owner: no backward compat in fast-release repos).

7. **UPath compatibility is cheap (checked in upath 0.3.10).** `UPath(url)` picks its class from
   upath's registry (`register_implementation` / entry points), creates `p.fs` through the
   overridable classmethod `_fs_factory` (upathtools' `BaseUPath` already overrides it), does all
   IO as ~16 sync calls on `p.fs`, and takes its path parsing ("flavour") from the fsspec class
   attributes found in fsspec's class registry. So per core filesystem: a UPath subclass whose
   `_fs_factory` returns `to_fsspec(CoreFS(**opts))`, and the adapter class registered in fsspec,
   which is what `register_fs` does today. No registry override needed. Limits: UPath is sync
   only (its calls go through the adapter's sync bridge; the async path methods in `BaseUPath`/
   `AsyncUPath` call the core directly), and `_fs_factory`/`_fs_cached` are private upath hooks
   (already relied on today). An own path class is only needed if upath itself becomes the problem.

Owner decisions needed before Step 1: section 8.2 below (naming, `FileInfo` dataclass, pathlib
verbs vs fsspec verbs, the `fsspec_filesystems/` wrappers, fsspec/upath optional in 2.0, path
layer timing), plus whether to send the fork-lock fix upstream.

---


## Executive Summary

`upathtools` currently builds ~45 custom filesystem implementations, combining/wrapper layers, async path utilities, and Pydantic config models directly on top of `fsspec`'s `AbstractFileSystem` / `AsyncFileSystem` and `universal-pathlib` (`UPath`). While `fsspec` provides a huge ecosystem of third-party storage drivers (`s3fs`, `gcsfs`, `adlfs`, `hf`, archives), its core architecture—global metaclass instance caching, a global background IO thread, runtime `__dict__` method-mirroring, untyped info dicts, and non-thread-safe mutable caches—repeatedly causes bugs, type-checker suppressions, and workarounds across `upathtools`.

This plan defines a modern, well-typed, async-first, free-threading-safe filesystem core inside `upathtools` (`upathtools.fs`), ports only the well-tested behavioral algorithms needed from `fsspec 2026.9.0` (with explicit per-module sync tracking), bridges bidirectionally to `fsspec` so the external ecosystem (`s3fs`, `pandas`, `pyarrow`, `UPath`, `fsspec.open`) continues to work seamlessly, and outlines a 12-step incremental migration path suitable for handing off to coding agents.

---

## 1. Inventory: Current `fsspec` & `upath` Footprint

### 1.1 `fsspec` Imports Across `src/` (AST Audit)

A full AST scan of `src/upathtools/` and `src/upathtools_config/` finds **41 distinct `fsspec` import targets** across **~40 files**. They fall into five tiers:

| Category | Symbol(s) | Import Count / Files | Primary Locations |
| --- | --- | --- | --- |
| **Base classes** | `fsspec.asyn.AsyncFileSystem`, `fsspec.spec.AbstractFileSystem`, `fsspec.AbstractFileSystem` | 33 imports / ~26 files | `filesystems/base/basefilesystem.py`, `wrapper.py`, `delegating_fs.py`, `auto_expand_fs.py`, `archive.py`, `async_ops.py`, `core.py`, `combining_filesystems/*`, `upathtools_config/base.py` |
| **Registry & URL resolution** | `fsspec.registry.get_filesystem_class`, `fsspec.filesystem`, `fsspec.register_implementation`, `fsspec.core._un_chain`, `fsspec.core.stringify_path`, `fsspec.utils.infer_storage_options`, `fsspec.open` | 19 imports / ~17 files | `core.py`, `async_ops.py`, `__init__.py`, `base/baseupath.py`, `base/basefilesystem.py`, `remote_filesystems/{gist,github,issue,linear,wiki}_fs.py`, `file_filesystems/*`, `package_fs.py`, `distribution_fs.py` |
| **Built-in implementations** | `LocalFileSystem`, `MemoryFileSystem`, `DirFileSystem`, `AsyncFileSystemWrapper`, `WholeFileCacheFileSystem`, `SimpleCacheFileSystem`, `DataFileSystem`, `GithubFileSystem`, `TarFileSystem`, `ZipFileSystem`, `HadoopFileSystem`, `SFTPFileSystem`, `SMBFileSystem` | 18 imports / ~12 files | `async_local_fs.py`, `isolated_memory_fs.py`, `helpers.py`, `base/wrapper.py`, `upathtools_config/base.py`, `filesystems/fsspec_filesystems/*`, `sqlite_fs.py`, `async_ops.py` |
| **Async / loop / streaming internals** | `fsspec.asyn.get_loop`, `fsspec.asyn.sync`, `fsspec.asyn._run_coros_in_chunks`, `fsspec.asyn.AbstractAsyncStreamedFile`, `fsspec.spec.AbstractBufferedFile`, `fsspec.caching.AllBytes`, `fsspec.callbacks.DEFAULT_CALLBACK`, `fsspec.exceptions.FSTimeoutError` | 9 imports / 6 files | `async_helpers.py`, `cli_ops.py`, `monty_os.py`, `base/basefilesystem.py`, `httpx_fs.py` |
| **Utilities & compression** | `fsspec.utils.tokenize`, `fsspec.utils.glob_translate`, `fsspec.utils.stringify_path`, `fsspec.utils.infer_compression`, `fsspec.utils.isfilelike`, `fsspec.utils.nullcontext`, `fsspec.utils.DEFAULT_BLOCK_SIZE`, `fsspec.compression.compr`, `fsspec.implementations.local.make_path_posix`, `fsspec.FSMap`, `fsspec.get_mapper` | 14 imports / 8 files | `async_ops.py`, `base/basefilesystem.py`, `base/archive.py`, `httpx_fs.py`, `tar_fs.py`, `async_local_fs.py`, `remote_filesystems/appwrite_fs.py`, `helpers.py` |

**Key observation:** Almost all `fsspec` surface area used by `upathtools` is concentrated in `base/`, `core.py`, `async_ops.py`, `helpers.py`, `httpx_fs.py`, and `tar_fs.py`. The ~35 concrete remote, sandbox, and file filesystems barely touch `fsspec` directly—they subclass `BaseAsyncFileSystem` / `BaseAsyncFileFileSystem` and implement `async def _ls`, `_info`, `_cat_file`, `_pipe_file`, `_rm_file`, `_mkdir`, `_rmdir`, `_exists`, `_isdir`, `_isfile`, plus boilerplate `sync_wrapper(...)` declarations (found in **28 files**).

### 1.2 Inventory of `upathtools` Filesystems

| Category | Count | Base Class | Notes |
| --- | --- | --- | --- |
| **Base / Wrappers** | 6 | `AsyncFileSystem` / `AbstractFileSystem` | `BaseAsyncFileSystem`, `BaseFileSystem`, `BaseAsyncFileFileSystem`, `WrapperFileSystem`, `AbstractArchiveFileSystem`, `skills_fs` (factory on `WrapperFileSystem`) |
| **Combining / Virtual** | 5 | `BaseAsyncFileSystem` / `AsyncFileSystem` | `UnionFileSystem`, `FlatUnionFileSystem`, `OverlayFileSystem`, `DelegatingFileSystem`, `AutoExpandFS` |
| **Core local / memory / HTTP** | 3 | Mixed | `AsyncLocalFileSystem` (`BaseAsyncFileSystem` + `LocalFileSystem`), `IsolatedMemoryFileSystem` (`BaseFileSystem` + `MemoryFileSystem`), `HTTPFileSystem` (`BaseAsyncFileSystem` + `httpx2`) |
| **Python introspection** | 4 | `BaseAsyncFileSystem` | `PackageFileSystem`, `DistributionFileSystem`, `BaseModelFileSystem`, `BaseModelInstanceFileSystem` |
| **File-based** | 6 | `BaseAsyncFileFileSystem` (5), `AbstractArchiveFileSystem` (1) | `Markdown`, `Sqlite`, `TreeSitter`, `OpenAPI`, `JsonSchema`, `Tar` (sync) |
| **Remote APIs** | 10 | `BaseAsyncFileSystem` | `Appwrite`, `Gist`, `GitHub`, `GitLab`, `Issue`, `Linear`, `MCP`, `MCPTools`, `Notion`, `Wiki` |
| **Sandboxes** | 12 | `BaseAsyncFileSystem` (11), `BaseAsyncFileFileSystem` (1: `PyodideFS`) | `Beam`, `Cloudflare`, `Codex`, `Daytona`, `E2B`, `HopX`, `Microsandbox`, `Modal`, `Pyodide`, `Sprites`, `SRT`, `Vercel` |
| **External fsspec mixin wrappers** | 14 | `BaseFileSystem` / `BaseAsyncFileSystem` + external class | `filesystems/fsspec_filesystems/`: `S3`, `GCS`, `AzureBlob`, `Hf`, `AsyncSSH`, `Webdav`, `Hadoop`, `SFTP`, `SMB`, `Github`, `Tar`, `Zip`, `SimpleCache`, `Data` |

Most implemented methods across concrete `upathtools` filesystems (excluding `base/` and `fsspec_filesystems/`):
`_ls` (38+ classes; 112 defs incl. overloads), `_info` (38), `_cat_file` (37), `_isdir` (37), `_exists` (30), `_strip_protocol` (30), `_isfile` (29), `_get_kwargs_from_urls` (29), `_pipe_file` (25), `_rm_file` (23), `_mkdir` (17), `_rmdir` (16), `set_session` (14), `_find` (12), `_modified` (12), `_rm` (11), `_size` (11), `_put_file` (10), `close_session` (9 async + 5 sync), `_cp_file` (7), `_glob` (7), `_open` (7), `_grep` (6), `_makedirs` (6), `_get_file` (3), `_mv_file` (3), `open_async` (2).

### 1.3 External `fsspec` Filesystems That Matter

Grounded in `pyproject.toml` optional dependencies, `src/upathtools/filesystems/fsspec_filesystems/`, and `src/upathtools_config/fsspec_fs_configs.py`:

- **Tier 1 — High-priority cloud / network drivers (external packages, declared as extras in `pyproject.toml`):**
  - `s3fs` (`s3://`, `s3a://`) — async (`AsyncFileSystem`)
  - `gcsfs` (`gcs://`, `gs://`) — async (`AsyncFileSystem`)
  - `adlfs` (`abfs://`, `abfss://`, `az://`) — async (`AsyncFileSystem`)
  - `huggingface_hub` (`hf://`) — sync (`AbstractFileSystem`)
  - `sshfs` (`ssh://`, `sftp://`) — async (`AsyncFileSystem`; wrapped in `AsyncSSHFileSystem` to avoid `__init__` loop issues)
  - `webdav4` (`webdav://`) — sync (`AbstractFileSystem`)
- **Tier 2 — `fsspec` built-ins actively used by `upathtools` / `upathtools_config`:**
  - `zip` (`ZipFileSystem`), `tar` (`TarFileSystem`), `libarchive` — used by `AutoExpandFS` (`auto_expand_fs.py:69-76`) and `upathtools_config`
  - `dir` (`DirFileSystem`) — used by `helpers.py:269`, `wrapper.py:311`, `upathtools_config/base.py:77,163` (will be replaced natively by core `DirFS`)
  - `filecache` (`WholeFileCacheFileSystem`) / `simplecache` (`SimpleCacheFileSystem`) — used by `upathtools_config/base.py:81,165`
  - `file` / `local` (`LocalFileSystem`) & `memory` (`MemoryFileSystem`) — will be replaced natively by core `LocalFS` and `MemoryFS`
- **Tier 3 — Long-tail `fsspec` built-ins/externals exposed in `upathtools_config/fsspec_fs_configs.py`:**
  - `ftp`, `sftp` (paramiko), `smb`, `hdfs` / `arrow_hdfs` (pyarrow), `webhdfs`, `git`, `github`, `data`, `jupyter`, `dask` — all cleanly handled via the inbound `FsspecAdapter`.

### 1.4 How `universal_pathlib` (`upath` `0.3.11`) Depends on `fsspec`

An AST audit of `.venv/lib/python3.15/site-packages/upath/` shows **12 modules** directly importing `fsspec`:

- `upath/core.py`: imports `AbstractFileSystem`, `get_filesystem_class`, and `MemoryFileSystem` (for `_check_fsspec_has_working_glob()`). `UPath.fs` is typed as `AbstractFileSystem`, cached on `self._fs_cached`, and created via `_fs_factory()` calling `get_filesystem_class(protocol)(**storage_options)`. Calls **16 sync methods** on `fs`: `rm`, `mv`, `open`, `isdir`, `info`, `exists`, `_strip_protocol`, `glob`, `touch`, `makedirs`, `copy`, `listdir`, `isfile`, `find`, `invalidate_cache`, `mkdir`.
- `upath/registry.py`: inspects `fsspec.registry.registry`, `known_implementations`, and `get_filesystem_class` for fallback path synthesis.
- `upath/_flavour.py` & `_flavour_sources.py`: wraps `AbstractFileSystem` class attributes (`sep`, `root_marker`, `protocol`, `_strip_protocol`, `_parent`, `_get_kwargs_from_urls`) as path parsers (`WrappedFileSystemFlavour`).
- `upath/_chain.py`: delegates URL chaining (`::`) to `fsspec.core._un_chain`.
- `upath/_protocol.py`: reads `fsspec.registry.registry` and `known_implementations`.
- `upath/implementations/http.py`: calls `fsspec.asyn.sync` directly.

**Implication:** As long as `UPath` is kept, any core filesystem used with `UPath` must either be exposed through the outbound `AsFsspecFileSystem` adapter (registered in `fsspec.registry`) or wrapped when attached to `_fs_cached`.

---

## 2. Known `fsspec` Pain Points Hurting `upathtools`

Every item below is directly evidenced in `upathtools` and `fsspec 2026.9.0`:

### 2.1 Global Instance Cache (`_Cached` Metaclass)
- **In fsspec (`fsspec/spec.py:52-149`):** `AbstractFileSystem` uses metaclass `_Cached` with `cachable = True` by default. `cls._cache` holds **strong references** forever until `clear_instance_cache()` is called, preventing garbage collection of clients, buffers, and credentials.
- **Identity & isolation bugs:** `AbstractFileSystem.__eq__` compares `_fs_token` (`spec.py:239-240`). Two `IsolatedMemoryFileSystem()` instances or `WrapperFileSystem` instances can compare equal or collide in caches. `upathtools` had to set `cachable = False` in **19 files** (all 12 sandboxes, all 3 combining filesystems, `MCP`, `MCPTools`, `IsolatedMemoryFileSystem`, `TarFileSystem`), pass `skip_instance_cache=True` in `async_ops.py:64` and `wrapper.py:119`, and store wrappers on `fs.__dict__` (`async_ops.py:116-121`; see `tests/test_async_fs_cache.py:17-31`).
- **Loop-unsafe keying:** For `asynchronous=True`, `_Cached.__call__` keys by `(cls, pid, threading.get_ident(), ...)` (`spec.py:102-107`)—**by thread, not by event loop**. Two sequential `asyncio.run()` calls on the same thread receive the same cached instance whose `httpx2.AsyncClient` / `aiohttp.ClientSession` is bound to the first (now closed) loop! `upathtools` had to build its own `_loop_fs_cache` in `src/upathtools/async_ops.py:28-52` (regression tested in `tests/test_async_fs_cache.py:56-80`).

### 2.2 Sync-Over-Async Loop Thread & `mirror_sync_methods`
- **Hidden global daemon thread (`fsspec/asyn.py:24-254`):** `get_loop()` spawns a daemon thread named `"fsspecIO"` with a process-global loop `loop[0]`.
  - When an `AsyncFileSystem` is instantiated with `asynchronous=True`, `self._loop = None` (`asyn.py:447-448`), so any accidental sync call (`fs.ls(...)` or `UPath.exists()`) crashes with `RuntimeError("Loop is not running")` (`asyn.py:78-79`).
  - Conversely, calling a sync wrapper from inside `self.loop` raises `NotImplementedError("Calling sync() from within a running loop")` (`asyn.py:82-83`).
  - `DelegatingFileSystem._call_fs_method` (`src/upathtools/filesystems/delegating_fs.py:250-260`) must dynamically probe `isinstance(file_fs, AsyncFileSystem)` and `inspect.iscoroutinefunction(getattr(file_fs, f"_{method}"))` just to avoid deadlocking sync calls (`tests/test_get_upath.py:34-41`).
  - `AsyncSSHFileSystem` (`src/upathtools/filesystems/fsspec_filesystems/ssh.py:1-35`) exists solely because upstream `sshfs` calls `sync_wrapper` inside `__init__`, crashing when constructed inside an event loop.
  - `cli_ops.py:922-930` and `monty_os.py:60-65` have to borrow `fsspec.asyn.get_loop()` and `sync()`.
- **Fork bug in fsspec (`fsspec/asyn.py:256-260`):** `reset_after_fork()` declares `global lock` instead of `global _lock`, leaving the parent's `_lock` alive in forked children (fixed on fsspec master, after 2026.9.0).
- **Runtime `__dict__` mutation destroys static typing (`fsspec/asyn.py:1090-1124`):** `mirror_sync_methods(obj)` injects sync methods onto each instance at `__call__` time. Type checkers (`ty`, `pyright`) cannot see them. Consequently:
  - **28 files** in `upathtools` manually re-declare `ls = sync_wrapper(_ls)` with `# pyright: ignore[reportAssignmentType]`.
  - `pyproject.toml:448-453` has to globally suppress `incompatible-attribute-override = "ignore"` and `incompatible-method-override = "ignore"` in `[tool.ty.rules]`, weakening type checking across the entire repository.
  - `AsyncLocalFileSystem` (`async_local_fs.py:72`) has to set `mirror_sync_methods = False` and maintain 24 manual `wrap_method(LocalFileSystem.*)` declarations.

### 2.3 Untyped Info Dicts & Inconsistent Return Shapes
- **`fsspec/spec.py:384`:** *"The specific keys, or perhaps a FileInfo class, or similar, is TBD"*.
- **Missing keys even inside fsspec itself:** `AsyncFileSystem._find` (`fsspec/asyn.py:1028-1031`) and `AbstractFileSystem.find` (`fsspec/spec.py:565-568`) insert `out[path] = {}` when `path` is a single file—missing `"name"`, `"type"`, and `"size"`!
- **Key aliasing chaos:** `UPathStatResult` (`upath/_stat.py:218-249`) has to try 4 different keys per field (`uid`/`owner`/`uname`/`unix.owner`, `atime`/`time`/`last_accessed`/`accessTime`). `monty_os.py:191` checks `info.get("mtime") or info.get("modified")`.
- **Polymorphic `detail: bool` return types:** `_ls` returns `list[str] | list[dict]`, `_find` and `_glob` return `list[str] | dict[str, dict]`, `_cat` returns `bytes | dict[str, bytes]`. Every caller in `upathtools` (`async_upath.py:136-139,168`, `baseupath.py:183,201`, `basefilesystem.py:398-399`, `auto_expand_fs.py:274`) carries defensive `isinstance(entry, dict)` branches.
- **Path normalization mismatches:** Some filesystems return leading `/`, others strip it, some include protocol prefixes (`union_fs.py:220-224`, `dirfs.py:88-98`), and some file filesystems return relative names while archives return full paths (`auto_expand_fs.py:301-308`).
- **Missing optional async methods:** `AsyncFileSystem` omits `_rmdir`, `_mv`, `_checksum`, `_modified`, `_created`, forcing `upathtools` to maintain `src/upathtools/filesystems/base/protocols.py` (`SupportsRmdir`, `SupportsMv`, etc.) and check `isinstance(...)` at every call site (`wrapper.py:595,602,720,727`, `overlay_fs.py:296,334,345`, `async_upath.py:124`).

### 2.4 Glob / Find / Walk / Error Semantics
- **Sequential `_walk` (`fsspec/asyn.py:854-920`):** `AsyncFileSystem._walk` recurses one directory at a time. `BaseAsyncFileSystem` had to re-implement BFS-parallel `_walk`, `_find`, and `_glob` (`basefilesystem.py:157-412`), and `HTTPFileSystem` duplicated `_glob` yet again (`httpx_fs.py:561-618`).
- **N+1 `_du` (`fsspec/asyn.py:1002-1011`):** `_du` calls `_find` (without `detail=True`) and then awaits `_info(f)` sequentially for every file.
- **Upstream churn breaking subclasses:** `fsspec 2026.9.0` added `kwargs["prefix"] = prefix` inside `_glob` (`asyn.py:978-982`), breaking `_find` implementations that don't accept `**kwargs`; `fsspec>=2026.6.0` broke `AbstractFileSystem.mv(..., recursive=True)` on object stores (`upath/core.py:165-169`).
- **Error swallowing:** `AbstractFileSystem.exists` (`spec.py:723`) has a bare `except: return False` and `isfile` (`spec.py:798`, `asyn.py:823`) catches `Exception: return False`—silently turning auth failures, network timeouts, and bugs into `False`.
- **`DirFileSystem` mode friction (`fsspec/implementations/dirfs.py:56-60`):** `DirFileSystem` raises `ValueError` if `self.asynchronous != fs.asynchronous`, forcing awkward triple-wrapping in `helpers.py:264-272` and `wrapper.py:307-313`.

### 2.5 Free-Threading (PEP 703 / Python 3.14t+) Hazards in `fsspec`
- `fsspec/dircache.py:48-88`: `DirCache` mutates `self._cache`, `self._times`, and an `lru_cache` eviction hook with zero synchronization.
- `fsspec/implementations/memory.py:25-26`: `MemoryFileSystem.store` and `pseudo_dirs` are unsynchronized class-level mutable globals shared across threads.
- `fsspec/spec.py:86-90,117`: `_Cached._check_instance_cache` reads `cls._cache` and mutates `cls._latest` outside `cls._instantiation_lock`.
- `fsspec/asyn.py:35-38`: `get_lock()` initializes `_lock` without atomic protection (TOCTOU race under free-threading).
- `fsspec/core.py:308-309`: `open_files()` mutates `fs.auto_mkdir = auto_mkdir` directly on a globally cached `LocalFileSystem` instance!

---

## 3. Core API Design (`upathtools.fs`)

### 3.1 Design Principles
1. **Async-native, public names:** No leading underscores for primary methods (`await fs.ls(...)`, `await fs.info(...)`, `await fs.read_bytes(...)`, `await fs.write_bytes(...)`).
2. **No polymorphic `detail: bool` traps:** Separate methods with unambiguous return types (`ls()` returns `list[FileInfo]`, `ls_names()` returns `list[str]`; `find()` returns `list[FileInfo]`, `find_paths()` returns `list[str]`).
3. **Typed, immutable metadata:** `FileInfo` is a `@dataclass(frozen=True, slots=True)`—hashable-friendly, fast, zero dict-mutation races under free-threading, with extensible typed `extra` or subclassing.
4. **Explicit instance lifetime:** No metaclass cache. Every `MyFS(...)` creates a fresh instance with `async def aclose()` and `async with fs:`. URL-based reuse lives in an explicit, loop-scoped, thread-safe `FilesystemPool`.
5. **Explicit sync facade (no `__dict__` magic):** `fs.sync` returns a typed `SyncFS[Self]` facade backed by an explicit `LoopRunner`, keeping static types 100% accurate and allowing `pyproject.toml` to re-enable `ty`'s override checks.
6. **Free-threading safe by construction:** No mutable class-level globals, frozen return objects, `threading.RLock` around shared mutable stores (`MemoryFS`, `DirCache`, `Registry`, `FilesystemPool`), and immutable snapshots on reads.
7. **Per-module sync provenance:** Every module ported from `fsspec` declares `__fsspec_sync__ = ("2026.9.0", "<upstream path>")`.

### 3.2 Package Layout

```text
src/upathtools/fs/
├── __init__.py          # Public exports
├── _sync.py             # FSSPEC_SYNC_VERSION = "2026.9.0" + SyncMeta helpers
├── types.py             # FileInfo, EntryType, GrepMatch, OpenMode, WriteMode
├── errors.py            # FSTimeoutError, UnsupportedOperation, PathEscapeError, ...
├── protocols.py         # Capability protocols (ReadableFS, WritableFS, SupportsGrep, ...)
├── paths.py             # Pure POSIX/protocol path normalization, split_protocol, unchain, check_contained
├── glob.py              # glob_translate, match_glob, expand_path, other_paths
├── batch.py             # Bounded concurrency (TaskGroup + Semaphore)
├── cache.py             # Thread-safe DirCache (TTL + LRU + prefix invalidation)
├── streams.py           # AsyncReadStream, AsyncWriteStream, AsyncBufferedFile
├── runner.py            # LoopRunner (fork-safe, free-threading safe explicit loop thread)
├── sync.py              # SyncFS[TFS] typed facade + SyncFileBridge (io.BufferedIOBase)
├── base.py              # AsyncFS[TInfo] ABC + FileBackedFS[TInfo]
├── registry.py          # Thread-safe ProtocolRegistry + entry-point discovery
├── pool.py              # Loop-scoped FilesystemPool (replaces _loop_fs_cache)
├── implementations/
│   ├── local.py         # LocalFS (aiofile/to_thread + ripgrep-rs find/grep)
│   ├── memory.py        # MemoryFS (isolated by default, RLock-protected)
│   ├── dirfs.py         # DirFS (root-jailed prefix wrapper, works on any AsyncFS)
│   └── cached.py        # WholeFileCacheFS (optional local disk cache wrapper)
└── interop/
    ├── from_fsspec.py   # FsspecAdapter: wrap any fsspec AbstractFileSystem -> AsyncFS
    └── to_fsspec.py     # AsFsspecFileSystem: expose any AsyncFS -> fsspec.AsyncFileSystem
```

### 3.3 Core Types & Errors

```python
# src/upathtools/fs/types.py
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Literal

EntryType = Literal["file", "directory", "symlink", "other"]
WriteMode = Literal["overwrite", "create"]
ReadMode = Literal["rb", "rt"]
OpenMode = Literal["r", "rb", "w", "wb", "x", "xb", "a", "ab", "r+", "r+b"]

_EMPTY_META: Mapping[str, Any] = MappingProxyType({})


@dataclass(frozen=True, slots=True, kw_only=True)
class FileInfo:
    """Normalized, immutable metadata for a filesystem entry."""

    path: str  # Canonical FS-relative path (no protocol, no trailing '/' except root)
    type: EntryType
    size: int | None = None
    mtime: float | None = None  # POSIX timestamp (UTC seconds)
    ctime: float | None = None  # Creation/birth timestamp if available
    mode: int | None = None  # POSIX st_mode bits if available
    etag: str | None = None  # Content hash / version / etag
    is_symlink: bool = False
    symlink_target: str | None = None
    extra: Mapping[str, Any] = field(default=_EMPTY_META)

    @property
    def name(self) -> str:
        """Final path segment (basename)."""
        stripped = self.path.rstrip("/")
        return stripped.rsplit("/", 1)[-1] if stripped else ""

    @property
    def is_file(self) -> bool:
        return self.type == "file"

    @property
    def is_dir(self) -> bool:
        return self.type == "directory"

    def to_fsspec_dict(self) -> dict[str, Any]:
        """Convert to a standard fsspec info dict for interop."""
        out: dict[str, Any] = {
            **self.extra,
            "name": self.path,
            "type": self.type,
            "size": self.size,
        }
        if self.mtime is not None:
            out["mtime"] = self.mtime
        if self.ctime is not None:
            out["created"] = self.ctime
        if self.mode is not None:
            out["mode"] = self.mode
        if self.etag is not None:
            out["etag"] = self.etag
        if self.is_symlink:
            out["islink"] = True
            if self.symlink_target is not None:
                out["destination"] = self.symlink_target
        return out
```

Custom filesystems can either populate `extra` or subclass `FileInfo` with `@dataclass(frozen=True, slots=True, kw_only=True)` (e.g. `class E2BInfo(FileInfo): owner: str; group: str; permissions: str`), parameterized on `AsyncFS[E2BInfo]`.

```python
# src/upathtools/fs/errors.py
class FSError(OSError):
    """Base error for filesystem operations (subclass of OSError for stdlib compat)."""


class FSTimeoutError(TimeoutError):
    """Raised when a filesystem operation or sync bridge times out."""


class UnsupportedOperation(FSError):
    """Raised when a backend does not support an optional capability (e.g., write, rmdir)."""


class PathEscapeError(ValueError):
    """Raised when a path attempts to escape a rooted filesystem (e.g., via '..')."""


class FilesystemClosedError(RuntimeError):
    """Raised when operating on a closed filesystem instance."""
```

Error semantics contract:
- Standard `FileNotFoundError`, `FileExistsError`, `IsADirectoryError`, `NotADirectoryError`, `PermissionError` are raised consistently for missing/conflicting/invalid-type/denied paths.
- `exists()`, `is_file()`, `is_dir()` **only** catch `FileNotFoundError` / `NotADirectoryError`—never swallowing `PermissionError`, `FSTimeoutError`, auth errors, or transport failures.

### 3.4 Capability Protocols & File Streams

```python
# src/upathtools/fs/streams.py
from collections.abc import AsyncIterator
from typing import Protocol, Self, runtime_checkable


@runtime_checkable
class AsyncReadStream(Protocol):
    """Async binary readable stream."""

    async def read(self, size: int = -1) -> bytes: ...
    async def readline(self, size: int = -1) -> bytes: ...
    def __aiter__(self) -> AsyncIterator[bytes]: ...
    async def aclose(self) -> None: ...
    async def __aenter__(self) -> Self: ...
    async def __aexit__(self, *exc: object) -> None: ...


@runtime_checkable
class AsyncSeekableStream(AsyncReadStream, Protocol):
    def tell(self) -> int: ...
    def seek(self, offset: int, whence: int = 0) -> int: ...


@runtime_checkable
class AsyncWriteStream(Protocol):
    """Async binary writable stream."""

    async def write(self, data: bytes | memoryview) -> int: ...
    async def flush(self) -> None: ...
    async def aclose(self) -> None: ...
    async def __aenter__(self) -> Self: ...
    async def __aexit__(self, *exc: object) -> None: ...
```

```python
# src/upathtools/fs/protocols.py
from collections.abc import AsyncIterator, Mapping
from typing import Any, Protocol, runtime_checkable

from upathtools.fs.types import FileInfo, GrepMatch, WriteMode


@runtime_checkable
class SupportsRangeRead(Protocol):
    async def read_range(
        self, path: str, start: int | None = None, end: int | None = None
    ) -> bytes: ...


@runtime_checkable
class SupportsWrite(Protocol):
    async def write_bytes(
        self, path: str, data: bytes, *, mode: WriteMode = "overwrite"
    ) -> None: ...
    async def remove(
        self, path: str, *, recursive: bool = False, maxdepth: int | None = None
    ) -> None: ...
    async def mkdir(self, path: str, *, parents: bool = True, exist_ok: bool = False) -> None: ...
    async def rmdir(self, path: str) -> None: ...


@runtime_checkable
class SupportsMove(Protocol):
    async def move(
        self, src: str, dst: str, *, recursive: bool = False, overwrite: bool = True
    ) -> None: ...


@runtime_checkable
class SupportsGrep(Protocol):
    async def grep(
        self,
        path: str,
        pattern: str,
        *,
        max_count: int | None = None,
        case_sensitive: bool | None = None,
        hidden: bool = False,
        no_ignore: bool = False,
        globs: list[str] | None = None,
        context_before: int | None = None,
        context_after: int | None = None,
        multiline: bool = False,
    ) -> list[GrepMatch]: ...


@runtime_checkable
class SupportsSign(Protocol):
    async def sign(self, path: str, *, expiration: int = 100, **kwargs: Any) -> str: ...
```

### 3.5 `AsyncFS` Base Class & `SyncFS` Facade

```python
# src/upathtools/fs/base.py
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Iterable, Sequence
from contextlib import asynccontextmanager
from typing import Any, ClassVar, Literal, Self, overload

from upathtools.fs.cache import DirCache
from upathtools.fs.errors import UnsupportedOperation
from upathtools.fs.streams import AsyncBufferedFile, AsyncReadStream, AsyncWriteStream
from upathtools.fs.sync import SyncFS
from upathtools.fs.types import FileInfo, GrepMatch, WriteMode


class AsyncFS[TInfo: FileInfo = FileInfo](ABC):
    """Async-native, free-threading-safe base filesystem.

    Subclasses implement at minimum:
      - `info(path)`
      - `ls(path)`
      - `read_bytes(path)` (or `read_range(path, start, end)`)
    Writable subclasses additionally implement:
      - `write_bytes(path, data, mode=...)`
      - `remove_file(path)`
      - `mkdir(path, parents=..., exist_ok=...)`
      - `rmdir(path)`
    """

    protocol: ClassVar[str | tuple[str, ...]] = "abstract"
    root_marker: ClassVar[str] = "/"
    sep: ClassVar[str] = "/"
    local_file: ClassVar[bool] = False

    def __init__(
        self,
        *,
        batch_size: int | None = None,
        use_listings_cache: bool = False,
        listings_expiry_time: float | None = None,
        max_cached_paths: int | None = None,
    ) -> None:
        self.batch_size = batch_size
        self.dircache: DirCache[TInfo] | None = (
            DirCache(expiry_time=listings_expiry_time, max_paths=max_cached_paths)
            if use_listings_cache
            else None
        )
        self._closed = False

    # --- Lifecycle (no global instance cache) ---
    async def aclose(self) -> None:
        """Release any underlying network sessions, handles, or sandboxes."""
        self._closed = True

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    @property
    def sync(self) -> SyncFS[Self, TInfo]:
        """Typed synchronous facade backed by an explicit LoopRunner."""
        return SyncFS(self)

    # --- Path helpers (pure, class-level, overrideable) ---
    @classmethod
    def strip_protocol(cls, path: str) -> str: ...
    @classmethod
    def parent(cls, path: str) -> str: ...
    @classmethod
    def kwargs_from_url(cls, url: str) -> dict[str, Any]:
        return {}

    # --- Required read primitives ---
    @abstractmethod
    async def info(self, path: str) -> TInfo: ...

    @abstractmethod
    async def ls(self, path: str) -> list[TInfo]: ...

    @abstractmethod
    async def read_bytes(
        self, path: str, *, start: int | None = None, end: int | None = None
    ) -> bytes: ...

    # --- Derived read helpers (concrete defaults, overrideable for speed) ---
    async def ls_names(self, path: str) -> list[str]:
        return [entry.path for entry in await self.ls(path)]

    async def read_text(self, path: str, *, encoding: str = "utf-8", errors: str = "strict") -> str:
        return (await self.read_bytes(path)).decode(encoding, errors=errors)

    async def read_many(
        self,
        paths: Sequence[str],
        *,
        batch_size: int | None = None,
        on_error: Literal["raise", "omit", "return"] = "raise",
    ) -> dict[str, bytes | Exception]: ...

    async def read_ranges(
        self,
        paths: Sequence[str],
        starts: Sequence[int | None],
        ends: Sequence[int | None],
        *,
        batch_size: int | None = None,
        on_error: Literal["raise", "return"] = "return",
    ) -> list[bytes | Exception]: ...

    async def exists(self, path: str) -> bool:
        try:
            await self.info(path)
        except FileNotFoundError, NotADirectoryError:
            return False
        return True

    async def is_file(self, path: str) -> bool:
        try:
            return (await self.info(path)).is_file
        except FileNotFoundError, NotADirectoryError:
            return False

    async def is_dir(self, path: str) -> bool:
        try:
            return (await self.info(path)).is_dir
        except FileNotFoundError, NotADirectoryError:
            return False

    async def size(self, path: str) -> int | None:
        return (await self.info(path)).size

    # --- Traversal (parallel BFS by default) ---
    async def walk(
        self,
        path: str,
        *,
        maxdepth: int | None = None,
        topdown: bool = True,
        on_error: Literal["omit", "raise"] | Any = "omit",
    ) -> AsyncIterator[tuple[str, list[TInfo], list[TInfo]]]: ...

    async def find(
        self, path: str, *, maxdepth: int | None = None, withdirs: bool = False
    ) -> list[TInfo]: ...

    async def find_paths(
        self, path: str, *, maxdepth: int | None = None, withdirs: bool = False
    ) -> list[str]:
        return [e.path for e in await self.find(path, maxdepth=maxdepth, withdirs=withdirs)]

    async def glob(
        self, path: str, *, maxdepth: int | None = None, withdirs: bool = True
    ) -> list[TInfo]: ...

    async def glob_paths(
        self, path: str, *, maxdepth: int | None = None, withdirs: bool = True
    ) -> list[str]:
        return [e.path for e in await self.glob(path, maxdepth=maxdepth, withdirs=withdirs)]

    async def du(
        self, path: str, *, maxdepth: int | None = None, withdirs: bool = False
    ) -> dict[str, int]:
        """Single-pass disk usage using `find()` metadata (no N+1 `info()` calls)."""
        entries = await self.find(path, maxdepth=maxdepth, withdirs=withdirs)
        return {e.path: e.size or 0 for e in entries}

    # --- Streaming ---
    @overload
    def open(self, path: str, mode: Literal["rb"] = "rb", **kwargs: Any) -> AsyncReadStream: ...
    @overload
    def open(
        self, path: str, mode: Literal["wb", "xb", "ab"], **kwargs: Any
    ) -> AsyncWriteStream: ...
    def open(self, path: str, mode: str = "rb", **kwargs: Any) -> AsyncBufferedFile:
        """Return an async file object (default: buffered over read_bytes/write_bytes)."""
        return AsyncBufferedFile(self, path, mode=mode, **kwargs)

    # --- Optional mutation primitives (raise UnsupportedOperation by default) ---
    async def write_bytes(self, path: str, data: bytes, *, mode: WriteMode = "overwrite") -> None:
        raise UnsupportedOperation(f"{type(self).__name__} is read-only")

    async def write_text(
        self, path: str, text: str, *, encoding: str = "utf-8", mode: WriteMode = "overwrite"
    ) -> None:
        await self.write_bytes(path, text.encode(encoding), mode=mode)

    async def mkdir(self, path: str, *, parents: bool = True, exist_ok: bool = False) -> None:
        raise UnsupportedOperation(f"{type(self).__name__} does not support mkdir")

    async def rmdir(self, path: str) -> None:
        raise UnsupportedOperation(f"{type(self).__name__} does not support rmdir")

    async def remove_file(self, path: str) -> None:
        raise UnsupportedOperation(f"{type(self).__name__} does not support remove_file")

    async def remove(
        self, path: str, *, recursive: bool = False, maxdepth: int | None = None
    ) -> None: ...  # default expands paths and calls remove_file / rmdir bottom-up

    async def copy_file(self, src: str, dst: str, *, overwrite: bool = True) -> None:
        if not overwrite and await self.exists(dst):
            raise FileExistsError(dst)
        await self.write_bytes(dst, await self.read_bytes(src))

    async def copy(
        self,
        src: str | Sequence[str],
        dst: str | Sequence[str],
        *,
        recursive: bool = False,
        maxdepth: int | None = None,
        overwrite: bool = True,
    ) -> None: ...

    async def move(
        self, src: str, dst: str, *, recursive: bool = False, overwrite: bool = True
    ) -> None: ...  # safe tree move (directories + files, then remove), unlike fsspec's broken mv

    async def get_file(self, rpath: str, lpath: str) -> None: ...
    async def put_file(self, lpath: str, rpath: str, *, mode: WriteMode = "overwrite") -> None: ...
    async def touch(self, path: str, *, truncate: bool = True) -> None: ...

    # --- Higher-level operations currently on BaseAsyncFileSystem ---
    async def grep(self, path: str, pattern: str, **kwargs: Any) -> list[GrepMatch]: ...
    async def diff(self, path1: str, path2: str, **kwargs: Any) -> str: ...
    async def jq(self, path: str, filter: str, **kwargs: Any) -> Any: ...
    def invalidate_cache(self, path: str | None = None) -> None:
        if self.dircache is not None:
            self.dircache.invalidate(path)
```

```python
# src/upathtools/fs/sync.py
class SyncFS[TFS: AsyncFS[Any], TInfo: FileInfo = FileInfo]:
    """Explicit, statically typed synchronous facade over an AsyncFS."""

    __slots__ = ("_fs", "_runner")

    def __init__(self, fs: TFS, runner: LoopRunner | None = None) -> None:
        self._fs = fs
        self._runner = runner or get_default_runner()

    def info(self, path: str) -> TInfo:
        return self._runner.run(self._fs.info(path))

    def ls(self, path: str) -> list[TInfo]:
        return self._runner.run(self._fs.ls(path))

    def read_bytes(self, path: str, *, start: int | None = None, end: int | None = None) -> bytes:
        return self._runner.run(self._fs.read_bytes(path, start=start, end=end))

    def write_bytes(self, path: str, data: bytes, *, mode: WriteMode = "overwrite") -> None:
        return self._runner.run(self._fs.write_bytes(path, data, mode=mode))

    def open(self, path: str, mode: str = "rb", **kwargs: Any) -> io.IOBase: ...
    def close(self) -> None:
        self._runner.run(self._fs.aclose())

    # ... 1:1 mirror of AsyncFS public methods, statically typed (no __dict__ magic)
```

### 3.6 `LoopRunner` & `FilesystemPool` (Lifetime & Free-Threading)

- **`LoopRunner` (`src/upathtools/fs/runner.py`):**
  - Owns a dedicated background `asyncio.AbstractEventLoop` thread created lazily under a module-level `threading.Lock()` (created at import time, never lazily allocated like `fsspec/asyn.py:36`).
  - Properly resets on `os.register_at_fork(after_in_child=...)` by resetting the actual lock & runner reference (fixing fsspec's `global lock` typo).
  - Uses `concurrent.futures.Future.result(timeout=...)` on `asyncio.run_coroutine_threadsafe` (clean cancellation on timeout, no 1-second polling loop like `fsspec/asyn.py:92-99` except optional interruptible chunking on Windows).
  - Detects same-loop re-entry and raises a clear `RuntimeError("Cannot call sync facade from the runner's own event loop; await the AsyncFS method instead")`.
- **`FilesystemPool` (`src/upathtools/fs/pool.py`):**
  - Replaces `async_ops.py`'s ad-hoc `_loop_fs_cache`.
  - Keyed by `(weakref to loop, protocol, canonicalized frozen storage_options)` protected by `threading.RLock`.
  - Automatically drops entries when an event loop is garbage-collected or closed, and provides `async def aclose_loop(loop=None)` to deterministically close HTTP sessions / sandboxes on shutdown.
  - Strictly **opt-in** (used by URL convenience functions like `read_path("https://...")`, never implicitly attached to `MyFS()` constructors).

---

## 4. Bidirectional `fsspec` Interop

### 4.1 Inbound: `FsspecAdapter` (`from_fsspec`)

Wraps any third-party or built-in `fsspec.AbstractFileSystem` (`s3fs`, `gcsfs`, `adlfs`, `zip`, `tar`, `webdav4`, `hf`) as a core `AsyncFS[FileInfo]`:

```python
# src/upathtools/fs/interop/from_fsspec.py
class FsspecAdapter(AsyncFS[FileInfo]):
    """Adapt any fsspec AbstractFileSystem or AsyncFileSystem into AsyncFS."""

    def __init__(
        self,
        fs: fsspec.AbstractFileSystem,
        *,
        max_concurrency: int | None = None,
        close_underlying: bool = False,
    ) -> None:
        super().__init__(batch_size=getattr(fs, "batch_size", None))
        self.raw = fs  # explicit escape hatch for backend-specific APIs
        self._is_async = isinstance(fs, fsspec.asyn.AsyncFileSystem)
        self._sem = asyncio.Semaphore(max_concurrency) if max_concurrency else None
        self._close_underlying = close_underlying
```

Key behaviors:
- **Async dispatch:** If `self._is_async`: awaits `fs._info`, `fs._ls`, `fs._cat_file`, `fs._pipe_file`, `fs._rm_file`, `fs._mkdir`, `fs._makedirs`, `fs._glob`, `fs._find`. Checks `SupportsRmdir` / `SupportsMv` (`_rmdir`, `_mv`) before falling back to `asyncio.to_thread(fs.rmdir/mv)` or generic copy+remove.
- **Sync dispatch:** For sync `AbstractFileSystem` (e.g. `ZipFileSystem`, `HfFileSystem`, `WebdavFileSystem`), offloads via `asyncio.to_thread` bounded by `self._sem`.
- **Info normalization:** Converts fsspec dicts to `FileInfo`, normalizing `name` -> `path`, `size`, `type`, `mtime`/`modified`/`LastModified`/`updated` -> `mtime`, `created`/`CreationTime` -> `ctime`, `mode`, `etag`/`ETag`, `islink`/`destination`, placing remaining keys into `extra`.
- **Streaming:** Uses `await fs.open_async(...)` when supported; otherwise wraps `fs.open(...)` in a threaded `AsyncStreamAdapter` (or `AsyncBufferedFile` if non-streamable).
- **Helper:** `ensure_async_fs(fs: AsyncFS | fsspec.AbstractFileSystem) -> AsyncFS` returns `fs` unchanged if already `AsyncFS`, else caches one `FsspecAdapter` per instance (via `WeakKeyDictionary` under lock, not mutating `fs.__dict__`).

### 4.2 Outbound: `AsFsspecFileSystem` (`to_fsspec`)

Exposes any core `AsyncFS` as a genuine `fsspec.asyn.AsyncFileSystem` subclass so `pandas`, `pyarrow`, `dask`, `universal-pathlib`, `fsspec.open`, and `fsspec.FSMap` work without changes:

```python
# src/upathtools/fs/interop/to_fsspec.py
class AsFsspecFileSystem(fsspec.asyn.AsyncFileSystem):
    """Expose a core AsyncFS as an fsspec AsyncFileSystem."""

    cachable = False
    mirror_sync_methods = True

    def __init__(
        self,
        core_fs: AsyncFS[Any],
        *,
        asynchronous: bool = False,
        loop: asyncio.AbstractEventLoop | None = None,
        runner: LoopRunner | None = None,
    ) -> None:
        # Always bind a usable loop when synchronous calls are expected
        resolved_loop = loop or (runner.loop if runner else get_default_runner().loop)
        super().__init__(asynchronous=asynchronous, loop=resolved_loop)
        self.core_fs = core_fs
        self.protocol = core_fs.protocol
        self.root_marker = core_fs.root_marker
        self.sep = core_fs.sep
        self.local_file = core_fs.local_file
```

Key behaviors:
- Implements all `fsspec.asyn.AsyncFileSystem` coroutine hooks (`_info`, `_ls`, `_cat_file`, `_pipe_file`, `_rm_file`, `_rm`, `_cp_file`, `_mkdir`, `_makedirs`, `_rmdir`, `_mv`, `_exists`, `_isfile`, `_isdir`, `_find`, `_glob`, `_walk`, `_du`, `_get_file`, `_put_file`, `open_async`) by delegating to `self.core_fs` and converting `FileInfo` via `.to_fsspec_dict()`.
- Implements `_open(path, mode, ...)` returning a sync `SyncFileBridge(io.BufferedIOBase)` (or `AbstractBufferedFile` subclass for range-capable backends) running `core_fs.open()` / `read_bytes()` / `write_bytes()` on `self.loop`.
- Provides `register_fsspec_bridge(protocol, core_cls)` to generate and register a thin `AsFsspecFileSystem` subclass in `fsspec.registry` (and `pyproject.toml` entry points) so `fsspec.open("md://...")` and `UPath("pkg://...")` keep working seamlessly.

### 4.3 What Cannot Be Bridged Cleanly

Be explicit about adapter boundaries—these are fundamental architectural mismatches, not bugs:

1. **Sync calls on a loop-bound `AsyncFS` from *inside* that same loop:** If a caller creates a core `AsyncFS` whose underlying client (`httpx2.AsyncClient`, `e2b.AsyncSandbox`, `fastmcp.Client`) is already bound to the caller's running event loop, calling `to_fsspec(fs).cat_file(...)` synchronously *on that same event loop thread* cannot work in Python (`asyncio` loops are non-reentrant). Mitigation: document clearly that `to_fsspec(fs)` for sync libraries (`pandas`, sync `UPath`) must either be created unbound (lazy session initialized on the `LoopRunner` thread) or called from outside that loop.
2. **`fsspec` Transactions (`autocommit=False`, `start_transaction()`):** `fsspec`'s multi-file deferred commit relies on mutable `AbstractBufferedFile.commit()`/`discard()`. The outbound adapter can buffer writes until close, and can queue `write_bytes()` on `end_transaction()`, but cannot provide atomic server-side commits for backends that lack them.
3. **Global instance caching & identity (`fs1 is fs2`, `cls.current()`, `clear_instance_cache()`):** Core `AsyncFS` intentionally drops `_Cached`. `AsFsspecFileSystem` sets `cachable = False`; code relying on `MyFS() is MyFS()` must migrate to explicit instance passing or `FilesystemPool`.
4. **Undocumented backend-specific methods/kwargs:** `FsspecAdapter` does **not** use `__getattr__` proxying (which breaks typing and hides bugs, as seen in `WrapperFileSystem`). Backend-specific extensions (e.g., `s3fs.get_tags()`, `gcsfs.setxattrs()`) are accessed explicitly via `adapter.raw`.
5. **Deep `fsspec` internal hooks:** `fsspec.parquet`, `fsspec.prefetcher`, and `MMapCache` reach into private `AbstractBufferedFile._fetch_range` / `dircache` attributes. Callers needing those must pass `to_fsspec(fs)` (outbound adapter) or the raw `s3fs`/`gcsfs` instance directly.
6. **Pickling / `to_json()` / `from_json()` of live stateful instances:** Live MCP clients, sandbox handles, or `WrapperFileSystem` with Python callbacks cannot be serialized via `__reduce__`. Serialization lives cleanly in `upathtools_config` (`FileSystemConfig.model_dump_json()`), not on live filesystem instances.

---

## 5. Conformance Strategy & Sync Tracking

### 5.1 Leveraging `fsspec`'s Test Suite

Installed `fsspec 2026.9.0` already ships a reusable abstract compliance suite at `fsspec/tests/abstract/`:
- `AbstractCopyTests` (`copy.py`, 14 tests)
- `AbstractGetTests` (`get.py`, 14 tests)
- `AbstractPutTests` (`put.py`, 14 tests)
- `AbstractMvTests` (`mv.py`, 2 tests)
- `AbstractOpenTests` (`open.py`, 1 test)
- `AbstractPipeTests` (`pipe.py`, 1 test)
- `GLOB_EDGE_CASES_TESTS` (`common.py`, 26 parametrized glob/maxdepth/recursive cases)

Upstream `fsspec` (`tag 2026.9.0`) additionally tests core utilities and implementations in:
- `fsspec/tests/test_utils.py` (`glob_translate`, `other_paths`, `check_contained`, `infer_storage_options`)
- `fsspec/tests/test_spec.py` (`walk`, `find`, `glob`, `expand_path`, `cat`, `pipe`, `du`)
- `fsspec/tests/test_async.py` (`_run_coros_in_chunks`, `sync`, cancellation, batching)
- `fsspec/implementations/tests/test_local.py`, `test_memory.py`, `test_dirfs.py`

### 5.2 Three-Layer Conformance Harness (`tests/conformance/`)

1. **Direct `fsspec.tests.abstract` execution (not vendored):**
   - `tests/conformance/test_fsspec_abstract.py` subclasses `AbstractFixtures` + all 6 `Abstract*Tests` mixins against:
     - `to_fsspec(LocalFS())`
     - `to_fsspec(MemoryFS())`
     - `to_fsspec(DirFS(MemoryFS(), root="/sub"))`
     - `to_fsspec(from_fsspec(fsspec.filesystem("memory", global_store=False, skip_instance_cache=True)))` (full round-trip!)
   - Because `fsspec.tests.abstract` is imported from the installed `fsspec` package, running `uv run pytest tests/conformance/` automatically tests compatibility against whichever `fsspec` version is installed.
2. **Vendored & adapted unit conformance suites (pinned to `2026.9.0`):**
   - `tests/conformance/test_glob_conformance.py`: imports `GLOB_EDGE_CASES_TESTS` from `fsspec.tests.abstract.common` plus `glob_translate` / `other_paths` / `check_contained` (path-traversal escape tests from `fsspec 2026.9.0`) and runs them natively against `AsyncFS.glob()`, `find()`, `walk()`, `copy()`.
   - `tests/conformance/test_core_contract.py`: reusable native async contract suite (`AsyncFSContractTests`) that every `upathtools` filesystem can inherit—verifying `FileInfo` invariants, exact exception types (`FileNotFoundError` vs `IsADirectoryError` vs `NotADirectoryError`), `exists()` not swallowing errors, concurrent reads/writes, cancellation safety, and `aclose()` idempotency.
   - `tests/conformance/test_free_threading.py`: multi-threaded stress suite (`ThreadPoolExecutor(max_workers=8)`) hammering `MemoryFS`, `LocalFS`, `DirCache`, `ProtocolRegistry`, `FilesystemPool`, and `LoopRunner` simultaneously (run in CI on both standard 3.14 and free-threaded `3.14t` if available).
3. **Explicit Intentional-Divergence Matrix:**
   Maintain a table in `tests/conformance/DIVERGENCES.md` listing every `fsspec` behavior intentionally *not* replicated (and why):
   - No global `_Cached` instance reuse (`MemoryFS()` is isolated by default).
   - `exists()` / `is_file()` / `is_dir()` do not swallow non-existence errors (`PermissionError`, `TimeoutError`).
   - `find()` on a single file returns `[FileInfo(...)]` with real metadata instead of `{path: {}}`.
   - `move(..., recursive=True)` works on directory trees without `cp_file` failing on pseudo-directories.
   - `DirFS` never requires matching an `asynchronous` boolean flag.
   - `LocalFS.open()` does not mutate a global `auto_mkdir` flag.

### 5.3 Keeping the "Synced With" Note Honest

Per-module sync notes rot unless verified mechanically:

1. **Single source of truth (`src/upathtools/fs/_sync.py`):**
   ```python
   FSSPEC_SYNC_VERSION = "2026.9.0"


   @dataclass(frozen=True, slots=True)
   class FsspecSyncNote:
       version: str
       upstream: tuple[str, ...]
       notes: str = ""
   ```
2. **Per-module declaration** at the top of every ported module (`paths.py`, `glob.py`, `cache.py`, `implementations/local.py`, `implementations/memory.py`, `implementations/dirfs.py`, `interop/from_fsspec.py`, `interop/to_fsspec.py`):
   ```python
   __fsspec_sync__ = FsspecSyncNote(
       version="2026.9.0",
       upstream=("fsspec/utils.py:glob_translate", "fsspec/asyn.py:AsyncFileSystem._glob"),
       notes="Uses parallel BFS find(); intentionally omits server-side prefix kwarg except in FsspecAdapter.",
   )
   ```
3. **Automated enforcement (`tests/conformance/test_sync_notes.py` + `scripts/check_fsspec_sync.py`):**
   - A fast unit test asserts that every module in `src/upathtools/fs/` containing ported logic defines `__fsspec_sync__` with a valid `FsspecSyncNote` whose `.version` is a known version and whose `upstream` file paths still exist in the installed `fsspec` wheel.
   - A `duty check-fsspec-sync` command (and optional scheduled CI job, *non-blocking* on regular PRs so an upstream `fsspec` release doesn't break unrelated PRs) compares `FSSPEC_SYNC_VERSION` against `fsspec.__version__`, lists every module carrying `__fsspec_sync__` and its upstream symbol targets, and prints the GitHub compare URL (`https://github.com/fsspec/filesystem_spec/compare/2026.9.0...<new>`). Updating `FSSPEC_SYNC_VERSION` requires auditing those listed files and updating each module's `__fsspec_sync__`.

---

## 6. The Path Layer: `UPath` vs. Own Path Class Later

### 6.1 Current Coupling & Friction

Today `upathtools` has **two overlapping async path layers** built on `UPath`:
- `AsyncUPath` (`src/upathtools/async_upath.py`, subclasses `upath.extensions.ProxyUPath`)
- `BaseUPath[TInfoDict]` (`src/upathtools/filesystems/base/baseupath.py`, subclasses `upath.UPath`)

Both duplicate ~15 `a*` methods (`aread_bytes`, `aread_text`, `awrite_bytes`, `aexists`, `aiterdir`, `aglob`, `aopen`, `acopy`, `amove`, ...), and `BaseUPath` carries workarounds for `upath` bugs (`BaseUPath.name` for `WrappedFileSystemFlavour.splitroot`, `BaseUPath._fs_factory` for `_get_kwargs_from_urls`, `_ensure_upath_registered` in `basefilesystem.py:44-58`). Meanwhile `cli_ops.py` ignores `a*` methods and wraps sync `UPath` calls in `asyncio.to_thread`.

### 6.2 Options & Trade-Offs

| Option | Description | Pros | Cons |
| --- | --- | --- | --- |
| **A. Keep `UPath` permanently** | Core `AsyncFS` stays internal/filesystem-level; `UPath` / `AsyncUPath` remain the sole path API (bridged via `AsFsspecFileSystem` for sync ops and direct `AsyncFS` dispatch for `a*` methods). | Zero breaking changes for `UPath` callers (`isinstance(p, UPath)`, `pydantic_type.py`, `jinjarope`, downstream tools); keeps `upath`'s cloud/archive/Windows path parsing. | Permanent hard dependency on both `universal-pathlib` and `fsspec` (because `upath` imports `fsspec` in 12 modules); must maintain `upath` bug workarounds and dual `BaseUPath`/`AsyncUPath` inheritance quirks. |
| **B. Keep `UPath` during migration, add native `AsyncPath` / `VPath` later (Recommended)** | Phases 1–11 keep `UPath` & `AsyncUPath` 100% working (upgrading `AsyncUPath` to talk directly to `AsyncFS`). Once `AsyncFS` is proven, introduce a clean, lightweight `AsyncPath[TInfo]` directly on `AsyncFS` (implementing `JoinablePath` / `__vfspath__`, pure POSIX/URL parsing, `.to_upath()` / `from_upath()`), unifying `AsyncUPath` + `BaseUPath`. | Incremental and low-risk; zero breakage during the core filesystem rewrite; unlocks a genuinely fsspec-free & upath-free `upathtools` core in v2.0 (with `upath` and `fsspec` as optional extras); `cli_ops` and `filetree` can run natively async without `to_thread`. | Temporarily two path types exist once `AsyncPath` is added (mitigated by clear deprecation of `BaseUPath` and `to_upath()` conversion). |
| **C. Replace `UPath` immediately in Phase 1** | Build a brand-new path class alongside `AsyncFS` from day one and migrate all 35+ `*Path` subclasses together. | Single clean cut. | Huge scope blast radius; re-implements tricky URL/Windows/cloud/chain path parsing (`upath` is ~5,200 LOC of edge cases) before `AsyncFS` itself is validated; high risk of breaking downstream consumers (`agentpool`, `docler`, `slashed`). |

### 6.3 Recommendation

**Choose Option B.** Specifically:
1. **Now (Steps 1–11):** Keep `UPath` and all `*Path` subclasses working via `AsFsspecFileSystem`. Upgrade `AsyncUPath.afs()` and `BaseUPath.afs()` to return `AsyncFS` (or `ensure_async_fs`), eliminating the `fsspec` `_loop_fs_cache` hacks and duplicating less code. Keep `upathtools.pydantic_type` intact.
2. **Later (Step 12 / v2.0):** Introduce `upathtools.fs.AsyncPath[TInfo]` (backed directly by `AsyncFS[TInfo]`, using `pathlib`'s `PurePosixPath` / `PurePath` semantics + `VFSPathLike.__vfspath__`, zero `fsspec`/`upath` imports), migrate `cli_ops` and `filetree` to accept `AsyncPath | UPath`, and offer `to_upath()` when `universal-pathlib` is installed.

---

## 7. Incremental Migration Order (Small Shippable Steps)

Each step below is a self-contained PR with clear boundaries and acceptance tests, suitable for handing directly to a coding agent.

### Step 1 — Core Types, Errors, Capability Protocols & Sync Metadata
- **Scope:** Create `src/upathtools/fs/{__init__,_sync,types,errors,protocols}.py`. Define `FileInfo`, `EntryType`, `WriteMode`, `GrepMatch` (re-exported by `filesystems/base` to avoid duplication), error hierarchy, runtime-checkable capability protocols, and `FsspecSyncNote` (`FSSPEC_SYNC_VERSION = "2026.9.0"`).
- **Non-goals:** No filesystem implementations, no changes to existing `filesystems/`.
- **Acceptance tests:**
  - `FileInfo` immutability, `.name`/`.is_file`/`.is_dir`, `.to_fsspec_dict()` round-trip.
  - `issubclass(FSError, OSError)`, `issubclass(FSTimeoutError, TimeoutError)`.
  - `uv run ty check` and `uvx ruff check .` pass with zero ignores.

### Step 2 — Ported Path, Glob, Batch & Thread-Safe Cache Primitives
- **Scope:** Create `src/upathtools/fs/{paths,glob,batch,cache}.py` with `__fsspec_sync__` notes referencing `fsspec 2026.9.0`:
  - `paths.py`: `split_protocol`, `strip_protocol`, `parent_path`, `normalize_path`, `unchain`, `check_contained` (path-traversal guard), `infer_storage_options`, `tokenize`.
  - `glob.py`: `glob_translate`, `compile_glob`, `other_paths`, `has_magic`.
  - `batch.py`: `run_bounded(coros, batch_size=..., return_exceptions=...)` using `asyncio.Semaphore` + structured cancellation (cleaner than `fsspec`'s `_run_coros_in_chunks`).
  - `cache.py`: `DirCache[TInfo]` with `threading.RLock`, TTL expiry, LRU cap, prefix invalidation (`invalidate("/a")` drops `/a` and `/a/*`).
- **Acceptance tests:**
  - Ported `glob_translate`, `other_paths`, `check_contained`, `unchain` test cases matching `fsspec 2026.9.0`.
  - `DirCache` TTL, LRU eviction, prefix invalidation, and 8-thread concurrent read/write/invalidate stress test.
  - `run_bounded` verifies concurrency limit, exception propagation, and clean task cancellation.

### Step 3 — `AsyncFS` Base Class, Streams, `LoopRunner` & `SyncFS` Facade
- **Scope:** Create `src/upathtools/fs/{streams,runner,sync,base}.py`.
  - `AsyncFS[TInfo]` with default parallel-BFS `walk()`, `find()`, `glob()`, single-pass `du()`, `read_many()`, `read_ranges()`, `copy()`, `move()`, `remove()`, `grep()`, `diff()`, `jq()`, `open()` returning `AsyncBufferedFile`.
  - `LoopRunner` (fork-safe, free-threading safe) + `SyncFS[TFS, TInfo]` + `SyncFileBridge(io.BufferedIOBase)`.
- **Acceptance tests:**
  - Minimal mock read-only and read-write `AsyncFS` subclasses pass tests for `walk` (topdown/bottomup/maxdepth/on_error), `find` (single file returns full `FileInfo`), `glob`, `du`, `copy`, `move` (trees + files), `remove`, `grep`, `open` (`rb`/`wb`/`ab`/`r+b`), and `fs.sync.*`.
  - Calling `fs.sync.*` from inside the runner's own loop raises a clear `RuntimeError`; calling from another thread or outside a loop succeeds.
  - `exists()` propagates `PermissionError` / `FSTimeoutError` rather than returning `False`.

### Step 4 — Bidirectional `fsspec` Interop, Registry & Loop-Scoped Pool
- **Scope:** Create `src/upathtools/fs/interop/{from_fsspec,to_fsspec}.py`, `src/upathtools/fs/{registry,pool}.py`.
  - `FsspecAdapter` + `ensure_async_fs()`.
  - `AsFsspecFileSystem` + `to_fsspec()` + `register_fsspec_bridge()`.
  - Thread-safe `ProtocolRegistry` (with entry point group `"upathtools.filesystems"` + fallback lookup via `fsspec.registry` wrapped in `FsspecAdapter`).
  - `FilesystemPool` (loop-scoped, weak-ref/closed-loop pruning, `aclose_loop()`).
- **Acceptance tests:**
  - Round-trip: `from_fsspec(fsspec.filesystem("memory", ...))` and `to_fsspec(mock_core_fs)` pass full read/write/ls/glob/open/stat checks.
  - `UPath` and `fsspec.open` work against `to_fsspec(mock_core_fs)` (including text mode and compression).
  - `FilesystemPool` isolates instances across two sequential `asyncio.run()` calls on the same thread and across threads (matching `tests/test_async_fs_cache.py`).

### Step 5 — Native Core Implementations: `MemoryFS`, `LocalFS`, `DirFS`
- **Scope:** Create `src/upathtools/fs/implementations/{memory,local,dirfs}.py`.
  - `MemoryFS`: isolated per instance by default (optional explicit shared `MemoryStore` object instead of hidden global class dict), `threading.RLock`-protected, full `FileInfo` (`mtime`, `ctime`, `size`).
  - `LocalFS`: async via `asyncio.to_thread` / `aiofile` (for `open()`), `ripgrep-rs` fast path for `find()` and `grep()`, proper symlink metadata (`is_symlink`, `symlink_target`, `mode`, `uid`, `gid`, `ino`, `nlink`), no mutable global `auto_mkdir`.
  - `DirFS`: root-jailed prefix wrapper over any `AsyncFS[TInfo]` with `PathEscapeError` on `..` escape, preserving `TInfo` and rewriting paths cleanly (no `asynchronous` flag mismatch).
- **Acceptance tests:**
  - Unit tests for `MemoryFS`, `LocalFS`, `DirFS` (including path-escape rejection, symlinks, `ripgrep-rs` `find` parity with `walk`, `auto_mkdir` per-instance isolation).
  - Multi-threaded stress tests on `MemoryFS` and `LocalFS`.

### Step 6 — `fsspec` Conformance Benchmark & Sync-Note Gate
- **Scope:** Create `tests/conformance/{test_fsspec_abstract,test_glob_conformance,test_core_contract,test_free_threading,test_sync_notes}.py`, `tests/conformance/DIVERGENCES.md`, and `scripts/check_fsspec_sync.py`.
- **Acceptance tests:**
  - All 46 tests in `fsspec.tests.abstract` (`copy`, `get`, `put`, `mv`, `open`, `pipe`) pass against `to_fsspec(LocalFS())`, `to_fsspec(MemoryFS())`, and `to_fsspec(DirFS(MemoryFS(), "/root"))`.
  - All 26 cases of `GLOB_EDGE_CASES_TESTS` pass natively on `LocalFS` and `MemoryFS`.
  - `test_sync_notes.py` verifies all `__fsspec_sync__` declarations match `FSSPEC_SYNC_VERSION`.

### Step 7 — Wire Core Helpers & `AsyncUPath` to `upathtools.fs`
- **Scope:** Update `src/upathtools/{async_ops,core,helpers,monty_os,async_upath,cli_ops}.py`:
  - `async_ops.py`: `read_path`, `read_folder`, `list_files`, `read_folder_as_text`, `fsspec_grep` use `FilesystemPool` + `ensure_async_fs()` internally; retain `get_async_fs` / `to_async_fs` as backwards-compatible wrappers.
  - `monty_os.py`: `FsspecOS` accepts `AsyncFS | fsspec.AbstractFileSystem` via `ensure_async_fs()`, using `LoopRunner` instead of `fsspec.asyn.get_loop()`.
  - `helpers.py`: `upath_to_fs()` uses `DirFS` when returning `AsyncFS` (or `to_fsspec(DirFS(...))` for legacy callers).
  - `cli_ops.py`: `_run_sync` uses `LoopRunner` instead of `fsspec.asyn.get_loop()`.
- **Acceptance tests:**
  - Entire existing `tests/` suite (230 unit tests) passes unchanged.
  - New tests passing core `AsyncFS` instances directly to `read_path`, `read_folder`, `list_files`, `FsspecOS`, and `fsspec_grep`.

### Step 8 — Migrate Combining & Wrapper Filesystems (`Wrapper`, `Union`, `FlatUnion`, `Overlay`, `Delegating`, `AutoExpand`, `Skills`)
- **Scope:** Port the 6 composition filesystems (`wrapper.py`, `union_fs.py`, `flat_union_fs.py`, `overlay_fs.py`, `delegating_fs.py`, `auto_expand_fs.py`, `skills_fs.py`) to subclass `AsyncFS`. Provide thin compatibility wrappers or `to_fsspec()` factories where fsspec protocol registration is expected (`union://`, `flatunion://`, `overlay://`).
- **Key cleanup:**
  - `WrapperFileSystem` drops `__getattr__` delegation and `AsyncFileSystemWrapper` hacks; uses `ensure_async_fs()`.
  - `DelegatingFileSystem` drops `_call_fs_method` sync/async introspection hack.
  - `AutoExpandFS` uses `ProtocolRegistry` instead of `fsspec.filesystem`.
- **Acceptance tests:**
  - All existing `test_wrapper_fs.py`, `test_union_fs.py`, `test_delegating_fs.py`, `test_auto_expand_fs.py` pass (both native `AsyncFS` and via `to_fsspec()`).

### Step 9 — Migrate File-Based & Python Introspection Filesystems
- **Scope:** Port `BaseAsyncFileFileSystem` -> `FileBackedFS` (`src/upathtools/fs/base.py`), then migrate:
  - File-based: `MarkdownFileSystem`, `SqliteFileSystem`, `TreeSitterFileSystem`, `OpenAPIFileSystem`, `JsonSchemaFileSystem`, `TarFileSystem` (replacing `AbstractArchiveFileSystem`).
  - Python introspection: `PackageFileSystem`, `DistributionFileSystem`, `BaseModelFileSystem`, `BaseModelInstanceFileSystem`.
  - Replace `fsspec.open(...)` inside `_load()` methods with `AsyncFS.read_bytes()` via `ProtocolRegistry` / `FilesystemPool`.
- **Acceptance tests:**
  - All existing `test_sqlite_fs.py`, `test_treesitter_fs.py`, `test_openapi_fs.py`, `test_jsonschema_from_type.py`, `test_package_fs.py`, `test_info_fields_annotations.py` pass.
  - `test_get_upath.py` passes via `AsFsspecFileSystem` bridge.

### Step 10 — Migrate Remote & HTTP Filesystems
- **Scope:** Port `HTTPFileSystem` (`httpx_fs.py`) and the 10 `remote_filesystems/` (`appwrite`, `gist`, `github`, `gitlab`, `issue`, `linear`, `mcp`, `mcp_tools`, `notion`, `wiki`) to subclass `AsyncFS[XInfo]`.
- **Key cleanup:**
  - Delete all manual `sync_wrapper(...)` declarations (replaced by `.sync` facade + `AsFsspecFileSystem` bridge for entry points).
  - Replace `weakref.finalize` loop-closing hacks in `httpx_fs.py` with explicit `async def aclose()` + `async with fs:`.
  - Replace `fsspec`'s `DirCache` usage (7 files) with core thread-safe `DirCache`.
- **Acceptance tests:**
  - `test_httpx_filesystem.py`, `test_mcp_fs.py`, `test_mcp_tools_fs.py`, `test_notion_fs.py`, `test_wiki_fs.py`, `test_misc_races.py` pass.
  - Integration tests (`pytest -m integration`) verified where credentials are available.

### Step 11 — Migrate Sandbox Filesystems & `upathtools_config`
- **Scope:**
  - Port all 12 `sandbox_filesystems/` (`beam`, `cloudflare`, `codex`, `daytona`, `e2b`, `hopx`, `microsandbox`, `modal`, `pyodide`, `sprites`, `srt`, `vercel`) to `AsyncFS[XInfo]`.
  - Delete ~150+ lines of repetitive `ls = sync_wrapper(_ls)` / `cat_file = sync_wrapper(_cat_file)` boilerplate across these 12 files.
  - Update `upathtools_config/base.py`: add `create_core_fs() -> AsyncFS` (using `DirFS` for `root_path` and core `WholeFileCacheFS` for `cached`), while keeping `create_fs() -> fsspec.AbstractFileSystem` returning `to_fsspec(self.create_core_fs())` for backwards compatibility during 1.x.
- **Acceptance tests:**
  - All tests in `tests/sandbox_filesystems/` pass.
  - `upathtools_config` round-trip tests verify both `create_core_fs()` and `create_fs()` for representative configs (local, memory, sandbox, remote, `URIFileSystemConfig`, `root_path`, `cached`).

### Step 12 — Type-Checker Hardening, Deprecation Cleanup & Optional Path Layer
- **Scope:**
  - Re-enable `incompatible-attribute-override` and `incompatible-method-override` in `[tool.ty.rules]` (`pyproject.toml:452-453`) for all `src/upathtools/fs/` and migrated modules.
  - Deprecate `BaseAsyncFileSystem` / `BaseFileSystem` / `filesystems/fsspec_filesystems/*` mixin wrappers (replaced by `from_fsspec()`).
  - Decide on v2.0 cutover: whether `fsspec` and `universal-pathlib` become optional extras (e.g. `upathtools[fsspec]`, `upathtools[upath]`) and whether to ship native `AsyncPath` (Option B from Section 6).
- **Acceptance tests:**
  - `uv run ty check`, `uvx ruff check .`, `uv run deptry src`, and `uv run pytest` all pass cleanly.
  - `test_import.py` verifies `import upathtools.fs` succeeds without importing optional backends (and, if v2.0 makes fsspec optional, without `fsspec` installed).

---

### 7.1 How `agentpool` (Main Consumer) Migrates

> **Grounded facts vs. guesses:** From `AGENTS.md`, `pyproject.toml:469` (`# "agentpool>=1.15.0"`), and modules built specifically for agent workflows (`skills_fs.py`, `monty_os.py`, `WrapperFileSystem.mount()`, `upathtools_config.FilesystemConfigType`, `sandbox_filesystems/*`, `BaseAsyncFileSystem._grep`, `MCPFileSystem`/`MCPToolsFileSystem`), we know `agentpool` consumes `upathtools` for agent workspace filesystems, sandbox backends, config-driven FS creation, skills discovery, and Monty OS bridging. Specific internal module/call-site names inside `agentpool` are **guesses** because `agentpool` lives outside this worktree.

Phased migration for `agentpool` (zero big-bang breakage):

1. **Phase A (during `upathtools` Steps 1–7): Zero changes required in `agentpool`.**
   - `upathtools` keeps `BaseAsyncFileSystem`, `upathtools_config.create_fs()`, `AsyncUPath`, `WrapperFileSystem`, `FsspecOS`, and all fsspec entry points backwards-compatible.
2. **Phase B (after `upathtools` Steps 7–8): Adopt `AsyncFS` at `agentpool`'s filesystem boundary.**
   - *(Guess)* In `agentpool`'s workspace/environment abstraction and toolsets (file read/write/list/edit/grep tools), accept `AsyncFS | fsspec.AbstractFileSystem` and normalize once at entry with `fs = ensure_async_fs(raw_fs)`.
   - Replace private fsspec coroutine calls (`await fs._cat_file(...)`, `await fs._pipe_file(...)`, `await fs._ls(..., detail=True)`, `await fs._grep(...)`, `await fs._exists(...)`) with the public, typed `AsyncFS` methods (`read_bytes`, `write_bytes`, `ls`, `grep`, `exists`) and `FileInfo` attribute access (`entry.path`, `entry.is_dir`, `entry.size`) instead of `entry["name"]` / `entry.get("type")`.
   - Remove any `is_directory(fs, ...)` or `SupportsRmdir` / `isinstance(fs, AsyncFileSystem)` workarounds in `agentpool`.
3. **Phase C (after `upathtools` Steps 10–11): Switch config construction & lifecycle.**
   - Switch `fs_config.create_fs(ensure_async=True)` -> `fs_config.create_core_fs()`.
   - Use `async with fs:` / `await fs.aclose()` in `agentpool`'s agent/environment shutdown hooks so HTTP clients, MCP sessions, and sandboxes are closed deterministically rather than leaking through fsspec's global cache or `weakref.finalize`.
   - Keep `to_fsspec(fs)` only at edges that pass filesystems into third-party fsspec consumers (e.g. `UPath`, `pandas`, `pyarrow`).
4. **Phase D (for `upathtools` 2.0):** Drop direct `fsspec` type annotations and imports in `agentpool` (except where external cloud drivers like `s3fs` are configured directly, wrapped via `from_fsspec`).

---

## 8. Risks & Open Decisions for the Owner

### 8.1 Technical Risks

| Risk | Likelihood | Impact | Mitigation |
| --- | --- | --- | --- |
| **Dual-inheritance MRO traps** if migrated classes try to inherit from both `AsyncFS` and `fsspec.AsyncFileSystem` | High | High | **Do not dual-inherit.** Migrated classes inherit *only* from `AsyncFS`; fsspec compatibility is provided strictly by composition via `AsFsspecFileSystem` (`to_fsspec`). |
| **Breaking `isinstance(fs, fsspec.AbstractFileSystem)` checks** in downstream code (`agentpool`, `docler`, `slashed`, `upath`) | High | Medium | During `1.x`, keep existing public classes (`MarkdownFileSystem`, `E2BFS`, etc.) either returning `AsFsspecFileSystem` shims or expose new core classes under `upathtools.fs` / cut over in `2.0` (see Decision #5). |
| **Sync-over-async deadlocks** when a loop-bound `AsyncFS` (e.g., `HTTPFileSystem` with active `httpx2.AsyncClient`) is passed to `to_fsspec()` and used synchronously on the same loop | Medium | High | Document contract clearly; initialize lazy clients on the loop where they are first used; fail fast with an actionable error message on same-loop sync re-entry. |
| **Free-threading C-extension compatibility** (`aiofile`, `ripgrep-rs`, `tree-sitter`, `jq`, `aiosqlite`) | Medium | Medium | Guard `LocalFS` to fall back cleanly to `asyncio.to_thread` if `aiofile` has GIL/free-threading issues; test under `3.14t` in CI; keep all pure-Python core state (`DirCache`, `MemoryFS`, `Registry`, `Pool`) lock-protected. |
| **`ripgrep-rs` vs `fsspec` `find()` semantic drift** (symlinks, hidden files, `.gitignore`, Windows separators) | Medium | Medium | Run `GLOB_EDGE_CASES_TESTS` and symlink/hidden-file tests against `LocalFS` in Step 6; preserve the existing `hidden=True, no_ignore=True` defaults (`async_local_fs.py:216-217`). |
| **Upstream `fsspec` behavioral changes** breaking `tests/conformance/test_fsspec_abstract.py` | Medium | Low | Pin the sync target in `_sync.py` (`FSSPEC_SYNC_VERSION = "2026.9.0"`); treat upstream test failures as signal to inspect the `fsspec` changelog and update either our implementation or `DIVERGENCES.md`. |
| **`upathtools_config` import-time weight** (package is explicitly lightweight, see `upathtools_config/__init__.py:3`) | Low | Medium | Keep all `upathtools.fs` imports lazy inside `create_core_fs()` / `create_fs()`, never at module top level in `upathtools_config`. |

### 8.2 Open Decisions for the Owner

Before starting Step 1, the owner should decide on the following:

1. **Package & class naming:**
   - Recommended: `upathtools.fs` with `AsyncFS[TInfo]`, `FileInfo`, `SyncFS`, `LocalFS`, `MemoryFS`, `DirFS`, `from_fsspec`, `to_fsspec`.
   - Alternative: `upathtools.core` (conflicts with existing `src/upathtools/core.py`) or `AsyncFileSystem` (collides visually with `fsspec.asyn.AsyncFileSystem`).
2. **Metadata representation (`FileInfo` dataclass vs. `TypedDict`):**
   - Recommended: `@dataclass(frozen=True, slots=True, kw_only=True) class FileInfo` (attribute access, immutable, free-threading safe, real defaults, clean subclassing; `.to_fsspec_dict()` for interop).
   - Trade-off: Existing code indexing `info["size"]` must migrate to `info.size` (or we can briefly support `__getitem__` on `FileInfo` as a deprecated migration bridge—owner's call whether that crutch is worth it).
3. **Method naming (`read_bytes`/`write_bytes`/`remove` vs. `cat_file`/`pipe_file`/`rm_file`):**
   - Recommended: Modern Python/pathlib names (`read_bytes`, `read_text`, `write_bytes`, `write_text`, `remove_file`, `remove`, `copy_file`, `move`, `ls`, `info`, `glob`, `find`, `walk`, `du`, `grep`), keeping fsspec names strictly on the `AsFsspecFileSystem` adapter.
   - Alternative: Keep fsspec verbs (`cat_file`, `pipe_file`, `rm_file`, `cp_file`) without leading underscores to minimize mental translation for fsspec veterans.
4. **`fsspec_filesystems/` mixin wrappers (`src/upathtools/filesystems/fsspec_filesystems/`):**
   - Today these 14 tiny classes multiple-inherit `(BaseFileSystem[S3Path], _S3FileSystem)` solely to attach `upath_cls`, `get_upath()`, `jq()`, `diff()`, `get_tree()`.
   - Decision: Deprecate and replace them with `from_fsspec(s3fs.S3FileSystem(...))` (much cleaner, no MRO conflicts), or keep thin wrappers?
5. **Release & backwards-compatibility strategy (`1.x` additive + `2.0` cutover vs. immediate `2.0`):**
   - Recommended: Ship Steps 1–7 in `1.27+` (100% additive, zero breaking changes for `agentpool` / `docler` / `slashed`), then cut `2.0.0` when Steps 8–11 flip existing `upathtools.filesystems.*` classes from `fsspec.AsyncFileSystem` subclasses to `AsyncFS` subclasses (with `to_fsspec()` available for any legacy caller).
6. **Making `fsspec` and `universal-pathlib` optional in `2.0`:**
   - Should `2.0` keep `fsspec` and `universal-pathlib` as core dependencies (simplest for users who expect `s3://` and `UPath` out of the box), or move them to `upathtools[fsspec]` / `upathtools[upath]` extras once the native `AsyncPath` layer (Option B) lands?
7. **`BaseUPath` / `AsyncPath` timeline:**
   - Confirm Option B (keep `UPath` through Steps 1–11, build native `AsyncPath` only after core `AsyncFS` is battle-tested in `agentpool`), or prioritize `AsyncPath` earlier so `cli_ops` and `filetree` can drop `UPath` sooner.
