# Filesystem Configs

The `upathtools_config` package contains lightweight Pydantic models describing every
filesystem upathtools (and fsspec) provides. They are discriminated by their `type` field,
so they can be embedded in YAML/JSON configuration files and turned into filesystem
instances with `create_fs()`.

```python
from pydantic import TypeAdapter
from upathtools_config import FilesystemConfigType, PackageFilesystemConfig

# Validate any config via the combined discriminated union
config = TypeAdapter(FilesystemConfigType).validate_python({
    "type": "pkg",
    "package": "upathtools",
})
assert isinstance(config, PackageFilesystemConfig)

fs = config.create_fs()  # PackageFileSystem
async_fs = config.create_fs(ensure_async=True)  # always an AsyncFileSystem
```

Common fields of all configs (`FileSystemConfig`): `type`, `root_path` (restrict the
filesystem to a sub directory) and `cached` (wrap in a caching filesystem).

The config unions per category are `CustomFilesystemConfig`, `FileBasedFilesystemConfig`,
`RemoteFilesystemConfig`, `SandboxFilesystemConfig` and `FsspecFilesystemConfig`.

## Custom filesystems

/// mknodes
{{ "upathtools_config.custom_fs_configs.CustomFilesystemConfig" | union_to_markdown(display_mode="yaml", header_style="pymdownx") }}
///

## File-based filesystems

/// mknodes
{{ "upathtools_config.file_based_fs_configs.FileBasedFilesystemConfig" | union_to_markdown(display_mode="yaml", header_style="pymdownx") }}
///

## Remote filesystems

/// mknodes
{{ "upathtools_config.remote_fs_configs.RemoteFilesystemConfig" | union_to_markdown(display_mode="yaml", header_style="pymdownx") }}
///

## Sandbox filesystems

/// mknodes
{{ "upathtools_config.sandbox_fs_configs.SandboxFilesystemConfig" | union_to_markdown(display_mode="yaml", header_style="pymdownx") }}
///
