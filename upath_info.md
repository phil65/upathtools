he complete mapping of fsspec info dict keys → `os.stat_result` fields:

| stat field | fsspec info keys (in priority order) | Notes |
|------------|--------------------------------------|-------|
| `st_mode` | `mode`, `type`, `isLink` | `mode` as int or octal string; `type` = `"file"` or `"directory"`; `isLink` = bool |
| `st_ino` | `ino`, `inode` | int |
| `st_dev` | `dev`, `device` | int |
| `st_nlink` | `nlink` | int |
| `st_uid` | `uid`, `owner`, `uname`, `unix.owner` | int |
| `st_gid` | `gid`, `group`, `gname`, `unix.group` | int |
| `st_size` | `size` | int |
| `st_atime` | `atime`, `time`, `last_accessed`, `accessTime` | timestamp (int/float/datetime/string) |
| `st_mtime` | `mtime`, `LastModified`, `last_modified`, `timeModified`, `modificationTime`, `modified_at` | timestamp |
| `st_ctime` | `ctime` | timestamp |
| `st_birthtime` | `birthtime`, `created`, `creation_time`, `timeCreated`, `created_at` | timestamp |

**Minimum viable info dict for your custom filesystem:**

```python
{
    "name": "/path/to/file",
    "type": "file",  # or "directory"
    "size": 1234,
    "mtime": 1699999999.0,  # or datetime object
}
```

Everything else defaults to 0 if not provided.
