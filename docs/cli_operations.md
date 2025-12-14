# CLI-like Operations

The `upathtools` library provides a unified CLI-like interface for executing filesystem commands on both local and remote filesystems through UPath objects.

## Overview

Instead of learning a new API for each filesystem operation, you can use familiar shell commands directly on UPath objects and filesystem instances:

```python
from upath import UPath

path = UPath(".")
result = path.cli("grep TODO *.py -r")
result = path.cli("find . -name '*.py' -type f")
result = path.cli("ls -lah")
```

## Available Commands

### grep - Search file contents

Search for patterns in files, similar to Unix `grep`:

```python
# Basic search
result = path.cli("grep pattern file.txt")

# Case-insensitive search
result = path.cli("grep TODO README.md -i")

# Recursive search with file pattern
result = path.cli("grep 'import os' . -r --include='*.py'")

# Fixed string search (no regex)
result = path.cli("grep 'function()' src/ -r -F")

# With context lines
result = path.cli("grep error log.txt -B 2 -A 2")

# Invert match
result = path.cli("grep test -v")
```

**Supported options:**
- `-r, --recursive`: Search recursively in directories
- `-i, --ignore-case`: Case-insensitive matching
- `-v, --invert-match`: Select non-matching lines
- `-w, --whole-word`: Match whole words only
- `-F, --fixed-string`: Treat pattern as fixed string, not regex
- `-m, --max-count N`: Stop after N matches per file
- `--include PATTERN`: Only search files matching glob pattern
- `--exclude PATTERN`: Skip files matching glob pattern
- `-B N, --context-before N`: Show N lines before match
- `-A N, --context-after N`: Show N lines after match

### find - Search for files

Find files and directories matching criteria:

```python
# Find all Python files
result = path.cli("find . -name '*.py'")

# Find files by type
result = path.cli("find . -type f")  # files only
result = path.cli("find . -type d")  # directories only

# Case-insensitive name search
result = path.cli("find . -iname 'readme*'")

# Limit depth
result = path.cli("find . -maxdepth 2")
result = path.cli("find . -mindepth 1 -maxdepth 3")

# Find by size
result = path.cli("find . -size-min 1000 -size-max 100000")

# Find using regex
result = path.cli("find . --regex '.*\\.py$'")
```

**Supported options:**
- `-name PATTERN`: Filename pattern (glob, case-sensitive)
- `-iname PATTERN`: Filename pattern (case-insensitive)
- `-type [f|d]`: File type (f=file, d=directory)
- `-maxdepth N`: Maximum directory depth
- `-mindepth N`: Minimum directory depth
- `--size-min N`: Minimum file size in bytes
- `--size-max N`: Maximum file size in bytes
- `--regex PATTERN`: Regex pattern for full path

### head - Show first lines

Display the first N lines of a file:

```python
# First 10 lines (default)
result = path.cli("head file.txt")

# First 20 lines
result = path.cli("head file.txt -n 20")
```

**Supported options:**
- `-n N`: Number of lines to show

### tail - Show last lines

Display the last N lines of a file:

```python
# Last 10 lines (default)
result = path.cli("tail file.txt")

# Last 50 lines
result = path.cli("tail log.txt -n 50")
```

**Supported options:**
- `-n N`: Number of lines to show

### cat - Concatenate files

Display file contents:

```python
# Single file
result = path.cli("cat file.txt")

# Multiple files
result = path.cli("cat file1.txt file2.txt file3.txt")
```

### wc - Word count

Count lines, words, and characters:

```python
result = path.cli("wc file.txt")
# Output: WcResult(path='file.txt', lines=100, words=500, chars=3000, bytes=3000)
```

### ls - List directory contents

List files and directories:

```python
# Basic listing
result = path.cli("ls")

# All files including hidden
result = path.cli("ls -a")

# Long format with details
result = path.cli("ls -l")

# Human-readable sizes
result = path.cli("ls -lh")

# Recursive listing
result = path.cli("ls -R")

# Sort by size or time
result = path.cli("ls -l --sort-by size")
result = path.cli("ls -l --sort-by mtime")

# Reverse sort
result = path.cli("ls -l --reverse")
```

**Supported options:**
- `-a, --all`: Include hidden files
- `-l, --long`: Long format with details
- `-h, --human-readable`: Human-readable file sizes
- `-r, -R, --recursive`: Recursive listing
- `--sort-by [name|size|mtime]`: Sort criteria
- `--reverse`: Reverse sort order

### du - Disk usage

Estimate file space usage:

```python
# Directory usage
result = path.cli("du .")

# Human-readable sizes
result = path.cli("du . -h")

# Summary only (total)
result = path.cli("du . -s")

# Max depth
result = path.cli("du . -d 2")
```

**Supported options:**
- `-h, --human-readable`: Human-readable sizes
- `-s, --summarize`: Show only total
- `-d N, --max-depth N`: Maximum depth to report

### diff - Compare files

Show differences between two files:

```python
result = path.cli("diff file1.txt file2.txt")

# Custom context lines
result = path.cli("diff file1.txt file2.txt --context-lines 5")
```

## Working with Results

The CLI commands return a `CLIResult` object that can be used in multiple ways:

```python
result = path.cli("grep TODO *.py -r")

# String representation
print(result)

# Iterate over results
for match in result:
    print(match)

# Access raw data
data = result.data
```

## Async Usage

For async filesystems and paths, use `acli()`:

```python
result = await path.acli("grep pattern file.txt -r")
result = await fs.acli("find . -name '*.py'")
```

## Using with Filesystems

CLI commands can also be executed on filesystem instances:

```python
import fsspec
from upathtools.filesystems.base import BaseFileSystem

# Get a filesystem instance
fs = fsspec.filesystem("file")

# Execute commands (all paths are relative to filesystem root)
result = fs.cli("grep TODO /path/to/file.txt")
result = fs.cli("ls /some/directory -lh")
```

## Examples

### Search for TODO comments in a project

```python
from upath import UPath

project = UPath(".")
todos = project.cli("grep TODO . -r --include='*.py'")

for match in todos:
    print(f"{match.path}:{match.line_number}: {match.line}")
```

### Find large files

```python
large_files = path.cli("find . -type f --size-min 1000000")
for file in large_files:
    print(f"{file.path}: {file.size} bytes")
```

### Analyze log files

```python
# Get last 100 lines
recent = path.cli("tail app.log -n 100")

# Count errors
errors = path.cli("grep ERROR app.log")
error_count = len(list(errors))
print(f"Found {error_count} errors")
```

### Directory overview

```python
# Get disk usage summary
usage = path.cli("du . -sh")
print(usage)

# List sorted by size
largest = path.cli("ls . -lh --sort-by size --reverse")
for entry in largest[:10]:  # Top 10
    print(f"{entry.size_human}\t{entry.name}")
```

## Remote Filesystems

The CLI interface works seamlessly with remote filesystems:

```python
from upath import UPath

# S3
s3_path = UPath("s3://bucket/prefix")
result = s3_path.cli("find . -name '*.json'")

# HTTP
http_path = UPath("https://example.com/data/")
result = http_path.cli("ls -l")

# Git
git_path = UPath("github://owner:repo@main/src")
result = git_path.cli("grep TODO . -r")
```

## Notes

- All paths in commands are relative to the base path (the path object you call `.cli()` on)
- Absolute paths in commands (starting with `/`) are resolved relative to the filesystem root
- Pattern matching (glob, regex) uses Python's `fnmatch` and `re` modules
- Binary files are automatically detected and skipped by default in `grep`
- The interface mimics common Unix command behavior but may not support all options