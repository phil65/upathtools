"""CLI command parser and executor for filesystem operations.

This module provides a shell-like interface for executing commands on filesystems and paths.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import shlex
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from upath import UPath


@dataclass
class CLIResult:
    """Result from CLI command execution."""

    data: Any
    command: str

    def __str__(self) -> str:
        """Format result as string."""
        if isinstance(self.data, str):
            return self.data
        if isinstance(self.data, list):
            return "\n".join(str(item) for item in self.data)
        return str(self.data)

    def __iter__(self):
        """Allow iteration over results."""
        if hasattr(self.data, "__iter__") and not isinstance(self.data, str):
            yield from self.data
        else:
            yield self.data


async def execute_cli_async(command: str, base: UPath) -> CLIResult:  # noqa: PLR0911
    """Execute a CLI-style command on a filesystem/path asynchronously.

    Args:
        command: Shell-like command string (e.g., "grep pattern file.txt -i")
        base: Base UPath to execute command relative to

    Returns:
        CLIResult with command output

    Examples:
        >>> path = UPath(".")
        >>> result = await execute_cli_async("grep TODO *.py -r", path)
        >>> for match in result:
        ...     print(match)
    """
    parts = shlex.split(command)
    if not parts:
        msg = "Empty command"
        raise ValueError(msg)

    cmd = parts[0]
    args, kwargs = _parse_args(parts[1:])

    match cmd:
        case "grep":
            return await _exec_grep(args, kwargs, base)
        case "find":
            return await _exec_find(args, kwargs, base)
        case "head":
            return await _exec_head(args, kwargs, base)
        case "tail":
            return await _exec_tail(args, kwargs, base)
        case "cat":
            return await _exec_cat(args, kwargs, base)
        case "wc":
            return await _exec_wc(args, kwargs, base)
        case "ls":
            return await _exec_ls(args, kwargs, base)
        case "du":
            return await _exec_du(args, kwargs, base)
        case "diff":
            return await _exec_diff(args, kwargs, base)
        case _:
            msg = f"Unknown command: {cmd}"
            raise ValueError(msg)


def execute_cli(command: str, base: UPath) -> CLIResult:
    """Execute a CLI-style command on a filesystem/path (sync wrapper).

    Args:
        command: Shell-like command string (e.g., "grep pattern file.txt -i")
        base: Base UPath to execute command relative to

    Returns:
        CLIResult with command output

    Examples:
        >>> path = UPath(".")
        >>> result = execute_cli("grep TODO *.py -r", path)
        >>> for match in result:
        ...     print(match)
    """
    return asyncio.run(execute_cli_async(command, base))


def _parse_args(args: list[str]) -> tuple[list[str], dict[str, Any]]:
    """Parse positional args and flags from command arguments.

    Args:
        args: List of argument strings

    Returns:
        Tuple of (positional_args, kwargs_dict)
    """
    positional: list[str] = []
    kwargs: dict[str, Any] = {}
    i = 0

    while i < len(args):
        arg = args[i]

        if arg.startswith("--"):
            # Long option
            key = arg[2:].replace("-", "_")
            if "=" in key:
                k, v = key.split("=", 1)
                kwargs[k] = _parse_value(v)
            elif i + 1 < len(args) and not args[i + 1].startswith("-"):
                kwargs[key] = _parse_value(args[i + 1])
                i += 1
            else:
                kwargs[key] = True
        elif arg.startswith("-") and len(arg) > 1:
            # Short options
            for char in arg[1:]:
                flag_name = _short_to_long(char)
                # Check if next arg is a value for this flag
                if char in "nBAmdc" and i + 1 < len(args) and not args[i + 1].startswith("-"):
                    kwargs[flag_name] = _parse_value(args[i + 1])
                    i += 1
                else:
                    kwargs[flag_name] = True
        else:
            positional.append(arg)

        i += 1

    return positional, kwargs


def _short_to_long(char: str) -> str:
    """Convert short flag to long name."""
    mapping = {
        "r": "recursive",
        "i": "ignore_case",
        "v": "invert_match",
        "w": "whole_word",
        "F": "fixed_string",
        "m": "max_count",
        "B": "context_before",
        "A": "context_after",
        "n": "n",
        "a": "all_",
        "l": "long",
        "h": "human_readable",
        "s": "summarize",
        "d": "max_depth",
        "f": "force",
        "p": "parents",
    }
    return mapping.get(char, char)


def _parse_value(value: str) -> Any:
    """Parse a string value to appropriate type."""
    if value.lower() == "true":
        return True
    if value.lower() == "false":
        return False
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    return value


async def _exec_grep(args: list[str], kwargs: dict[str, Any], base: UPath) -> CLIResult:
    """Execute grep command."""
    from upathtools.cli_ops import agrep

    if len(args) < 1:
        msg = "grep requires pattern argument"
        raise ValueError(msg)

    pattern = args[0]
    path = args[1] if len(args) > 1 else "."
    results = [result async for result in agrep(pattern, path, base, **kwargs)]
    return CLIResult(results, f"grep {pattern} {path}")


async def _exec_find(args: list[str], kwargs: dict[str, Any], base: UPath) -> CLIResult:
    """Execute find command."""
    from upathtools.cli_ops import afind

    path = args[0] if args else "."

    # Map common find args
    if "name" in args:
        idx = args.index("name")
        if idx + 1 < len(args):
            kwargs["name"] = args[idx + 1]

    results = [result async for result in afind(path, base, **kwargs)]
    return CLIResult(results, f"find {path}")


async def _exec_head(args: list[str], kwargs: dict[str, Any], base: UPath) -> CLIResult:
    """Execute head command."""
    from upathtools.cli_ops import ahead

    if not args:
        msg = "head requires file argument"
        raise ValueError(msg)

    path = args[0]
    result = await ahead(path, base, **kwargs)
    return CLIResult(result, f"head {path}")


async def _exec_tail(args: list[str], kwargs: dict[str, Any], base: UPath) -> CLIResult:
    """Execute tail command."""
    from upathtools.cli_ops import atail

    if not args:
        msg = "tail requires file argument"
        raise ValueError(msg)

    path = args[0]
    result = await atail(path, base, **kwargs)
    return CLIResult(result, f"tail {path}")


async def _exec_cat(args: list[str], kwargs: dict[str, Any], base: UPath) -> CLIResult:
    """Execute cat command."""
    from upathtools.cli_ops import acat

    if not args:
        msg = "cat requires file argument(s)"
        raise ValueError(msg)

    result = await acat(*args, base=base, **kwargs)
    return CLIResult(result, f"cat {' '.join(args)}")


async def _exec_wc(args: list[str], kwargs: dict[str, Any], base: UPath) -> CLIResult:
    """Execute wc command."""
    from upathtools.cli_ops import awc

    if not args:
        msg = "wc requires file argument"
        raise ValueError(msg)

    path = args[0]
    result = await awc(path, base, **kwargs)
    return CLIResult(result, f"wc {path}")


async def _exec_ls(args: list[str], kwargs: dict[str, Any], base: UPath) -> CLIResult:
    """Execute ls command."""
    from upathtools.cli_ops import als

    path = args[0] if args else "."
    results = await als(path, base, **kwargs)
    return CLIResult(results, f"ls {path}")


async def _exec_du(args: list[str], kwargs: dict[str, Any], base: UPath) -> CLIResult:
    """Execute du command."""
    from upathtools.cli_ops import adu

    path = args[0] if args else "."
    results = await adu(path, base, **kwargs)
    return CLIResult(results, f"du {path}")


async def _exec_diff(args: list[str], kwargs: dict[str, Any], base: UPath) -> CLIResult:
    """Execute diff command."""
    from upathtools.cli_ops import adiff

    if len(args) < 2:  # noqa: PLR2004
        msg = "diff requires two file arguments"
        raise ValueError(msg)

    result = await adiff(args[0], args[1], base, **kwargs)
    return CLIResult(result, f"diff {args[0]} {args[1]}")
